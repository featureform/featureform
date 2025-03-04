// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/featureform/fferr"
	"github.com/featureform/logging"
	pb "github.com/featureform/metadata/proto"
	"github.com/featureform/storage"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/protojson"
)

// Create Resource Lookup Using ETCD
type MetadataStorageResourceLookup struct {
	Connection storage.MetadataStorage
}

// Wrapper around Resource/Job messages. Allows top level storage for info about saved value
type MetadataStorageRow struct {
	ResourceType      ResourceType //Resource Type. For use when getting stored keys
	StorageType       StorageType  //Type of storage. Resource or Job
	Message           []byte       //Contents to be stored
	SerializedVersion int          //Checks if its serialized using JSON or proto
}

type MetadataStorageRowTemp struct {
	ResourceType      ResourceType
	StorageType       StorageType //Type of storage. Resource or Job
	Message           []byte      //Contents to be stored
	SerializedVersion int         //Checks if its serialized using JSON or proto
}

// Serializes the entire ETCD Storage Object to be put into ETCD
func (lookup MetadataStorageResourceLookup) serializeResource(res Resource) ([]byte, error) {
	p, err := protojson.Marshal(res.Proto())
	if err != nil {
		return nil, err
	}
	msg := StoredRowTemp{
		ResourceType:      res.ID().Type,
		Message:           string(p),
		StorageType:       RESOURCE,
		SerializedVersion: 1,
	}

	serialMsg, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return serialMsg, nil
}

// Deserializes object into the Storage Object
func (lookup MetadataStorageResourceLookup) deserialize(value []byte) (StoredRow, error) {
	var tmp StoredRowTemp
	if err := json.Unmarshal(value, &tmp); err != nil {
		return StoredRow{}, errors.Wrap(err, fmt.Sprintf("failed to parse resource: %s", value))
	}
	msg := StoredRow{
		ResourceType:      ResourceType(tmp.ResourceType),
		StorageType:       tmp.StorageType,
		Message:           tmp.Message,
		SerializedVersion: tmp.SerializedVersion,
	}
	return msg, nil
}

func (lookup MetadataStorageResourceLookup) Lookup(ctx context.Context, id ResourceID, opts ...ResourceLookupOption) (Resource, error) {
	logger := logging.GetLoggerFromContext(ctx)
	key := createKey(id)
	logger.With("lookup-key", key)
	logger.Info("Performing lookup on DB")

	options, err := parseResourceLookupOptions(opts...)
	logger.Debugw("Resource lookup options", "options", options)
	if err != nil {
		logger.Errorw("Failed to create resource lookup options", "error", err)
		return nil, err
	}
	qOpts := options.generateQueryOpts()
	logger.Debugw("Query options", "options", qOpts, "key", key, "id", id)
	logger.Info("Performing lookup on DB")
	resp, err := lookup.Connection.Get(key, qOpts...)

	if err != nil || len(resp) == 0 {
		logger.Debug("Key not found")
		return nil, fferr.NewKeyNotFoundError(key, err)
	}
	logger.Debug("Deserializing key")
	msg, err := lookup.deserialize([]byte(resp))
	if err != nil {
		logger.Errorw("Failed to deserialize resource from DB", "err", err)
		return nil, err
	}
	logger.Debug("Create empty resource")
	resType, err := CreateEmptyResource(msg.ResourceType)
	if err != nil {
		logger.Errorw("Failed to create empty resource", "err", err)
		return nil, err
	}
	logger.Debug("Parsing resource")
	resource, err := ParseResource(msg, resType)
	if err != nil {
		logger.Errorw("Failed to parse resource from DB", "err", err)
		return nil, err
	}
	logger.Info("DB lookup successful")
	return resource, nil
}

func (lookup MetadataStorageResourceLookup) Delete(ctx context.Context, id ResourceID) error {
	logger := logging.NewLogger("lookup")
	key := createKey(id)
	logger = logger.With("key", key)
	logger.Infow("Delete key")
	_, err := lookup.Connection.Delete(key)
	if err != nil {
		logger.Errorw("Failed to delete key", "error", err)
		return err
	}
	return nil
}

func (lookup MetadataStorageResourceLookup) GetCountWithPrefix(ctx context.Context, id string) (int, error) {
	get, err := lookup.Connection.List(id)
	if err != nil {
		return 0, err
	}
	return len(get), nil
}

func (lookup MetadataStorageResourceLookup) Has(ctx context.Context, id ResourceID) (bool, error) {
	key := createKey(id)
	count, err := lookup.GetCountWithPrefix(ctx, key)
	if err != nil {
		return false, err
	}
	if count == 0 {
		return false, nil
	}
	return true, nil
}

func (lookup MetadataStorageResourceLookup) HasJob(ctx context.Context, id ResourceID) (bool, error) {
	job_key := GetJobKey(id)
	count, err := lookup.GetCountWithPrefix(ctx, job_key)
	if err != nil {
		return false, err
	}
	if count == 0 {
		return false, nil
	}
	return true, nil
}

func (lookup MetadataStorageResourceLookup) SetJob(ctx context.Context, id ResourceID, schedule string) error {
	if jobAlreadySet, _ := lookup.HasJob(ctx, id); jobAlreadySet {
		return fferr.NewInternalErrorf("job %v has already been created", id)
	}
	coordinatorJob := CoordinatorJob{
		Attempts: 0,
		Resource: id,
		Schedule: schedule,
	}
	serialized, err := coordinatorJob.Serialize()
	if err != nil {
		return err
	}
	jobKey := GetJobKey(id)
	if err := lookup.Connection.Create(ctx, jobKey, string(serialized)); err != nil {
		return err
	}

	return nil
}

func (lookup MetadataStorageResourceLookup) SetSchedule(ctx context.Context, id ResourceID, schedule string) error {
	coordinatorScheduleJob := CoordinatorScheduleJob{
		Attempts: 0,
		Resource: id,
		Schedule: schedule,
	}
	serialized, err := coordinatorScheduleJob.Serialize()
	if err != nil {
		return err
	}
	jobKey := GetScheduleJobKey(id)
	if err := lookup.Connection.Create(ctx, jobKey, string(serialized)); err != nil {
		return err
	}
	return nil
}

func (lookup MetadataStorageResourceLookup) Set(ctx context.Context, id ResourceID, res Resource) error {
	logger := logging.GetLoggerFromContext(ctx).With("resource-id", id)
	logger.Debugw("Serializing resource")
	serRes, err := lookup.serializeResource(res)
	if err != nil {
		logger.Errorw("Failed to serialize resource", "error", err)
		return err
	}
	key := createKey(id)
	logger = logger.With("key", key)
	logger.Debugw("Creating resource")
	if err := lookup.Connection.Create(logger.AttachToContext(ctx), key, string(serRes)); err != nil {
		logger.Errorw("Failed to create resource", "error", err)
		return err
	}
	logger.Infow("Resource set successfully")
	return nil
}

func (lookup MetadataStorageResourceLookup) Submap(ctx context.Context, ids []ResourceID) (ResourceLookup, error) {
	resources := make(LocalResourceLookup, len(ids))

	for _, id := range ids {
		key := createKey(id)
		resp, err := lookup.Connection.Get(key)
		if err != nil {
			return nil, fferr.NewKeyNotFoundError(key, err)
		}
		storedRow, err := lookup.deserialize([]byte(resp))
		if err != nil {
			return nil, err
		}

		resource, err := CreateEmptyResource(storedRow.ResourceType)
		if err != nil {
			return nil, err
		}

		res, err := ParseResource(storedRow, resource)
		if err != nil {
			return nil, err
		}
		resources[id] = res
	}
	return resources, nil
}

func (lookup MetadataStorageResourceLookup) ListForType(ctx context.Context, t ResourceType) ([]Resource, error) {
	resources := make([]Resource, 0)
	resp, err := lookup.Connection.List(t.String())
	if err != nil {
		return nil, err
	}
	for _, v := range resp {
		storedRow, err := lookup.deserialize([]byte(v))
		if err != nil {
			return nil, err
		}
		resource, err := CreateEmptyResource(storedRow.ResourceType)
		if err != nil {
			return nil, err
		}
		parsedResource, err := ParseResource(storedRow, resource)
		if err != nil {
			logging.GlobalLogger.Errorw("Failed to parse resource", "error", err)
			return nil, err
		}
		if parsedResource.ID().Type == t {
			resources = append(resources, parsedResource)
		}
	}
	return resources, nil
}

func (lookup MetadataStorageResourceLookup) ListVariants(ctx context.Context, t ResourceType, name string, opts ...ResourceLookupOption) ([]Resource, error) {
	logger := logging.NewLogger("memmory_lookup.go:ListVariants")
	startTime := time.Now()
	resources := make([]Resource, 0)
	logger.Infow("list variants with prefix", "type", t, "name", name)

	options, err := parseResourceLookupOptions(opts...)
	if err != nil {
		logger.Errorw("Failed to create resource lookup options", "error", err)
		return nil, err
	}
	qOpts := options.generateQueryOpts()

	resp, err := lookup.Connection.List(variantLookupPrefix(t, name), qOpts...)
	logger.Infow("listed variants with prefix", "type", t, "name", name, "duration", time.Since(startTime))
	if err != nil {
		return nil, err
	}
	startParsingTime := time.Now()
	for _, v := range resp {
		storedRow, err := lookup.deserialize([]byte(v))
		if err != nil {
			return nil, err
		}
		resource, err := CreateEmptyResource(storedRow.ResourceType)
		if err != nil {
			return nil, err
		}
		resource, err = ParseResource(storedRow, resource)
		if err != nil {
			return nil, err
		}
		id := resource.ID()
		if id.Type == t && id.Name == name {
			resources = append(resources, resource)
		}
	}
	logger.Infow("parsed variants with prefix", "type", t, "name", name, "duration", time.Since(startParsingTime))
	return resources, nil
}

func (lookup MetadataStorageResourceLookup) List(ctx context.Context) ([]Resource, error) {
	resources := make([]Resource, 0)
	resp, err := lookup.Connection.List("")
	if err != nil {
		return nil, err
	}
	for _, v := range resp {
		storedRow, err := lookup.deserialize([]byte(v))
		if err != nil {
			return nil, err
		}
		resource, err := CreateEmptyResource(storedRow.ResourceType)
		if err != nil {
			return nil, err
		}
		resource, err = ParseResource(storedRow, resource)
		if err != nil {
			return nil, err
		}
		resources = append(resources, resource)
	}
	return resources, nil
}

func (lookup *MetadataStorageResourceLookup) SetStatus(ctx context.Context, id ResourceID, status *pb.ResourceStatus) error {
	res, err := lookup.Lookup(ctx, id)
	if err != nil {
		return err
	}
	if err := res.UpdateStatus(status); err != nil {
		return err
	}
	if err := lookup.Set(ctx, id, res); err != nil {
		return err
	}
	return nil
}

func (lookup MetadataStorageResourceLookup) Search(ctx context.Context, q string) ([]Resource, error) {
	logger := logging.GetLoggerFromContext(ctx)
	logger.Infow("Searching for resources", "searchquery", q)

	searchResults, err := lookup.Connection.Storage.Search(ctx, q)
	if err != nil {
		logger.Errorw("failed to execute search", "query", q, "err", err)
		return nil, fferr.NewExecutionError("Postgres", fmt.Errorf("failed to execute search: %v", err))
	}

	results := make([]Resource, 0)
	for key, val := range searchResults {
		individualLogger := logger.With("key", key)
		individualLogger.Debugw("Deserializing key")
		msg, err := lookup.deserialize([]byte(val))
		if err != nil {
			individualLogger.Errorw("Failed to deserialize resource from DB", "err", err)
			continue
		}
		individualLogger.Debug("Create empty resource", "resource-type", msg.ResourceType)
		resType, err := CreateEmptyResource(msg.ResourceType)
		if err != nil {
			individualLogger.Errorw("Failed to create empty resource", "resource-type", msg.ResourceType, "err", err)
			continue
		}
		individualLogger.Debug("Parsing resource")
		resource, err := ParseResource(msg, resType)
		if err != nil {
			individualLogger.Errorw("Failed to parse resource from DB", "err", err)
			continue
		}
		results = append(results, resource)
	}
	return results, nil
}
