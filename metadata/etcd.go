// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metadata

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/featureform/fferr"
	"github.com/featureform/logging"
	pb "github.com/featureform/metadata/proto"
	"google.golang.org/protobuf/encoding/protojson"
	gpb "google.golang.org/protobuf/proto"
)

type StorageType string

const (
	RESOURCE StorageType = "Resource"
	JOB                  = "Job"
)

type CoordinatorJob struct {
	Attempts int
	Resource ResourceID
	Schedule string
}

type CoordinatorScheduleJob struct {
	Attempts int
	Resource ResourceID
	Schedule string
}

func (c *CoordinatorScheduleJob) Serialize() ([]byte, error) {
	serialized, err := json.Marshal(c)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	return serialized, nil
}

func (c *CoordinatorScheduleJob) Deserialize(serialized []byte) error {
	err := json.Unmarshal(serialized, c)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	return nil
}

type TempJob struct {
	Attempts int
	Name     string
	Variant  string
	Type     string
	Schedule string
}

func (c *CoordinatorJob) Serialize() ([]byte, error) {
	job := TempJob{
		Attempts: c.Attempts,
		Name:     c.Resource.Name,
		Variant:  c.Resource.Variant,
		Type:     c.Resource.Type.String(),
		Schedule: c.Schedule,
	}
	serialized, err := json.Marshal(job)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	return serialized, nil
}

func (c *CoordinatorJob) Deserialize(serialized []byte) error {
	job := TempJob{}
	err := json.Unmarshal(serialized, &job)

	if err != nil {
		return fferr.NewInternalError(err)
	}
	c.Attempts = job.Attempts
	c.Resource.Name = job.Name
	c.Resource.Variant = job.Variant
	c.Resource.Type = ResourceType(pb.ResourceType_value[job.Type])
	c.Schedule = job.Schedule
	return nil
}

// Wrapper around Resource/Job messages. Allows top level storage for info about saved value
type StoredRow struct {
	ResourceType ResourceType //Resource Type. For use when getting stored keys
	//ResourceType string
	StorageType       StorageType //Type of storage. Resource or Job
	Message           string      //Contents to be stored
	SerializedVersion int         //Checks if its serialized using JSON or proto
}

type StoredRowTemp struct {
	//ResourceType ResourceType //Resource Type. For use when getting stored keys
	ResourceType      ResourceType
	StorageType       StorageType //Type of storage. Resource or Job
	Message           string      //Contents to be stored
	SerializedVersion int         //Checks if its serialized using JSON or proto
}

// Uses Storage Type as prefix so Resources and Jobs can be queried more easily
func createKey(id ResourceID) string {
	return fmt.Sprintf("%s__%s__%s", id.Type, id.Name, id.Variant)
}

// The prefix of createKey without the variant
func variantLookupPrefix(t ResourceType, name string) string {
	return fmt.Sprintf("%s__%s__", t, name)
}

// Takes a populated ETCD storage struct and a resource
// Checks to make sure the given ETCD storage Object contains a Resource, not job
// Deserializes Resource value into the provided Resource object
func ParseResource(res StoredRow, resType Resource) (Resource, error) {
	id := resType.ID()
	if res.StorageType != RESOURCE {
		logging.GlobalLogger.Errorw("Invalid storage type", "storage type", res.StorageType)
		return nil, fferr.NewInvalidResourceTypeError(id.Name, id.Variant, fferr.ResourceType(id.Type.String()), nil)
	}

	if !resType.Proto().ProtoReflect().IsValid() {
		logging.GlobalLogger.Errorw("Invalid resource proto", "proto", resType.Proto())
		return nil, fferr.NewInvalidResourceTypeErrorf(id.Name, id.Variant, fferr.ResourceType(id.Type.String()), "invalid proto")
	}

	if res.Message == "" {
		logging.GlobalLogger.Errorw("Message is empty", "message", res.Message)
		return nil, fferr.NewInvalidResourceTypeErrorf(id.Name, id.Variant, fferr.ResourceType(id.Type.String()), "invalid message")
	}

	switch res.SerializedVersion {
	case 1:
		logging.GlobalLogger.Debug("Serialized version 1")
		if err := protojson.Unmarshal([]byte(res.Message), resType.Proto()); err != nil {
			logging.GlobalLogger.Errorw("Failed to unmarshal", "serialized version", 1, "error", err, "resource", resType.Proto())
			return nil, fferr.NewInvalidResourceTypeError(id.Name, id.Variant, fferr.ResourceType(id.Type.String()), err)
		}
	case 0:
		// Default serialization version if it is not set, must decode the base64 encoded message first
		logging.GlobalLogger.Debug("Serialized version 0")
		decodedMessage, decodeErr := base64.StdEncoding.DecodeString(res.Message)
		if decodeErr != nil {
			logging.GlobalLogger.Errorw("Failed to decode proto message", "serialized version", 0, "error", decodeErr, "resource", resType.Proto())
			return nil, fferr.NewParsingError(decodeErr)
		}
		if err := gpb.Unmarshal(decodedMessage, resType.Proto()); err != nil {
			logging.GlobalLogger.Errorw("Failed to unmarshal", "serialized version", 0, "error", err, "resource", resType.Proto())
			return nil, fferr.NewInvalidResourceTypeError(id.Name, id.Variant, fferr.ResourceType(id.Type.String()), err)
		}
	default:
		logging.GlobalLogger.Errorw("Invalid serialized version", "version", res.SerializedVersion)
		return nil, fferr.NewInternalError(fmt.Errorf("invalid serialized version"))
	}

	return resType, nil
}

func GetJobKey(id ResourceID) string {
	return fmt.Sprintf("JOB__%s__%s__%s", id.Type, id.Name, id.Variant)
}

func GetScheduleJobKey(id ResourceID) string {
	return fmt.Sprintf("SCHEDULEJOB__%s__%s__%s", id.Type, id.Name, id.Variant)
}
