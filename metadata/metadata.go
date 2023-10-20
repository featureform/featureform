// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metadata

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/pkg/errors"

	slices "golang.org/x/exp/slices"

	pb "github.com/featureform/metadata/proto"
	"github.com/featureform/metadata/search"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	tspb "google.golang.org/protobuf/types/known/timestamppb"
)

const TIME_FORMAT = time.RFC1123

type operation int

const (
	create_op operation = iota
)

type ResourceType int32

const (
	FEATURE              ResourceType = ResourceType(pb.ResourceType_FEATURE)
	FEATURE_VARIANT                   = ResourceType(pb.ResourceType_FEATURE_VARIANT)
	LABEL                             = ResourceType(pb.ResourceType_LABEL)
	LABEL_VARIANT                     = ResourceType(pb.ResourceType_LABEL_VARIANT)
	USER                              = ResourceType(pb.ResourceType_USER)
	ENTITY                            = ResourceType(pb.ResourceType_ENTITY)
	PROVIDER                          = ResourceType(pb.ResourceType_PROVIDER)
	SOURCE                            = ResourceType(pb.ResourceType_SOURCE)
	SOURCE_VARIANT                    = ResourceType(pb.ResourceType_SOURCE_VARIANT)
	TRAINING_SET                      = ResourceType(pb.ResourceType_TRAINING_SET)
	TRAINING_SET_VARIANT              = ResourceType(pb.ResourceType_TRAINING_SET_VARIANT)
	MODEL                             = ResourceType(pb.ResourceType_MODEL)
)

func (r ResourceType) String() string {
	return pb.ResourceType_name[int32(r)]
}

func (r ResourceType) Serialized() pb.ResourceType {
	return pb.ResourceType(r)
}

type ResourceStatus int32

const (
	NO_STATUS ResourceStatus = ResourceStatus(pb.ResourceStatus_NO_STATUS)
	CREATED                  = ResourceStatus(pb.ResourceStatus_CREATED)
	PENDING                  = ResourceStatus(pb.ResourceStatus_PENDING)
	READY                    = ResourceStatus(pb.ResourceStatus_READY)
	FAILED                   = ResourceStatus(pb.ResourceStatus_FAILED)
)

func (r ResourceStatus) String() string {
	return pb.ResourceStatus_Status_name[int32(r)]
}

func (r ResourceStatus) Serialized() pb.ResourceStatus_Status {
	return pb.ResourceStatus_Status(r)
}

type ComputationMode int32

const (
	PRECOMPUTED     ComputationMode = ComputationMode(pb.ComputationMode_PRECOMPUTED)
	CLIENT_COMPUTED                 = ComputationMode(pb.ComputationMode_CLIENT_COMPUTED)
)

func (cm ComputationMode) Equals(mode pb.ComputationMode) bool {
	return cm == ComputationMode(mode)
}

func (cm ComputationMode) String() string {
	return pb.ComputationMode_name[int32(cm)]
}

var parentMapping = map[ResourceType]ResourceType{
	FEATURE_VARIANT:      FEATURE,
	LABEL_VARIANT:        LABEL,
	SOURCE_VARIANT:       SOURCE,
	TRAINING_SET_VARIANT: TRAINING_SET,
}

func (serv *MetadataServer) needsJob(res Resource) bool {
	if res.ID().Type == TRAINING_SET_VARIANT ||
		res.ID().Type == SOURCE_VARIANT ||
		res.ID().Type == LABEL_VARIANT {
		return true
	}
	if res.ID().Type == FEATURE_VARIANT {
		if fv, ok := res.(*featureVariantResource); !ok {
			serv.Logger.Errorf("resource has type FEATURE VARIANT but failed to cast %s", res.ID())
			return false
		} else {
			return PRECOMPUTED.Equals(fv.serialized.Mode)
		}
	}
	return false
}

type ResourceID struct {
	Name    string
	Variant string
	Type    ResourceType
}

func (id ResourceID) Proto() *pb.NameVariant {
	return &pb.NameVariant{
		Name:    id.Name,
		Variant: id.Variant,
	}
}

func (id ResourceID) Parent() (ResourceID, bool) {
	parentType, has := parentMapping[id.Type]
	if !has {
		return ResourceID{}, false
	}
	return ResourceID{
		Name: id.Name,
		Type: parentType,
	}, true
}

func (id ResourceID) String() string {
	if id.Variant == "" {
		return fmt.Sprintf("%s %s", id.Type, id.Name)
	}
	return fmt.Sprintf("%s %s (%s)", id.Type, id.Name, id.Variant)
}

var bannedStrings = [...]string{"__"}
var bannedPrefixes = [...]string{"_"}
var bannedSuffixes = [...]string{"_"}

func resourceNamedSafely(id ResourceID) error {
	for _, substr := range bannedStrings {
		if strings.Contains(id.Name, substr) {
			return fmt.Errorf("resource name %s contains banned string %s", id.Name, substr)
		}
		if strings.Contains(id.Variant, substr) {
			return fmt.Errorf("resource variant %s contains banned string %s", id.Name, substr)
		}
	}
	for _, substr := range bannedPrefixes {
		if strings.HasPrefix(id.Name, substr) {
			return fmt.Errorf("resource name %s contains banned prefix %s", id.Name, substr)
		}
		if strings.HasPrefix(id.Variant, substr) {
			return fmt.Errorf("resource variant %s contains banned prefix %s", id.Name, substr)
		}
	}
	for _, substr := range bannedSuffixes {
		if strings.HasSuffix(id.Name, substr) {
			return fmt.Errorf("resource name %s contains banned suffix %s", id.Name, substr)
		}
		if strings.HasSuffix(id.Variant, substr) {
			return fmt.Errorf("resource variant %s contains banned suffix %s", id.Name, substr)
		}
	}
	return nil
}

type ResourceNotFound struct {
	ID ResourceID
	E  error
}

func (err *ResourceNotFound) Error() string {
	id := err.ID
	var errMsg string
	if err.E != nil {
		errMsg = fmt.Sprintf("resource not found. %s err: %v", id.String(), err.E)
	} else {
		errMsg = fmt.Sprintf("resource not found. %s", id.String())
	}
	return errMsg
}

func (err *ResourceNotFound) GRPCStatus() *status.Status {
	return status.New(codes.NotFound, err.Error())
}

type ResourceExists struct {
	ID ResourceID
}

func (err *ResourceExists) Error() string {
	id := err.ID
	name, variant, t := id.Name, id.Variant, id.Type
	errMsg := fmt.Sprintf("%s Exists.\nName: %s", t, name)
	if variant != "" {
		errMsg += "\nVariant: " + variant
	}
	return errMsg
}

func (err *ResourceExists) GRPCStatus() *status.Status {
	return status.New(codes.AlreadyExists, err.Error())
}

type Resource interface {
	Notify(ResourceLookup, operation, Resource) error
	ID() ResourceID
	Schedule() string
	Dependencies(ResourceLookup) (ResourceLookup, error)
	Proto() proto.Message
	UpdateStatus(pb.ResourceStatus) error
	UpdateSchedule(string) error
	Update(ResourceLookup, Resource) error
}

func isDirectDependency(lookup ResourceLookup, dependency, parent Resource) (bool, error) {
	depId := dependency.ID()
	deps, depsErr := parent.Dependencies(lookup)
	if depsErr != nil {
		return false, depsErr
	}
	return deps.Has(depId)
}

type ResourceLookup interface {
	Lookup(ResourceID) (Resource, error)
	Has(ResourceID) (bool, error)
	Set(ResourceID, Resource) error
	Submap([]ResourceID) (ResourceLookup, error)
	ListForType(ResourceType) ([]Resource, error)
	List() ([]Resource, error)
	HasJob(ResourceID) (bool, error)
	SetJob(ResourceID, string) error
	SetStatus(ResourceID, pb.ResourceStatus) error
	SetSchedule(ResourceID, string) error
}

type SearchWrapper struct {
	Searcher search.Searcher
	ResourceLookup
}

func (wrapper SearchWrapper) Set(id ResourceID, res Resource) error {
	if err := wrapper.ResourceLookup.Set(id, res); err != nil {
		return err
	}
	doc := search.ResourceDoc{
		Name:    id.Name,
		Type:    id.Type.String(),
		Variant: id.Variant,
	}
	return wrapper.Searcher.Upsert(doc)
}

type LocalResourceLookup map[ResourceID]Resource

func (lookup LocalResourceLookup) Lookup(id ResourceID) (Resource, error) {
	res, has := lookup[id]
	if !has {
		return nil, &ResourceNotFound{id, nil}
	}
	return res, nil
}

func (lookup LocalResourceLookup) Has(id ResourceID) (bool, error) {
	_, has := lookup[id]
	return has, nil
}

func (lookup LocalResourceLookup) Set(id ResourceID, res Resource) error {
	lookup[id] = res
	return nil
}

func (lookup LocalResourceLookup) Submap(ids []ResourceID) (ResourceLookup, error) {
	resources := make(LocalResourceLookup, len(ids))
	for _, id := range ids {
		resource, has := lookup[id]
		if !has {
			return nil, &ResourceNotFound{id, nil}
		}
		resources[id] = resource
	}
	return resources, nil
}

func (lookup LocalResourceLookup) ListForType(t ResourceType) ([]Resource, error) {
	resources := make([]Resource, 0)
	for id, res := range lookup {
		if id.Type == t {
			resources = append(resources, res)
		}
	}
	return resources, nil
}

func (lookup LocalResourceLookup) List() ([]Resource, error) {
	resources := make([]Resource, 0, len(lookup))
	for _, res := range lookup {
		resources = append(resources, res)
	}
	return resources, nil
}

func (lookup LocalResourceLookup) SetStatus(id ResourceID, status pb.ResourceStatus) error {
	res, has := lookup[id]
	if !has {
		return &ResourceNotFound{id, nil}
	}
	if err := res.UpdateStatus(status); err != nil {
		return err
	}
	lookup[id] = res
	return nil
}

func (lookup LocalResourceLookup) SetJob(id ResourceID, schedule string) error {
	return nil
}

func (lookup LocalResourceLookup) SetSchedule(id ResourceID, schedule string) error {
	res, has := lookup[id]
	if !has {
		return &ResourceNotFound{id, nil}
	}
	if err := res.UpdateSchedule(schedule); err != nil {
		return err
	}
	lookup[id] = res
	return nil
}

func (lookup LocalResourceLookup) HasJob(id ResourceID) (bool, error) {
	return false, nil
}

type SourceResource struct {
	serialized *pb.Source
}

func (resource *SourceResource) ID() ResourceID {
	return ResourceID{
		Name: resource.serialized.Name,
		Type: SOURCE,
	}
}

func (resource *SourceResource) Schedule() string {
	return ""
}

func (resource *SourceResource) Dependencies(lookup ResourceLookup) (ResourceLookup, error) {
	return make(LocalResourceLookup), nil
}

func (resource *SourceResource) Proto() proto.Message {
	return resource.serialized
}

func (this *SourceResource) Notify(lookup ResourceLookup, op operation, that Resource) error {
	otherId := that.ID()
	isVariant := otherId.Type == SOURCE_VARIANT && otherId.Name == this.serialized.Name
	if !isVariant {
		return nil
	}
	if slices.Contains(this.serialized.Variants, otherId.Variant) {
		fmt.Printf("source %s already has variant %s\n", this.serialized.Name, otherId.Variant)
		return nil
	}
	this.serialized.Variants = append(this.serialized.Variants, otherId.Variant)
	return nil
}

func (resource *SourceResource) UpdateStatus(status pb.ResourceStatus) error {
	resource.serialized.Status = &status
	return nil
}

func (resource *SourceResource) UpdateSchedule(schedule string) error {
	return fmt.Errorf("not implemented")
}

func (resource *SourceResource) Update(lookup ResourceLookup, updateRes Resource) error {
	return &ResourceExists{updateRes.ID()}
}

type sourceVariantResource struct {
	serialized *pb.SourceVariant
}

func (resource *sourceVariantResource) ID() ResourceID {
	return ResourceID{
		Name:    resource.serialized.Name,
		Variant: resource.serialized.Variant,
		Type:    SOURCE_VARIANT,
	}
}

func (resource *sourceVariantResource) Schedule() string {
	return resource.serialized.Schedule
}

func (resource *sourceVariantResource) Dependencies(lookup ResourceLookup) (ResourceLookup, error) {
	serialized := resource.serialized
	depIds := []ResourceID{
		{
			Name: serialized.Owner,
			Type: USER,
		},
		{
			Name: serialized.Provider,
			Type: PROVIDER,
		},
		{
			Name: serialized.Name,
			Type: SOURCE,
		},
	}
	deps, err := lookup.Submap(depIds)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("could not create submap for IDs: %v", depIds))
	}
	return deps, nil
}

func (resource *sourceVariantResource) Proto() proto.Message {
	return resource.serialized
}

func (this *sourceVariantResource) Notify(lookup ResourceLookup, op operation, that Resource) error {
	id := that.ID()
	t := id.Type
	key := id.Proto()
	serialized := this.serialized
	switch t {
	case TRAINING_SET_VARIANT:
		serialized.Trainingsets = append(serialized.Trainingsets, key)
	case FEATURE_VARIANT:
		serialized.Features = append(serialized.Features, key)
	case LABEL_VARIANT:
		serialized.Labels = append(serialized.Labels, key)
	}
	return nil
}

func (resource *sourceVariantResource) UpdateStatus(status pb.ResourceStatus) error {
	resource.serialized.LastUpdated = tspb.Now()
	resource.serialized.Status = &status
	return nil
}

func (resource *sourceVariantResource) UpdateSchedule(schedule string) error {
	resource.serialized.Schedule = schedule
	return nil
}

func (resource *sourceVariantResource) Update(lookup ResourceLookup, updateRes Resource) error {
	deserialized := updateRes.Proto()
	variantUpdate, ok := deserialized.(*pb.SourceVariant)
	if !ok {
		return errors.New("failed to deserialize existing source variant record")
	}
	resource.serialized.Tags = UnionTags(resource.serialized.Tags, variantUpdate.Tags)
	resource.serialized.Properties = mergeProperties(resource.serialized.Properties, variantUpdate.Properties)
	return nil
}

type featureResource struct {
	serialized *pb.Feature
}

func (resource *featureResource) ID() ResourceID {
	return ResourceID{
		Name: resource.serialized.Name,
		Type: FEATURE,
	}
}

func (resource *featureResource) Schedule() string {
	return ""
}

func (resource *featureResource) Dependencies(lookup ResourceLookup) (ResourceLookup, error) {
	return make(LocalResourceLookup), nil
}

func (resource *featureResource) Proto() proto.Message {
	return resource.serialized
}

func (this *featureResource) Notify(lookup ResourceLookup, op operation, that Resource) error {
	otherId := that.ID()
	isVariant := otherId.Type == FEATURE_VARIANT && otherId.Name == this.serialized.Name
	if !isVariant {
		return nil
	}
	if slices.Contains(this.serialized.Variants, otherId.Variant) {
		fmt.Printf("source %s already has variant %s\n", this.serialized.Name, otherId.Variant)
		return nil
	}
	this.serialized.Variants = append(this.serialized.Variants, otherId.Variant)
	return nil
}

func (resource *featureResource) UpdateStatus(status pb.ResourceStatus) error {
	resource.serialized.Status = &status
	return nil
}

func (resource *featureResource) UpdateSchedule(schedule string) error {
	return fmt.Errorf("not implemented")
}

func (resource *featureResource) Update(lookup ResourceLookup, updateRes Resource) error {
	return &ResourceExists{updateRes.ID()}
}

type featureVariantResource struct {
	serialized *pb.FeatureVariant
}

func (resource *featureVariantResource) ID() ResourceID {
	return ResourceID{
		Name:    resource.serialized.Name,
		Variant: resource.serialized.Variant,
		Type:    FEATURE_VARIANT,
	}
}

func (resource *featureVariantResource) Schedule() string {
	return resource.serialized.Schedule
}

func (resource *featureVariantResource) Dependencies(lookup ResourceLookup) (ResourceLookup, error) {
	serialized := resource.serialized
	depIds := []ResourceID{
		{
			Name: serialized.Owner,
			Type: USER,
		},
		{
			Name: serialized.Name,
			Type: FEATURE,
		},
	}
	if PRECOMPUTED.Equals(serialized.Mode) {
		depIds = append(depIds, ResourceID{
			Name:    serialized.Source.Name,
			Variant: serialized.Source.Variant,
			Type:    SOURCE_VARIANT,
		},
			ResourceID{
				Name: serialized.Entity,
				Type: ENTITY,
			},
			ResourceID{
				Name: serialized.Provider,
				Type: PROVIDER,
			})
	}
	deps, err := lookup.Submap(depIds)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("could not create submap for IDs: %v", depIds))
	}
	return deps, nil
}

func (resource *featureVariantResource) Proto() proto.Message {
	return resource.serialized
}

func (this *featureVariantResource) Notify(lookup ResourceLookup, op operation, that Resource) error {
	if !PRECOMPUTED.Equals(this.serialized.Mode) {
		return nil
	}
	id := that.ID()
	relevantOp := op == create_op && id.Type == TRAINING_SET_VARIANT
	if !relevantOp {
		return nil
	}
	key := id.Proto()
	this.serialized.Trainingsets = append(this.serialized.Trainingsets, key)
	return nil
}

func (resource *featureVariantResource) UpdateStatus(status pb.ResourceStatus) error {
	resource.serialized.LastUpdated = tspb.Now()
	resource.serialized.Status = &status
	return nil
}

func (resource *featureVariantResource) UpdateSchedule(schedule string) error {
	resource.serialized.Schedule = schedule
	return nil
}

func (resource *featureVariantResource) Update(lookup ResourceLookup, updateRes Resource) error {
	deserialized := updateRes.Proto()
	variantUpdate, ok := deserialized.(*pb.FeatureVariant)
	if !ok {
		return errors.New("failed to deserialize existing feature variant record")
	}
	resource.serialized.Tags = UnionTags(resource.serialized.Tags, variantUpdate.Tags)
	resource.serialized.Properties = mergeProperties(resource.serialized.Properties, variantUpdate.Properties)
	return nil
}

type labelResource struct {
	serialized *pb.Label
}

func (resource *labelResource) ID() ResourceID {
	return ResourceID{
		Name: resource.serialized.Name,
		Type: LABEL,
	}
}

func (resource *labelResource) Schedule() string {
	return ""
}

func (resource *labelResource) Dependencies(lookup ResourceLookup) (ResourceLookup, error) {
	return make(LocalResourceLookup), nil
}

func (resource *labelResource) Proto() proto.Message {
	return resource.serialized
}

func (this *labelResource) Notify(lookup ResourceLookup, op operation, that Resource) error {
	otherId := that.ID()
	isVariant := otherId.Type == LABEL_VARIANT && otherId.Name == this.serialized.Name
	if !isVariant {
		return nil
	}
	if slices.Contains(this.serialized.Variants, otherId.Variant) {
		fmt.Printf("source %s already has variant %s\n", this.serialized.Name, otherId.Variant)
		return nil
	}
	this.serialized.Variants = append(this.serialized.Variants, otherId.Variant)
	return nil
}

func (resource *labelResource) UpdateStatus(status pb.ResourceStatus) error {
	resource.serialized.Status = &status
	return nil
}

func (resource *labelResource) UpdateSchedule(schedule string) error {
	return fmt.Errorf("not implemented")
}

func (resource *labelResource) Update(lookup ResourceLookup, updateRes Resource) error {
	return &ResourceExists{updateRes.ID()}
}

type labelVariantResource struct {
	serialized *pb.LabelVariant
}

func (resource *labelVariantResource) ID() ResourceID {
	return ResourceID{
		Name:    resource.serialized.Name,
		Variant: resource.serialized.Variant,
		Type:    LABEL_VARIANT,
	}
}

func (resource *labelVariantResource) Schedule() string {
	return ""
}

func (resource *labelVariantResource) Dependencies(lookup ResourceLookup) (ResourceLookup, error) {
	serialized := resource.serialized
	depIds := []ResourceID{
		{
			Name:    serialized.Source.Name,
			Variant: serialized.Source.Variant,
			Type:    SOURCE_VARIANT,
		},
		{
			Name: serialized.Entity,
			Type: ENTITY,
		},
		{
			Name: serialized.Owner,
			Type: USER,
		},
		{
			Name: serialized.Provider,
			Type: PROVIDER,
		},
		{
			Name: serialized.Name,
			Type: LABEL,
		},
	}
	deps, err := lookup.Submap(depIds)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("could not create submap for IDs: %v", depIds))
	}
	return deps, nil
}

func (resource *labelVariantResource) Proto() proto.Message {
	return resource.serialized
}

func (this *labelVariantResource) Notify(lookup ResourceLookup, op operation, that Resource) error {
	id := that.ID()
	releventOp := op == create_op && id.Type == TRAINING_SET_VARIANT
	if !releventOp {
		return nil
	}
	key := id.Proto()
	this.serialized.Trainingsets = append(this.serialized.Trainingsets, key)
	return nil
}

func (resource *labelVariantResource) UpdateStatus(status pb.ResourceStatus) error {
	resource.serialized.Status = &status
	return nil
}

func (resource *labelVariantResource) UpdateSchedule(schedule string) error {
	return fmt.Errorf("not implemented")
}

func (resource *labelVariantResource) Update(lookup ResourceLookup, updateRes Resource) error {
	deserialized := updateRes.Proto()
	variantUpdate, ok := deserialized.(*pb.LabelVariant)
	if !ok {
		return errors.New("failed to deserialize existing label variant record")
	}
	resource.serialized.Tags = UnionTags(resource.serialized.Tags, variantUpdate.Tags)
	resource.serialized.Properties = mergeProperties(resource.serialized.Properties, variantUpdate.Properties)
	return nil
}

type trainingSetResource struct {
	serialized *pb.TrainingSet
}

func (resource *trainingSetResource) ID() ResourceID {
	return ResourceID{
		Name: resource.serialized.Name,
		Type: TRAINING_SET,
	}
}

func (resource *trainingSetResource) Schedule() string {
	return ""
}

func (resource *trainingSetResource) Dependencies(lookup ResourceLookup) (ResourceLookup, error) {
	return make(LocalResourceLookup), nil
}

func (resource *trainingSetResource) Proto() proto.Message {
	return resource.serialized
}

func (this *trainingSetResource) Notify(lookup ResourceLookup, op operation, that Resource) error {
	otherId := that.ID()
	isVariant := otherId.Type == TRAINING_SET_VARIANT && otherId.Name == this.serialized.Name
	if !isVariant {
		return nil
	}
	if slices.Contains(this.serialized.Variants, otherId.Variant) {
		fmt.Printf("source %s already has variant %s\n", this.serialized.Name, otherId.Variant)
		return nil
	}
	this.serialized.Variants = append(this.serialized.Variants, otherId.Variant)
	return nil
}

func (resource *trainingSetResource) UpdateStatus(status pb.ResourceStatus) error {
	resource.serialized.Status = &status
	return nil
}

func (resource *trainingSetResource) UpdateSchedule(schedule string) error {
	return fmt.Errorf("not implemented")
}

func (resource *trainingSetResource) Update(lookup ResourceLookup, updateRes Resource) error {
	return &ResourceExists{updateRes.ID()}
}

type trainingSetVariantResource struct {
	serialized *pb.TrainingSetVariant
}

func (resource *trainingSetVariantResource) ID() ResourceID {
	return ResourceID{
		Name:    resource.serialized.Name,
		Variant: resource.serialized.Variant,
		Type:    TRAINING_SET_VARIANT,
	}
}

func (resource *trainingSetVariantResource) Schedule() string {
	return resource.serialized.Schedule
}

func (resource *trainingSetVariantResource) Dependencies(lookup ResourceLookup) (ResourceLookup, error) {
	serialized := resource.serialized
	depIds := []ResourceID{
		{
			Name: serialized.Owner,
			Type: USER,
		},
		{
			Name: serialized.Provider,
			Type: PROVIDER,
		},
		{
			Name:    serialized.Label.Name,
			Variant: serialized.Label.Variant,
			Type:    LABEL_VARIANT,
		},
		{
			Name: serialized.Name,
			Type: TRAINING_SET,
		},
	}
	for _, feature := range serialized.Features {
		depIds = append(depIds, ResourceID{
			Name:    feature.Name,
			Variant: feature.Variant,
			Type:    FEATURE_VARIANT,
		})
	}
	deps, err := lookup.Submap(depIds)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("could not create submap for IDs: %v", depIds))
	}
	return deps, nil
}

func (resource *trainingSetVariantResource) Proto() proto.Message {
	return resource.serialized
}

func (this *trainingSetVariantResource) Notify(lookup ResourceLookup, op operation, that Resource) error {
	return nil
}

func (resource *trainingSetVariantResource) UpdateStatus(status pb.ResourceStatus) error {
	resource.serialized.LastUpdated = tspb.Now()
	resource.serialized.Status = &status
	return nil
}

func (resource *trainingSetVariantResource) UpdateSchedule(schedule string) error {
	resource.serialized.Schedule = schedule
	return nil
}

func (resource *trainingSetVariantResource) Update(lookup ResourceLookup, updateRes Resource) error {
	deserialized := updateRes.Proto()
	variantUpdate, ok := deserialized.(*pb.TrainingSetVariant)
	if !ok {
		return errors.New("failed to deserialize existing training set variant record")
	}
	resource.serialized.Tags = UnionTags(resource.serialized.Tags, variantUpdate.Tags)
	resource.serialized.Properties = mergeProperties(resource.serialized.Properties, variantUpdate.Properties)
	return nil
}

type modelResource struct {
	serialized *pb.Model
}

func (resource *modelResource) ID() ResourceID {
	return ResourceID{
		Name: resource.serialized.Name,
		Type: MODEL,
	}
}

func (resource *modelResource) Schedule() string {
	return ""
}

func (resource *modelResource) Dependencies(lookup ResourceLookup) (ResourceLookup, error) {
	serialized := resource.serialized
	depIds := make([]ResourceID, 0)
	for _, feature := range serialized.Features {
		depIds = append(depIds, ResourceID{
			Name:    feature.Name,
			Variant: feature.Variant,
			Type:    FEATURE_VARIANT,
		})
	}
	for _, label := range serialized.Labels {
		depIds = append(depIds, ResourceID{
			Name:    label.Name,
			Variant: label.Variant,
			Type:    LABEL_VARIANT,
		})
	}
	for _, ts := range serialized.Trainingsets {
		depIds = append(depIds, ResourceID{
			Name:    ts.Name,
			Variant: ts.Variant,
			Type:    TRAINING_SET_VARIANT,
		})
	}
	deps, err := lookup.Submap(depIds)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("could not create submap for IDs: %v", depIds))
	}
	return deps, nil
}

func (resource *modelResource) Proto() proto.Message {
	return resource.serialized
}

func (this *modelResource) Notify(lookup ResourceLookup, op operation, that Resource) error {
	return nil
}

func (resource *modelResource) UpdateStatus(status pb.ResourceStatus) error {
	return nil
}

func (resource *modelResource) UpdateSchedule(schedule string) error {
	return fmt.Errorf("not implemented")
}

func (resource *modelResource) Update(lookup ResourceLookup, updateRes Resource) error {
	deserialized := updateRes.Proto()
	modelUpdate, ok := deserialized.(*pb.Model)
	if !ok {
		return errors.New("failed to deserialize existing model record")
	}
	resource.serialized.Features = unionNameVariants(resource.serialized.Features, modelUpdate.Features)
	resource.serialized.Trainingsets = unionNameVariants(resource.serialized.Trainingsets, modelUpdate.Trainingsets)
	resource.serialized.Tags = UnionTags(resource.serialized.Tags, modelUpdate.Tags)
	resource.serialized.Properties = mergeProperties(resource.serialized.Properties, modelUpdate.Properties)
	return nil
}

type userResource struct {
	serialized *pb.User
}

func (resource *userResource) ID() ResourceID {
	return ResourceID{
		Name: resource.serialized.Name,
		Type: USER,
	}
}

func (resource *userResource) Schedule() string {
	return ""
}

func (resource *userResource) Dependencies(lookup ResourceLookup) (ResourceLookup, error) {
	return make(LocalResourceLookup), nil
}

func (resource *userResource) Proto() proto.Message {
	return resource.serialized
}

func (this *userResource) Notify(lookup ResourceLookup, op operation, that Resource) error {
	if isDep, err := isDirectDependency(lookup, this, that); err != nil {
		return err
	} else if !isDep {
		return nil
	}
	id := that.ID()
	key := id.Proto()
	t := id.Type
	serialized := this.serialized
	switch t {
	case TRAINING_SET_VARIANT:
		serialized.Trainingsets = append(serialized.Trainingsets, key)
	case FEATURE_VARIANT:
		serialized.Features = append(serialized.Features, key)
	case LABEL_VARIANT:
		serialized.Labels = append(serialized.Labels, key)
	case SOURCE_VARIANT:
		serialized.Sources = append(serialized.Sources, key)
	}
	return nil
}

func (resource *userResource) UpdateStatus(status pb.ResourceStatus) error {
	resource.serialized.Status = &status
	return nil
}

func (resource *userResource) UpdateSchedule(schedule string) error {
	return fmt.Errorf("not implemented")
}

func (resource *userResource) Update(lookup ResourceLookup, updateRes Resource) error {
	deserialized := updateRes.Proto()
	userUpdate, ok := deserialized.(*pb.User)
	if !ok {
		return errors.New("failed to deserialize existing user record")
	}
	resource.serialized.Tags = UnionTags(resource.serialized.Tags, userUpdate.Tags)
	resource.serialized.Properties = mergeProperties(resource.serialized.Properties, userUpdate.Properties)
	return nil
}

type providerResource struct {
	serialized *pb.Provider
}

func (resource *providerResource) ID() ResourceID {
	return ResourceID{
		Name: resource.serialized.Name,
		Type: PROVIDER,
	}
}

func (resource *providerResource) Schedule() string {
	return ""
}

func (resource *providerResource) Dependencies(lookup ResourceLookup) (ResourceLookup, error) {
	return make(LocalResourceLookup), nil
}

func (resource *providerResource) Proto() proto.Message {
	return resource.serialized
}

func (this *providerResource) Notify(lookup ResourceLookup, op operation, that Resource) error {
	if isDep, err := isDirectDependency(lookup, this, that); err != nil {
		return err
	} else if !isDep {
		return nil
	}
	id := that.ID()
	key := id.Proto()
	t := id.Type
	serialized := this.serialized
	switch t {
	case SOURCE_VARIANT:
		serialized.Sources = append(serialized.Sources, key)
	case FEATURE_VARIANT:
		serialized.Features = append(serialized.Features, key)
	case TRAINING_SET_VARIANT:
		serialized.Trainingsets = append(serialized.Trainingsets, key)
	case LABEL_VARIANT:
		serialized.Labels = append(serialized.Labels, key)
	}
	return nil
}

func (resource *providerResource) UpdateStatus(status pb.ResourceStatus) error {
	resource.serialized.Status = &status
	return nil
}

func (resource *providerResource) UpdateSchedule(schedule string) error {
	return fmt.Errorf("not implemented")
}

func (resource *providerResource) Update(lookup ResourceLookup, resourceUpdate Resource) error {
	providerUpdate, ok := resourceUpdate.Proto().(*pb.Provider)
	if !ok {
		return errors.New("failed to deserialize existing provider record")
	}
	isValid, err := resource.isValidConfigUpdate(providerUpdate.SerializedConfig)
	if err != nil {
		return err
	}
	if !isValid {
		return &ResourceExists{resourceUpdate.ID()}
	}
	resource.serialized.SerializedConfig = providerUpdate.SerializedConfig
	resource.serialized.Description = providerUpdate.Description
	resource.serialized.Tags = UnionTags(resource.serialized.Tags, providerUpdate.Tags)
	resource.serialized.Properties = mergeProperties(resource.serialized.Properties, providerUpdate.Properties)
	return nil
}

func (resource *providerResource) isValidConfigUpdate(configUpdate pc.SerializedConfig) (bool, error) {
	switch pt.Type(resource.serialized.Type) {
	case pt.BigQueryOffline:
		return isValidBigQueryConfigUpdate(resource.serialized.SerializedConfig, configUpdate)
	case pt.CassandraOnline:
		return isValidCassandraConfigUpdate(resource.serialized.SerializedConfig, configUpdate)
	case pt.DynamoDBOnline:
		return isValidDynamoConfigUpdate(resource.serialized.SerializedConfig, configUpdate)
	case pt.FirestoreOnline:
		return isValidFirestoreConfigUpdate(resource.serialized.SerializedConfig, configUpdate)
	case pt.MongoDBOnline:
		return isValidMongoConfigUpdate(resource.serialized.SerializedConfig, configUpdate)
	case pt.PostgresOffline:
		return isValidPostgresConfigUpdate(resource.serialized.SerializedConfig, configUpdate)
	case pt.RedisOnline:
		return isValidRedisConfigUpdate(resource.serialized.SerializedConfig, configUpdate)
	case pt.SnowflakeOffline:
		return isValidSnowflakeConfigUpdate(resource.serialized.SerializedConfig, configUpdate)
	case pt.RedshiftOffline:
		return isValidRedshiftConfigUpdate(resource.serialized.SerializedConfig, configUpdate)
	case pt.K8sOffline:
		return isValidK8sConfigUpdate(resource.serialized.SerializedConfig, configUpdate)
	case pt.SparkOffline:
		return isValidSparkConfigUpdate(resource.serialized.SerializedConfig, configUpdate)
	case pt.S3, pt.HDFS, pt.GCS, pt.AZURE, pt.BlobOnline:
		return true, nil
	default:
		return false, fmt.Errorf("unable to update config for provider. Provider type %s not found", resource.serialized.Type)
	}
}

type entityResource struct {
	serialized *pb.Entity
}

func (resource *entityResource) ID() ResourceID {
	return ResourceID{
		Name: resource.serialized.Name,
		Type: ENTITY,
	}
}

func (resource *entityResource) Schedule() string {
	return ""
}

func (resource *entityResource) Dependencies(lookup ResourceLookup) (ResourceLookup, error) {
	return make(LocalResourceLookup), nil
}

func (resource *entityResource) Proto() proto.Message {
	return resource.serialized
}

func (this *entityResource) Notify(lookup ResourceLookup, op operation, that Resource) error {
	id := that.ID()
	key := id.Proto()
	t := id.Type
	serialized := this.serialized
	switch t {
	case TRAINING_SET_VARIANT:
		serialized.Trainingsets = append(serialized.Trainingsets, key)
	case FEATURE_VARIANT:
		serialized.Features = append(serialized.Features, key)
	case LABEL_VARIANT:
		serialized.Labels = append(serialized.Labels, key)
	}
	return nil
}

func (resource *entityResource) UpdateStatus(status pb.ResourceStatus) error {
	resource.serialized.Status = &status
	return nil
}

func (resource *entityResource) UpdateSchedule(schedule string) error {
	return fmt.Errorf("not implemented")
}

func (resource *entityResource) Update(lookup ResourceLookup, updateRes Resource) error {
	deserialized := updateRes.Proto()
	entityUpdate, ok := deserialized.(*pb.Entity)
	if !ok {
		return errors.New("failed to deserialize existing training entity record")
	}
	resource.serialized.Tags = UnionTags(resource.serialized.Tags, entityUpdate.Tags)
	resource.serialized.Properties = mergeProperties(resource.serialized.Properties, entityUpdate.Properties)
	return nil
}

type MetadataServer struct {
	Logger     *zap.SugaredLogger
	lookup     ResourceLookup
	address    string
	grpcServer *grpc.Server
	listener   net.Listener
	pb.UnimplementedMetadataServer
}

func NewMetadataServer(config *Config) (*MetadataServer, error) {
	config.Logger.Debug("Creating new metadata server", "Address:", config.Address)
	lookup, err := config.StorageProvider.GetResourceLookup()

	if err != nil {
		return nil, fmt.Errorf("could not configure storage provider: %v", err)
	}
	if config.SearchParams != nil {
		searcher, errInitializeSearch := search.NewMeilisearch(config.SearchParams)
		if errInitializeSearch != nil {
			return nil, errInitializeSearch
		}
		lookup = &SearchWrapper{
			Searcher:       searcher,
			ResourceLookup: lookup,
		}
	}
	return &MetadataServer{
		lookup:  lookup,
		address: config.Address,
		Logger:  config.Logger,
	}, nil
}

func (serv *MetadataServer) Serve() error {
	if serv.grpcServer != nil {
		return fmt.Errorf("Server already running")
	}
	lis, err := net.Listen("tcp", serv.address)
	if err != nil {
		return err
	}
	return serv.ServeOnListener(lis)
}

func (serv *MetadataServer) ServeOnListener(lis net.Listener) error {
	serv.listener = lis
	grpcServer := grpc.NewServer()
	pb.RegisterMetadataServer(grpcServer, serv)
	serv.grpcServer = grpcServer
	serv.Logger.Infow("Server starting", "Address", serv.listener.Addr().String())
	return grpcServer.Serve(lis)
}

func (serv *MetadataServer) GracefulStop() error {
	if serv.grpcServer == nil {
		return fmt.Errorf("Server not running")
	}
	serv.grpcServer.GracefulStop()
	serv.grpcServer = nil
	serv.listener = nil
	return nil
}

func (serv *MetadataServer) Stop() error {
	if serv.grpcServer == nil {
		return fmt.Errorf("Server not running")
	}
	serv.grpcServer.Stop()
	serv.grpcServer = nil
	serv.listener = nil
	return nil
}

type StorageProvider interface {
	GetResourceLookup() (ResourceLookup, error)
}

type LocalStorageProvider struct {
}

func (sp LocalStorageProvider) GetResourceLookup() (ResourceLookup, error) {
	lookup := make(LocalResourceLookup)
	return lookup, nil
}

type EtcdStorageProvider struct {
	Config EtcdConfig
}

func (sp EtcdStorageProvider) GetResourceLookup() (ResourceLookup, error) {
	client, err := sp.Config.InitClient()
	if err != nil {
		return nil, fmt.Errorf("could not init etcd client: %v", err)
	}
	lookup := EtcdResourceLookup{
		Connection: EtcdStorage{
			Client: client,
		},
	}

	return lookup, nil
}

type Config struct {
	Logger          *zap.SugaredLogger
	SearchParams    *search.MeilisearchParams
	StorageProvider StorageProvider
	Address         string
}

func (serv *MetadataServer) RequestScheduleChange(ctx context.Context, req *pb.ScheduleChangeRequest) (*pb.Empty, error) {
	resID := ResourceID{Name: req.ResourceId.Resource.Name, Variant: req.ResourceId.Resource.Variant, Type: ResourceType(req.ResourceId.ResourceType)}
	err := serv.lookup.SetSchedule(resID, req.Schedule)
	return &pb.Empty{}, err
}

func (serv *MetadataServer) SetResourceStatus(ctx context.Context, req *pb.SetStatusRequest) (*pb.Empty, error) {
	serv.Logger.Infow("Setting resource status", "request", req.String())
	resID := ResourceID{Name: req.ResourceId.Resource.Name, Variant: req.ResourceId.Resource.Variant, Type: ResourceType(req.ResourceId.ResourceType)}
	err := serv.lookup.SetStatus(resID, *req.Status)
	if err != nil {
		serv.Logger.Errorw("Could not set resource status", "error", err.Error())
	}

	return &pb.Empty{}, err
}

func (serv *MetadataServer) ListFeatures(_ *pb.Empty, stream pb.Metadata_ListFeaturesServer) error {
	return serv.genericList(FEATURE, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Feature))
	})
}

func (serv *MetadataServer) CreateFeatureVariant(ctx context.Context, variant *pb.FeatureVariant) (*pb.Empty, error) {
	variant.Created = tspb.New(time.Now())
	return serv.genericCreate(ctx, &featureVariantResource{variant}, func(name, variant string) Resource {
		return &featureResource{
			&pb.Feature{
				Name:           name,
				DefaultVariant: variant,
				// This will be set when the change is propagated to dependencies.
				Variants: []string{},
			},
		}
	})
}

func (serv *MetadataServer) GetFeatures(stream pb.Metadata_GetFeaturesServer) error {
	return serv.genericGet(stream, FEATURE, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Feature))
	})
}

func (serv *MetadataServer) GetFeatureVariants(stream pb.Metadata_GetFeatureVariantsServer) error {
	return serv.genericGet(stream, FEATURE_VARIANT, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.FeatureVariant))
	})
}

func (serv *MetadataServer) ListLabels(_ *pb.Empty, stream pb.Metadata_ListLabelsServer) error {
	return serv.genericList(LABEL, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Label))
	})
}

func (serv *MetadataServer) CreateLabelVariant(ctx context.Context, variant *pb.LabelVariant) (*pb.Empty, error) {
	variant.Created = tspb.New(time.Now())
	return serv.genericCreate(ctx, &labelVariantResource{variant}, func(name, variant string) Resource {
		return &labelResource{
			&pb.Label{
				Name:           name,
				DefaultVariant: variant,
				// This will be set when the change is propagated to dependencies.
				Variants: []string{},
			},
		}
	})
}

func (serv *MetadataServer) GetLabels(stream pb.Metadata_GetLabelsServer) error {
	return serv.genericGet(stream, LABEL, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Label))
	})
}

func (serv *MetadataServer) GetLabelVariants(stream pb.Metadata_GetLabelVariantsServer) error {
	return serv.genericGet(stream, LABEL_VARIANT, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.LabelVariant))
	})
}

func (serv *MetadataServer) ListTrainingSets(_ *pb.Empty, stream pb.Metadata_ListTrainingSetsServer) error {
	return serv.genericList(TRAINING_SET, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.TrainingSet))
	})
}

func (serv *MetadataServer) CreateTrainingSetVariant(ctx context.Context, variant *pb.TrainingSetVariant) (*pb.Empty, error) {
	variant.Created = tspb.New(time.Now())
	return serv.genericCreate(ctx, &trainingSetVariantResource{variant}, func(name, variant string) Resource {
		return &trainingSetResource{
			&pb.TrainingSet{
				Name:           name,
				DefaultVariant: variant,
				// This will be set when the change is propagated to dependencies.
				Variants: []string{},
			},
		}
	})
}

func (serv *MetadataServer) GetTrainingSets(stream pb.Metadata_GetTrainingSetsServer) error {
	return serv.genericGet(stream, TRAINING_SET, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.TrainingSet))
	})
}

func (serv *MetadataServer) GetTrainingSetVariants(stream pb.Metadata_GetTrainingSetVariantsServer) error {
	return serv.genericGet(stream, TRAINING_SET_VARIANT, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.TrainingSetVariant))
	})
}

func (serv *MetadataServer) ListSources(_ *pb.Empty, stream pb.Metadata_ListSourcesServer) error {
	return serv.genericList(SOURCE, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Source))
	})
}

func (serv *MetadataServer) CreateSourceVariant(ctx context.Context, variant *pb.SourceVariant) (*pb.Empty, error) {
	variant.Created = tspb.New(time.Now())
	return serv.genericCreate(ctx, &sourceVariantResource{variant}, func(name, variant string) Resource {
		return &SourceResource{
			&pb.Source{
				Name:           name,
				DefaultVariant: variant,
				// This will be set when the change is propagated to dependencies.
				Variants: []string{},
			},
		}
	})
}

func (serv *MetadataServer) GetSources(stream pb.Metadata_GetSourcesServer) error {
	return serv.genericGet(stream, SOURCE, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Source))
	})
}

func (serv *MetadataServer) GetSourceVariants(stream pb.Metadata_GetSourceVariantsServer) error {
	serv.Logger.Infow("Getting Source Variant In Metadata")
	return serv.genericGet(stream, SOURCE_VARIANT, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.SourceVariant))
	})
}

func (serv *MetadataServer) ListUsers(_ *pb.Empty, stream pb.Metadata_ListUsersServer) error {
	return serv.genericList(USER, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.User))
	})
}

func (serv *MetadataServer) CreateUser(ctx context.Context, user *pb.User) (*pb.Empty, error) {
	return serv.genericCreate(ctx, &userResource{user}, nil)
}

func (serv *MetadataServer) GetUsers(stream pb.Metadata_GetUsersServer) error {
	return serv.genericGet(stream, USER, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.User))
	})
}

func (serv *MetadataServer) ListProviders(_ *pb.Empty, stream pb.Metadata_ListProvidersServer) error {
	return serv.genericList(PROVIDER, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Provider))
	})
}

func (serv *MetadataServer) CreateProvider(ctx context.Context, provider *pb.Provider) (*pb.Empty, error) {
	return serv.genericCreate(ctx, &providerResource{provider}, nil)
}

func (serv *MetadataServer) GetProviders(stream pb.Metadata_GetProvidersServer) error {
	return serv.genericGet(stream, PROVIDER, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Provider))
	})
}

func (serv *MetadataServer) ListEntities(_ *pb.Empty, stream pb.Metadata_ListEntitiesServer) error {
	return serv.genericList(ENTITY, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Entity))
	})
}

func (serv *MetadataServer) CreateEntity(ctx context.Context, entity *pb.Entity) (*pb.Empty, error) {
	return serv.genericCreate(ctx, &entityResource{entity}, nil)
}

func (serv *MetadataServer) GetEntities(stream pb.Metadata_GetEntitiesServer) error {
	return serv.genericGet(stream, ENTITY, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Entity))
	})
}

func (serv *MetadataServer) ListModels(_ *pb.Empty, stream pb.Metadata_ListModelsServer) error {
	return serv.genericList(MODEL, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Model))
	})
}

func (serv *MetadataServer) CreateModel(ctx context.Context, model *pb.Model) (*pb.Empty, error) {
	return serv.genericCreate(ctx, &modelResource{model}, nil)
}

func (serv *MetadataServer) GetModels(stream pb.Metadata_GetModelsServer) error {
	return serv.genericGet(stream, MODEL, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Model))
	})
}

type nameStream interface {
	Recv() (*pb.Name, error)
}

type variantStream interface {
	Recv() (*pb.NameVariant, error)
}

type sendFn func(proto.Message) error

type initParentFn func(name, variant string) Resource

func (serv *MetadataServer) genericCreate(ctx context.Context, res Resource, init initParentFn) (*pb.Empty, error) {
	serv.Logger.Info("Creating Generic Resource", res.ID().Name, res.ID().Variant)
	id := res.ID()
	if err := resourceNamedSafely(id); err != nil {
		return nil, err
	}
	existing, err := serv.lookup.Lookup(id)
	if _, isResourceError := err.(*ResourceNotFound); err != nil && !isResourceError {
		return nil, err
	}
	if existing != nil {
		if err := existing.Update(serv.lookup, res); err != nil {
			return nil, err
		}
		res = existing
	}
	if err := serv.lookup.Set(id, res); err != nil {
		return nil, err
	}
	if serv.needsJob(res) && existing == nil {
		serv.Logger.Info("Creating Job", res.ID().Name, res.ID().Variant)
		if err := serv.lookup.SetJob(id, res.Schedule()); err != nil {
			return nil, fmt.Errorf("set job: %w", err)
		}
		serv.Logger.Info("Successfully Created Job", res.ID().Name, res.ID().Variant)
	}
	parentId, hasParent := id.Parent()
	if hasParent {
		parentExists, err := serv.lookup.Has(parentId)
		if err != nil {
			return nil, err
		}

		if !parentExists {
			parent := init(id.Name, id.Variant)
			err = serv.lookup.Set(parentId, parent)
			if err != nil {
				return nil, err
			}
		}
	}
	if err := serv.propagateChange(res); err != nil {
		err := errors.Wrap(err, fmt.Sprintf("failed to update parent resources for: %s", res.ID().String()))
		serv.Logger.Error(errors.WithStack(err))
		return nil, err
	}
	return &pb.Empty{}, nil
}

func (serv *MetadataServer) propagateChange(newRes Resource) error {
	visited := make(map[ResourceID]struct{})
	// We have to make it a var so that the anonymous function can call itself.
	var propagateChange func(parent Resource) error
	propagateChange = func(parent Resource) error {
		deps, err := parent.Dependencies(serv.lookup)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("could not get dependencies for parent: %s", parent.ID().String()))
		}
		depList, err := deps.List()
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("could not get dependencies list for parent: %s", parent))
		}
		for _, res := range depList {
			id := res.ID()
			if _, has := visited[id]; has {
				continue
			}
			visited[id] = struct{}{}
			if err := res.Notify(serv.lookup, create_op, newRes); err != nil {
				return err
			}
			if err := serv.lookup.Set(res.ID(), res); err != nil {
				return nil
			}
			if err := propagateChange(res); err != nil {
				return err
			}
		}
		return nil
	}
	return propagateChange(newRes)
}

func (serv *MetadataServer) genericGet(stream interface{}, t ResourceType, send sendFn) error {
	for {
		var recvErr error
		var id ResourceID
		switch casted := stream.(type) {
		case nameStream:
			req, err := casted.Recv()
			recvErr = err
			id = ResourceID{
				Name: req.GetName(),
				Type: t,
			}
		case variantStream:
			req, err := casted.Recv()
			recvErr = err
			id = ResourceID{
				Name:    req.GetName(),
				Variant: req.GetVariant(),
				Type:    t,
			}
		default:
			return fmt.Errorf("Invalid Stream for Get: %T", casted)
		}
		if recvErr == io.EOF {
			return nil
		}
		if recvErr != nil {
			serv.Logger.Errorw("Generic Get receive error", "error", recvErr)
			return recvErr
		}
		resource, err := serv.lookup.Lookup(id)
		if err != nil {
			serv.Logger.Errorw("Generic Get lookup error", "error", err)
			return err
		}
		serialized := resource.Proto()
		if err := send(serialized); err != nil {
			serv.Logger.Errorw("Generic Get send error", "error", err)
			return err
		}
	}
}

func (serv *MetadataServer) genericList(t ResourceType, send sendFn) error {
	resources, err := serv.lookup.ListForType(t)
	if err != nil {
		return err
	}
	for _, res := range resources {
		serialized := res.Proto()
		if err := send(serialized); err != nil {
			return err
		}
	}
	return nil
}

type TrainingSetVariantResource struct {
	Created     time.Time                           `json:"created"`
	Description string                              `json:"description"`
	Name        string                              `json:"name"`
	Owner       string                              `json:"owner"`
	Provider    string                              `json:"provider"`
	Variant     string                              `json:"variant"`
	Label       NameVariant                         `json:"label"`
	Features    map[string][]FeatureVariantResource `json:"features"`
	Status      string                              `json:"status"`
	Error       string                              `json:"error"`
	Tags        Tags                                `json:"tags"`
	Properties  Properties                          `json:"properties"`
}

type FeatureVariantResource struct {
	Created      time.Time                               `json:"created"`
	Description  string                                  `json:"description"`
	Entity       string                                  `json:"entity"`
	Name         string                                  `json:"name"`
	Owner        string                                  `json:"owner"`
	Provider     string                                  `json:"provider"`
	DataType     string                                  `json:"data-type"`
	Variant      string                                  `json:"variant"`
	Status       string                                  `json:"status"`
	Error        string                                  `json:"error"`
	Location     map[string]string                       `json:"location"`
	Source       NameVariant                             `json:"source"`
	TrainingSets map[string][]TrainingSetVariantResource `json:"training-sets"`
	Tags         Tags                                    `json:"tags"`
	Properties   Properties                              `json:"properties"`
	Mode         string                                  `json:"mode"`
	IsOnDemand   bool                                    `json:"is-on-demand"`
}

type LabelVariantResource struct {
	Created      time.Time                               `json:"created"`
	Description  string                                  `json:"description"`
	Entity       string                                  `json:"entity"`
	Name         string                                  `json:"name"`
	Owner        string                                  `json:"owner"`
	Provider     string                                  `json:"provider"`
	DataType     string                                  `json:"data-type"`
	Variant      string                                  `json:"variant"`
	Location     map[string]string                       `json:"location"`
	Source       NameVariant                             `json:"source"`
	TrainingSets map[string][]TrainingSetVariantResource `json:"training-sets"`
	Status       string                                  `json:"status"`
	Error        string                                  `json:"error"`
	Tags         Tags                                    `json:"tags"`
	Properties   Properties                              `json:"properties"`
}

type SourceVariantResource struct {
	Name           string                                  `json:"name"`
	Variant        string                                  `json:"variant"`
	Definition     string                                  `json:"definition"`
	Owner          string                                  `json:"owner"`
	Description    string                                  `json:"description"`
	Provider       string                                  `json:"provider"`
	Created        time.Time                               `json:"created"`
	Status         string                                  `json:"status"`
	Table          string                                  `json:"table"`
	TrainingSets   map[string][]TrainingSetVariantResource `json:"training-sets"`
	Features       map[string][]FeatureVariantResource     `json:"features"`
	Labels         map[string][]LabelVariantResource       `json:"labels"`
	LastUpdated    time.Time                               `json:"lastUpdated"`
	Schedule       string                                  `json:"schedule"`
	Tags           Tags                                    `json:"tags"`
	Properties     Properties                              `json:"properties"`
	SourceType     string                                  `json:"source-type"`
	Error          string                                  `json:"error"`
	Specifications map[string]string                       `json:"specifications"`
	Inputs         []NameVariant                           `json:"inputs"`
}

func getSourceString(variant *SourceVariant) string {
	if variant.IsSQLTransformation() {
		return variant.SQLTransformationQuery()
	} else {
		return variant.PrimaryDataSQLTableName()
	}
}

func getSourceType(variant *SourceVariant) string {
	if variant.IsSQLTransformation() {
		return "SQL Transformation"
	} else if variant.IsDFTransformation() {
		return "Dataframe Transformation"
	} else {
		return "Primary Table"
	}
}

func getSourceArgs(variant *SourceVariant) map[string]string {
	if variant.HasKubernetesArgs() {
		return variant.TransformationArgs().Format()
	}
	return map[string]string{}
}

func SourceShallowMap(variant *SourceVariant) SourceVariantResource {
	return SourceVariantResource{
		Name:           variant.Name(),
		Variant:        variant.Variant(),
		Definition:     getSourceString(variant),
		Owner:          variant.Owner(),
		Description:    variant.Description(),
		Provider:       variant.Provider(),
		Created:        variant.Created(),
		Status:         variant.Status().String(),
		LastUpdated:    variant.LastUpdated(),
		Schedule:       variant.Schedule(),
		Tags:           variant.Tags(),
		SourceType:     getSourceType(variant),
		Properties:     variant.Properties(),
		Error:          variant.Error(),
		Specifications: getSourceArgs(variant),
	}
}
