package resource

import (
	"fmt"
	pb "github.com/featureform/metadata/proto"
)

type Operation int

const (
	CREATE_OP Operation = iota
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
