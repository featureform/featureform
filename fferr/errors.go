package fferr

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

type ResourceType string

const (
	// PROVIDERS:
	EXECUTION_ERROR  = "Execution Error"
	CONNECTION_ERROR = "Connection Error"
	// DATA:
	DATASET_NOT_FOUND             = "Dataset Not Found"
	DATASET_ALREADY_EXISTS        = "Dataset Already Exists"
	DATATYPE_NOT_FOUND            = "Datatype Not Found"
	TRANSFORMATION_NOT_FOUND      = "Transformation Not Found"
	ENTITY_NOT_FOUND              = "Entity Not Found"
	FEATURE_NOT_FOUND             = "Feature Not Found"
	TRAINING_SET_NOT_FOUND        = "Training Set Not Found"
	INVALID_RESOURCE_TYPE         = "Invalid Resource Type"
	INVALID_RESOURCE_NAME_VARIANT = "Invalid Resource Name Variant"
	INVALID_FILE_TYPE             = "Invalid File Type"
	RESOURCE_CHANGED              = "Resource Changed"
	// MISCELLANEOUS:
	INTERNAL_ERROR   = "Internal Error"
	INVALID_ARGUMENT = "Invalid Argument"
	// JOBS:
	JOB_DOES_NOT_EXIST        = "Job Does Not Exist"
	RESOURCE_ALREADY_COMPLETE = "Resource Already Complete"
	RESOURCE_ALREADY_FAILED   = "Resource Already Failed"
	RESOURCE_NOT_READY        = "Resource Not Ready"
	RESOURCE_FAILED           = "Resource Failed"

	// RESOURCE TYPES:
	FEATURE              ResourceType = "FEATURE"
	LABEL                ResourceType = "LABEL"
	TRAINING_SET         ResourceType = "TRAINING_SET"
	SOURCE               ResourceType = "SOURCE"
	FEATURE_VARIANT      ResourceType = "FEATURE_VARIANT"
	LABEL_VARIANT        ResourceType = "LABEL_VARIANT"
	TRAINING_SET_VARIANT ResourceType = "TRAINING_SET_VARIANT"
	SOURCE_VARIANT       ResourceType = "SOURCE_VARIANT"
	PROVIDER             ResourceType = "PROVIDER"
	ENTITY               ResourceType = "ENTITY"
	MODEL                ResourceType = "MODEL"
	USER                 ResourceType = "USER"
)

type JSONStackTrace map[string]interface{}

type GRPCError interface {
	GRPCErrorType() codes.Code
	ToString() string
	ToMetadataPairs() metadata.MD
}

func newBaseGRPCError(err error, errorType string, code codes.Code) baseGRPCError {
	if err == nil {
		err = fmt.Errorf("initial error")
	}
	genericError := NewGenericError(err)

	return baseGRPCError{
		Code:         code,
		Type:         errorType,
		GenericError: genericError,
	}
}

type baseGRPCError struct {
	Code codes.Code
	Type string
	GenericError
}

func (e *baseGRPCError) GRPCErrorType() codes.Code {
	return e.Code
}

func (e *baseGRPCError) ToMetadataPairs() metadata.MD {
	mdPairs := make(metadata.MD)
	for key, value := range e.GenericError.details {
		mdPairs.Append(key, value)
	}
	return mdPairs
}

func (e *baseGRPCError) ToString() string {
	return e.GenericError.Error()
}

func (e *baseGRPCError) StringType() string {
	return e.Type
}
