package fferr

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

const (
	// PROVIDERS:
	EXECUTION_ERROR  = "Execution Error"
	CONNECTION_ERROR = "Connection Error"
	// DATA:
	DATASET_NOT_FOUND        = "Dataset Not Found"
	TRANSFORMATION_NOT_FOUND = "Transformation Not Found"
	ENTITY_NOT_FOUND         = "Entity Not Found"
	FEATURE_NOT_FOUND        = "Feature Not Found"
	TRAINING_SET_NOT_FOUND   = "Training Set Not Found"
	INVALID_RESOURCE_TYPE    = "Invalid Resource Type"
	// MISCELLANEOUS:
	INTERNAL_ERROR = "Internal Error"
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
