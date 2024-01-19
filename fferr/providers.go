package fferr

import (
	"fmt"

	"google.golang.org/grpc/codes"
)

func NewConnectionError(providerName string, err error) *ConnectionError {
	if err == nil {
		err = fmt.Errorf("initial connection error")
	}
	baseError := newBaseGRPCError(err, CONNECTION_ERROR, codes.Internal)
	baseError.AddDetail("Provider", providerName)

	return &ConnectionError{
		baseError,
	}
}

type ConnectionError struct {
	baseGRPCError
}

func NewExecutionError(providerName, resourceName, resourceVariant, resourceType string, err error) *ExecutionError {
	if err == nil {
		err = fmt.Errorf("initial execution error")
	}
	baseError := newBaseGRPCError(err, EXECUTION_ERROR, codes.Internal)
	baseError.AddDetail("Provider", providerName)
	baseError.AddDetail("Resource_Name", resourceName)
	baseError.AddDetail("Resource_Variant", resourceVariant)

	return &ExecutionError{
		baseError,
	}
}

type ExecutionError struct {
	baseGRPCError
}
