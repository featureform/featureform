package fferr

import (
	"fmt"

	"google.golang.org/grpc/codes"
)

func NewConnectionError(providerName string, err error) *ConnectionError {
	if err == nil {
		err = fmt.Errorf("failed connection")
	}
	baseError := newBaseGRPCError(err, CONNECTION_ERROR, codes.Internal)
	baseError.AddDetail("provider", providerName)

	return &ConnectionError{
		baseError,
	}
}

type ConnectionError struct {
	baseGRPCError
}

func NewExecutionError(providerName string, err error) *ExecutionError {
	if err == nil {
		err = fmt.Errorf("execution failed")
	}
	baseError := newBaseGRPCError(err, EXECUTION_ERROR, codes.Internal)
	baseError.AddDetail("provider", providerName)

	return &ExecutionError{
		baseError,
	}
}

func NewResourceExecutionError(providerName, resourceName, resourceVariant string, resourceType ResourceType, err error) *ExecutionError {
	if err == nil {
		err = fmt.Errorf("execution failed on resource")
	}
	baseError := newBaseGRPCError(err, EXECUTION_ERROR, codes.Internal)
	baseError.AddDetail("provider", providerName)
	baseError.AddDetail("resource_name", resourceName)
	baseError.AddDetail("resource_variant", resourceVariant)
	baseError.AddDetail("resource_type", string(resourceType))

	return &ExecutionError{
		baseError,
	}
}

type ExecutionError struct {
	baseGRPCError
}

func NewProviderConfigError(providerName string, err error) *ProviderConfigError {
	if err == nil {
		err = fmt.Errorf("provider config")
	}
	baseError := newBaseGRPCError(err, EXECUTION_ERROR, codes.Internal)
	baseError.AddDetail("provider", providerName)

	return &ProviderConfigError{
		baseError,
	}
}

type ProviderConfigError struct {
	baseGRPCError
}
