// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package fferr

import (
	"fmt"

	"google.golang.org/grpc/codes"
)

func NewConnectionError(providerName string, err error) *ConnectionError {
	if err == nil {
		err = fmt.Errorf("failed connection")
	}
	baseError := newBaseError(err, CONNECTION_ERROR, codes.Internal)
	baseError.AddDetail("provider", providerName)

	return &ConnectionError{
		baseError,
	}
}

type ConnectionError struct {
	baseError
}

func NewExecutionError(providerName string, err error) *ExecutionError {
	if err == nil {
		err = fmt.Errorf("execution failed")
	}
	baseError := newBaseError(err, EXECUTION_ERROR, codes.Internal)
	baseError.AddDetail("provider", providerName)

	return &ExecutionError{
		baseError,
	}
}

func NewResourceExecutionError(providerName, resourceName, resourceVariant string, resourceType ResourceType, err error) *ExecutionError {
	if err == nil {
		err = fmt.Errorf("execution failed on resource")
	}
	baseError := newBaseError(err, EXECUTION_ERROR, codes.Internal)
	baseError.AddDetail("provider", providerName)
	baseError.AddDetail("resource_name", resourceName)
	baseError.AddDetail("resource_variant", resourceVariant)
	baseError.AddDetail("resource_type", string(resourceType))

	return &ExecutionError{
		baseError,
	}
}

type ExecutionError struct {
	baseError
}

func NewProviderConfigError(providerName string, err error) *ProviderConfigError {
	if err == nil {
		err = fmt.Errorf("provider config")
	}
	baseError := newBaseError(err, EXECUTION_ERROR, codes.Internal)
	baseError.AddDetail("provider", providerName)

	return &ProviderConfigError{
		baseError,
	}
}

type ProviderConfigError struct {
	baseError
}
