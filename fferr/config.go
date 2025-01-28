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

func NewMissingConfigEnv(env string) *InvalidConfigError {
	return NewInvalidConfigf("Env %s must be set", env)
}

func NewInvalidConfigEnv(env string, val any, possibleValues any) *InvalidConfigError {
	return NewInvalidConfigf("Env %s set to %v. Must be %v", env, val, possibleValues)
}

func NewInvalidConfigf(format string, a ...any) *InvalidConfigError {
	err := fmt.Errorf("Failed to Parse Config: "+format, a...)
	baseError := newBaseError(err, INVALID_ARGUMENT, codes.InvalidArgument)
	return &InvalidConfigError{
		baseError,
	}
}

type InvalidConfigError struct {
	baseError
}
