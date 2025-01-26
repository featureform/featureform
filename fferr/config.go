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
