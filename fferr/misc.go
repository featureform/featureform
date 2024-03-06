package fferr

import (
	"fmt"

	"google.golang.org/grpc/codes"
)

func NewInternalError(err error) *InternalError {
	if err == nil {
		err = fmt.Errorf("internal")
	}
	baseError := newBaseGRPCError(err, INTERNAL_ERROR, codes.Internal)

	return &InternalError{
		baseError,
	}
}

type InternalError struct {
	baseGRPCError
}

func NewInvalidArgumentError(err error) *InvalidArgumentError {
	if err == nil {
		err = fmt.Errorf("invalid argument")
	}
	baseError := newBaseGRPCError(err, INVALID_ARGUMENT, codes.InvalidArgument)

	return &InvalidArgumentError{
		baseError,
	}
}

type InvalidArgumentError struct {
	baseGRPCError
}

// TODO: Consider moving to etcd.go
func NewKeyNotFoundError(key string, err error) *KeyNotFoundError {
	if err == nil {
		err = fmt.Errorf("key not found")
	}
	baseError := newBaseGRPCError(err, KEY_NOT_FOUND, codes.NotFound)
	baseError.AddDetail("key", key)

	return &KeyNotFoundError{
		baseError,
	}
}

type KeyNotFoundError struct {
	baseGRPCError
}
