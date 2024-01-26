package fferr

import (
	"fmt"

	"google.golang.org/grpc/codes"
)

func NewInternalError(err error) *InternalError {
	if err == nil {
		err = fmt.Errorf("initial internal error")
	}
	baseError := newBaseGRPCError(err, INTERNAL_ERROR, codes.Internal)

	return &InternalError{
		baseError,
	}
}

type InternalError struct {
	baseGRPCError
}

func NewInvalidArgument(err error) *InvalidArgument {
	if err == nil {
		err = fmt.Errorf("initial invalid argument error")
	}
	baseError := newBaseGRPCError(err, INVALID_ARGUMENT, codes.InvalidArgument)

	return &InvalidArgument{
		baseError,
	}
}

type InvalidArgument struct {
	baseGRPCError
}

// TODO: Consider moving to etcd.go
func NewKeyNotFoundError(key string, err error) *KeyNotFoundError {
	if err == nil {
		err = fmt.Errorf("initial key not found error")
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
