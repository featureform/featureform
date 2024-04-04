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

func NewInternalErrorf(format string, a ...any) *InternalError {
	err := fmt.Errorf(format, a...)
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

func NewInvalidArgumentErrorf(format string, a ...any) *InvalidArgumentError {
	err := fmt.Errorf(format, a...)
	return NewInvalidArgumentError(err)
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

func NewParsingError(err error) *ParsingError {
	if err == nil {
		err = fmt.Errorf("parsing error")
	}
	baseError := newBaseGRPCError(err, PARSING_ERROR, codes.InvalidArgument)

	return &ParsingError{
		baseError,
	}
}

type ParsingError struct {
	baseGRPCError
}

type UnimplementedError struct {
	baseGRPCError
}

func NewUnimplementedErrorf(format string, a ...any) *UnimplementedError {
	err := fmt.Errorf(format, a...)
	return NewUnimplementedError(err)
}

func NewUnimplementedError(err error) *UnimplementedError {
	baseError := newBaseGRPCError(err, UNIMPLEMENTED_ERROR, codes.Unimplemented)

	return &UnimplementedError{
		baseError,
	}
}
