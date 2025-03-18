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

	types "github.com/featureform/fftypes"
)

func NewInternalError(err error) *InternalError {
	if err == nil {
		err = fmt.Errorf("internal")
	}
	baseError := newBaseError(err, INTERNAL_ERROR, codes.Internal)
	return &InternalError{
		baseError,
	}
}

func NewInternalErrorf(format string, args ...any) *InternalError {
	return NewInternalError(fmt.Errorf(format, args...))
}

type InternalError struct {
	baseError
}

func NewInvalidArgumentError(err error) *InvalidArgumentError {
	if err == nil {
		err = fmt.Errorf("invalid argument")
	}
	baseError := newBaseError(err, INVALID_ARGUMENT, codes.InvalidArgument)

	return &InvalidArgumentError{
		baseError,
	}
}

func NewInvalidArgumentErrorf(format string, a ...any) *InvalidArgumentError {
	err := fmt.Errorf(format, a...)
	return NewInvalidArgumentError(err)
}

type InvalidArgumentError struct {
	baseError
}

// TODO: Consider moving to etcd.go
func NewKeyNotFoundError(key string, err error) *KeyNotFoundError {
	if err == nil {
		err = fmt.Errorf("key not found")
	}
	baseError := newBaseError(err, KEY_NOT_FOUND, codes.NotFound)
	baseError.AddDetail("key", key)

	return &KeyNotFoundError{
		baseError,
	}
}

type KeyNotFoundError struct {
	baseError
}

func NewParsingError(err error) *ParsingError {
	if err == nil {
		err = fmt.Errorf("parsing error")
	}
	baseError := newBaseError(err, PARSING_ERROR, codes.InvalidArgument)

	return &ParsingError{
		baseError,
	}
}

type ParsingError struct {
	baseError
}

type UnimplementedError struct {
	baseError
}

func NewUnimplementedErrorf(format string, a ...any) *UnimplementedError {
	err := fmt.Errorf(format, a...)
	return NewUnimplementedError(err)
}

func NewUnimplementedError(err error) *UnimplementedError {
	baseError := newBaseError(err, UNIMPLEMENTED_ERROR, codes.Unimplemented)

	return &UnimplementedError{
		baseError,
	}
}

type UnsupportedTypeError struct {
	NativeType types.NativeType
	baseError
}

func NewUnsupportedTypeError(nativeType types.NativeType) *UnsupportedTypeError {
	err := fmt.Errorf("unsupported type: %s", nativeType)
	baseError := newBaseError(err, "UNSUPPORTED_TYPE", codes.InvalidArgument)
	baseError.AddDetail("native_type", string(nativeType))

	return &UnsupportedTypeError{
		NativeType: nativeType,
		baseError:  baseError,
	}
}
