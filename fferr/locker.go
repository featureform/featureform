package fferr

import (
	"fmt"

	"google.golang.org/grpc/codes"
)

type KeyAlreadyLockedError struct {
	baseGRPCError
}

func NewKeyAlreadyLockedError(key string, lockId string, err error) *KeyAlreadyLockedError {
	if err == nil {
		err = fmt.Errorf("key '%s' is already locked by: %s", key, lockId)
	}
	baseError := newBaseGRPCError(err, KEY_ALREADY_LOCKED, codes.AlreadyExists)

	return &KeyAlreadyLockedError{
		baseError,
	}
}

type KeyNotLockedError struct {
	baseGRPCError
}

func NewKeyNotLockedError(key string, err error) *KeyNotLockedError {
	if err == nil {
		err = fmt.Errorf("key '%s' is not locked", key)
	}
	baseError := newBaseGRPCError(err, KEY_NOT_LOCKED, codes.NotFound)

	return &KeyNotLockedError{
		baseError,
	}
}
