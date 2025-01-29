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

type KeyAlreadyLockedError struct {
	baseError
}

func NewKeyAlreadyLockedError(key string, lockId string, err error) *KeyAlreadyLockedError {
	if err == nil {
		err = fmt.Errorf("key '%s' is already locked by: %s", key, lockId)
	}
	baseError := newBaseError(err, KEY_ALREADY_LOCKED, codes.AlreadyExists)

	return &KeyAlreadyLockedError{
		baseError,
	}
}

func IsKeyAlreadyLockedError(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := err.(*KeyAlreadyLockedError); ok {
		return true
	} else {
		return false
	}
}

type KeyNotLockedError struct {
	baseError
}

func NewKeyNotLockedError(key string, err error) *KeyNotLockedError {
	if err == nil {
		err = fmt.Errorf("key '%s' is not locked", key)
	}
	baseError := newBaseError(err, KEY_NOT_LOCKED, codes.NotFound)

	return &KeyNotLockedError{
		baseError,
	}
}

type LockEmptyKeyError struct {
	baseError
}

func NewLockEmptyKeyError() *LockEmptyKeyError {
	baseError := newBaseError(nil, LOCK_EMPTY_KEY, codes.NotFound)

	return &LockEmptyKeyError{
		baseError,
	}
}

type UnlockEmptyKeyError struct {
	baseError
}

func NewUnlockEmptyKeyError() *UnlockEmptyKeyError {
	err := fmt.Errorf("cannot unlock an empty key")
	baseError := newBaseError(err, UNLOCK_EMPTY_KEY, codes.NotFound)

	return &UnlockEmptyKeyError{
		baseError,
	}
}

type ExceededWaitTimeError struct {
	baseError
}

func NewExceededWaitTimeError(stateManager string, key string) *ExceededWaitTimeError {
	err := fmt.Errorf("locker has exceeded max wait time while waiting for lock")
	baseError := newBaseError(err, EXCEEDED_WAIT_TIME, codes.Internal)
	baseError.AddDetail("state_manager", stateManager)
	baseError.AddDetail("key", key)

	return &ExceededWaitTimeError{
		baseError,
	}
}
