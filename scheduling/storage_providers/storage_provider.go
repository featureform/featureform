// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package scheduling

import (
	"fmt"
)

type StorageProvider interface {
	Set(key string, value string, lock LockObject) error
	Get(key string, prefix bool) (map[string]string, error)
	ListKeys(prefix string) ([]string, error)
	Lock(key string) (LockObject, error)
	Unlock(key string, lock LockObject) error
}

type KeyNotFoundError struct {
	Key string
}

func (e *KeyNotFoundError) Error() string {
	return fmt.Sprintf("Key not found: %s", e.Key)
}
