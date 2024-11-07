// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package scheduling

import (
	"fmt"
	"strings"
)

type StorageProvider interface {
	Set(key string, value string) error
	Get(key string, prefix bool) ([]string, error)
}

type MemoryStorageProvider struct {
	storage map[string]string
}

func (m *MemoryStorageProvider) Set(key string, value string) error {
	if m.storage == nil || len(m.storage) == 0 {
		m.storage = make(map[string]string)
	}
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}
	if value == "" {
		return fmt.Errorf("value cannot be empty")
	}
	m.storage[key] = value
	return nil
}

func (m *MemoryStorageProvider) Get(key string, prefix bool) ([]string, error) {
	if !prefix {
		if key == "" {
			return nil, fmt.Errorf("key cannot be empty")
		}
		value, ok := m.storage[key]
		if !ok {
			return nil, &KeyNotFoundError{Key: key}
		}
		return []string{value}, nil
	}

	var result []string
	for k, v := range m.storage {
		if strings.HasPrefix(k, key) {
			result = append(result, v)
		}
	}
	if len(result) == 0 {
		return nil, &KeyNotFoundError{Key: key}
	}
	return result, nil
}

// KeyNotFoundError represents an error when a key is not found.
type KeyNotFoundError struct {
	Key string
}

// Error returns the error message for KeyNotFoundError.
func (e *KeyNotFoundError) Error() string {
	return fmt.Sprintf("Key not found: %s", e.Key)
}
