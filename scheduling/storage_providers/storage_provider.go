package scheduling

import (
	"fmt"
)

type StorageProvider interface {
	Set(id string, key string, value string) error
	Get(key string, prefix bool) ([]string, error)
	ListKeys(prefix string) ([]string, error)
	Lock(id string, key string, lockChannel chan error) error
	Unlock(id string, key string) error
}

type KeyNotFoundError struct {
	Key string
}

func (e *KeyNotFoundError) Error() string {
	return fmt.Sprintf("Key not found: %s", e.Key)
}
