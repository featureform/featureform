package scheduling

import (
	"fmt"
)

type StorageProvider interface {
	Set(key string, value string, lock LockObject) error
	Get(key string, prefix bool) ([]string, error)
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
