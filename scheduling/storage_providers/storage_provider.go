package scheduling

import (
	"fmt"
)

type StorageProvider interface {
	Set(key string, value string) error
	Get(key string, prefix bool) ([]string, error)
}

type KeyNotFoundError struct {
	Key string
}

func (e *KeyNotFoundError) Error() string {
	return fmt.Sprintf("Key not found: %s", e.Key)
}
