package scheduling

import (
	"fmt"
)

type StorageProvider interface {
	Set(key string, value string) error
	Get(key string, prefix bool) ([]string, error)
}

// KeyNotFoundError represents an error when a key is not found.
type KeyNotFoundError struct {
	Key string
}

// Error returns the error message for KeyNotFoundError.
func (e *KeyNotFoundError) Error() string {
	return fmt.Sprintf("Key not found: %s", e.Key)
}
