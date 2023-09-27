package metadata

import "fmt"

type KeyNotFoundError struct {
	key string
}

func (e *KeyNotFoundError) Error() string {
	return fmt.Sprintf("Key Not Found: %s", e.key)
}
