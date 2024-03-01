package storage_storer

type metadataStorerImplementation interface {
	Set(key string, value string) error            // Set stores the value for the key and updates it if it already exists
	Get(key string) (string, error)                // Get returns the value for the key
	List(prefix string) (map[string]string, error) // List returns all the keys and values with the given prefix
	Delete(key string) error                       // Delete removes the key and its value from the store
}
