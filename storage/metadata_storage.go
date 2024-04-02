package storage

import (
	"github.com/featureform/ffsync"
)

type MetadataStorage struct {
	Locker  ffsync.Locker
	Storage metadataStorageImplementation
}

func (s *MetadataStorage) Create(key string, value string) error {
	lock, err := s.Locker.Lock(key)
	if err != nil {
		return err
	}
	defer s.Locker.Unlock(lock)

	return s.Storage.Set(key, value)
}

func (s *MetadataStorage) MultiCreate(data map[string]string) error {
	// Lock all keys before setting any values
	for key := range data {
		lock, err := s.Locker.Lock(key)
		if err != nil {
			return err
		}
		defer s.Locker.Unlock(lock)
	}

	// Set all values
	for key, value := range data {
		err := s.Storage.Set(key, value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *MetadataStorage) Update(key string, updateFn func(string) (string, error)) error {
	lock, err := s.Locker.Lock(key)
	if err != nil {
		return err
	}
	defer s.Locker.Unlock(lock)

	currentValue, err := s.Storage.Get(key)
	if err != nil {
		return err
	}

	newValue, err := updateFn(currentValue)
	if err != nil {
		return err
	}

	return s.Storage.Set(key, newValue)
}

func (s *MetadataStorage) List(prefix string) (map[string]string, error) {
	lock, err := s.Locker.Lock(prefix)
	if err != nil {
		return nil, err
	}
	defer s.Locker.Unlock(lock)

	return s.Storage.List(prefix)
}

func (s *MetadataStorage) Get(key string) (string, error) {
	lock, err := s.Locker.Lock(key)
	if err != nil {
		return "", err
	}
	defer s.Locker.Unlock(lock)

	return s.Storage.Get(key)
}

func (s *MetadataStorage) Delete(key string) (string, error) {
	lock, err := s.Locker.Lock(key)
	if err != nil {
		return "", err
	}
	defer s.Locker.Unlock(lock)

	value, err := s.Storage.Delete(key)
	if err != nil {
		return "", err
	}
	return value, nil
}

func (s *MetadataStorage) Close() {
	s.Locker.Close()
	s.Storage.Close()
}

type metadataStorageImplementation interface {
	Set(key string, value string) error            // Set stores the value for the key and updates it if it already exists
	Get(key string) (string, error)                // Get returns the value for the key
	List(prefix string) (map[string]string, error) // List returns all the keys and values with the given prefix
	Delete(key string) (string, error)             // Delete removes the key and its value from the store
	Close()                                        // Close closes the storage
}
