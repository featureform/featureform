package storage

import (
	"github.com/featureform/fferr"
	"github.com/featureform/ffsync"
)

type MetadataStorage struct {
	Locker  ffsync.Locker
	Storage MetadataStorageImplementation
}

func (s *MetadataStorage) Create(key string, value string) fferr.GRPCError {
	lock, err := s.Locker.Lock(key)
	if err != nil {
		return err
	}
	defer s.Locker.Unlock(lock)

	return s.Storage.Set(key, value)
}

func (s *MetadataStorage) MultiCreate(data map[string]string) fferr.GRPCError {
	for key, value := range data {
		lock, err := s.Locker.Lock(key)
		if err != nil {
			return err
		}
		defer s.Locker.Unlock(lock)

		err = s.Storage.Set(key, value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *MetadataStorage) Update(key string, updateFn func(string) (string, fferr.GRPCError)) fferr.GRPCError {
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

func (s *MetadataStorage) List(prefix string) (map[string]string, fferr.GRPCError) {
	lock, err := s.Locker.Lock(prefix)
	if err != nil {
		return nil, err
	}
	defer s.Locker.Unlock(lock)

	return s.Storage.List(prefix)
}

func (s *MetadataStorage) Get(key string) (string, fferr.GRPCError) {
	lock, err := s.Locker.Lock(key)
	if err != nil {
		return "", err
	}
	defer s.Locker.Unlock(lock)

	return s.Storage.Get(key)
}

func (s *MetadataStorage) Delete(key string) (string, fferr.GRPCError) {
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

type MetadataStorageImplementation interface {
	Set(key string, value string) fferr.GRPCError            // Set stores the value for the key and updates it if it already exists
	Get(key string) (string, fferr.GRPCError)                // Get returns the value for the key
	List(prefix string) (map[string]string, fferr.GRPCError) // List returns all the keys and values with the given prefix
	Delete(key string) (string, fferr.GRPCError)             // Delete removes the key and its value from the store
}
