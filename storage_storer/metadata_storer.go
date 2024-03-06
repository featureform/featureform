package storage_storer

import (
	"github.com/featureform/fferr"
	"github.com/featureform/locker"
)

type MetadataStorer struct {
	Locker locker.MultiLock
	Storer metadataStorerImplementation
}

func (s *MetadataStorer) Create(key string, value string) fferr.GRPCError {
	lock, err := s.Locker.Lock(key)
	if err != nil {
		return err
	}
	defer s.Locker.Unlock(lock)

	return s.Storer.Set(key, value)
}

func (s *MetadataStorer) Update(key string, updateFn func(string) (string, fferr.GRPCError)) fferr.GRPCError {
	lock, err := s.Locker.Lock(key)
	if err != nil {
		return err
	}
	defer s.Locker.Unlock(lock)

	currentValue, err := s.Storer.Get(key)
	if err != nil {
		return err
	}

	newValue, err := updateFn(currentValue)
	if err != nil {
		return err
	}

	return s.Storer.Set(key, newValue)
}

func (s *MetadataStorer) List(prefix string) (map[string]string, fferr.GRPCError) {
	// TODO: how do we lock a prefix?
	return s.Storer.List(prefix)
}

func (s *MetadataStorer) Get(key string) (string, fferr.GRPCError) {
	lock, err := s.Locker.Lock(key)
	if err != nil {
		return "", err
	}
	defer s.Locker.Unlock(lock)

	return s.Storer.Get(key)
}

func (s *MetadataStorer) Delete(key string) (string, fferr.GRPCError) {
	lock, err := s.Locker.Lock(key)
	if err != nil {
		return "", err
	}
	defer s.Locker.Unlock(lock)

	value, err := s.Storer.Delete(key)
	if err != nil {
		return "", err
	}
	return value, nil
}

type metadataStorerImplementation interface {
	Set(key string, value string) fferr.GRPCError            // Set stores the value for the key and updates it if it already exists
	Get(key string) (string, fferr.GRPCError)                // Get returns the value for the key
	List(prefix string) (map[string]string, fferr.GRPCError) // List returns all the keys and values with the given prefix
	Delete(key string) (string, fferr.GRPCError)             // Delete removes the key and its value from the store
}
