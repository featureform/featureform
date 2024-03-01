package storage_storer

import (
	"fmt"
	"testing"
)

type MetadataStorerTest struct {
	t       *testing.T
	storage metadataStorerImplementation
}

func (test *MetadataStorerTest) Run() {
	t := test.t
	storage := test.storage

	testFns := map[string]func(*testing.T, metadataStorerImplementation){
		"SetStorageProvider": StorerSet,
		"GetStorageProvider": StorerGet,
	}

	for name, fn := range testFns {
		t.Run(name, func(t *testing.T) {
			fn(t, storage)
		})
	}
}

func StorerSet(t *testing.T, storage metadataStorerImplementation) {
	type TestCase struct {
		key   string
		value string
		err   error
	}
	tests := map[string]TestCase{
		"Simple":   {"setTest/key1", "value1", nil},
		"EmptyKey": {"", "value1", fmt.Errorf("key is empty")},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := storage.Set(test.key, test.value)
			if err != nil && err.Error() != test.err.Error() {
				t.Errorf("Set(%s, %s): expected error %v, got %v", test.key, test.value, test.err, err)
			}
		})
	}
}

func StorerGet(t *testing.T, storage metadataStorerImplementation) {
	type TestCase struct {
		key string
	}
	tests := map[string]TestCase{
		"Simple": {"setTest/key1"},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := storage.Get(test.key)
			if err != nil {
				t.Errorf("Get(%s): expected no error, got %v", test.key, err)
			}
		})
	}
}
