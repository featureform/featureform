package storage

import (
	"fmt"
	"testing"

	"github.com/featureform/fferr"
	"github.com/featureform/ffsync"
)

type MetadataStorageTest struct {
	t       *testing.T
	storage metadataStorageImplementation
}

func (test *MetadataStorageTest) Run() {
	t := test.t
	storage := test.storage

	testFns := map[string]func(*testing.T, metadataStorageImplementation){
		"SetStorageProvider":    StorageSet,
		"GetStorageProvider":    StorageGet,
		"ListStorageProvider":   StorageList,
		"DeleteStorageProvider": StorageDelete,
	}

	for name, fn := range testFns {
		t.Run(name, func(t *testing.T) {
			fn(t, storage)
		})
	}
}

func StorageSet(t *testing.T, storage metadataStorageImplementation) {
	type keyValue struct {
		key   string
		value string
	}
	type TestCase struct {
		keys []keyValue
		err  error
	}
	tests := map[string]TestCase{
		"Simple": {
			[]keyValue{
				{
					"setTest/key1",
					"value1",
				},
			},
			nil,
		},
		"EmptyKey": {
			[]keyValue{
				{
					"",
					"value1",
				},
			},
			fferr.NewInvalidArgumentError(fmt.Errorf("cannot set an empty key")),
		},
		"SetExistingKey": {
			[]keyValue{
				{
					"key1",
					"value1",
				},
				{
					"key1",
					"value2",
				},
			},
			fferr.NewInternalError(fmt.Errorf("key '%s' already exists", "key1")),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			for _, kv := range test.keys {
				err := storage.Set(kv.key, kv.value)
				if err != nil && err.Error() != test.err.Error() {
					t.Errorf("Set(%s, %s): expected error %v, got %v", kv.key, kv.value, test.err, err)
				}

				// continue to next test case
				if kv.key == "" {
					return
				}

				value, err := storage.Delete(kv.key)
				if err != nil {
					t.Fatalf("Delete(%s) failed: %v", kv.key, err)
				}
				if value != kv.value {
					t.Fatalf("Delete(%s): expected value %s, got %s", kv.key, kv.value, value)
				}
			}
		})
	}
}

func StorageGet(t *testing.T, storage metadataStorageImplementation) {
	type TestCase struct {
		key   string
		value string
	}
	tests := map[string]TestCase{
		"Simple": {
			key:   "setTest/key1",
			value: "value1",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := storage.Set(test.key, test.value)
			if err != nil {
				t.Fatalf("Set(%s, %s) failed: %v", test.key, test.value, err)
			}

			value, err := storage.Get(test.key)
			if err != nil {
				t.Errorf("Get(%s): expected no error, got %v", test.key, err)
			}
			if value != test.value {
				t.Errorf("Get(%s): expected value %s, got %s", test.key, test.value, value)
			}

			_, err = storage.Delete(test.key)
			if err != nil {
				t.Fatalf("Delete(%s) failed: %v", test.key, err)
			}
		})
	}
}

func StorageList(t *testing.T, storage metadataStorageImplementation) {
	type TestCase struct {
		keys          map[string]string
		prefix        string
		expectedKeys  map[string]string
		expectedError error
	}

	tests := map[string]TestCase{
		"Simple": {
			keys: map[string]string{
				"simple/key1": "v1",
				"simple/key2": "v2",
				"simple/key3": "v3",
			},
			prefix: "simple",
			expectedKeys: map[string]string{
				"simple/key1": "v1",
				"simple/key2": "v2",
				"simple/key3": "v3",
			},
			expectedError: nil,
		},
		"NotAllKeys": {
			keys: map[string]string{
				"u/key1": "v1",
				"u/key2": "v2",
				"u/key3": "v3",
				"v/key4": "v4",
			},
			prefix: "u",
			expectedKeys: map[string]string{
				"u/key1": "v1",
				"u/key2": "v2",
				"u/key3": "v3",
			},
			expectedError: nil,
		},
		"EmptyPrefix": {
			keys: map[string]string{
				"x/key1": "v1",
				"y/key2": "v2",
				"z/key3": "v3",
			},
			prefix: "",
			expectedKeys: map[string]string{
				"x/key1": "v1",
				"y/key2": "v2",
				"z/key3": "v3",
			},
			expectedError: nil,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			for key, value := range test.keys {
				err := storage.Set(key, value)
				if err != nil {
					t.Fatalf("Set(%s, %s) failed: %v", key, value, err)
				}
			}
			defer func() {
				for key, _ := range test.keys {
					_, err := storage.Delete(key)
					if err != nil {
						t.Fatalf("Delete(%s) failed: %v", key, err)
					}
				}
			}()

			keys, err := storage.List(test.prefix)
			if err != nil {
				t.Errorf("List(%s): expected no error, got %v", test.prefix, err)
			}

			if len(keys) != len(test.expectedKeys) {
				t.Fatalf("List(%s): expected %d keys, got %d keys", test.prefix, len(test.expectedKeys), len(keys))
			}

			for key, value := range test.expectedKeys {
				if keys[key] != value {
					t.Fatalf("List(%s): expected key %s to have value %s, got %s", test.prefix, key, value, keys[key])
				}
			}
		})
	}
}

func StorageDelete(t *testing.T, storage metadataStorageImplementation) {
	type TestCase struct {
		setKey      string
		setValue    string
		deleteKey   string
		deleteValue string
	}
	tests := map[string]TestCase{
		"DeleteSimple": {
			setKey:      "deleteTest/key1",
			setValue:    "value1",
			deleteKey:   "deleteTest/key1",
			deleteValue: "value1",
		},
		"DeleteWrongKey": {
			setKey:      "key1",
			setValue:    "value1",
			deleteKey:   "key2",
			deleteValue: "",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := storage.Set(test.setKey, test.setValue)
			if err != nil {
				t.Fatalf("Set(%s, %s) failed: %v", test.setKey, test.setValue, err)
			}
			defer func() {
				storage.Delete(test.setKey)
			}()

			value, err := storage.Delete(test.deleteKey)
			if err != nil {
				return
			}

			if test.deleteValue != "" && value != test.setValue {
				t.Fatalf("Delete(%s): expected value %s, got %s", test.deleteKey, test.setValue, value)
			}
		})
	}
}

func TestMetadataStorage(t *testing.T) {
	locker, err := ffsync.NewMemoryLocker()
	if err != nil {
		t.Fatalf("Failed to create Memory locker: %v", err)
	}

	storage, err := NewMemoryStorageImplementation()
	if err != nil {
		t.Fatalf("Failed to create Memory storage: %v", err)
	}

	metadataStorage := MetadataStorage{
		Locker:  &locker,
		Storage: &storage,
	}

	tests := map[string]func(*testing.T, MetadataStorage){
		"TestCreate":      testCreate,
		"TestMultiCreate": testMultiCreate,
		"TestUpdate":      testUpdate,
		"TestList":        testList,
		"TestGet":         testGet,
		"TestDelete":      testDelete,
	}

	for name, fn := range tests {
		t.Run(name, func(t *testing.T) {
			fn(t, metadataStorage)
		})
	}
}

func testCreate(t *testing.T, ms MetadataStorage) {
	type TestCase struct {
		key   string
		value string
		err   error
	}
	tests := map[string]TestCase{
		"Simple":   {"createTest/key1", "value1", nil},
		"EmptyKey": {"", "value1", fferr.NewLockEmptyKeyError()},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := ms.Create(test.key, test.value)
			if err != nil && err.Error() != test.err.Error() {
				t.Errorf("Create(%s, %s): expected error %v, got %v", test.key, test.value, test.err, err)
			}

			// continue to next test case
			if test.key == "" {
				return
			}

			value, err := ms.Delete(test.key)
			if err != nil {
				t.Fatalf("Delete(%s) failed: %v", test.key, err)
			}

			if value != test.value {
				t.Fatalf("Delete(%s): expected value %s, got %s", test.key, test.value, value)
			}
		})
	}
}

func testMultiCreate(t *testing.T, ms MetadataStorage) {
	type TestCase struct {
		data map[string]string
		err  error
	}
	tests := map[string]TestCase{
		"Simple": {
			data: map[string]string{
				"multiCreateTest/key1": "value1",
				"multiCreateTest/key2": "value2",
				"multiCreateTest/key3": "value3",
			},
			err: nil,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := ms.MultiCreate(test.data)
			if err != nil && err.Error() != test.err.Error() {
				t.Errorf("MultiCreate(%v): expected error %v, got %v", test.data, test.err, err)
			}

			// continue to next test case
			if len(test.data) == 0 {
				return
			}

			for key, value := range test.data {
				v, err := ms.Delete(key)
				if err != nil {
					t.Fatalf("Delete(%s) failed: %v", key, err)
				}

				if v != value {
					t.Fatalf("Delete(%s): expected value %s, got %s", key, value, v)
				}
			}
		})
	}
}

func updateFn(currentValue string) (string, error) {
	return fmt.Sprintf("%s_updated", currentValue), nil
}

func testUpdate(t *testing.T, ms MetadataStorage) {
	type TestCase struct {
		key          string
		value        string
		updatedValue string
		err          error
	}

	tests := map[string]TestCase{
		"Simple": {
			key:          "updateTest/key1",
			value:        "value1",
			updatedValue: "value1_updated",
			err:          nil,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := ms.Create(test.key, test.value)
			if err != nil {
				t.Fatalf("Create(%s, %s) failed: %v", test.key, test.value, err)
			}

			err = ms.Update(test.key, updateFn)
			if err != nil {
				t.Fatalf("Update(%s) failed: %v", test.key, err)
			}

			value, err := ms.Delete(test.key)
			if err != nil {
				t.Fatalf("Delete(%s) failed: %v", test.key, err)
			}

			if value != test.updatedValue {
				t.Fatalf("Update(%s): expected value %s, got %s", test.key, test.updatedValue, value)
			}
		})
	}
}

func testList(t *testing.T, ms MetadataStorage) {
	type TestCase struct {
		keys          map[string]string
		prefix        string
		expectedKeys  map[string]string
		expectedError error
	}

	tests := map[string]TestCase{
		"Simple": {
			keys: map[string]string{
				"simple/key1": "v1",
				"simple/key2": "v2",
				"simple/key3": "v3",
			},
			prefix: "simple",
			expectedKeys: map[string]string{
				"simple/key1": "v1",
				"simple/key2": "v2",
				"simple/key3": "v3",
			},
			expectedError: nil,
		},
		"NotAllKeys": {
			keys: map[string]string{
				"u/key1": "v1",
				"u/key2": "v2",
				"u/key3": "v3",
				"v/key4": "v4",
			},
			prefix: "u",
			expectedKeys: map[string]string{
				"u/key1": "v1",
				"u/key2": "v2",
				"u/key3": "v3",
			},
			expectedError: nil,
		},
		"EmptyPrefix": {
			keys: map[string]string{
				"x/key1": "v1",
				"y/key2": "v2",
				"z/key3": "v3",
			},
			prefix: "",
			expectedKeys: map[string]string{
				"x/key1": "v1",
				"y/key2": "v2",
				"z/key3": "v3",
			},
			expectedError: nil,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			for key, value := range test.keys {
				err := ms.Create(key, value)
				if err != nil {
					t.Fatalf("Create(%s, %s) failed: %v", key, value, err)
				}
			}
			defer func() {
				for key := range test.keys {
					_, err := ms.Delete(key)
					if err != nil {
						t.Fatalf("Delete(%s) failed: %v", key, err)
					}
				}
			}()

			keys, err := ms.List(test.prefix)
			if err != nil && err.Error() != test.expectedError.Error() {
				t.Fatalf("List(%s): expected: %v, got %v", test.prefix, test.expectedError, err)
			} else if err != nil && err.Error() == test.expectedError.Error() {
				return
			}

			if len(keys) != len(test.expectedKeys) {
				t.Fatalf("List(%s): expected %d keys, got %d keys", test.prefix, len(test.expectedKeys), len(keys))
			}

			for key, value := range test.expectedKeys {
				if keys[key] != value {
					t.Fatalf("List(%s): expected key %s to have value %s, got %s", test.prefix, key, value, keys[key])
				}
			}
		})
	}
}

func testGet(t *testing.T, ms MetadataStorage) {
	type TestCase struct {
		key   string
		value string
	}
	tests := map[string]TestCase{
		"Simple": {
			key:   "setTest/key1",
			value: "value1",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := ms.Create(test.key, test.value)
			if err != nil {
				t.Fatalf("Create(%s, %s) failed: %v", test.key, test.value, err)
			}

			value, err := ms.Get(test.key)
			if err != nil {
				t.Errorf("Get(%s): expected no error, got %v", test.key, err)
			}
			if value != test.value {
				t.Errorf("Get(%s): expected value %s, got %s", test.key, test.value, value)
			}

			_, err = ms.Delete(test.key)
			if err != nil {
				t.Fatalf("Delete(%s) failed: %v", test.key, err)
			}
		})
	}
}

func testDelete(t *testing.T, ms MetadataStorage) {
	type TestCase struct {
		key   string
		value string
		err   error
	}
	tests := map[string]TestCase{
		"Simple": {"deleteTest/key1", "value1", nil},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := ms.Create(test.key, test.value)
			if err != nil {
				t.Fatalf("Create(%s, %s) failed: %v", test.key, test.value, err)
			}

			// continue to next test case
			if test.key == "" {
				return
			}

			value, err := ms.Delete(test.key)
			if err != nil {
				t.Fatalf("Delete(%s) failed: %v", test.key, err)
			}

			if value != test.value {
				t.Fatalf("Delete(%s): expected value %s, got %s", test.key, test.value, value)
			}
		})
	}
}
