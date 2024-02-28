package scheduling

import (
	"encoding/json"
	"fmt"
	"time"
)

const (
	UpdateSleepTime = 2 * time.Second
	ValidTimePeriod = 5 * time.Second
)

type StorageProvider interface {
	Set(key string, value string, lock LockObject) error
	Get(key string, prefix bool) (map[string]string, error)
	ListKeys(prefix string) ([]string, error)
	Lock(key string) (LockObject, error)
	Unlock(key string, lock LockObject) error
	Delete(key string, lock LockObject) error // deletes key and releases lock
}

func NewStorageProvider(provider string) (StorageProvider, error) {
	switch provider {
	case "etcd":
		return NewETCDStorageProvider()
	case "memory":
		return NewMemoryStorageProvider()
	default:
		return nil, fmt.Errorf("unknown storage provider: %s", provider)
	}
}

type KeyNotFoundError struct {
	Key string
}

func (e *KeyNotFoundError) Error() string {
	return fmt.Sprintf("Key not found: %s", e.Key)
}

type LockObject struct {
	ID      string
	Channel *chan error
}

type LockInformation struct {
	ID   string
	Key  string
	Date time.Time
}

func (l *LockInformation) Unmarshal(data []byte) error {
	var tmp struct {
		ID   string
		Key  string
		Date string
	}
	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}

	if tmp.ID == "" {
		return fmt.Errorf("lock information is missing ID")
	}
	if tmp.Key == "" {
		return fmt.Errorf("lock information is missing Key")
	}

	l.ID = tmp.ID
	l.Key = tmp.Key

	// Parse the date string with UTC time zone
	parsedTime, err := time.Parse(time.RFC3339, tmp.Date)
	if err != nil {
		return err
	}
	l.Date = parsedTime.UTC()

	return nil
}

func (l *LockInformation) Marshal() ([]byte, error) {
	return json.Marshal(l)
}
