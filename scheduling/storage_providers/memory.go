package scheduling

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

type LockInformation struct {
	ID   string
	Key  string
	Date time.Time
}

const (
	UpdateSleepTime = 2 * time.Second
	ValidTimePeriod = 5 * time.Second
	CleanupInterval = 15 * time.Second
)

type MemoryStorageProvider struct {
	storage     map[string]string
	lockedItems map[string]LockInformation
}

func NewMemoryStorageProvider(channel chan int) *MemoryStorageProvider {
	// Start a goroutine to cleanup locked items every minute
	// and send the number of deleted items to the channel
	m := &MemoryStorageProvider{storage: make(map[string]string), lockedItems: make(map[string]LockInformation)}
	go func(channel chan int) {
		for {
			time.Sleep(CleanupInterval)
			deleteCount := m.Cleanup()
			channel <- deleteCount
		}
	}(channel)

	return m
}

func (m *MemoryStorageProvider) Set(id string, key string, value string) error {
	lockInfo, ok := m.lockedItems[key]
	if !ok {
		return fmt.Errorf("key is not locked")
	} else if lockInfo.ID != id {
		return fmt.Errorf("key is locked by another id")
	}

	if key == "" {
		return fmt.Errorf("key is empty")
	}
	if value == "" {
		return fmt.Errorf("value is empty for key %s", key)
	}
	m.storage[key] = value
	return nil
}

func (m *MemoryStorageProvider) Get(key string, prefix bool) ([]string, error) {
	if key == "" {
		return nil, fmt.Errorf("key is empty")
	}
	if !prefix {
		value, ok := m.storage[key]
		if !ok {
			return nil, &KeyNotFoundError{Key: key}
		}
		return []string{value}, nil
	}

	var result []string
	var keys []string
	for k, _ := range m.storage {
		if strings.HasPrefix(k, key) {
			keys = append(keys, k)
		}
	}

	sort.Strings(keys)

	for _, k := range keys {
		if strings.HasPrefix(k, key) {
			result = append(result, m.storage[k])
		}
	}
	if len(result) == 0 {
		return nil, &KeyNotFoundError{Key: key}
	}
	return result, nil
}

func (m *MemoryStorageProvider) ListKeys(prefix string) ([]string, error) {
	var result []string
	for k, _ := range m.storage {
		if strings.HasPrefix(k, prefix) {
			result = append(result, k)
		}
	}
	sort.Strings(result)

	return result, nil
}

type LockObject struct {
	ID      string
	Channel chan error
}

func (m *MemoryStorageProvider) Lock(id, key string, lockChannel chan error) (LockObject, error) {
	keyLockInfo, ok := m.lockedItems[key]
	if ok && keyLockInfo.ID == id && time.Since(keyLockInfo.Date) < ValidTimePeriod {
		return LockObject{ID: id, Channel: lockChannel}, nil
	} else if ok {
		return LockObject{}, fmt.Errorf("key is already locked by %s", keyLockInfo.ID)
	}
	m.lockedItems[key] = LockInformation{
		ID:   id,
		Key:  key,
		Date: time.Now(),
	}

	go m.updateLockTime(id, key, lockChannel)
	lockChannel <- nil

	return LockObject{ID: id, Channel: lockChannel}, nil
}

func (m *MemoryStorageProvider) Unlock(id, key string) error {
	keyLockInfo, ok := m.lockedItems[key]
	if !ok {
		return fmt.Errorf("key is not locked")
	}
	if keyLockInfo.ID != id {
		return fmt.Errorf("key is locked by another id")
	}
	delete(m.lockedItems, key)
	return nil
}

func (m *MemoryStorageProvider) Cleanup() int {
	deleteCount := 0
	for key, lockInfo := range m.lockedItems {
		if time.Since(lockInfo.Date) > ValidTimePeriod {
			fmt.Println("Deleting key", key, "from locked items", lockInfo.Date, time.Now())
			delete(m.lockedItems, key)
			deleteCount++
		}
	}
	return deleteCount
}

func (m *MemoryStorageProvider) updateLockTime(id string, key string, lockChannel chan error) {
	keyFound := true
	for keyFound {
		time.Sleep(UpdateSleepTime)
		fmt.Println("Updating lock time for key", key, time.Now())
		if _, ok := m.lockedItems[key]; ok {
			m.lockedItems[key] = LockInformation{
				ID:   id,
				Key:  key,
				Date: time.Now(),
			}
		} else {
			keyFound = false
		}
		select {
		case channelValue := <-lockChannel:
			if channelValue != nil {
				return
			}
		default:
		}
	}
}
