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
	CleanupInterval = 10 * time.Second
)

type MemoryStorageProvider struct {
	storage     map[string]string
	lockedItems map[string]LockInformation
	Channel     chan int
}

func NewMemoryStorageProvider() *MemoryStorageProvider {
	// Start a goroutine to cleanup locked items every minute
	// and send the number of deleted items to the channel
	m := &MemoryStorageProvider{storage: make(map[string]string), lockedItems: make(map[string]LockInformation), Channel: make(chan int)}
	go func(channel chan int) {
		for {
			time.Sleep(CleanupInterval)
			deleteCount := m.Cleanup()
			channel <- deleteCount
		}
	}(m.Channel)

	return m
}

func (m *MemoryStorageProvider) Set(key string, value string, lock LockObject) error {
	lockInfo, ok := m.lockedItems[key]
	if !ok {
		return fmt.Errorf("key is not locked")
	} else if lockInfo.ID != lock.ID {
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
	Channel *chan error
}

func (m *MemoryStorageProvider) Lock(id string, key string) (LockObject, error) {
	if key == "" {
		return LockObject{}, fmt.Errorf("key is empty")
	}

	lockChannel := make(chan error)
	keyLockInfo, ok := m.lockedItems[key]
	if ok && keyLockInfo.ID == id && time.Since(keyLockInfo.Date) < ValidTimePeriod {
		return LockObject{ID: id, Channel: &lockChannel}, nil
	} else if ok {
		return LockObject{}, fmt.Errorf("key is already locked by %s", keyLockInfo.ID)
	}
	m.lockedItems[key] = LockInformation{
		ID:   id,
		Key:  key,
		Date: time.Now(),
	}

	lockObj := LockObject{ID: id, Channel: &lockChannel}
	go m.updateLockTime(id, key, *lockObj.Channel)
	*lockObj.Channel <- nil

	return lockObj, nil
}

func (m *MemoryStorageProvider) Unlock(key string, lock LockObject) error {
	keyLockInfo, ok := m.lockedItems[key]
	if !ok {
		return fmt.Errorf("key is not locked")
	}
	if keyLockInfo.ID != lock.ID {
		return fmt.Errorf("key is locked by another id")
	}
	delete(m.lockedItems, key)
	return nil
}

func (m *MemoryStorageProvider) Cleanup() int {
	deleteCount := 0
	for key, lockInfo := range m.lockedItems {
		if time.Since(lockInfo.Date) > ValidTimePeriod {
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
		fmt.Println("Updating lock time", time.Now().String())
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
			fmt.Println("Received none from channel")
		default:
		}
	}
}
