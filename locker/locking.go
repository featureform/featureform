package locker

import (
	"encoding/json"
	"fmt"
	"time"
)

const (
	UpdateSleepTime = 2 * time.Second
	ValidTimePeriod = 5 * time.Second
)

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

type MultiLock interface {
	Lock(lock string) (Key, error)
	Unlock(key Key) error
}

type Key interface {
	ID() string
	Key() string
}
