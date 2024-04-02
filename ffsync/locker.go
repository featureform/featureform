package ffsync

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/featureform/fferr"
)

// TimeWindow is a struct that represents a duration that will be
// used to sleep as well as valid time period.
type TimeWindow struct {
	duration time.Duration
}

func (t TimeWindow) Duration() time.Duration {
	return t.duration
}

func (t TimeWindow) AsRDSString() string {
	totalSeconds := int(t.duration.Seconds())
	if totalSeconds < 60 {
		return fmt.Sprintf("%d seconds", totalSeconds)
	} else if totalSeconds < 3600 {
		return fmt.Sprintf("%d minutes", totalSeconds/60)
	} else {
		return fmt.Sprintf("%d hours", totalSeconds/3600)
	}
}

var (
	// UpdateSleepTime is used to sleep between each update.
	// Best to keep it less than half of ValidTimePeriod.
	UpdateSleepTime = TimeWindow{2 * time.Minute}
	ValidTimePeriod = TimeWindow{5 * time.Minute}
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
		return fferr.NewInternalError(err)
	}

	if tmp.ID == "" {
		err := fmt.Errorf("lock information is missing ID")
		return fferr.NewInvalidArgumentError(err)
	}
	if tmp.Key == "" {
		err := fmt.Errorf("lock information is missing Key")
		return fferr.NewInvalidArgumentError(err)
	}

	l.ID = tmp.ID
	l.Key = tmp.Key

	// Parse the date string with UTC time zone
	parsedTime, err := time.Parse(time.RFC3339, tmp.Date)
	if err != nil {
		parsingErr := fmt.Errorf("failed to parse date, '%s': %v", tmp.Date, err)
		return fferr.NewParsingError(parsingErr)
	}
	l.Date = parsedTime.UTC()

	return nil
}

func (l *LockInformation) Marshal() ([]byte, error) {
	bytes, err := json.Marshal(l)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	return bytes, nil
}

/*
Locker interface is used to lock and unlock keys within different storage systems.
example: etcd, memory, etc.
*/
type Locker interface {
	Lock(lock string) (Key, error)
	Unlock(key Key) error
	Close()
}

type Key interface {
	ID() string
	Key() string
}
