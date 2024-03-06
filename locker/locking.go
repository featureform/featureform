package locker

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/featureform/fferr"
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

func (l *LockInformation) Unmarshal(data []byte) fferr.GRPCError {
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
		return fferr.NewInternalError(err)
	}
	l.Date = parsedTime.UTC()

	return nil
}

func (l *LockInformation) Marshal() ([]byte, fferr.GRPCError) {
	bytes, err := json.Marshal(l)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	return bytes, nil
}

type MultiLock interface {
	Lock(lock string) (Key, fferr.GRPCError)
	Unlock(key Key) fferr.GRPCError
}

type Key interface {
	ID() string
	Key() string
}
