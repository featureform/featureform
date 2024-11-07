// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package ffsync

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/featureform/fferr"
)

// lockDuration is a struct that represents a duration that will be
// used to sleep as well as valid time period.
type lockDuration struct {
	duration time.Duration
}

func (t lockDuration) Duration() time.Duration {
	return t.duration
}

func (t lockDuration) AsPSQLString() string {
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
	UpdateSleepTime = lockDuration{2 * time.Second}
	ValidTimePeriod = lockDuration{1 * time.Minute}

	// MaxWaitTime is the limit for how long a thread should block to wait for a key to be free.
	// For long-running locks, waiting should not be used and should periodically check the lock status
	// at a larger interval
	//MaxWaitTime = 5 * time.Minute
	MaxWaitTime = 5 * time.Minute
)

func hasExceededWaitTime(start time.Time) bool {
	if time.Now().Sub(start) > MaxWaitTime {
		return true
	}
	return false
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
	Lock(ctx context.Context, lock string, wait bool) (Key, error)
	Unlock(ctx context.Context, key Key) error
	Close()
}

type Key interface {
	ID() string
	Key() string
}
