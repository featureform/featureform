// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package ffsync

import (
	"testing"
	"time"

	"github.com/featureform/helpers"
	"github.com/featureform/helpers/etcd"

	"github.com/jonboulle/clockwork"
)

func TestETCDLocker(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	var host, port string
	if *useEnv {
		host = helpers.GetEnv("ETCD_HOST", "localhost")
		port = helpers.GetEnv("ETCD_PORT", "2379")
	} else {
		host = "127.0.0.1"
		port = etcdPort
	}

	etcdConfig := etcd.Config{
		Host:        host,
		Port:        port,
		DialTimeout: time.Second * 5,
	}

	locker, err := NewETCDLocker(etcdConfig)
	if err != nil {
		t.Fatalf("Failed to create ETCD locker: %v", err)
	}

	clock := clockwork.NewFakeClock()
	locker.(*etcdLocker).clock = clock

	test := LockerTest{
		t:          t,
		locker:     locker,
		lockerType: "etcd",
	}
	test.Run(clock)
}
