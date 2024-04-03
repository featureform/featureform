package ffsync

import (
	"testing"

	"github.com/featureform/helpers"
)

func TestETCDLocker(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	host := helpers.GetEnv("ETCD_HOST", "localhost")
	port := helpers.GetEnv("ETCD_PORT", "2379")

	etcdConfig := helpers.ETCDConfig{
		Host: host,
		Port: port,
	}

	locker, err := NewETCDLocker(etcdConfig)
	if err != nil {
		t.Fatalf("Failed to create ETCD locker: %v", err)
	}

	test := LockerTest{
		t:          t,
		locker:     locker,
		lockerType: "etcd",
	}
	test.Run()
}
