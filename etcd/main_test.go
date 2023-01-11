package main

import (
	"fmt"
	"testing"
	"time"
)

func TestGenerateSnapshotName(t *testing.T) {
	var Now = func() time.Time { return time.Date(2020, 11, 12, 10, 5, 1, 0, time.UTC) }
	expectedName := fmt.Sprintf("%s__%s.db", "featureform_etcd_snapshot", "2020-11-12 10:05:01")
	snapshot := generateSnapshotName()

	if snapshot != expectedName {
		t.Fatalf("the snapshot names do not match. Expected '%s', received '%s'", expectedName, snapshot)
	}
}
