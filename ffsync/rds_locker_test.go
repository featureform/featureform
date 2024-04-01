package ffsync

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestRDSLocker(t *testing.T) {
	locker, err := NewRDSLocker()
	if err != nil {
		t.Fatalf("Failed to create RDS locker: %v", err)
	}

	test := LockerTest{
		t:      t,
		locker: locker,
	}
	test.Run()

	// clean up
	defer func() {
		rLocker := locker.(*rdsLocker)

		_, err := rLocker.db.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", rLocker.tableName))
		if err != nil {
			t.Fatalf("Failed to drop table: %v", err)
		}

		// Close the connection
		rLocker.Close()
	}()
	time.Sleep(1 * time.Second)
}
