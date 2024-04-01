package ffsync

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/featureform/helpers"
)

func TestRDSLocker(t *testing.T) {
	host := helpers.GetEnv("POSTGRES_HOST", "localhost")
	port := helpers.GetEnv("POSTGRES_PORT", "5432")
	username := helpers.GetEnv("POSTGRES_USER", "postgres")
	password := helpers.GetEnv("POSTGRES_PASSWORD", "mysecretpassword")
	dbName := helpers.GetEnv("POSTGRES_DB", "postgres")
	sslMode := helpers.GetEnv("POSTGRES_SSL_MODE", "disable")

	config := helpers.RDSConfig{
		Host:     host,
		Port:     port,
		User:     username,
		Password: password,
		DBName:   dbName,
		SSLMode:  sslMode,
	}

	locker, err := NewRDSLocker(config)
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
