package provider

import (
	"database/sql"
	"fmt"
	"github.com/featureform/helpers"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/joho/godotenv"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestOfflineStoreClickhouse(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	err := godotenv.Load("../.env")
	if err != nil {
		t.Logf("could not open .env file... Checking environment: %s", err)
	}

	clickHouseDb := ""
	ok := true
	if clickHouseDb, ok = os.LookupEnv("CLICKHOUSE_DB"); !ok {
		clickHouseDb = fmt.Sprintf("feature_form_%d", time.Now().UnixMilli())
	}

	username, ok := os.LookupEnv("CLICKHOUSE_USER")
	if !ok {
		t.Fatalf("missing CLICKHOUSE_USER variable")
	}
	password, ok := os.LookupEnv("CLICKHOUSE_PASSWORD")
	if !ok {
		t.Fatalf("missing CLICKHOUSE_PASSWORD variable")
	}
	host, ok := os.LookupEnv("CLICKHOUSE_HOST")
	if !ok {
		t.Fatalf("missing CLICKHOUSE_HOST variable")
	}
	portStr, ok := os.LookupEnv("CLICKHOUSE_PORT")
	if !ok {
		t.Fatalf("missing CLICKHOUSE_PORT variable")
	}
	ssl := helpers.GetEnvBool("CLICKHOUSE_SSL", false)

	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("Failed to parse port to numeric: %v", portStr)
	}

	var clickHouseConfig = pc.ClickHouseConfig{
		Host:     host,
		Port:     uint16(port),
		Username: username,
		Password: password,
		Database: clickHouseDb,
		SSL:      ssl,
	}

	if err := createClickHouseDatabase(clickHouseConfig); err != nil {
		t.Fatalf("%v", err)
	}

	store, err := GetOfflineStore(pt.ClickHouseOffline, clickHouseConfig.Serialize())
	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	test := OfflineStoreTest{
		t:     t,
		store: store,
	}
	test.Run()
	test.RunSQL()
}

func createClickHouseDatabase(c pc.ClickHouseConfig) error {
	conn, err := sql.Open("clickhouse", fmt.Sprintf("clickhouse://%s:%d?username=%s&password=%s&secure=%t", c.Host, c.Port, c.Username, c.Password, c.SSL))
	if err != nil {
		return err
	}
	if _, err := conn.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", sanitizeCH(c.Database))); err != nil {
		return err
	}
	if _, err := conn.Exec(fmt.Sprintf("CREATE DATABASE %s", sanitizeCH(c.Database))); err != nil {
		return err
	}
	return nil
}
