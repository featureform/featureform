package helpers

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/featureform/fferr"
	"github.com/jackc/pgx/v4/pgxpool"
)

func GetPostgresConfig(connectionString string) (*pgxpool.Config, error) {
	const defaultMaxConns = 100
	const defaultMinConns = 0
	const defaultMaxConnLifetime = time.Hour
	const defaultMaxConnIdleTime = time.Minute
	const defaultHealthCheckPeriod = time.Minute
	const defaultConnectTimeout = time.Second * 5

	dbConfig, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		return nil, fferr.NewConnectionError(connectionString, err)
	}

	dbConfig.MaxConns = defaultMaxConns
	dbConfig.MinConns = defaultMinConns
	dbConfig.MaxConnLifetime = defaultMaxConnLifetime
	dbConfig.MaxConnIdleTime = defaultMaxConnIdleTime
	dbConfig.HealthCheckPeriod = defaultHealthCheckPeriod
	dbConfig.ConnConfig.ConnectTimeout = defaultConnectTimeout

	return dbConfig, nil
}

func NewRDSPoolConnection() (*pgxpool.Pool, error) {
	host := GetEnv("POSTGRES_HOST", "localhost")
	port := GetEnv("POSTGRES_PORT", "5432")
	username := GetEnv("POSTGRES_USER", "postgres")
	password := GetEnv("POSTGRES_PASSWORD", "mysecretpassword")
	dbName := GetEnv("POSTGRES_DB", "postgres")
	sslMode := GetEnv("POSTGRES_SSL_MODE", "disable")

	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(username, password),
		Host:   fmt.Sprintf("%s:%s", host, port),
		Path:   dbName,
		RawQuery: (url.Values{
			"sslmode": []string{sslMode},
		}).Encode(),
	}

	poolConfig, err := GetPostgresConfig(u.String())
	if err != nil {
		return nil, err
	}

	db, err := pgxpool.ConnectConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to open connection to Postgres: %w", err))
	}

	return db, nil
}
