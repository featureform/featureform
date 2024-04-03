package helpers

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/featureform/fferr"
	psql "github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

// Postgres
func getPostgresConfig(connectionString string) (*pgxpool.Config, error) {
	const defaultMaxConns = 10
	const defaultMinConns = 0
	const defaultMaxConnLifetime = time.Hour
	const defaultMaxConnIdleTime = time.Minute
	const defaultHealthCheckPeriod = time.Minute
	const defaultConnectTimeout = time.Minute * 5

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

func NewRDSPoolConnection(config RDSConfig) (*pgxpool.Pool, error) {
	poolConfig, err := getPostgresConfig(config.ConnectionString())
	if err != nil {
		return nil, err
	}

	db, err := pgxpool.ConnectConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to open connection to Postgres: %w", err))
	}

	return db, nil
}

type RDSConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	DBName   string
	SSLMode  string
}

func (c RDSConfig) ConnectionString() string {
	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(c.User, c.Password),
		Host:   fmt.Sprintf("%s:%s", c.Host, c.Port),
		Path:   c.DBName,
		RawQuery: (url.Values{
			"sslmode": []string{c.SSLMode},
		}).Encode(),
	}

	return u.String()
}

func SanitizePostgres(ident string) string {
	return psql.Identifier{ident}.Sanitize()
}

// ETCD
type ETCDConfig struct {
	Host string
	Port string
}

func (c ETCDConfig) URL() string {
	u := &url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", c.Host, c.Port),
	}
	return u.String()
}
