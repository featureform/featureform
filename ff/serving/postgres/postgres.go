package postgres

import (
	"database/sql"
	"fmt"
	"net/url"

	_ "github.com/lib/pq"
	"go.uber.org/zap"
)

type PostgresProvider struct {
	Logger *zap.SugaredLogger
	db     *sql.DB
}

type ConnectionParams struct {
	User, Password, DBName, Host string
	Port                         int
	Mode                         VerifyMode
}

type VerifyMode string

const (
	Disable    VerifyMode = "disable"
	Require               = "require"
	VerifyCA              = "verify-ca"
	VerifyFull            = "verify-full"
)

func (params *ConnectionParams) toURLString() string {
	if params.Port == 0 {
		params.Port = 5432
	}
	if params.Mode == "" {
		params.Mode = Disable
	}
	hostStr := fmt.Sprintf("%s:%d", params.Host, params.Port)
	qry := url.Values{}
	qry.Set("sslmode", string(params.Mode))
	uri := &url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(params.User, params.Password),
		Host:     hostStr,
		Path:     params.DBName,
		RawQuery: qry.Encode(),
	}
	return uri.String()
}
