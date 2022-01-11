package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/featureform/serving/dataset"
	"github.com/lib/pq"
	"go.uber.org/zap"
)

type Provider struct {
	Logger *zap.SugaredLogger
	DB     *sql.DB
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

func (params *ConnectionParams) urlString() string {
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

func NewProvider(params ConnectionParams, logger *zap.SugaredLogger) (*Provider, error) {
	connLogger := logger.With("Connection", params.Host)
	connLogger.Info("Opening SQL connection")
	db, err := sql.Open("postgres", params.urlString())
	if err != nil {
		connLogger.Error("Failed to open SQL connection")
		return nil, err
	}
	return NewProviderWithDB(db, logger)
}

func NewProviderWithDB(db *sql.DB, logger *zap.SugaredLogger) (*Provider, error) {
	return &Provider{
		Logger: logger,
		DB:     db,
	}, nil
}

func (provider *Provider) ToKey(name string, schema TableSchema) map[string]string {
	logger := provider.Logger
	key := make(map[string]string)
	key["tablename"] = name
	schemaJson, err := json.Marshal(schema)
	if err != nil {
		logger.Panicw("Failed to marshal SQL schema", "Error", err)
	}
	key["schema"] = string(schemaJson)
	logger.Debugw("Generated SQL key", "Key", key)
	return key
}

type TableSchema struct {
	Label    string
	Features []string
}

func (provider *Provider) GetDatasetReader(key map[string]string) (dataset.Reader, error) {
	logger := provider.Logger.With("Key", key)
	bldr := strings.Builder{}
	var schema TableSchema
	err := json.Unmarshal([]byte(key["schema"]), &schema)
	if err != nil {
		logger.Errorw("Failed to unmarshal schema", "Error", err)
	}
	bldr.WriteString("SELECT ")
	for _, feature := range schema.Features {
		bldr.WriteString(pq.QuoteIdentifier(feature))
		bldr.WriteString(",")
	}
	bldr.WriteString(schema.Label)
	bldr.WriteString(" FROM ")
	bldr.WriteString(pq.QuoteIdentifier(key["tablename"]))
	qry := bldr.String()
	logger = logger.With("Query", qry)
	logger.Info("Running query")
	rows, err := provider.DB.QueryContext(context.Background(), qry)
	if err != nil {
		logger.Errorw("Failed to run query", "Error", err)
		return nil, err
	}
	return &Dataset{
		rows:   rows,
		schema: schema}, nil
}

type Dataset struct {
	rows   *sql.Rows
	schema TableSchema
	row    *dataset.Row
	err    error
}

func (ds *Dataset) Scan() bool {
	if !ds.rows.Next() {
		ds.err = ds.rows.Err()
		return false
	}
	numFeatures := len(ds.schema.Features)
	numLabels := 1
	numValues := numFeatures + numLabels
	vals := make([]interface{}, numValues)
	indirect := make([]interface{}, numValues)
	for i := range vals {
		indirect[i] = &vals[i]
	}
	if err := ds.rows.Scan(indirect...); err != nil {
		ds.err = err
		return false
	}
	row := dataset.NewRow()
	for i := range ds.schema.Features {
		if err := row.AddFeature(vals[i]); err != nil {
			ds.err = err
			return false
		}
	}
	lastIdx := len(ds.schema.Features)
	if err := row.SetLabel(vals[lastIdx]); err != nil {
		ds.err = err
		return false
	}
	ds.row = row
	return true
}

func (ds *Dataset) Row() *dataset.Row {
	return ds.row
}

func (ds *Dataset) Err() error {
	return ds.err
}
