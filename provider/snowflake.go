package provider

import (
	"errors"
	"fmt"

	pc "github.com/featureform/provider/provider_config"
	_ "github.com/snowflakedb/gosnowflake"
)

// sqlColumnType is used to specify the column type of a resource value.
type snowflakeColumnType string

const (
	sfInt       snowflakeColumnType = "integer"
	sfNumber                        = "NUMBER"
	sfFloat                         = "FLOAT"
	sfString                        = "varchar"
	sfBool                          = "BOOLEAN"
	sfTimestamp                     = "TIMESTAMP_NTZ"
)

type snowflakeSQLQueries struct {
	defaultOfflineSQLQueries
}

func snowflakeOfflineStoreFactory(config pc.SerializedConfig) (Provider, error) {
	sc := pc.SnowflakeConfig{}
	if err := sc.Deserialize(config); err != nil {
		return nil, errors.New("invalid snowflake config")
	}
	queries := snowflakeSQLQueries{}
	queries.setVariableBinding(MySQLBindingStyle)
	connectionString, err := sc.ConnectionString()
	if err != nil {
		return nil, fmt.Errorf("could not get snowflake connection string: %v", err)
	}
	sgConfig := SQLOfflineStoreConfig{
		Config:        config,
		ConnectionURL: connectionString,
		Driver:        "snowflake",
		ProviderType:  SnowflakeOffline,
		QueryImpl:     &queries,
	}

	store, err := NewSQLOfflineStore(sgConfig)
	if err != nil {
		return nil, err
	}
	return store, nil
}

func (q snowflakeSQLQueries) materializationDrop(tableName string) string {
	return fmt.Sprintf("DROP TABLE %s", sanitize(tableName))
}
