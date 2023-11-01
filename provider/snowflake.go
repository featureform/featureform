package provider

import (
	"fmt"

	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	_ "github.com/snowflakedb/gosnowflake"
)

// sqlColumnType is used to specify the column type of a resource value.
type snowflakeColumnType string

const (
	sfInt       snowflakeColumnType = "integer"
	sfNumber    snowflakeColumnType = "NUMBER"
	sfFloat     snowflakeColumnType = "FLOAT"
	sfString    snowflakeColumnType = "varchar"
	sfBool      snowflakeColumnType = "BOOLEAN"
	sfTimestamp snowflakeColumnType = "TIMESTAMP_NTZ"
)

type snowflakeSQLQueries struct {
	defaultOfflineSQLQueries
}

func snowflakeOfflineStoreFactory(config pc.SerializedConfig) (Provider, error) {
	sc := pc.SnowflakeConfig{}
	if err := sc.Deserialize(config); err != nil {
		return nil, NewProviderError(Runtime, pt.SnowflakeOffline, ConfigDeserialize, err.Error())
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
		ProviderType:  pt.SnowflakeOffline,
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
