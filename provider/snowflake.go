package provider

import (
	"encoding/json"
	"errors"
	"fmt"
	sr "github.com/featureform/helpers/struct_iterator"
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

var snowflakeParameterAlias = map[string]string{
	"Warehouse": "warehouse",
	"Role":      "role",
}

type SnowflakeConfig struct {
	Username       string
	Password       string
	AccountLocator string
	Organization   string
	Account        string
	Database       string
	Schema         string
	Warehouse      string
	Role           string
}

func (sf *SnowflakeConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, sf)
	if err != nil {
		return err
	}
	return nil
}

func (sf *SnowflakeConfig) Serialize() []byte {
	conf, err := json.Marshal(sf)
	if err != nil {
		panic(err)
	}
	return conf
}

func (sf *SnowflakeConfig) ConnectionString() (string, error) {
	connString, err := sf.buildConnectionString()
	if err != nil {
		return "", fmt.Errorf("could not build connecting string: %v", err)
	}
	return connString, nil
}

func (sf *SnowflakeConfig) buildConnectionString() (string, error) {
	base, err := sf.getBaseConnection()
	if err != nil {
		return "", err
	}
	parameters, err := sf.getConnectionParameters()
	if err != nil {
		return "", fmt.Errorf("could not build parameters: %v", err)
	}
	return sf.makeFullConnection(base, parameters), nil
}

func (sf *SnowflakeConfig) makeFullConnection(base, parameters string) string {
	return fmt.Sprintf("%s%s", base, parameters)
}

const emptyParameters = "?"

func (sf *SnowflakeConfig) getConnectionParameters() (string, error) {
	base := emptyParameters

	iter, err := sr.NewStructIterator(*sf)
	if err != nil {
		return "", err
	}
	for iter.Next() {
		base = sf.addParameter(base, iter.ItemName(), iter.ItemValue())
	}

	if base == emptyParameters {
		return "", nil
	}
	return base, nil
}

func (sf *SnowflakeConfig) addParameter(base, key string, val interface{}) string {
	if val == "" {
		return base
	}
	if base != emptyParameters {
		base += "&"
	}
	if alias, ok := snowflakeParameterAlias[key]; ok {
		base += fmt.Sprintf("%s=%s", alias, val)
	}
	return base
}

func (sf *SnowflakeConfig) getBaseConnection() (string, error) {
	isLegacy := sf.hasLegacyCredentials()
	isCurrent, err := sf.hasCurrentCredentials()
	if err != nil {
		return "", fmt.Errorf("could not check credentials: %v", err)
	}
	if isLegacy && isCurrent {
		return "", fmt.Errorf("cannot use both legacy and current credentials")
	} else if isLegacy && !isCurrent {
		return fmt.Sprintf("%s:%s@%s/%s/%s", sf.Username, sf.Password, sf.AccountLocator, sf.Database, sf.schema()), nil
	} else if !isLegacy && isCurrent {
		return fmt.Sprintf("%s:%s@%s-%s/%s/%s", sf.Username, sf.Password, sf.Organization, sf.Account, sf.Database, sf.schema()), nil
	} else {
		return "", fmt.Errorf("credentials not found")
	}
}

func (sf *SnowflakeConfig) hasLegacyCredentials() bool {
	return sf.AccountLocator != ""
}

func (sf *SnowflakeConfig) hasCurrentCredentials() (bool, error) {
	if (sf.Account != "" && sf.Organization == "") || (sf.Account == "" && sf.Organization != "") {
		return false, fmt.Errorf("credentials must include both Account and Organization")
	} else {
		return sf.Account != "" && sf.Organization != "", nil
	}
}

func (sf *SnowflakeConfig) schema() string {
	if sf.Schema == "" {
		return "PUBLIC"
	}
	return sf.Schema
}

type snowflakeSQLQueries struct {
	defaultOfflineSQLQueries
}

func snowflakeOfflineStoreFactory(config SerializedConfig) (Provider, error) {
	sc := SnowflakeConfig{}
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
