package provider

import (
	"encoding/json"
	"errors"
	"fmt"
	_ "github.com/snowflakedb/gosnowflake"
	"reflect"
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

var snowflakeParameters = map[string]string{
	"Region":    "region",
	"Warehouse": "warehouse",
	"Role":      "role",
}

type snowflakeConnectionString struct {
	Username       string
	Password       string
	AccountLocator string
	Organization   string
	Account        string
	Database       string
	Schema         string
	connection     string
	baseConnection string
}

func (sc *snowflakeConnectionString) String() string {
	return sc.connection
}

func (sc *snowflakeConnectionString) UseConfig(config *SnowflakeConfig) error {
	sc.Username = config.Username
	sc.Password = config.Password
	sc.AccountLocator = config.AccountLocator
	sc.Organization = config.Organization
	sc.Account = config.Account
	sc.Database = config.Database
	sc.Schema = config.Schema
	if err := sc.setBaseConnection(); err != nil {
		return err
	}
	sc.connection = fmt.Sprintf("%s%s", sc.baseConnection, sc.getConnectionArgs(config))
	return nil
}

func (sc *snowflakeConnectionString) getConnectionArgs(config *SnowflakeConfig) string {
	base := "?"
	v := reflect.ValueOf(config)
	typeOfS := v.Type()

	for i := 0; i < v.NumField(); i++ {
		if base != "?" {
			base += ","
		}
		if val, ok := snowflakeParameters[typeOfS.Field(i).Name]; ok && v.Field(i).Interface() != "" {
			base += fmt.Sprintf("%s=%s", val, v.Field(i).Interface())
		}
	}
	if base == "?" {
		return ""
	}
	return base
}

func (sc *snowflakeConnectionString) setBaseConnection() error {
	isLegacy := sc.hasLegacyCredentials()
	isCurrent, err := sc.hasCurrentCredentials()
	if err != nil {
		return fmt.Errorf("could not check credentials: %v", err)
	}
	if isLegacy && isCurrent {
		return fmt.Errorf("cannot use both legacy and current credentials")
	} else if isLegacy && !isCurrent {
		sc.baseConnection = fmt.Sprintf("%s:%s@%s/%s/%s", sc.Username, sc.Password, sc.AccountLocator, sc.Database, sc.schema())
	} else if !isLegacy && isCurrent {
		sc.baseConnection = fmt.Sprintf("%s:%s@%s-%s/%s/%s", sc.Username, sc.Password, sc.Organization, sc.Account, sc.Database, sc.schema())
	} else {
		return fmt.Errorf("credentials not found")
	}
	return nil
}

func (sc *snowflakeConnectionString) hasLegacyCredentials() bool {
	return sc.AccountLocator != ""
}

func (sc *snowflakeConnectionString) hasCurrentCredentials() (bool, error) {
	if (sc.Account != "" && sc.Organization == "") || (sc.Account == "" && sc.Organization != "") {
		return false, fmt.Errorf("credentials must include both Account and Organization")
	} else {
		return sc.Account != "" && sc.Organization != "", nil
	}
}

func (sc *snowflakeConnectionString) schema() string {
	if sc.Schema == "" {
		return "PUBLIC"
	}
	return sc.Schema
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
	connectionString := snowflakeConnectionString{}
	err := connectionString.UseConfig(sf)
	if err != nil {
		return "", err
	}
	return connectionString.String(), nil
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
