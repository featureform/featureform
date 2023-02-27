package provider_config

import (
	"encoding/json"
	"fmt"

	ss "github.com/featureform/helpers/string_set"
	sr "github.com/featureform/helpers/struct_iterator"
)

type SnowflakeConfig struct {
	Username       string
	Password       string
	AccountLocator string
	Organization   string
	Account        string
	Database       string
	Schema         string
	Warehouse      string `snowflake:"warehouse"`
	Role           string `snowflake:"role"`
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

func (pg SnowflakeConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Username": true,
		"Password": true,
		"Role":     true,
	}
}

func (a SnowflakeConfig) DifferingFields(b SnowflakeConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}

func (sf *SnowflakeConfig) HasLegacyCredentials() bool {
	return sf.AccountLocator != ""
}

func (sf *SnowflakeConfig) HasCurrentCredentials() (bool, error) {
	if (sf.Account != "" && sf.Organization == "") || (sf.Account == "" && sf.Organization != "") {
		return false, fmt.Errorf("credentials must include both Account and Organization")
	} else {
		return sf.Account != "" && sf.Organization != "", nil
	}
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
		if tag := iter.Tag("snowflake"); tag != "" {
			base = sf.addParameter(base, tag, iter.Value())
		}
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
	base += fmt.Sprintf("%s=%s", key, val)
	return base
}

func (sf *SnowflakeConfig) getBaseConnection() (string, error) {
	isLegacy := sf.HasLegacyCredentials()
	isCurrent, err := sf.HasCurrentCredentials()
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

func (sf *SnowflakeConfig) schema() string {
	if sf.Schema == "" {
		return "PUBLIC"
	}
	return sf.Schema
}
