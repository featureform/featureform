package provider_config

import (
	"encoding/json"
	"fmt"

	ss "github.com/featureform/helpers/string_set"
)

type CassandraConfig struct {
	Keyspace    string
	Addr        string
	Username    string
	Password    string
	Consistency string
	Replication int
}

func (cass CassandraConfig) Serialize() ([]byte, error) {
	config, err := json.Marshal(cass)
	if err != nil {
		return nil, err
	}
	return config, nil
}

func (cass *CassandraConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, cass)
	if err != nil {
		return err
	}
	return nil
}

func (cass *CassandraConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Username":    true,
		"Password":    true,
		"Consistency": true,
		"Replication": true,
	}
}

func (a *CassandraConfig) DifferingFields(b ProviderConfig) (ss.StringSet, error) {
	if _, ok := b.(*CassandraConfig); !ok {
		return nil, fmt.Errorf("cannot compare different config types")
	}
	return differingFields(a, b)
}
