package provider_config

import (
	"reflect"
	"testing"

	ss "github.com/featureform/helpers/string_set"
)

func TestCassandraConfigMutableFields(t *testing.T) {
	expected := ss.StringSet{
		"Username":    true,
		"Password":    true,
		"Consistency": true,
		"Replication": true,
	}

	config := CassandraConfig{
		Addr:        "0.0.0.0:9042",
		Username:    "cassandra",
		Password:    "password",
		Keyspace:    "ff_ks",
		Consistency: "THREE",
		Replication: 3,
	}
	actual := config.MutableFields()

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v but received %v", expected, actual)
	}
}

func TestCassandraConfigDifferingFields(t *testing.T) {
	type args struct {
		a CassandraConfig
		b CassandraConfig
	}

	tests := []struct {
		name     string
		args     args
		expected ss.StringSet
	}{
		{"No Differing Fields", args{
			a: CassandraConfig{
				Addr:        "0.0.0.0:9042",
				Username:    "cassandra",
				Password:    "password",
				Keyspace:    "ff_ks",
				Consistency: "THREE",
				Replication: 3,
			},
			b: CassandraConfig{
				Addr:        "0.0.0.0:9042",
				Username:    "cassandra",
				Password:    "password",
				Keyspace:    "ff_ks",
				Consistency: "THREE",
				Replication: 3,
			},
		}, ss.StringSet{}},
		{"Differing Fields", args{
			a: CassandraConfig{
				Addr:        "0.0.0.0:9042",
				Username:    "cassandra",
				Password:    "password",
				Keyspace:    "ff_ks",
				Consistency: "THREE",
				Replication: 3,
			},
			b: CassandraConfig{
				Addr:        "0.0.0.0:9042",
				Username:    "cass2",
				Password:    "password",
				Keyspace:    "ff_ks_v2",
				Consistency: "FOUR",
				Replication: 4,
			},
		}, ss.StringSet{
			"Username":    true,
			"Keyspace":    true,
			"Consistency": true,
			"Replication": true,
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := tt.args.a.DifferingFields(tt.args.b)

			if err != nil {
				t.Errorf("Failed to get differing fields due to error: %v", err)
			}

			if !reflect.DeepEqual(actual, tt.expected) {
				t.Errorf("Expected %v, but instead found %v", tt.expected, actual)
			}

		})
	}

}
