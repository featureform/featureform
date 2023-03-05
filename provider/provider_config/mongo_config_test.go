package provider_config

import (
	"reflect"
	"testing"

	ss "github.com/featureform/helpers/string_set"
)

func TestMongoConfigMutableFields(t *testing.T) {
	expected := ss.StringSet{
		"Username":   true,
		"Password":   true,
		"Port":       true,
		"Throughput": true,
	}

	config := MongoDBConfig{
		Host:       "0.0.0.0",
		Port:       "27017",
		Username:   "root",
		Password:   "password",
		Database:   "mongo",
		Throughput: 1000,
	}
	actual := config.MutableFields()

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v but received %v", expected, actual)
	}
}

func TestMongoConfigDifferingFields(t *testing.T) {
	type args struct {
		a MongoDBConfig
		b MongoDBConfig
	}

	tests := []struct {
		name     string
		args     args
		expected ss.StringSet
	}{
		{"No Differing Fields", args{
			a: MongoDBConfig{
				Host:       "0.0.0.0",
				Port:       "27017",
				Username:   "root",
				Password:   "password",
				Database:   "mongo",
				Throughput: 1000,
			},
			b: MongoDBConfig{
				Host:       "0.0.0.0",
				Port:       "27017",
				Username:   "root",
				Password:   "password",
				Database:   "mongo",
				Throughput: 1000,
			},
		}, ss.StringSet{}},
		{"Differing Fields", args{
			a: MongoDBConfig{
				Host:       "0.0.0.0",
				Port:       "27017",
				Username:   "root",
				Password:   "password",
				Database:   "mongo",
				Throughput: 1000,
			},
			b: MongoDBConfig{
				Host:       "0.0.0.0",
				Port:       "27017",
				Username:   "featureformer",
				Password:   "pass123word",
				Database:   "mongo_2",
				Throughput: 1000,
			},
		}, ss.StringSet{
			"Username": true,
			"Password": true,
			"Database": true,
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
