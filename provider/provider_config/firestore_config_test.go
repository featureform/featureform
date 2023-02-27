package provider_config

import (
	"encoding/json"
	"reflect"
	"testing"

	ss "github.com/featureform/helpers/string_set"
)

func TestFirestoreConfigMutableFields(t *testing.T) {
	expected := ss.StringSet{
		"Credentials": true,
	}

	config := FirestoreConfig{
		ProjectID:   "ff-gcp-proj-id",
		Collection:  "transactions-ds",
		Credentials: map[string]interface{}{},
	}
	actual := config.MutableFields()

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v but received %v", expected, actual)
	}
}

func TestFirestoreConfigDifferingFields(t *testing.T) {
	type args struct {
		a FirestoreConfig
		b FirestoreConfig
	}

	var credentialsDictA map[string]interface{}
	err := json.Unmarshal([]byte(EXAMPLE_GCP_CREDENTIALS), &credentialsDictA)
	if err != nil {
		t.Errorf("failed to unmarshal big query credentials: %v", err)
	}
	var credentialsDictB map[string]interface{}
	err = json.Unmarshal([]byte(EXAMPLE_GCP_CREDENTIALS), &credentialsDictB)
	if err != nil {
		t.Errorf("failed to unmarshal big query credentials: %v", err)
	}
	credentialsDictB["client_email"] = "test@featureform.com"

	tests := []struct {
		name     string
		args     args
		expected ss.StringSet
	}{
		{"No Differing Fields", args{
			a: FirestoreConfig{
				ProjectID:   "ff-gcp-proj-id",
				Collection:  "transactions-ds",
				Credentials: map[string]interface{}{},
			},
			b: FirestoreConfig{
				ProjectID:   "ff-gcp-proj-id",
				Collection:  "transactions-ds",
				Credentials: map[string]interface{}{},
			},
		}, ss.StringSet{}},
		{"Differing Fields", args{
			a: FirestoreConfig{
				ProjectID:   "ff-gcp-proj-id",
				Collection:  "transactions-ds",
				Credentials: credentialsDictA,
			},
			b: FirestoreConfig{
				ProjectID:   "ff-gcp-proj-v2-id",
				Collection:  "transactions-ds",
				Credentials: credentialsDictB,
			},
		}, ss.StringSet{
			"ProjectID":   true,
			"Credentials": true,
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
