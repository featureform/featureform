package provider_config

import (
	"encoding/json"
	"io/ioutil"
	"reflect"
	"testing"

	ss "github.com/featureform/helpers/string_set"
)

func TestBigQueryConfigMutableFields(t *testing.T) {
	expected := ss.StringSet{
		"Credentials": true,
	}

	config := BigQueryConfig{
		ProjectId:   "ff-gcp-proj-id",
		DatasetId:   "transactions-ds",
		Credentials: map[string]interface{}{},
	}
	actual := config.MutableFields()

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v but received %v", expected, actual)
	}
}

func TestBigQueryConfigDifferingFields(t *testing.T) {
	type args struct {
		a *BigQueryConfig
		b *BigQueryConfig
	}

	gcpCredsBytes, err := ioutil.ReadFile("../test_files/gcp_creds.json")
	if err != nil {
		t.Errorf("failed to read gcp_creds.json due to %v", err)
	}

	var credentialsDictA map[string]interface{}
	err = json.Unmarshal(gcpCredsBytes, &credentialsDictA)
	if err != nil {
		t.Errorf("failed to unmarshal GCP credentials: %v", err)
	}
	var credentialsDictB map[string]interface{}
	err = json.Unmarshal([]byte(gcpCredsBytes), &credentialsDictB)
	if err != nil {
		t.Errorf("failed to unmarshal GCP credentials: %v", err)
	}
	credentialsDictB["client_email"] = "test@featureform.com"

	tests := []struct {
		name     string
		args     args
		expected ss.StringSet
	}{
		{"No Differing Fields", args{
			a: &BigQueryConfig{
				ProjectId:   "ff-gcp-proj-id",
				DatasetId:   "transactions-ds",
				Credentials: map[string]interface{}{},
			},
			b: &BigQueryConfig{
				ProjectId:   "ff-gcp-proj-id",
				DatasetId:   "transactions-ds",
				Credentials: map[string]interface{}{},
			},
		}, ss.StringSet{}},
		{"Differing Fields", args{
			a: &BigQueryConfig{
				ProjectId:   "ff-gcp-proj-id",
				DatasetId:   "transactions-ds",
				Credentials: credentialsDictA,
			},
			b: &BigQueryConfig{
				ProjectId:   "ff-gcp-proj-v2-id",
				DatasetId:   "transactions-ds",
				Credentials: credentialsDictB,
			},
		}, ss.StringSet{
			"ProjectId":   true,
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
