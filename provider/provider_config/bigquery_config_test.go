package provider_config

import (
	"encoding/json"
	"reflect"
	"testing"

	ss "github.com/featureform/helpers/string_set"
)

const EXAMPLE_GCP_CREDENTIALS = `{
	"type": "service_account",
	"project_id": "",
	"private_key_id": "",
	"private_key": "-----BEGIN PRIVATE KEY----------END PRIVATE KEY-----\n",
	"client_email": "",
	"client_id": "",
	"auth_uri": "https://accounts.google.com/o/oauth2/auth",
	"token_uri": "https://oauth2.googleapis.com/token",
	"auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
	"client_x509_cert_url": ""
}`

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
		a BigQueryConfig
		b BigQueryConfig
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
			a: BigQueryConfig{
				ProjectId:   "ff-gcp-proj-id",
				DatasetId:   "transactions-ds",
				Credentials: map[string]interface{}{},
			},
			b: BigQueryConfig{
				ProjectId:   "ff-gcp-proj-id",
				DatasetId:   "transactions-ds",
				Credentials: map[string]interface{}{},
			},
		}, ss.StringSet{}},
		{"Differing Fields", args{
			a: BigQueryConfig{
				ProjectId:   "ff-gcp-proj-id",
				DatasetId:   "transactions-ds",
				Credentials: credentialsDictA,
			},
			b: BigQueryConfig{
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
