package provider_config

import (
	"encoding/json"
	"io/ioutil"
	"reflect"
	"testing"

	ss "github.com/featureform/helpers/string_set"
)

func TestGCSFileStoreConfigMutableFields(t *testing.T) {
	expected := ss.StringSet{
		"Credentials": true,
	}

	config := GCSFileStoreConfig{
		BucketName: "transactions-ds",
		BucketPath: "gs://transactions-ds",
		Credentials: GCPCredentials{
			ProjectId:      "ff-gcp-proj-id",
			SerializedFile: []byte{},
		},
	}
	actual := config.MutableFields()

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v but received %v", expected, actual)
	}
}

func TestGCSFileStoreDifferingFields(t *testing.T) {
	type args struct {
		a GCSFileStoreConfig
		b GCSFileStoreConfig
	}

	gcpCredsBytes, err := ioutil.ReadFile("../test_files/gcp_creds.json")
	if err != nil {
		t.Errorf("failed to read gcp_creds.json due to %v", err)
	}

	var credentialsDict map[string]interface{}
	err = json.Unmarshal(gcpCredsBytes, &credentialsDict)
	if err != nil {
		t.Errorf("failed to unmarshal GCP credentials: %v", err)
	}
	credentialsDict["client_email"] = "test@featureform.com"
	gcpCredsBytesB, err := json.Marshal(credentialsDict)
	if err != nil {
		t.Errorf("failed to marshal GCP credentials: %v", err)
	}

	tests := []struct {
		name     string
		args     args
		expected ss.StringSet
	}{
		{"No Differing Fields", args{
			a: GCSFileStoreConfig{
				BucketName: "transactions-ds",
				BucketPath: "gs://transactions-ds",
				Credentials: GCPCredentials{
					ProjectId:      "ff-gcp-proj-id",
					SerializedFile: gcpCredsBytes,
				},
			},
			b: GCSFileStoreConfig{
				BucketName: "transactions-ds",
				BucketPath: "gs://transactions-ds",
				Credentials: GCPCredentials{
					ProjectId:      "ff-gcp-proj-id",
					SerializedFile: gcpCredsBytes,
				},
			},
		}, ss.StringSet{}},
		{"Differing Fields", args{
			a: GCSFileStoreConfig{
				BucketName: "transactions-ds",
				BucketPath: "gs://transactions-ds",
				Credentials: GCPCredentials{
					ProjectId:      "ff-gcp-proj-id",
					SerializedFile: gcpCredsBytes,
				},
			},
			b: GCSFileStoreConfig{
				BucketName: "transactions-ds2",
				BucketPath: "gs://transactions-ds2",
				Credentials: GCPCredentials{
					ProjectId:      "ff-gcp-proj-id",
					SerializedFile: gcpCredsBytesB,
				},
			},
		}, ss.StringSet{
			"BucketName":  true,
			"BucketPath":  true,
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
