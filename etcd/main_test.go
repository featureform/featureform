package main

import (
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	help "github.com/featureform/helpers"
	"github.com/featureform/provider"
	"github.com/joho/godotenv"
)

func TestGenerateSnapshotName(t *testing.T) {
	currentTimestamp, _ := time.Parse(time.RFC3339, "2020-11-12T10:05:01Z")
	expectedName := fmt.Sprintf("%s__%s.db", "featureform_etcd_snapshot", "2020-11-12 10:05:01")
	snapshot := generateSnapshotName(currentTimestamp)

	if snapshot != expectedName {
		t.Fatalf("the snapshot names do not match. Expected '%s', received '%s'", expectedName, snapshot)
	}
}

func TestGetFilestore(t *testing.T) {
	type TestCase struct {
		EnvironmentVariables   map[string]string
		ExpectedFilestoreType  provider.FileStore
		ExpectedFailure        bool
		ExpectedFailureMessage string
	}

	_ = godotenv.Load(".env")

	testCases := map[string]TestCase{
		"Not Suppport Provider": TestCase{
			map[string]string{
				"CLOUD_PROVIDER": "NOT_SUPPORT_PROVIDER",
			},
			provider.FileFileStore{
				DirPath: "",
			},
			true,
			"the cloud provider 'NOT_SUPPORT_PROVIDER' is not supported",
		},
		"Azure Filestore": TestCase{
			map[string]string{
				"CLOUD_PROVIDER":        "AZURE",
				"AZURE_STORAGE_ACCOUNT": help.GetEnv("AZURE_STORAGE_ACCOUNT", ""),
				"AZURE_STORAGE_TOKEN":   help.GetEnv("AZURE_STORAGE_TOKEN", ""),
				"AZURE_CONTAINER_NAME":  help.GetEnv("AZURE_CONTAINER_NAME", ""),
				"AZURE_STORAGE_PATH":    help.GetEnv("AZURE_STORAGE_PATH", ""),
			},
			provider.AzureFileStore{},
			false,
			"",
		},
	}

	for name, c := range testCases {
		t.Run(name, func(t *testing.T) {
			for env, value := range c.EnvironmentVariables {
				err := os.Setenv(env, value)
				if err != nil {
					t.Fatalf("could not set environment variables: %v", err)
				}
			}
			defer unsetEnvVariables(c.EnvironmentVariables)

			filestore, err := getFilestore()
			if c.ExpectedFailure == true && fmt.Sprintf("%v", err) != c.ExpectedFailureMessage {
				t.Fatalf("failed with different error messaged. Expected '%s', but got '%v'", c.ExpectedFailureMessage, err)
			}
			if c.ExpectedFailure == false && err != nil {
				t.Fatalf("failed to get filestore: %v", err)
			}

			if c.ExpectedFailure == false && reflect.TypeOf(filestore) != reflect.TypeOf(c.ExpectedFilestoreType) {
				t.Fatalf("failed to get the right filestore. Expected '%s' but got '%s'", reflect.TypeOf(c.ExpectedFilestoreType), reflect.TypeOf(filestore))
			}
		})
	}
}

func unsetEnvVariables(env map[string]string) error {
	for key := range env {
		err := os.Unsetenv(key)
		if err != nil {
			return fmt.Errorf("could not unset '%s' environment variable: %v", key, err)
		}
	}
	return nil
}
