//go:build filestore
// +build filestore

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"os"
	"testing"
)

func TestFileStore(t *testing.T) {
	err := godotenv.Load("../.env")
	if err != nil {
		fmt.Println(err)
	}

	fileStores := getFileStores(t)
	if err != nil {
		t.Fatalf("could not get filestores: %s", err)
	}

	testFns := map[string]func(*testing.T, FileStore, string){
		"TestPathPrefix": testPathPrefix,
	}

	t.Run("FILESTORE_FUNCTIONS", func(t *testing.T) {
		for testName, testFn := range testFns {
			for storeName, store := range fileStores {
				t.Run(fmt.Sprintf("%s_%s", testName, storeName), func(t *testing.T) {
					testFn(t, store, storeName)
				})
			}
		}
	})
}

func getFileStores(t *testing.T) map[string]FileStore {
	err := godotenv.Load("../.env")
	if err != nil {
		t.Fatalf("could not load env: %s", err)
	}

	localFileStore := getLocalFileStore(t)
	s3FileStore := getS3FileStore(t, false)
	azureFileStore := getAzureFileStore(t, false)
	gcsFileStore := getGCSFileStore(t, false)

	fileStoresMap := map[string]FileStore{
		"local": localFileStore,
		"s3":    s3FileStore,
		"azure": azureFileStore,
		"gcs":   gcsFileStore,
	}

	return fileStoresMap
}

func testPathPrefix(t *testing.T, store FileStore, storeName string) {
	type TestCase struct {
		Path         string
		Remote       bool
		ExpectedPath string
	}

	s3BucketName := os.Getenv("S3_BUCKET_PATH")
	azureAccountName := os.Getenv("AZURE_ACCOUNT_NAME")
	azureContainerName := os.Getenv("AZURE_CONTAINER_NAME")
	gcsBucketName := os.Getenv("GCS_BUCKET_NAME")

	fileStoresTests := map[string]map[string]TestCase{
		"local": map[string]TestCase{
			"path_with_slash_not_remote": TestCase{
				Path:         "/path_to_file/file.csv",
				Remote:       false,
				ExpectedPath: "tmp/path_to_file/file.csv",
			},
			"path_without_slash_not_remote": TestCase{
				Path:         "path_to_file/file.csv",
				Remote:       false,
				ExpectedPath: "tmp/path_to_file/file.csv",
			},
			"path_with_slash_remote": TestCase{
				Path:         "/path_to_file/file.csv",
				Remote:       true,
				ExpectedPath: "tmp/path_to_file/file.csv",
			},
			"path_without_slash_remote": TestCase{
				Path:         "path_to_file/file.csv",
				Remote:       true,
				ExpectedPath: "tmp/path_to_file/file.csv",
			},
		},
		"s3": map[string]TestCase{
			"path_with_slash_not_remote": TestCase{
				Path:         "/path_to_file/file.csv",
				Remote:       false,
				ExpectedPath: "featureform-unit-test/path_to_file/file.csv",
			},
			"path_without_slash_not_remote": TestCase{
				Path:         "path_to_file/file.csv",
				Remote:       false,
				ExpectedPath: "featureform-unit-test/path_to_file/file.csv",
			},
			"path_with_slash_remote": TestCase{
				Path:         "/path_to_file/file.csv",
				Remote:       true,
				ExpectedPath: fmt.Sprintf("s3://%s/featureform-unit-test/path_to_file/file.csv", s3BucketName),
			},
			"path_without_slash_remote": TestCase{
				Path:         "path_to_file/file.csv",
				Remote:       true,
				ExpectedPath: fmt.Sprintf("s3://%s/featureform-unit-test/path_to_file/file.csv", s3BucketName),
			},
		},
		"azure": map[string]TestCase{
			"path_with_slash_not_remote": TestCase{
				Path:         "/path_to_file/file.csv",
				Remote:       false,
				ExpectedPath: "featureform-unit-test/path_to_file/file.csv",
			},
			"path_without_slash_not_remote": TestCase{
				Path:         "path_to_file/file.csv",
				Remote:       false,
				ExpectedPath: "featureform-unit-test/path_to_file/file.csv",
			},
			"path_with_slash_remote": TestCase{
				Path:         "/path_to_file/file.csv",
				Remote:       true,
				ExpectedPath: fmt.Sprintf("abfss://%s@%s.dfs.core.windows.net/featureform-unit-test/path_to_file/file.csv", azureContainerName, azureAccountName),
			},
			"path_without_slash_remote": TestCase{
				Path:         "path_to_file/file.csv",
				Remote:       true,
				ExpectedPath: fmt.Sprintf("abfss://%s@%s.dfs.core.windows.net/featureform-unit-test/path_to_file/file.csv", azureContainerName, azureAccountName),
			},
		},
		"gcs": map[string]TestCase{
			"path_with_slash_not_remote": TestCase{
				Path:         "/path_to_file/file.csv",
				Remote:       false,
				ExpectedPath: "featureform-unit-test/path_to_file/file.csv",
			},
			"path_without_slash_not_remote": TestCase{
				Path:         "path_to_file/file.csv",
				Remote:       false,
				ExpectedPath: "featureform-unit-test/path_to_file/file.csv",
			},
			"path_with_slash_remote": TestCase{
				Path:         "/path_to_file/file.csv",
				Remote:       true,
				ExpectedPath: fmt.Sprintf("gs://%s/featureform-unit-test/path_to_file/file.csv", gcsBucketName),
			},
			"path_without_slash_remote": TestCase{
				Path:         "path_to_file/file.csv",
				Remote:       true,
				ExpectedPath: fmt.Sprintf("gs://%s/featureform-unit-test/path_to_file/file.csv", gcsBucketName),
			},
		},
	}

	runTestCase := func(t *testing.T, tc TestCase) {
		path := store.PathWithPrefix(tc.Path, tc.Remote)

		if path != tc.ExpectedPath {
			t.Fatalf("received '%s' but expected '%s'", path, tc.ExpectedPath)
		}
	}

	storeTests, found := fileStoresTests[storeName]
	if !found {
		t.Fatalf("could not find tests for '%s' store.", storeName)
	}

	for name, tc := range storeTests {
		t.Run(name, func(t *testing.T) {
			runTestCase(t, tc)
		})
	}
}

func TestDoesSourceUrlExist(t *testing.T) {
	err := godotenv.Load("../.env")
	if err != nil {
		t.Fatalf("could not load env: %s", err)
	}

	tests := []struct {
		name  string
		store FileStore
	}{
		{"Azure", getAzureFileStore(t, false)},
		{"S3", getS3FileStore(t, false)},
		{"GCP", getGCSFileStore(t, false)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			fileNameToCreate := fmt.Sprintf("%s.parquet", uuid.New().String())
			sourcePathForFile := tt.store.PathWithPrefix(fileNameToCreate, false)
			if err := tt.store.Write(sourcePathForFile, []byte("test")); err != nil {
				t.Fatalf("could not write to store: %s", err)
			}
			existingSourceUrl := tt.store.PathWithPrefix(sourcePathForFile, true)
			if err != nil {
				t.Fatalf("could not get existing source url: %s", err)
			}
			sourceUrlExists, err := tt.store.SourceUrlExists(existingSourceUrl)
			if err != nil {
				t.Errorf("SourceUrlExists() returned error: %v", err)
				return
			}
			if !sourceUrlExists {
				t.Errorf("SourceUrlExists() returned false for existing file")
				return
			}

			nonExistentFileName := fmt.Sprintf("%s.parquet", uuid.New().String())
			sourcePathForFile = tt.store.PathWithPrefix(nonExistentFileName, false)
			sourceUrlForNonExistentFile := tt.store.PathWithPrefix(nonExistentFileName, true)

			sourceUrlExists, err = tt.store.ExistsByUrl(tt.store, sourceUrlForNonExistentFile)
			if err != nil {
				t.Errorf("ExistsByUrl() returned error: %v", err)
				return
			}
			if sourceUrlExists {
				t.Errorf("ExistsByUrl() returned true for non-existant file")
				return
			}

			// cleanup
			if err := tt.store.Delete(sourcePathForFile); err != nil {
				t.Fatalf("could not delete existing source key: %s", err)
			}
		})
	}
}
