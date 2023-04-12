//go:build filestore
// +build filestore

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/joho/godotenv"

	pc "github.com/featureform/provider/provider_config"
)

func TestFileStore(t *testing.T) {
	err := godotenv.Load("../.env")
	if err != nil {
		fmt.Println(err)
	}

	fileStores, err := getFileStores()
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
		// "hdfs": map[string]TestCase{
		// 	"path_with_slash_not_remote": TestCase{
		// 		Path:         "/path_to_file/file.csv",
		// 		Remote:       false,
		// 		ExpectedPath: "tmp/path_to_file/file.csv",
		// 	},
		// 	"path_without_slash_not_remote": TestCase{
		// 		Path:         "path_to_file/file.csv",
		// 		Remote:       false,
		// 		ExpectedPath: "tmp/path_to_file/file.csv",
		// 	},
		// 	"path_with_slash_remote": TestCase{
		// 		Path:         "/path_to_file/file.csv",
		// 		Remote:       true,
		// 		ExpectedPath: "tmp/path_to_file/file.csv",
		// 	},
		// 	"path_without_slash_remote": TestCase{
		// 		Path:         "path_to_file/file.csv",
		// 		Remote:       true,
		// 		ExpectedPath: "tmp/path_to_file/file.csv",
		// 	},
		// },
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

func getFileStores() (map[string]FileStore, error) {
	localFileStore, err := getLocalFileStore()
	if err != nil {
		return nil, fmt.Errorf("could not get local filestore: %v", err)
	}

	s3FileStore, err := getS3FileStore()
	if err != nil {
		return nil, fmt.Errorf("could not get s3 filestore: %v", err)
	}

	// hdfsFileStore, err := getHDFSFileStore()
	// if err != nil {
	// 	return nil, fmt.Errorf("could not get hdfs filestore: %v", err)
	// }

	azureFileStore, err := getAzureFileStore()
	if err != nil {
		return nil, fmt.Errorf("could not get azure filestore: %v", err)
	}

	gcsFileStore, err := getGCSFileStore()
	if err != nil {
		return nil, fmt.Errorf("could not get gcs filestore: %v", err)
	}

	fileStoresMap := map[string]FileStore{
		"local": localFileStore,
		"s3":    s3FileStore,
		// "hdfs":  hdfsFileStore,
		"azure": azureFileStore,
		"gcs":   gcsFileStore,
	}

	return fileStoresMap, nil
}

func getLocalFileStore() (FileStore, error) {
	config := pc.LocalFileStoreConfig{
		DirPath: "file:///tmp/",
	}

	serializedConfig, err := config.Serialize()
	if err != nil {
		return nil, fmt.Errorf("could not serialized local config: %s", err)
	}
	fileStore, err := NewLocalFileStore(serializedConfig)
	if err != nil {
		return nil, fmt.Errorf("could not initialize local filestore: %s", err)
	}

	return fileStore, nil
}

func getS3FileStore() (FileStore, error) {
	s3Config := pc.S3FileStoreConfig{
		Credentials: pc.AWSCredentials{
			AWSAccessKeyId: os.Getenv("AWS_ACCESS_KEY_ID"),
			AWSSecretKey:   os.Getenv("AWS_SECRET_KEY"),
		},
		BucketRegion: os.Getenv("S3_BUCKET_REGION"),
		BucketPath:   os.Getenv("S3_BUCKET_PATH"),
		Path:         "featureform-unit-test",
	}

	serializedConfig, err := s3Config.Serialize()
	if err != nil {
		return nil, fmt.Errorf("could not serialized s3 config: %s", err)
	}
	fileStore, err := NewS3FileStore(serializedConfig)
	if err != nil {
		return nil, fmt.Errorf("could not initialize s3 filestore: %s", err)
	}

	return fileStore, nil
}

func getHDFSFileStore() (FileStore, error) {
	hdfsConfig := pc.HDFSFileStoreConfig{
		Host:     "localhost",
		Port:     "9000",
		Username: "hduser",
		Path:     "/tmp/",
	}

	serializedConfig, err := hdfsConfig.Serialize()
	if err != nil {
		return nil, fmt.Errorf("could not serialized hdfs config: %s", err)
	}
	fileStore, err := NewHDFSFileStore(serializedConfig)
	if err != nil {
		return nil, fmt.Errorf("could not initialize hdfs filestore: %s", err)
	}

	return fileStore, nil
}

func getAzureFileStore() (FileStore, error) {
	azureConfig := pc.AzureFileStoreConfig{
		AccountName:   os.Getenv("AZURE_ACCOUNT_NAME"),
		AccountKey:    os.Getenv("AZURE_ACCOUNT_KEY"),
		ContainerName: os.Getenv("AZURE_CONTAINER_NAME"),
		Path:          "featureform-unit-test",
	}

	serializedConfig, err := azureConfig.Serialize()
	if err != nil {
		return nil, fmt.Errorf("could not serialized azure config: %s", err)
	}
	fileStore, err := NewAzureFileStore(serializedConfig)
	if err != nil {
		return nil, fmt.Errorf("could not initialize azure filestore: %s", err)
	}

	return fileStore, nil
}

func getGCSFileStore() (FileStore, error) {
	credsFile := os.Getenv("GCP_CREDENTIALS_FILE")
	content, err := ioutil.ReadFile(credsFile)
	if err != nil {
		return nil, fmt.Errorf("Error when opening file: %v", err)
	}

	var creds map[string]interface{}
	err = json.Unmarshal(content, &creds)
	if err != nil {
		return nil, fmt.Errorf("Error during Unmarshal() creds: %v", err)
	}

	gcsConfig := pc.GCSFileStoreConfig{
		BucketName: os.Getenv("GCS_BUCKET_NAME"),
		BucketPath: "featureform-unit-test",
		Credentials: pc.GCPCredentials{
			ProjectId: os.Getenv("GCP_PROJECT_ID"),
			JSON:      creds,
		},
	}

	serializedConfig, err := gcsConfig.Serialize()
	if err != nil {
		return nil, fmt.Errorf("could not serialized gcs config: %v", err)
	}
	fileStore, err := NewGCSFileStore(serializedConfig)
	if err != nil {
		return nil, fmt.Errorf("could not initialize gcs filestore: %s", err)
	}

	return fileStore, nil
}
