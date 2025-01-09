package provider

import (
	"context"
	"fmt"
	"path"
	"testing"

	"github.com/featureform/helpers"
	"github.com/featureform/logging"
	"github.com/featureform/provider/biglake"
	"github.com/featureform/provider/location"

	"github.com/google/uuid"
)

func TestSuiteSparkFileStoreV2(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}
	ctx := context.Background()
	logger := logging.NewTestLogger(t)
	blFS, err := biglake.NewSparkFileStore(ctx, biglake.SparkFileStoreConfig{
		Bucket: helpers.MustGetTestingEnv(t, "GCS_BUCKET_NAME"),
		BaseDir: uuid.NewString(),
		CredsPath: helpers.MustGetTestingEnv(t, "BIGQUERY_CREDENTIALS"),
		Logger: logger,
	})
	if err != nil {
		t.Fatalf("Failed to create biglake filestore: %v", err)
	}
	stores := map[string]SparkFileStoreV2{
		"biglake": blFS,
	}
	tests := map[string]func(t *testing.T, store SparkFileStoreV2){
		"BasicOps": BasicFileStoreOperationsTest,
	}
	for storeName, store := range stores {
		for testName, test := range tests {
			name := fmt.Sprintf("%s_%s", storeName, testName)
			constStore := store
			constTest := test
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				constTest(t, constStore)
			})
		}
	}
}

func BasicFileStoreOperationsTest(t *testing.T, store SparkFileStoreV2) {
	dir := uuid.NewString()
	fileName := "test_file.txt"
	testPath, err := store.CreateFilePath(path.Join(dir, fileName), false)
	if err != nil {
		t.Fatalf("Failed to create filepath: %v", err)
	}
	testPathLoc := location.NewFileLocation(testPath)

	testData := []byte("Hello World!")
	if err = store.Write(testPath, testData); err != nil {
		t.Errorf("Write failed: %v", err)
	}
	// Even if a test fails, we shouldn't leave the file hanging.
	defer store.Delete(testPath)
	exists, err := store.Exists(testPathLoc)
	if err != nil {
		t.Errorf("Exists check failed: %v", err)
	}
	if !exists {
		t.Errorf("File %s should exist but does not", testPath)
	}
	data, err := store.Read(testPath)
	if err != nil {
		t.Errorf("Read failed: %v", err)
	}
	if string(data) != string(testData) {
		t.Errorf("Read data mismatch: got %s, want %s", string(data), string(testData))
	}
	err = store.Delete(testPath)
	if err != nil {
		t.Errorf("Delete failed: %v", err)
	}
	exists, err = store.Exists(testPathLoc)
	if err != nil {
		t.Errorf("Exists check failed after delete: %v", err)
	}
	if exists {
		t.Errorf("File %s should not exist after deletion", testPath)
	}
}
