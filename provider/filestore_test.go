// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import (
	"fmt"
	"github.com/google/uuid"
	"testing"

	"github.com/joho/godotenv"
)

func TestDoesSourceUrlExist(t *testing.T) {
	_ = godotenv.Load("../.env")

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
			filePath, err := tt.store.CreateFilePath(fileNameToCreate)
			if err != nil {
				t.Fatalf("could not create file path: %s", err)
			}
			if err := tt.store.Write(filePath, []byte("test")); err != nil {
				t.Fatalf("could not write to store: %s", err)
			}
			sourceUrlExists, err := ExistsByUrl(tt.store, filePath.ToURI())
			if err != nil {
				t.Errorf("ExistsByUrl() returned error: %v", err)
				return
			}
			if !sourceUrlExists {
				t.Errorf("ExistsByUrl() returned false for existing file")
				return
			}

			nonExistentFileName := fmt.Sprintf("%s.parquet", uuid.New().String())
			nonExistentFilePath, err := tt.store.CreateFilePath(nonExistentFileName)
			if err != nil {
				t.Fatalf("could not create file path: %s", err)
			}
			sourceUrlForNonExistentFile := nonExistentFilePath.ToURI()

			sourceUrlExists, err = ExistsByUrl(tt.store, sourceUrlForNonExistentFile)
			if err != nil {
				t.Errorf("ExistsByUrl() returned error: %v", err)
				return
			}
			if sourceUrlExists {
				t.Errorf("ExistsByUrl() returned true for non-existant file")
				return
			}

			// cleanup
			if err := tt.store.Delete(filePath); err != nil {
				t.Fatalf("could not delete existing source key: %s", err)
			}
		})
	}
}
