// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package biglake

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/featureform/helpers"
	"github.com/featureform/logging"
)

func TestCatalog(t *testing.T) {
	t.Skip("BIG LAKE ENV variables not set yet")
	ctx := context.Background()
	logger := logging.NewTestLogger(t)
	blFS, err := NewSparkFileStore(ctx, SparkFileStoreConfig{
		Bucket:    helpers.MustGetTestingEnv(t, "GCS_BUCKET_NAME"),
		BaseDir:   uuid.NewString(),
		CredsPath: helpers.MustGetTestingEnv(t, "BIGQUERY_CREDENTIALS"),
		Region:    helpers.MustGetTestingEnv(t, "BIGLAKE_REGION"),
		ProjectID: helpers.MustGetTestingEnv(t, "BIGLAKE_PROJECT_ID"),
		Logger:    logger,
	})
	// Catalog names can't have dashes
	catalogName := strings.ReplaceAll(uuid.NewString(), "-", "_")
	if err != nil {
		t.Fatalf("Failed to create biglake filestore: %s", err)
	}

	created, err := blFS.CreateCatalog(catalogName)
	if err != nil {
		t.Fatalf("CreateCatalog failed: %v", err)
	}
	defer func() {
		blFS.DeleteCatalog(catalogName)
	}()
	if !created {
		t.Fatalf("CreateCatalog returned false")
	}
	created, err = blFS.CreateCatalog(catalogName)
	if err != nil {
		t.Fatalf("CreateCatalog failed 2nd time: %v", err)
	} else if created {
		t.Fatalf("Catalog created 2nd time")
	}
	if err := blFS.DeleteCatalog(catalogName); err != nil {
		t.Fatalf("DeleteCatalog failed: %v", err)
	}
}
