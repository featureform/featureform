// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"fmt"
	"testing"
	"time"

	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	vt "github.com/featureform/provider/types"
)

func TestDirectMaterialization(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	offlineStores := map[string]OfflineStore{
		"IcebergEMR": GetTestingEMRGlue(t, pc.Iceberg),
	}
	onlineStores := map[string]OnlineStore{
		"Dynamo": GetTestingDynamoDB(t, map[string]string{}),
	}

	variantUUID := uuidWithoutDashes()[0:8]
	for offlineName, offlineStore := range offlineStores {
		t.Logf("Testing offline store: %s", offlineName)
		setupResourceTable(t, offlineStore, variantUUID)
		for onlineName, onlineStore := range onlineStores {
			setupOnlineTable(t, onlineStore, variantUUID)
			t.Logf("Testing offline store %s online store %s", offlineName, onlineName)
			testName := fmt.Sprintf("direct_materialize_%s_into_%s", offlineName, onlineName)
			// Setup the closure
			constOnline := onlineStore
			constOffline := offlineStore
			t.Run(testName, func(t *testing.T) {
				directMaterializeTest(t, constOffline, constOnline, variantUUID)
			})
		}
	}
}

func setupResourceTable(t *testing.T, offline OfflineStore, variant string) {
	// TODO make this work for all offline stores
	location := pl.NewCatalogLocation("ff", "transactions2", "iceberg")
	primaryID := ResourceID{
		Name:    "direct_materialize_test",
		Variant: variant,
		Type:    Primary,
	}
	if _, err := offline.RegisterPrimaryFromSourceTable(primaryID, location); err != nil {
		t.Fatalf("Failed to register primary: %s", err)
	}
	featureID := ResourceID{
		Name:    "direct_materialize_test",
		Variant: variant,
		Type:    Feature,
	}
	schema := ResourceSchema{
		Entity:      "customerid",
		Value:       "transactionamount",
		TS:          "timestamp",
		SourceTable: location,
	}
	if _, err := offline.RegisterResourceFromSourceTable(featureID, schema); err != nil {
		t.Fatalf("Failed to register resource: %s", err)
	}
}

func setupOnlineTable(t *testing.T, online OnlineStore, variant string) {
	if _, err := online.CreateTable("direct_materialize_test", variant, vt.Float32); err != nil {
		t.Logf("Failed to create online table: %s, but continuing", err)
	}
}

func directMaterializeTest(t *testing.T, offline OfflineStore, online OnlineStore, variant string) {
	matOpt := DirectCopyOptionType(online)
	supports, err := offline.SupportsMaterializationOption(matOpt)
	if err != nil {
		t.Fatalf(
			"Failed to check if online store %T supports materialization option: %s",
			online, err,
		)
	}
	if !supports {
		t.Fatalf(
			"%T can't materialize to %T",
			offline, online,
		)
	}
	featureID := ResourceID{
		Name:    "direct_materialize_test",
		Variant: variant,
		Type:    Feature,
	}
	matOpts := MaterializationOptions{
		DirectCopyTo:   online,
		MaxJobDuration: time.Minute * 50,
	}
	if _, err := offline.CreateMaterialization(featureID, matOpts); err != nil {
		t.Fatalf("Failed to create materialization: %s", err)
	}
}
