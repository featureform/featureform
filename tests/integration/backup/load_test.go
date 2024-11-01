// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package main

import (
	"context"
	"fmt"
	"testing"

	help "github.com/featureform/helpers"
	client "go.etcd.io/etcd/client/v3"
)

func TestReadETCD(t *testing.T) {
	t.Skip("unmaintained")
	host := help.GetEnv("ETCD_HOST", "localhost")
	port := help.GetEnv("ETCD_PORT", "2379")
	address := fmt.Sprintf("%s:%s", host, port)

	t.Log("Creating Client")
	c := NewClient(address)
	t.Log("Reading File")
	data := ReadFile("./testcases.json")

	resp, err := c.Get(context.Background(), "", client.WithPrefix())
	if err != nil {
		t.Fatalf("Could not get keys: %s", err.Error())
	}

	if int(resp.Count) != len(data) {
		t.Fatalf("Expected %d keys, got %d keys", len(data), resp.Count)
	}

	for _, k := range resp.Kvs {
		key := string(k.Key)
		value := string(k.Value)
		expectedVal, ok := data[key]
		if !ok {
			t.Errorf("Recieved key not in expected data: %s", key)
		}
		if expectedVal != value {
			t.Errorf("Expected value (%s), got (%s) for key %s", expectedVal, value, key)
		}
	}
}
