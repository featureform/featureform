package main

import (
	"context"
	"fmt"
	help "github.com/featureform/helpers"
	client "go.etcd.io/etcd/client/v3"
	"testing"
)

func TestReadETCD(t *testing.T) {
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
			t.Errorf("Expected key not in results: %s", key)
		}
		if expectedVal != value {
			t.Errorf("Expected value (%s), got (%s) for key %s", expectedVal, value, key)
		}
	}
}
