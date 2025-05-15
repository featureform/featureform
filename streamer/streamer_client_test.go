package streamer

import (
	"testing"

	"github.com/featureform/core"
	fs "github.com/featureform/filestore"
	"github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
)

func validCatalogLocation() *location.CatalogLocation {
	return location.NewCatalogLocation("test_db", "test_table", string(pc.Iceberg))
}

func TestGlueRequestWithAssumeRole_ToRequest(t *testing.T) {
	validLoc := validCatalogLocation()

	t.Run("Valid request", func(t *testing.T) {
		req := glueRequestWithAssumeRole{
			Location: validLoc,
			Region:   "us-east-1",
			RoleArn:  "arn:aws:iam::123456789012:role/MyRole",
		}
		result, err := req.ToRequest()
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		expected := map[string]any{
			"catalog":         "default",
			"namespace":       "test_db",
			"table":           "test_table",
			"catalog_type":    "glue",
			"client.region":   "us-east-1",
			"client.role-arn": "arn:aws:iam::123456789012:role/MyRole",
		}
		assertRequestsEqual(t, result, expected)
	})

	t.Run("Nil location", func(t *testing.T) {
		req := glueRequestWithAssumeRole{
			Location: nil,
			Region:   "us-east-1",
			RoleArn:  "arn:aws:iam::123456789012:role/MyRole",
		}
		_, err := req.ToRequest()
		if err == nil {
			t.Fatalf("expected error for nil CatalogLocation, got: %v", err)
		}
	})

	t.Run("Empty region", func(t *testing.T) {
		req := glueRequestWithAssumeRole{
			Location: validLoc,
			Region:   "",
			RoleArn:  "arn:aws:iam::123456789012:role/MyRole",
		}
		_, err := req.ToRequest()
		if err == nil {
			t.Fatalf("expected error for empty region, got: %v", err)
		}
	})

	t.Run("Empty role ARN", func(t *testing.T) {
		req := glueRequestWithAssumeRole{
			Location: validLoc,
			Region:   "us-east-1",
			RoleArn:  "",
		}
		_, err := req.ToRequest()
		if err == nil {
			t.Fatalf("expected error for empty role ARN, got: %v", err)
		}
	})
}

func TestGlueRequestWithStaticCreds_ToRequest(t *testing.T) {
	validLoc := validCatalogLocation()
	validCreds := pc.AWSStaticCredentials{AccessKeyId: "AKIA", SecretKey: "secret"}

	t.Run("Valid request", func(t *testing.T) {
		req := glueRequestWithStaticCreds{
			Location: validLoc,
			Region:   "us-west-2",
			Creds:    validCreds,
		}
		result, err := req.ToRequest()
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		expected := map[string]any{
			"catalog":                  "default",
			"namespace":                "test_db",
			"table":                    "test_table",
			"catalog_type":             "glue",
			"client.region":            "us-west-2",
			"client.access-key-id":     "AKIA",
			"client.secret-access-key": "secret",
		}
		assertRequestsEqual(t, result, expected)
	})

	t.Run("Nil location", func(t *testing.T) {
		req := glueRequestWithStaticCreds{
			Location: nil,
			Region:   "us-west-2",
			Creds:    validCreds,
		}
		_, err := req.ToRequest()
		if err == nil {
			t.Fatalf("expected error for nil CatalogLocation, got: %v", err)
		}
	})

	t.Run("Empty region", func(t *testing.T) {
		req := glueRequestWithStaticCreds{
			Location: validLoc,
			Region:   "",
			Creds:    validCreds,
		}
		_, err := req.ToRequest()
		if err == nil {
			t.Fatalf("expected error for empty region, got: %v", err)
		}
	})

	t.Run("Empty access key", func(t *testing.T) {
		req := glueRequestWithStaticCreds{
			Location: validLoc,
			Region:   "us-west-2",
			Creds:    pc.AWSStaticCredentials{AccessKeyId: "", SecretKey: "secret"},
		}
		_, err := req.ToRequest()
		if err == nil {
			t.Fatalf("expected error for empty access key, got: %v", err)
		}
	})

	t.Run("Empty secret key", func(t *testing.T) {
		req := glueRequestWithStaticCreds{
			Location: validLoc,
			Region:   "us-west-2",
			Creds:    pc.AWSStaticCredentials{AccessKeyId: "AKIA", SecretKey: ""},
		}
		_, err := req.ToRequest()
		if err == nil {
			t.Fatalf("expected error for empty secret key, got: %v", err)
		}
	})
}

func TestDatasetOptions_ToRequest(t *testing.T) {
	t.Run("Negative limit", func(t *testing.T) {
		opts := DatasetOptions{Limit: -5}
		_, err := opts.ToRequest()
		if err == nil {
			t.Fatalf("expected error for negative limit, got: %v", err)
		}
	})

	t.Run("Zero limit", func(t *testing.T) {
		opts := DatasetOptions{Limit: 0}
		result, err := opts.ToRequest()
		if err != nil {
			t.Fatalf("expected no error for zero limit, got: %v", err)
		}
		if len(result) != 0 {
			t.Errorf("expected empty request map for zero limit, got: %v", result)
		}
	})

	t.Run("Positive limit", func(t *testing.T) {
		opts := DatasetOptions{Limit: 10}
		result, err := opts.ToRequest()
		if err != nil {
			t.Fatalf("expected no error for positive limit, got: %v", err)
		}
		if result["limit"] != int64(10) {
			t.Errorf("expected limit 10, got: %v", result["limit"])
		}
	})
}

func TestCatalogLocationToRequest(t *testing.T) {
	t.Run("Nil location", func(t *testing.T) {
		_, err := catalogLocationToRequest(nil)
		if err == nil {
			t.Fatalf("expected error for nil CatalogLocation, got: %v", err)
		}
	})

	t.Run("Unsupported table format", func(t *testing.T) {
		loc := location.NewCatalogLocation("test_db", "test_table", "csv")
		_, err := catalogLocationToRequest(loc)
		if err == nil {
			t.Fatalf("expected error for unsupported table format, got: %v", err)
		}
	})

	t.Run("Missing database", func(t *testing.T) {
		loc := location.NewCatalogLocation("", "test_table", string(pc.Iceberg))
		_, err := catalogLocationToRequest(loc)
		if err == nil {
			t.Fatalf("expected error for missing database, got: %v", err)
		}
	})

	t.Run("Missing table", func(t *testing.T) {
		loc := location.NewCatalogLocation("test_db", "", string(pc.Iceberg))
		_, err := catalogLocationToRequest(loc)
		if err == nil {
			t.Fatalf("expected error for missing table, got: %v", err)
		}
	})

	t.Run("Valid location", func(t *testing.T) {
		loc := location.NewCatalogLocation("test_db", "test_table", string(pc.Iceberg))
		req, err := catalogLocationToRequest(loc)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		expected := Request{
			"catalog":   "default",
			"namespace": "test_db",
			"table":     "test_table",
		}
		assertRequestsEqual(t, req, expected)
	})
}

func TestRequest_Add(t *testing.T) {
	t.Run("Merge without duplicate keys", func(t *testing.T) {
		base := Request{"key1": "value1", "key2": "value2"}
		toAdd := Request{"key3": "value3", "key4": "value4"}
		merged, err := base.Add(toAdd)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		expected := Request{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
			"key4": "value4",
		}
		assertRequestsEqual(t, merged, expected)
	})

	t.Run("Error on duplicate key", func(t *testing.T) {
		base := Request{"key1": "value1"}
		toAdd := Request{"key1": "value2", "key2": "value3"}
		_, err := base.Add(toAdd)
		if err == nil {
			t.Fatalf("expected duplicate key error, got: %v", err)
		}
	})
}

func TestRequestFromSerializedSparkConfig_Serialized_AssumeRole(t *testing.T) {
	ctx := core.NewTestContext(t)

	// Build a valid SparkConfig with GlueConfig (using AssumeRole).
	config := &pc.SparkConfig{
		ExecutorType: pc.SparkGeneric,
		ExecutorConfig: &pc.SparkGenericConfig{
			Master:        "local",
			DeployMode:    "client",
			PythonVersion: "3.8",
		},
		StoreType: fs.S3,
		StoreConfig: &pc.S3FileStoreConfig{
			Credentials: pc.AWSStaticCredentials{AccessKeyId: "AKIA123", SecretKey: "secret"},
		},
		GlueConfig: &pc.GlueConfig{
			Database:      "test_db",
			Warehouse:     "wh",
			Region:        "us-east-1",
			AssumeRoleArn: "arn:aws:iam::123456789012:role/MyRole",
			TableFormat:   pc.Iceberg,
		},
	}

	serConf, err := config.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize SparkConfig: %v", err)
	}

	loc := validCatalogLocation()
	req, err := RequestFromSerializedSparkConfig(ctx, serConf, loc)
	if err != nil {
		t.Fatalf("expected valid request, got error: %v", err)
	}

	// Verify the request contains the expected keys and values.
	expected := map[string]any{
		"catalog":         "default",
		"namespace":       "test_db",
		"table":           "test_table",
		"catalog_type":    "glue",
		"client.region":   "us-east-1",
		"client.role-arn": "arn:aws:iam::123456789012:role/MyRole",
	}
	assertRequestsEqual(t, req, expected)
}

func TestRequestFromSerializedSparkConfig_Serialized_StaticCreds(t *testing.T) {
	ctx := core.NewTestContext(t)

	// Build a valid SparkConfig with GlueConfig (using AssumeRole).
	config := &pc.SparkConfig{
		ExecutorType: pc.SparkGeneric,
		ExecutorConfig: &pc.SparkGenericConfig{
			Master:        "local",
			DeployMode:    "client",
			PythonVersion: "3.8",
		},
		StoreType: fs.S3,
		StoreConfig: &pc.S3FileStoreConfig{
			Credentials: pc.AWSStaticCredentials{AccessKeyId: "AKIA123", SecretKey: "secret"},
		},
		GlueConfig: &pc.GlueConfig{
			Database:    "test_db",
			Warehouse:   "wh",
			Region:      "us-east-1",
			TableFormat: pc.Iceberg,
		},
	}

	serConf, err := config.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize SparkConfig: %v", err)
	}

	loc := validCatalogLocation()
	req, err := RequestFromSerializedSparkConfig(ctx, serConf, loc)
	if err != nil {
		t.Fatalf("expected valid request, got error: %v", err)
	}

	// Verify the request contains the expected keys and values.
	expected := map[string]any{
		"catalog":                  "default",
		"namespace":                "test_db",
		"table":                    "test_table",
		"catalog_type":             "glue",
		"client.region":            "us-east-1",
		"client.access-key-id":     "AKIA123",
		"client.secret-access-key": "secret",
	}
	assertRequestsEqual(t, req, expected)
}

func TestRequestFromSerializedSparkConfig_Serialized_MissingGlue(t *testing.T) {
	ctx := core.NewTestContext(t)

	// Build a SparkConfig without GlueConfig.
	config := &pc.SparkConfig{
		ExecutorType: pc.SparkGeneric,
		ExecutorConfig: &pc.SparkGenericConfig{
			Master:        "local",
			DeployMode:    "client",
			PythonVersion: "3.8",
			CoreSite:      "",
			YarnSite:      "",
		},
		StoreType: fs.S3,
		StoreConfig: &pc.S3FileStoreConfig{
			Credentials: pc.AWSStaticCredentials{AccessKeyId: "AKIA123", SecretKey: "secret"},
		},
		GlueConfig: nil,
	}

	serConf, err := config.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize SparkConfig: %v", err)
	}

	loc := validCatalogLocation()
	_, err = RequestFromSerializedSparkConfig(ctx, serConf, loc)
	if err == nil {
		t.Errorf("expected error for missing GlueConfig, got: %v", err)
	}
}

func TestRequestFromSerializedSparkConfig_Serialized_InvalidLocation(t *testing.T) {
	ctx := core.NewTestContext(t)

	// Build a valid SparkConfig.
	config := &pc.SparkConfig{
		ExecutorType: pc.SparkGeneric,
		ExecutorConfig: &pc.SparkGenericConfig{
			Master:        "local",
			DeployMode:    "client",
			PythonVersion: "3.8",
			CoreSite:      "",
			YarnSite:      "",
		},
		StoreType: fs.S3,
		StoreConfig: &pc.S3FileStoreConfig{
			Credentials: pc.AWSStaticCredentials{AccessKeyId: "AKIA123", SecretKey: "secret"},
		},
		GlueConfig: &pc.GlueConfig{
			Database:      "test_db",
			Warehouse:     "wh",
			Region:        "us-east-1",
			AssumeRoleArn: "arn:aws:iam::123456789012:role/MyRole",
			TableFormat:   pc.Iceberg,
		},
	}

	serConf, err := config.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize SparkConfig: %v", err)
	}

	invalidLoc := location.NewCatalogLocation("test_db", "test_table", "csv")
	_, err = RequestFromSerializedSparkConfig(ctx, serConf, invalidLoc)
	if err == nil {
		t.Errorf("expected error for invalid location type, got: %v", err)
	}
}

func TestRequestFromSerializedSparkConfig_InvalidJSON(t *testing.T) {
	ctx := core.NewTestContext(t)
	loc := validCatalogLocation()
	invalidCfg := pc.SerializedConfig([]byte("{}"))
	if _, err := RequestFromSerializedSparkConfig(ctx, invalidCfg, loc); err == nil {
		t.Errorf("expected error for invalid serialized, got: %v", err)
	}
}

func assertRequestsEqual(t *testing.T, actual, expected Request) {
	for k, actVal := range actual {
		if expVal, found := expected[k]; !found {
			t.Fatalf("expected key %s not found", k)
		} else if expVal != actVal {
			t.Fatalf("For key %s: found %v expected %v", k, actVal, expVal)
		}
	}
	if len(expected) != len(actual) {
		for k, _ := range expected {
			if _, found := actual[k]; !found {
				t.Fatalf("Request missing key %s", k)
			}
		}
	}
}
