package streamer

import (
	"os/exec"
	"testing"
	"time"

	"github.com/featureform/core"
	fs "github.com/featureform/filestore"
	"github.com/featureform/helpers"
	"github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
)

func runServer(t *testing.T) {
	cmd := exec.Command("python3", "./iceberg_streamer.py")
	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to startup server")
	}
	t.Logf("Server started")
	time.Sleep(time.Second)
	t.Cleanup(func() {
		go func() {
			cmd.Process.Kill()
		}()
		t.Logf("Server exited: %s", cmd.Wait())
	})
}

func TestStreamerIntegration(t *testing.T) {
	t.Skip("TODO fix paths")
	if testing.Short() {
		t.Skip("Skipping streamer integration test")
	}
	ctx := core.NewTestContext(t)
	db := helpers.MustGetTestingEnv(t, "AWS_GLUE_DATABASE")
	table := "transactions"
	region := helpers.MustGetTestingEnv(t, "AWS_GLUE_REGION")
	wh := helpers.MustGetTestingEnv(t, "AWS_GLUE_WAREHOUSE")
	accessKey := helpers.MustGetTestingEnv(t, "AWS_ACCESS_KEY_ID")
	secretKey := helpers.MustGetTestingEnv(t, "AWS_SECRET_KEY")
	limit := 10
	config := &pc.SparkConfig{
		ExecutorType: pc.SparkGeneric,
		ExecutorConfig: &pc.SparkGenericConfig{
			Master:        "local",
			DeployMode:    "client",
			PythonVersion: "3.8",
		},
		StoreType: fs.S3,
		StoreConfig: &pc.S3FileStoreConfig{
			Credentials: pc.AWSStaticCredentials{AccessKeyId: accessKey, SecretKey: secretKey},
		},
		GlueConfig: &pc.GlueConfig{
			Database:    db,
			Warehouse:   wh,
			Region:      region,
			TableFormat: pc.Iceberg,
		},
	}
	serConf, err := config.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize SparkConfig: %v", err)
	}
	loc := location.NewCatalogLocation(db, table, string(pc.Iceberg))
	req, err := RequestFromSerializedSparkConfig(ctx, serConf, loc)
	if err != nil {
		t.Fatalf("expected valid request, got error: %v", err)
	}
	opts := DatasetOptions{
		Limit: int64(limit),
	}
	runServer(t)
	client, err := NewClient(ctx, "localhost:8085")
	if err != nil {
		t.Fatalf("Failed to create client: %s", err)
	}
	defer client.Close()
	schema, err := client.GetSchema(ctx, req, opts)
	if err != nil {
		t.Fatalf("Failed to get schema: %s", err)
	}
	reader, err := client.GetReader(ctx, req, opts)
	if err != nil {
		t.Fatalf("Failed to get dataset: %s", err)
	}
	t.Fatalf("%T %T", schema, reader)
}
