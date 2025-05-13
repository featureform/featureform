package arrow

import (
	"os/exec"
	"reflect"
	"testing"
	"time"

	"github.com/featureform/core"
	types "github.com/featureform/fftypes"
	fs "github.com/featureform/filestore"
	"github.com/featureform/helpers"
	"github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	"github.com/featureform/streamer"
)

func runServer(t *testing.T) {
	cmd := exec.Command("python3", "../../streamer/iceberg_streamer.py")
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
	limit := int64(10)
	config := pc.SparkConfig{
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
	loc := location.NewCatalogLocation(db, table, string(pc.Iceberg))
	runServer(t)
	client, err := streamer.NewClient(ctx, "localhost:8085")
	if err != nil {
		t.Fatalf("Failed to create client: %s", err)
	}
	defer client.Close()
	dataset := &StreamerDataset{
		Ctx:    ctx,
		Client: client,
		Loc:    loc,
		Config: config,
		Limit:  limit,
	}
	schema, err := dataset.Schema()
	if err != nil {
		t.Fatalf("Failed to get schema: %s", err)
	}
	wantFields := []types.ColumnSchema{
		{
			Name:       "TransactionID",
			NativeType: "large_utf8",
			Type:       types.String,
		},
		{
			Name:       "CustomerID",
			NativeType: "large_utf8",
			Type:       types.String,
		},
		{
			Name:       "CustomerDOB",
			NativeType: "large_utf8",
			Type:       types.String,
		},
		{
			Name:       "CustLocation",
			NativeType: "large_utf8",
			Type:       types.String,
		},
		{
			Name:       "CustAccountBalance",
			NativeType: "large_utf8",
			Type:       types.String,
		},
		{
			Name:       "TransactionAmount",
			NativeType: "large_utf8",
			Type:       types.String,
		},
		{
			Name:       "Timestamp",
			NativeType: "large_utf8",
			Type:       types.String,
		},
		{
			Name:       "IsFraud",
			NativeType: "large_utf8",
			Type:       types.String,
		},
	}
	for i := range wantFields {
		if !reflect.DeepEqual(schema.Fields[i], wantFields[i]) {
			t.Errorf("Fields mismatch.\nGot:\n%#v\nWant:\n%#v",
				schema.Fields[i], wantFields[i])
		}
	}
	iter, err := dataset.Iterator()
	if err != nil {
		t.Fatalf("Failed to get iterator: %s", err)
	}
	rows := make(types.Rows, 0)
	for iter.Next(ctx) {
		rows = append(rows, iter.Values())
	}
	if err := iter.Err(); err != nil {
		t.Fatalf("Iteratation returned error: %v", err)
	}
	if err := iter.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	expected := types.Rows{
		{
			{NativeType: "large_utf8", Type: types.String, Value: "T31270"},
			{NativeType: "large_utf8", Type: types.String, Value: "C1433534"},
			{NativeType: "large_utf8", Type: types.String, Value: "9/3/73"},
			{NativeType: "large_utf8", Type: types.String, Value: nil},
			{NativeType: "large_utf8", Type: types.String, Value: "2780.38"},
			{NativeType: "large_utf8", Type: types.String, Value: "75"},
			{NativeType: "large_utf8", Type: types.String, Value: "2022-04-12 12:52:20 UTC"},
			{NativeType: "large_utf8", Type: types.String, Value: "false"},
		},
		{
			{NativeType: "large_utf8", Type: types.String, Value: "T42841"},
			{NativeType: "large_utf8", Type: types.String, Value: "C7233515"},
			{NativeType: "large_utf8", Type: types.String, Value: "9/3/73"},
			{NativeType: "large_utf8", Type: types.String, Value: nil},
			{NativeType: "large_utf8", Type: types.String, Value: "2780.38"},
			{NativeType: "large_utf8", Type: types.String, Value: "14"},
			{NativeType: "large_utf8", Type: types.String, Value: "2022-04-11 03:20:14 UTC"},
			{NativeType: "large_utf8", Type: types.String, Value: "false"},
		},
		{
			{NativeType: "large_utf8", Type: types.String, Value: "T45632"},
			{NativeType: "large_utf8", Type: types.String, Value: "C5333538"},
			{NativeType: "large_utf8", Type: types.String, Value: "9/3/73"},
			{NativeType: "large_utf8", Type: types.String, Value: nil},
			{NativeType: "large_utf8", Type: types.String, Value: "2780.38"},
			{NativeType: "large_utf8", Type: types.String, Value: "233"},
			{NativeType: "large_utf8", Type: types.String, Value: "2022-04-06 03:15:13 UTC"},
			{NativeType: "large_utf8", Type: types.String, Value: "false"},
		},
		{
			{NativeType: "large_utf8", Type: types.String, Value: "T43999"},
			{NativeType: "large_utf8", Type: types.String, Value: "C8233533"},
			{NativeType: "large_utf8", Type: types.String, Value: "9/3/73"},
			{NativeType: "large_utf8", Type: types.String, Value: nil},
			{NativeType: "large_utf8", Type: types.String, Value: "2780.38"},
			{NativeType: "large_utf8", Type: types.String, Value: "149"},
			{NativeType: "large_utf8", Type: types.String, Value: "2022-04-01 20:27:17 UTC"},
			{NativeType: "large_utf8", Type: types.String, Value: "false"},
		},
		{
			{NativeType: "large_utf8", Type: types.String, Value: "T32732"},
			{NativeType: "large_utf8", Type: types.String, Value: "C8837983"},
			{NativeType: "large_utf8", Type: types.String, Value: "21/9/86"},
			{NativeType: "large_utf8", Type: types.String, Value: "."},
			{NativeType: "large_utf8", Type: types.String, Value: "153455.63"},
			{NativeType: "large_utf8", Type: types.String, Value: "1875"},
			{NativeType: "large_utf8", Type: types.String, Value: "2022-04-21 04:36:29 UTC"},
			{NativeType: "large_utf8", Type: types.String, Value: "false"},
		},
		{
			{NativeType: "large_utf8", Type: types.String, Value: "T5745"},
			{NativeType: "large_utf8", Type: types.String, Value: "C4715927"},
			{NativeType: "large_utf8", Type: types.String, Value: "6/4/83"},
			{NativeType: "large_utf8", Type: types.String, Value: "."},
			{NativeType: "large_utf8", Type: types.String, Value: "11357.95"},
			{NativeType: "large_utf8", Type: types.String, Value: "469"},
			{NativeType: "large_utf8", Type: types.String, Value: "2022-04-10 12:58:52 UTC"},
			{NativeType: "large_utf8", Type: types.String, Value: "false"},
		},
		{
			{NativeType: "large_utf8", Type: types.String, Value: "T5629"},
			{NativeType: "large_utf8", Type: types.String, Value: "C3537961"},
			{NativeType: "large_utf8", Type: types.String, Value: "21/9/86"},
			{NativeType: "large_utf8", Type: types.String, Value: "."},
			{NativeType: "large_utf8", Type: types.String, Value: "153455.63"},
			{NativeType: "large_utf8", Type: types.String, Value: "826.25"},
			{NativeType: "large_utf8", Type: types.String, Value: "2022-04-02 08:46:23 UTC"},
			{NativeType: "large_utf8", Type: types.String, Value: "false"},
		},
		{
			{NativeType: "large_utf8", Type: types.String, Value: "T39898"},
			{NativeType: "large_utf8", Type: types.String, Value: "C3530756"},
			{NativeType: "large_utf8", Type: types.String, Value: "22/7/89"},
			{NativeType: "large_utf8", Type: types.String, Value: "AMB"},
			{NativeType: "large_utf8", Type: types.String, Value: "2827.24"},
			{NativeType: "large_utf8", Type: types.String, Value: "55"},
			{NativeType: "large_utf8", Type: types.String, Value: "2022-04-10 03:17:06 UTC"},
			{NativeType: "large_utf8", Type: types.String, Value: "false"},
		},
		{
			{NativeType: "large_utf8", Type: types.String, Value: "T45644"},
			{NativeType: "large_utf8", Type: types.String, Value: "C6717858"},
			{NativeType: "large_utf8", Type: types.String, Value: "1/6/78"},
			{NativeType: "large_utf8", Type: types.String, Value: "GOA"},
			{NativeType: "large_utf8", Type: types.String, Value: "274001.97"},
			{NativeType: "large_utf8", Type: types.String, Value: "399"},
			{NativeType: "large_utf8", Type: types.String, Value: "2022-04-05 04:17:28 UTC"},
			{NativeType: "large_utf8", Type: types.String, Value: "false"},
		},
		{
			{NativeType: "large_utf8", Type: types.String, Value: "T44248"},
			{NativeType: "large_utf8", Type: types.String, Value: "C1434966"},
			{NativeType: "large_utf8", Type: types.String, Value: "1/1/1800"},
			{NativeType: "large_utf8", Type: types.String, Value: "GOA"},
			{NativeType: "large_utf8", Type: types.String, Value: "44081.63"},
			{NativeType: "large_utf8", Type: types.String, Value: "3000"},
			{NativeType: "large_utf8", Type: types.String, Value: "2022-04-16 13:12:22 UTC"},
			{NativeType: "large_utf8", Type: types.String, Value: "false"},
		},
	}
	if len(rows) != len(expected) {
		t.Fatalf("Lengths do not match: %d %d", len(rows), len(expected))
	}
	for i, expRow := range expected {
		if !reflect.DeepEqual(expRow, rows[i]) {
			t.Fatalf("Row %d not equal\n%v\n%v\n", i, expRow, rows[i])
		}
	}
}
