package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/featureform/helpers"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"google.golang.org/api/option"
)

func TestOfflineStoreBigQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	secrets := GetSecrets("testing/bigquery")
	err := godotenv.Load("../.env")
	if err != nil {
		t.Logf("could not open .env file... Checking environment: %s", err)
	}

	credentials := helpers.GetEnv("BIGQUERY_CREDENTIALS", secrets["BIGQUERY_CREDENTIALS"])

	projectID := helpers.GetEnv("BIGQUERY_PROJECT_ID", secrets["BIGQUERY_PROJECT_ID"])

	JSONCredentials, err := ioutil.ReadFile(credentials)
	if err != nil {
		panic(fmt.Errorf("cannot find big query credentials: %v", err))
	}

	var credentialsDict map[string]interface{}
	err = json.Unmarshal(JSONCredentials, &credentialsDict)
	if err != nil {
		panic(fmt.Errorf("cannot unmarshal big query credentials: %v", err))
	}

	bigQueryDatasetId := strings.Replace(strings.ToUpper(uuid.NewString()), "-", "_", -1)
	os.Setenv("BIGQUERY_DATASET_ID", bigQueryDatasetId)
	t.Log("BigQuery Dataset: ", bigQueryDatasetId)

	var bigQueryConfig = pc.BigQueryConfig{
		ProjectId:   projectID,
		DatasetId:   os.Getenv("BIGQUERY_DATASET_ID"),
		Credentials: credentialsDict,
	}
	serialBQConfig := bigQueryConfig.Serialize()

	if err := createBigQueryDataset(bigQueryConfig); err != nil {
		t.Fatalf("Cannot create BigQuery Dataset: %v", err)
	}

	t.Cleanup(func() {
		err := destroyBigQueryDataset(bigQueryConfig)
		if err != nil {
			t.Logf("failed to cleanup database: %s\n", err)
		}
	})

	store, err := GetOfflineStore(pt.BigQueryOffline, serialBQConfig)
	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	test := OfflineStoreTest{
		t:     t,
		store: store,
	}
	test.Run()
	test.RunSQL()
}

func createBigQueryDataset(c pc.BigQueryConfig) error {
	sCreds, err := json.Marshal(c.Credentials)
	if err != nil {
		return err
	}

	client, err := bigquery.NewClient(context.TODO(), c.ProjectId, option.WithCredentialsJSON(sCreds))
	if err != nil {
		return err
	}
	defer client.Close()

	meta := &bigquery.DatasetMetadata{
		Location:               "US",
		DefaultTableExpiration: 24 * time.Hour,
	}
	err = client.Dataset(c.DatasetId).Create(context.TODO(), meta)

	return err
}

func destroyBigQueryDataset(c pc.BigQueryConfig) error {
	sCreds, err := json.Marshal(c.Credentials)
	if err != nil {
		return err
	}

	time.Sleep(10 * time.Second)

	client, err := bigquery.NewClient(context.TODO(), c.ProjectId, option.WithCredentialsJSON(sCreds))
	if err != nil {
		return err
	}
	defer client.Close()

	err = client.Dataset(c.DatasetId).DeleteWithContents(context.TODO())

	return err
}
