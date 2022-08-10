package provider

import (
	"os"
	"testing"
	"time"
)

func testTableUploadCompare(store *SparkOfflineStore) error {
	testTable := "featureform/tests/testFile.parquet"
	testData := make([]ResourceRecord, 10)
	for i := range testData {
		testData[i].Entity = "a"
		testData[i].Value = i
		testData[i].TS = time.Now()
	}
	exists, err := store.Store.FileExists(testTable)
	if err != nil {
		return err
	}
	if exists {
		if err := store.Store.DeleteFile(testTable); err != nil {
			return err
		}
	}
	if err := store.Store.UploadParquetTable(testTable, testData); err != nil {
		return err
	}
	if err := store.Store.CompareParquetTable(testTable, testData); err != nil {
		return err
	}
	if err := store.Store.DeleteFile(testTable); err != nil {
		return err
	}
	exists, err = store.Store.FileExists(testTable)
	if err != nil {
		return err
	}
	if exists {
		return err
	}
	return nil
}

func TestParquetUpload(t *testing.T) {
	if err := RegisterFactory("SPARK_OFFLINE", SparkOfflineStoreFactory); err != nil {
		t.Fatalf("Could not register Spark factory: %s", err)
	}
	emrConf := EMRConfig{
		AWSAccessKeyId: os.Getenv("AWS_ACCESS_KEY_ID"),
		AWSSecretKey:   os.Getenv("AWS_SECRET_KEY"),
		ClusterRegion:  os.Getenv("AWS_EMR_CLUSTER_REGION"),
		ClusterName:    os.Getenv("AWS_EMR_CLUSTER_ID"),
	}
	emrSerializedConfig := emrConf.Serialize()
	s3Conf := S3Config{
		AWSAccessKeyId: os.Getenv("AWS_ACCESS_KEY_ID"),
		AWSSecretKey:   os.Getenv("AWS_SECRET_ACCESS_KEY"),
		BucketRegion:   os.Getenv("S3_BUCKET_REGION"),
		BucketPath:     os.Getenv("S3_BUCKET_PATH"),
	}
	s3SerializedConfig := s3Conf.Serialize()
	SparkOfflineConfig := SparkConfig{
		ExecutorType:   EMR,
		ExecutorConfig: string(emrSerializedConfig),
		StoreType:      S3,
		StoreConfig:    string(s3SerializedConfig),
	}
	sparkSerializedConfig := SparkOfflineConfig.Serialize()
	sparkProvider, err := Get("SPARK_OFFLINE", sparkSerializedConfig)
	if err != nil {
		t.Fatalf("Could not create spark provider: %s", err)
	}
	sparkStore, err := sparkProvider.AsOfflineStore()
	if err != nil {
		t.Fatalf("Could not convert spark provider to offline store: %s", err)
	}
	sparkOfflineStore := sparkStore.(*SparkOfflineStore)
	if err := testTableUploadCompare(sparkOfflineStore); err != nil {
		t.Fatalf("Upload test failed: %s", err)
	}
}
