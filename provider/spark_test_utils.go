// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"testing"

	fs "github.com/featureform/filestore"
	"github.com/featureform/helpers"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

func GetTestingBlobDatabricks(t *testing.T) *SparkOfflineStore {
	err := godotenv.Load("../.env")
	if err != nil {
		t.Logf("could not open .env file... Checking environment: %s", err)
	}
	azureConfig := &pc.AzureFileStoreConfig{
		AccountName:   helpers.MustGetTestingEnv(t, "AZURE_ACCOUNT_NAME"),
		AccountKey:    helpers.MustGetTestingEnv(t, "AZURE_ACCOUNT_KEY"),
		ContainerName: helpers.MustGetTestingEnv(t, "AZURE_CONTAINER_NAME"),
		Path:          helpers.MustGetTestingEnv(t, "AZURE_CONTAINER_PATH"),
	}
	return getTestingDatabricks(t, azureConfig, fs.Azure)
}

func GetTestingS3Databricks(t *testing.T) *SparkOfflineStore {
	err := godotenv.Load("../.env")
	if err != nil {
		t.Logf("could not open .env file... Checking environment: %s", err)
	}
	s3Config := &pc.S3FileStoreConfig{
		Credentials:  getTestingAWSCreds(t),
		BucketRegion: helpers.MustGetTestingEnv(t, "S3_BUCKET_REGION"),
		BucketPath:   helpers.MustGetTestingEnv(t, "S3_BUCKET_PATH"),
		Path:         uuid.NewString(),
	}
	t.Logf("S3 CONFIG TestingS3Databricks: %+v\n", s3Config)
	return getTestingDatabricks(t, s3Config, fs.S3)
}

func getTestingDatabricks(t *testing.T, cfg SparkFileStoreConfig, fst fs.FileStoreType) *SparkOfflineStore {
	databricksConfig := pc.DatabricksConfig{
		Username: helpers.GetEnv("DATABRICKS_USERNAME", ""),
		Password: helpers.GetEnv("DATABRICKS_PASSWORD", ""),
		Host:     helpers.MustGetTestingEnv(t, "DATABRICKS_HOST"),
		Token:    helpers.GetEnv("DATABRICKS_TOKEN", ""),
		Cluster:  helpers.MustGetTestingEnv(t, "DATABRICKS_CLUSTER"),
	}
	sparkOfflineConfig := pc.SparkConfig{
		ExecutorType:   pc.Databricks,
		ExecutorConfig: &databricksConfig,
		StoreType:      fst,
		StoreConfig:    cfg,
	}
	return getTestingSparkFromConfig(t, sparkOfflineConfig)
}

func GetTestingEMRGlue(t *testing.T, tableFormat pc.TableFormat) *SparkOfflineStore {
	// TODO put these env variables into secrets
	glueConfig := &pc.GlueConfig{
		Database:    "ff",
		Warehouse:   helpers.MustGetTestingEnv(t, "GLUE_S3_WAREHOUSE"),
		Region:      helpers.MustGetTestingEnv(t, "S3_BUCKET_REGION"),
		TableFormat: tableFormat,
	}
	emrConfig := pc.EMRConfig{
		Credentials:   getTestingAWSCreds(t),
		ClusterRegion: helpers.MustGetTestingEnv(t, "AWS_EMR_CLUSTER_REGION"),
		ClusterName:   helpers.MustGetTestingEnv(t, "AWS_EMR_CLUSTER_ID"),
	}
	s3Config := &pc.S3FileStoreConfig{
		Credentials:  getTestingAWSCreds(t),
		BucketRegion: helpers.MustGetTestingEnv(t, "S3_BUCKET_REGION"),
		BucketPath:   helpers.MustGetTestingEnv(t, "S3_BUCKET_PATH"),
		Path:         uuid.NewString(),
	}
	sparkOfflineConfig := pc.SparkConfig{
		ExecutorType:   pc.EMR,
		ExecutorConfig: &emrConfig,
		StoreType:      fs.S3,
		StoreConfig:    s3Config,
		GlueConfig:     glueConfig,
	}
	return getTestingSparkFromConfig(t, sparkOfflineConfig)
}

func getTestingSparkFromConfig(t *testing.T, config pc.SparkConfig) *SparkOfflineStore {
	sparkSerializedConfig, err := config.Serialize()
	if err != nil {
		t.Fatalf("could not serialize the SparkOfflineConfig: %s", err)
	}

	sparkProvider, err := Get(pt.SparkOffline, sparkSerializedConfig)
	if err != nil {
		t.Fatalf("Could not create spark provider: %s", err)
	}
	return sparkProvider.(*SparkOfflineStore)
}

func getTestingAWSCreds(t *testing.T) pc.AWSStaticCredentials {
	return pc.AWSStaticCredentials{
		AccessKeyId: helpers.MustGetTestingEnv(t, "AWS_ACCESS_KEY_ID"),
		SecretKey:   helpers.MustGetTestingEnv(t, "AWS_SECRET_ACCESS_KEY"),
	}
}
