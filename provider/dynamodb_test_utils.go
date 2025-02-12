// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"os"
	"testing"

	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/joho/godotenv"
)

func GetTestingDynamoDB(t *testing.T, tags map[string]string) OnlineStore {
	err := godotenv.Load("../.env")
	if err != nil {
		t.Logf("could not open .env file... Checking environment: %s", err)
	}
	dynamoAccessKey, ok := os.LookupEnv("DYNAMO_ACCESS_KEY")
	if !ok {
		t.Fatalf("missing DYNAMO_ACCESS_KEY variable")
	}
	dynamoSecretKey, ok := os.LookupEnv("DYNAMO_SECRET_KEY")
	if !ok {
		t.Fatalf("missing DYNAMO_SECRET_KEY variable")
	}
	awsCreds := pc.AWSStaticCredentials{
		AccessKeyId: dynamoAccessKey,
		SecretKey:   dynamoSecretKey,
	}
	endpoint := os.Getenv("DYNAMO_ENDPOINT")
	dynamoConfig := &pc.DynamodbConfig{
		Credentials:        awsCreds,
		Region:             "us-east-1",
		Endpoint:           endpoint,
		StronglyConsistent: true,
	}

	if len(tags) > 0 {
		dynamoConfig.Tags = tags
	}

	store, err := GetOnlineStore(pt.DynamoDBOnline, dynamoConfig.Serialized())
	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}
	return store
}
