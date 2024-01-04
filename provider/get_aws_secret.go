// Use this code snippet in your app.
// If you need more information about configurations or implementing the sample code, visit the AWS docs:
// https://aws.github.io/aws-sdk-go-v2/docs/getting-started/
package provider

import (
	"context"
	"fmt"
	"log"
	"os"

	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

func GetSecrets(secretName string) map[string]string {
	region := os.Getenv("AWS_REGION")

	config, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
		log.Fatal("Config error: ", err)
	}

	// Create Secrets Manager client
	svc := secretsmanager.NewFromConfig(config)

	input := &secretsmanager.GetSecretValueInput{
		SecretId:     aws.String(secretName),
		VersionStage: aws.String("AWSCURRENT"), // VersionStage defaults to AWSCURRENT if unspecified
	}

	result, err := svc.GetSecretValue(context.TODO(), input)
	if err != nil {
		log.Fatal("Result error: ", err.Error())
	}

	// Decrypts secret using the associated KMS key.
	secretString := *result.SecretString

	var secret map[string]string
	// Unmarshal the JSON string into the map
	unmarshalErr := json.Unmarshal([]byte(secretString), &secret)
	if unmarshalErr != nil {
		fmt.Println("Error:", err)
		return nil
	}

	return secret
}

/*
All provider variables

REDIS_SECURE_PORT
REDIS_PASSWORD
REDIS_INSECURE_PORT

SNOWFLAKE_USERNAME
SNOWFLAKE_PASSWORD
SNOWFLAKE_ORG
SNOWFLAKE_ACCOUNT

REDSHIFT_ENDPOINT
REDSHIFT_PORT
REDSHIFT_USERNAME
REDSHIFT_PASSWORD

POSTGRES_DB
POSTGRES_USER
POSTGRES_PASSWORD

PINECONE_PROJECT_ID
PINECONE_ENVIRONMENT
PINECONE_API_KEY

MONGODB_HOST
MONGODB_PORT
MONGODB_USERNAME
MONGODB_PASSWORD
MONGODB_DATABASE

FIRESTORE_PROJECT
FIRESTORE_CRED

DYNAMO_ACCESS_KEY
DYNAMO_SECRET_KEY

BIGQUERY_CREDENTIALS
BIGQUERY_PROJECT_ID

CASSANDRA_PASSWORD
CASSANDRA_USER
*/
