package main

import (
	"context"
	"fmt"

	"github.com/featureform/serving/metadata"
	"go.uber.org/zap"
)

func main() {
	logger := zap.NewExample().Sugar()
	client, err := metadata.NewClient("localhost:8080", logger)
	if err != nil {
		logger.Panicw("Failed to connect", "Err", err)
	}

	err = client.CreateEntity(context.Background(), metadata.EntityDef{
		Name: "user",
		Description: "user description",
	})	
	if err != nil {
		logger.Panicw("Failed to create entity", "Err", err)
	}

	
	entities, err := client.ListEntities(context.Background())
	fmt.Printf("Entities: %+v\n", entities)

	err = client.CreateProvider(context.Background(), metadata.ProviderDef{
		Name: "demo-redis",
		Description: "Bitnami redis deployment",
		Type: "Online",
		Software: "Redis",
		Team: "",
	})	
	if err != nil {
		logger.Panicw("Failed to create provider", "Err", err)
	}

	err = client.CreateProvider(context.Background(), metadata.ProviderDef{
		Name: "demo-s3",
		Description: "local S3 deployment",
		Type: "Batch",
		Software: "BigQuery",
		Team: "",
	})	
	if err != nil {
		logger.Panicw("Failed to create provider", "Err", err)
	}

	
	providers, err := client.ListProviders(context.Background())
	fmt.Printf("Providers: %+v\n", providers)

	err = client.CreateUser(context.Background(), metadata.UserDef{
		Name: "Simba Khadder",
	})
	if err != nil {
		logger.Panicw("Failed to create user", "Err", err)
	}

	users, err := client.ListEntities(context.Background())
	fmt.Printf("Entities: %+v\n", users)

	err = client.CreateSourceVariant(context.Background(), metadata.SourceDef{
		Name: "Transactions",
		Variant: "default",
		Description: "Source of user transactions",
		Type: "CSV",
		Owner: "Simba Khadder",
		Provider: "demo-redis",
	})
	if err != nil {
		logger.Panicw("Failed to create source variant", "Err", err)
	}

	sources, err := client.ListSources(context.Background())
	fmt.Printf("Sources: %+v\n", sources)

	new_providers, err := client.ListProviders(context.Background())
	fmt.Printf("Providers: %+v\n", new_providers)

	err = client.CreateFeatureVariant(context.Background(), metadata.FeatureDef{
		Name: "amt_spent",
		Variant: "30d",
		Source: metadata.NameVariant {"Transactions", "default"},
		Type: "int",
		Entity: "user",
		Owner: "Simba Khadder",
		Description: "Total amount spent in the last 30 days.",
		Provider: "demo-redis",
	})
	if err != nil {
		logger.Panicw("Failed to create feature variant", "Err", err)
	}

	err = client.CreateFeatureVariant(context.Background(), metadata.FeatureDef{
		Name: "user_transaction_count",
		Variant: "7d",
		Source: metadata.NameVariant {"Transactions", "default"},
		Type: "int",
		Entity: "user",
		Owner: "Simba Khadder",
		Description: "Number of transcations the user performed in the last 7 days.",
		Provider: "demo-s3",
	})
	if err != nil {
		logger.Panicw("Failed to create feature variant", "Err", err)
	}
	err = client.CreateFeatureVariant(context.Background(), metadata.FeatureDef{
		Name: "avg_transaction_amt",
		Variant: "default",
		Source: metadata.NameVariant {"Transactions", "default"},
		Type: "int",
		Entity: "user",
		Owner: "Simba Khadder",
		Description: "Average transaction amount",
		Provider: "demo-redis",
	})
	if err != nil {
		logger.Panicw("Failed to create feature variant", "Err", err)
	}
	err = client.CreateFeatureVariant(context.Background(), metadata.FeatureDef{
		Name: "user_transaction_count",
		Variant: "30d",
		Source: metadata.NameVariant {"Transactions", "default"},
		Type: "int",
		Entity: "user",
		Owner: "Simba Khadder",
		Description: "Number of transcations the user performed in the last 30 days.",
		Provider: "demo-redis",
	})
	if err != nil {
		logger.Panicw("Failed to create feature variant", "Err", err)
	}
	err = client.CreateFeatureVariant(context.Background(), metadata.FeatureDef{
		Name: "user_credit_score",
		Variant: "default",
		Source: metadata.NameVariant {"Transactions", "default"},
		Type: "int",
		Entity: "user",
		Owner: "Simba Khadder",
		Description: "User's credit score",
		Provider: "demo-s3",
	})
	if err != nil {
		logger.Panicw("Failed to create feature variant", "Err", err)
	}
	err = client.CreateFeatureVariant(context.Background(), metadata.FeatureDef{
		Name: "user_account_age",
		Variant: "default",
		Source: metadata.NameVariant {"Transactions", "default"},
		Type: "int",
		Entity: "user",
		Owner: "Simba Khadder",
		Description: "Seconds since the user's account was created.",
		Provider: "demo-redis",
	})
	if err != nil {
		logger.Panicw("Failed to create feature variant", "Err", err)
	}

	err = client.CreateFeatureVariant(context.Background(), metadata.FeatureDef{
		Name: "user_2fa",
		Variant: "default",
		Source: metadata.NameVariant {"Transactions", "default"},
		Type: "boolean",
		Entity: "user",
		Owner: "Simba Khadder",
		Description: "If user has 2fa",
		Provider: "demo-redis",
	})
	if err != nil {
		logger.Panicw("Failed to create feature variant", "Err", err)
	}

	err = client.CreateFeatureVariant(context.Background(), metadata.FeatureDef{
		Name: "number_of_fraud",
		Variant: "90d",
		Source: metadata.NameVariant {"Transactions", "default"},
		Type: "int",
		Entity: "user",
		Owner: "Simba Khadder",
		Description: "Number of fraud transactions in the last 90 days.",
		Provider: "demo-redis",
	})
	if err != nil {
		logger.Panicw("Failed to create feature variant", "Err", err)
	}

	features, err := client.ListFeatures(context.Background())
	fmt.Printf("Features: %+v\n", features)

	err = client.CreateLabelVariant(context.Background(), metadata.LabelDef{
		Name: "is_fraud",
		Variant: "default",
		Description: "if a transaction is fraud",
		Type: "boolean",
		Source: metadata.NameVariant {"Transactions", "default"},
		Entity: "user",
		Owner: "Simba Khadder",
		Provider: "demo-redis",
	})
	if err != nil {
		logger.Panicw("Failed to create label variant", "Err", err)
	}

	labels, err := client.ListLabels(context.Background())
	fmt.Printf("Labels: %+v\n", labels)

	err = client.CreateTrainingSetVariant(context.Background(), metadata.TrainingSetDef{
		Name: "is_fraud",
		Variant: "default",
		Description: "if a transaction is fraud",
		Owner: "Simba Khadder",
		Provider: "demo-s3",
		Label: metadata.NameVariant {"is_fraud", "default"},
		Features: []metadata.NameVariant {{"user_transaction_count","7d"},{"number_of_fraud", "90d"},{"amt_spent", "30d"},{"avg_transaction_amt","default"},{"user_account_age","default"},{"user_credit_score","default"},{"user_2fa","default"}},
	})
	if err != nil {
		logger.Panicw("Failed to create training set variant", "Err", err)
	}

	trainingSets, err := client.ListTrainingSets(context.Background())
	fmt.Printf("Training Sets: %+v\n", trainingSets)

	err = client.CreateModel(context.Background(), metadata.ModelDef{
		Name: "user_fraud_random_forest",
		Description: "Classifier on whether user commited fraud",
	})
	if err != nil {
		logger.Panicw("Failed to create model", "Err", err)
	}

	models, err := client.ListModels(context.Background())
	fmt.Printf("Models: %+v\n", models)
}