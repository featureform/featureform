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

	defs := []metadata.ResourceDef{
		metadata.UserDef{
			Name: "Simba Khadder",
		},
		metadata.ProviderDef{
			Name:        "demo-s3",
			Description: "local S3 deployment",
			Type:        "Batch",
			Software:    "BigQuery",
			Team:        "",
		},
		metadata.ProviderDef{
			Name:        "demo-redis",
			Description: "Bitnami redis deployment",
			Type:        "Online",
			Software:    "Redis",
			Team:        "",
		},
		metadata.EntityDef{
			Name:        "user",
			Description: "user description",
		},
		metadata.SourceDef{
			Name:        "Transactions",
			Variant:     "default",
			Description: "Source of user transactions",
			Type:        "CSV",
			Owner:       "Simba Khadder",
			Provider:    "demo-redis",
		},
		metadata.LabelDef{
			Name:        "is_fraud",
			Variant:     "default",
			Description: "if a transaction is fraud",
			Type:        "boolean",
			Source:      metadata.NameVariant{"Transactions", "default"},
			Entity:      "user",
			Owner:       "Simba Khadder",
			Provider:    "demo-redis",
		},
		metadata.FeatureDef{
			Name:        "number_of_fraud",
			Variant:     "90d",
			Source:      metadata.NameVariant{"Transactions", "default"},
			Type:        "int",
			Entity:      "user",
			Owner:       "Simba Khadder",
			Description: "Number of fraud transactions in the last 90 days.",
			Provider:    "demo-redis",
		},
		metadata.FeatureDef{
			Name:        "user_2fa",
			Variant:     "default",
			Source:      metadata.NameVariant{"Transactions", "default"},
			Type:        "boolean",
			Entity:      "user",
			Owner:       "Simba Khadder",
			Description: "If user has 2fa",
			Provider:    "demo-redis",
		},
		metadata.FeatureDef{
			Name:        "user_account_age",
			Variant:     "default",
			Source:      metadata.NameVariant{"Transactions", "default"},
			Type:        "int",
			Entity:      "user",
			Owner:       "Simba Khadder",
			Description: "Seconds since the user's account was created.",
			Provider:    "demo-redis",
		},
		metadata.FeatureDef{
			Name:        "user_credit_score",
			Variant:     "default",
			Source:      metadata.NameVariant{"Transactions", "default"},
			Type:        "int",
			Entity:      "user",
			Owner:       "Simba Khadder",
			Description: "User's credit score",
			Provider:    "demo-s3",
		},
		metadata.FeatureDef{
			Name:        "user_transaction_count",
			Variant:     "30d",
			Source:      metadata.NameVariant{"Transactions", "default"},
			Type:        "int",
			Entity:      "user",
			Owner:       "Simba Khadder",
			Description: "Number of transcations the user performed in the last 30 days.",
			Provider:    "demo-redis",
		},
		metadata.FeatureDef{
			Name:        "avg_transaction_amt",
			Variant:     "default",
			Source:      metadata.NameVariant{"Transactions", "default"},
			Type:        "int",
			Entity:      "user",
			Owner:       "Simba Khadder",
			Description: "Average transaction amount",
			Provider:    "demo-redis",
		},
		metadata.FeatureDef{
			Name:        "amt_spent",
			Variant:     "30d",
			Source:      metadata.NameVariant{"Transactions", "default"},
			Type:        "int",
			Entity:      "user",
			Owner:       "Simba Khadder",
			Description: "Total amount spent in the last 30 days.",
			Provider:    "demo-redis",
		},
		metadata.FeatureDef{
			Name:        "user_transaction_count",
			Variant:     "7d",
			Source:      metadata.NameVariant{"Transactions", "default"},
			Type:        "int",
			Entity:      "user",
			Owner:       "Simba Khadder",
			Description: "Number of transcations the user performed in the last 7 days.",
			Provider:    "demo-s3",
		},
		metadata.TrainingSetDef{
			Name:        "is_fraud",
			Variant:     "default",
			Description: "if a transaction is fraud",
			Owner:       "Simba Khadder",
			Provider:    "demo-s3",
			Label:       metadata.NameVariant{"is_fraud", "default"},
			Features:    []metadata.NameVariant{{"user_transaction_count", "7d"}, {"number_of_fraud", "90d"}, {"amt_spent", "30d"}, {"avg_transaction_amt", "default"}, {"user_account_age", "default"}, {"user_credit_score", "default"}, {"user_2fa", "default"}},
		},
		metadata.ModelDef{
			Name:        "user_fraud_random_forest",
			Description: "Classifier on whether user commited fraud",
		},
	}
	if err := client.CreateAll(context.Background(), defs); err != nil {
		panic(err)
	}
	entities, err := client.ListEntities(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Printf("Entities: %+v\n", entities)

	providers, err := client.ListProviders(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Printf("Providers: %+v\n", providers)

	users, err := client.ListEntities(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Printf("Entities: %+v\n", users)

	sources, err := client.ListSources(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Printf("Sources: %+v\n", sources)

	new_providers, err := client.ListProviders(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Printf("Providers: %+v\n", new_providers)

	features, err := client.ListFeatures(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Printf("Features: %+v\n", features)

	labels, err := client.ListLabels(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Printf("Labels: %+v\n", labels)

	trainingSets, err := client.ListTrainingSets(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Printf("Training Sets: %+v\n", trainingSets)

	models, err := client.ListModels(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Printf("Models: %+v\n", models)
}
