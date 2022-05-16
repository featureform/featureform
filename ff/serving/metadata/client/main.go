// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

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
			Name:             "demo-s3",
			Description:      "local S3 deployment",
			Type:             "Batch",
			Software:         "BigQuery",
			SerializedConfig: []byte("OFFLINE CONFIG"),
			Team:             "Fraud detection",
		},
		metadata.ProviderDef{
			Name:             "demo-redis",
			Description:      "Bitnami redis deployment",
			Type:             "Online",
			Software:         "Redis",
			SerializedConfig: []byte("ONLINE CONFIG"),
			Team:             "Fraud detection",
		},
		metadata.ProviderDef{
			Name:             "demo-postgres",
			Description:      "Postgres deployment",
			Type:             "Offline",
			Software:         "PostgreSQL",
			SerializedConfig: []byte("OFFLINE CONFIG"),
			Team:             "Fraud detection",
		},
		metadata.EntityDef{
			Name:        "user",
			Description: "user description",
		},
		metadata.SourceDef{
			Name:        "Transactions",
			Variant:     "default",
			Description: "Source of user transactions",
			Definition: metadata.PrimaryDataSource{
				Location: metadata.SQLTable{
					Name: "transactions_table",
				},
			},
			Owner:    "Simba Khadder",
			Provider: "demo-postgres",
		},
		metadata.SourceDef{
			Name:        "Transactions",
			Variant:     "transformation",
			Description: "user transactions transformation",
			Definition: metadata.TransformationSource{
				TransformationType: metadata.SQLTransformationType{
					Query: "SELECT * FROM {{Transactions.default}}",
					Sources: []metadata.NameVariant{{
						Name:    "Transactions",
						Variant: "default"},
					},
				},
			},
			Owner:    "Simba Khadder",
			Provider: "demo-postgres",
		},
		metadata.LabelDef{
			Name:        "is_fraud",
			Variant:     "default",
			Description: "if a transaction is fraud",
			Type:        "boolean",
			Source:      metadata.NameVariant{"Transactions", "default"},
			Entity:      "user",
			Owner:       "Simba Khadder",
			Location: metadata.ResourceVariantColumns{
				Entity: "entity",
				Value:  "is_fraud",
				TS:     "ts",
			},
			Provider: "demo-postgres",
		},
		metadata.FeatureDef{
			Name:        "number_of_fraud",
			Variant:     "90d",
			Source:      metadata.NameVariant{"Transactions", "default"},
			Type:        "int",
			Entity:      "user",
			Owner:       "Simba Khadder",
			Description: "Number of fraud transactions in the last 90 days.",
			Location: metadata.ResourceVariantColumns{
				Entity: "entity",
				Value:  "number_of_fraud",
				TS:     "ts",
			},
			Provider: "demo-redis",
		},
		metadata.FeatureDef{
			Name:        "user_2fa",
			Variant:     "default",
			Source:      metadata.NameVariant{"Transactions", "default"},
			Type:        "boolean",
			Entity:      "user",
			Owner:       "Simba Khadder",
			Description: "If user has 2fa",
			Location: metadata.ResourceVariantColumns{
				Entity: "entity",
				Value:  "user_2fa",
				TS:     "ts",
			},
			Provider: "demo-redis",
		},
		metadata.FeatureDef{
			Name:        "user_account_age",
			Variant:     "default",
			Source:      metadata.NameVariant{"Transactions", "default"},
			Type:        "int",
			Entity:      "user",
			Owner:       "Simba Khadder",
			Description: "Seconds since the user's account was created.",
			Location: metadata.ResourceVariantColumns{
				Entity: "entity",
				Value:  "user_account_age",
				TS:     "ts",
			},
			Provider: "demo-redis",
		},
		metadata.FeatureDef{
			Name:        "user_credit_score",
			Variant:     "default",
			Source:      metadata.NameVariant{"Transactions", "default"},
			Type:        "int",
			Entity:      "user",
			Owner:       "Simba Khadder",
			Description: "User's credit score",
			Location: metadata.ResourceVariantColumns{
				Entity: "entity",
				Value:  "user_credit_score",
				TS:     "ts",
			},
			Provider: "demo-redis",
		},
		metadata.FeatureDef{
			Name:        "user_transaction_count",
			Variant:     "30d",
			Source:      metadata.NameVariant{"Transactions", "default"},
			Type:        "int",
			Entity:      "user",
			Owner:       "Simba Khadder",
			Description: "Number of transcations the user performed in the last 30 days.",
			Location: metadata.ResourceVariantColumns{
				Entity: "entity",
				Value:  "user_transaction_count",
				TS:     "ts",
			},
			Provider: "demo-redis",
		},
		metadata.FeatureDef{
			Name:        "avg_transaction_amt",
			Variant:     "default",
			Source:      metadata.NameVariant{"Transactions", "default"},
			Type:        "int",
			Entity:      "user",
			Owner:       "Simba Khadder",
			Description: "Average transaction amount",
			Location: metadata.ResourceVariantColumns{
				Entity: "entity",
				Value:  "avg_transaction_amt",
				TS:     "ts",
			},
			Provider: "demo-redis",
		},
		metadata.FeatureDef{
			Name:        "amt_spent",
			Variant:     "30d",
			Source:      metadata.NameVariant{"Transactions", "default"},
			Type:        "int",
			Entity:      "user",
			Owner:       "Simba Khadder",
			Description: "Total amount spent in the last 30 days.",
			Location: metadata.ResourceVariantColumns{
				Entity: "entity",
				Value:  "amt_spent",
				TS:     "ts",
			},
			Provider: "demo-redis",
		},
		metadata.FeatureDef{
			Name:        "user_transaction_count",
			Variant:     "7d",
			Source:      metadata.NameVariant{"Transactions", "default"},
			Type:        "int",
			Entity:      "user",
			Owner:       "Simba Khadder",
			Description: "Number of transcations the user performed in the last 7 days.",
			Location: metadata.ResourceVariantColumns{
				Entity: "entity",
				Value:  "user_transaction_count",
				TS:     "ts",
			},
			Provider: "demo-redis",
		},
		metadata.TrainingSetDef{
			Name:        "is_fraud",
			Variant:     "default",
			Description: "if a transaction is fraud",
			Owner:       "Simba Khadder",
			Label:       metadata.NameVariant{"is_fraud", "default"},
			Features:    []metadata.NameVariant{{"user_transaction_count", "7d"}, {"number_of_fraud", "90d"}, {"amt_spent", "30d"}, {"avg_transaction_amt", "default"}, {"user_account_age", "default"}, {"user_credit_score", "default"}, {"user_2fa", "default"}},
			Provider:    "demo-postgres",
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
