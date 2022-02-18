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
		Name:        "f1",
		Description: "desc",
	})
	if err != nil {
		logger.Panicw("Failed to create entity", "Err", err)
	}
	entities, err := client.ListEntities(context.Background())
	fmt.Printf("%+v\n", entities)
	logger.Infow("Listed Entities", "Entities", entities, "Err", err)
	err = client.CreateUser(context.Background(), metadata.UserDef{
		Name: "f1",
	})
	if err != nil {
		logger.Panicw("Failed to create user", "Err", err)
	}
	users, err := client.ListUsers(context.Background())
	fmt.Printf("%+v\n", users)
	logger.Infow("Listed Users", "Users", users, "Err", err)
	err = client.CreateFeatureVariant(context.Background(), metadata.FeatureDef{
		Name:        "f1",
		Variant:     "v1",
		Source:      metadata.NameVariant{"Users", "V1"},
		Type:        "int",
		Entity:      "users",
		Owner:       "simba",
		Description: "Our first feature",
	})
	if err != nil {
		logger.Panicw("Failed to create feature", "Err", err)
	}
	features, err := client.ListFeatures(context.Background())
	fmt.Printf("%+v\n", features)
	logger.Infow("Listed features", "Features", features, "Err", err)
}
