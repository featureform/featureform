// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/featureform/helpers"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	"github.com/featureform/metadata/search"
)

type Upload struct {
	ctx          context.Context
	resourceType metadata.ResourceType
	client       *metadata.Client
	searcher     search.Searcher
	logger       logging.Logger
}

func main() {
	baseLogger := logging.NewLogger("search-loader")
	requestID, ctx, logger := baseLogger.InitializeRequestID(context.Background())
	logger.Infof("Beginning search load job with request Id: %s", requestID)

	metadataHost := helpers.GetEnv("METADATA_HOST", "localhost")
	metadataPort := helpers.GetEnv("METADATA_PORT", "8080")
	searchHost := helpers.GetEnv("MEILISEARCH_HOST", "localhost")
	searchPort := helpers.GetEnv("MEILISEARCH_PORT", "7700")
	searchApiKey := helpers.GetEnv("MEILISEARCH_APIKEY", "")

	logger.Infof("METADATA_HOST:", metadataHost)
	logger.Infof("METADATA_PORT:", metadataPort)
	logger.Infof("MEILISEARCH_HOST:", searchHost)
	logger.Infof("MEILISEARCH_PORT:", searchPort)
	logger.Infof("MEILISEARCH_APIKEY:", searchApiKey)

	searcher, err := search.NewMeilisearch(&search.MeilisearchParams{
		Host:   searchHost,
		Port:   searchPort,
		ApiKey: searchApiKey,
	})
	if err != nil {
		logger.Panicw("Failed to create meilisearch connection", err)
	}

	metadataAddress := fmt.Sprintf("%s:%s", metadataHost, metadataPort)
	logger.Infof("Connecting to metadata address: %s\n", metadataAddress)

	client, err := metadata.NewClient(metadataAddress, logger)
	if err != nil {
		logger.Panicw("Failed to connect", "error", err)
	}

	resourceTypes := []metadata.ResourceType{
		metadata.TRAINING_SET,
		metadata.FEATURE,
		metadata.ENTITY,
		metadata.LABEL,
		metadata.MODEL,
		metadata.SOURCE,
		metadata.PROVIDER,
	}

	var wg sync.WaitGroup
	for _, resourceTypeItem := range resourceTypes {
		logger.Infof("Looping...", resourceTypeItem.String())
		wg.Add(1)
		go func(resourceType metadata.ResourceType) {
			defer wg.Done()
			up := Upload{
				ctx:          ctx,
				resourceType: resourceType,
				client:       client,
				searcher:     searcher,
				logger:       logger,
			}
			fetchAndUpload(up)
		}(resourceTypeItem)
	}
	wg.Wait()
	logger.Info("The search loader job is complete!")
}

func fetchAndUpload(up Upload) {
	switch up.resourceType {
	case metadata.TRAINING_SET:
		records, err := up.client.ListTrainingSets(up.ctx)
		if err != nil {
			up.logger.Error(err)
			return
		}
		up.logger.Infof(up.resourceType.String(), "count: %d", len(records))
		for _, rec := range records {
			variants, err := up.client.GetTrainingSetVariants(up.ctx, rec.NameVariants())
			if err != nil {
				up.logger.Errorw("failed to loop through training set variants", "error", err)
				return
			}
			for _, variant := range variants {
				doc := getDocument(variant.Name(), variant.Variant(), up.resourceType.String(), variant.Tags())
				up.searcher.Upsert(doc)
			}
		}
	case metadata.FEATURE:
		records, err := up.client.ListFeatures(up.ctx)
		if err != nil {
			up.logger.Error(err)
			return
		}
		up.logger.Infof(up.resourceType.String(), "count: %d", len(records))
		for _, rec := range records {
			variants, err := up.client.GetFeatureVariants(up.ctx, rec.NameVariants())
			if err != nil {
				up.logger.Errorw("failed to loop through feature variants", "error", err)
				return
			}
			for _, variant := range variants {
				doc := getDocument(variant.Name(), variant.Variant(), up.resourceType.String(), variant.Tags())
				up.searcher.Upsert(doc)
			}
		}
	case metadata.ENTITY:
		records, err := up.client.ListEntities(up.ctx)
		if err != nil {
			up.logger.Error(err)
			return
		}
		up.logger.Infof(up.resourceType.String(), "count: %d", len(records))
		for _, rec := range records {
			doc := getDocument(rec.Name(), rec.Variant(), up.resourceType.String(), rec.Tags())
			up.searcher.Upsert(doc)
		}
	case metadata.LABEL:
		records, err := up.client.ListLabels(up.ctx)
		if err != nil {
			up.logger.Error(err)
			return
		}
		up.logger.Infof(up.resourceType.String(), "count: %d", len(records))
		for _, rec := range records {
			variants, err := up.client.GetLabelVariants(up.ctx, rec.NameVariants())
			if err != nil {
				up.logger.Errorw("failed to loop through label variants", "error", err)
				return
			}
			for _, variant := range variants {
				doc := getDocument(variant.Name(), variant.Variant(), up.resourceType.String(), variant.Tags())
				up.searcher.Upsert(doc)
			}
		}
	case metadata.MODEL:
		records, err := up.client.ListModels(up.ctx)
		if err != nil {
			up.logger.Error(err)
			return
		}
		up.logger.Infof(up.resourceType.String(), "count: %d", len(records))
		for _, rec := range records {
			doc := getDocument(rec.Name(), rec.Variant(), up.resourceType.String(), rec.Tags())
			up.searcher.Upsert(doc)
		}
	case metadata.SOURCE:
		records, err := up.client.ListSources(up.ctx)
		if err != nil {
			up.logger.Error(err)
			return
		}
		up.logger.Infof(up.resourceType.String(), "count: %d", len(records))
		for _, rec := range records {
			variants, err := up.client.GetSourceVariants(up.ctx, rec.NameVariants())
			if err != nil {
				up.logger.Errorw("failed to loop through source variants", "error", err)
				return
			}
			for _, variant := range variants {
				doc := getDocument(variant.Name(), variant.Variant(), up.resourceType.String(), variant.Tags())
				up.searcher.Upsert(doc)
			}
		}
	case metadata.PROVIDER:
		records, err := up.client.ListProviders(up.ctx)
		if err != nil {
			up.logger.Error(err)
			return
		}
		up.logger.Infof(up.resourceType.String(), "count: %d", len(records))
		for _, rec := range records {
			doc := getDocument(rec.Name(), rec.Variant(), up.resourceType.String(), rec.Tags())
			up.searcher.Upsert(doc)
		}
	default:
		up.logger.Warnf("no case matches type:", up.resourceType.String())
		return
	} //switch
}

func getDocument(name, variant, resType string, tags metadata.Tags) search.ResourceDoc {
	doc := search.ResourceDoc{
		Name:    name,
		Variant: variant,
		Type:    resType,
		Tags:    tags,
	}
	return doc
}
