package tasks

import (
	"context"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	pt "github.com/featureform/provider/provider_type"
)

type providerFetcher interface {
	FetchProvider(*metadata.Client, context.Context) (*metadata.Provider, error)
}

func getStore(baseTask BaseTask, client *metadata.Client, pf providerFetcher, logger logging.Logger) (provider.OfflineStore, error) {
	if err := client.Tasks.AddRunLog(baseTask.taskDef.TaskId, baseTask.taskDef.ID, "Fetching Offline Store..."); err != nil {
		logger.Warnw("Failed to add run log", "error", err)
	}

	providerEntry, err := pf.FetchProvider(client, context.Background())
	if err != nil {
		logger.Errorw("Failed to fetch provider", "error", err)
		return nil, err
	}

	p, err := provider.Get(pt.Type(providerEntry.Type()), providerEntry.SerializedConfig())
	if err != nil {
		logger.Errorw("Failed to get provider", "error", err)
		return nil, err
	}

	store, err := p.AsOfflineStore()
	if err != nil {
		logger.Errorw("Retrieved provider is not an offline store", "provider-type", p.Type(), "error", err)
		return nil, err
	}

	return store, nil
}
