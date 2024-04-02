package main

import (
	"fmt"
	"github.com/featureform/ffsync"
	help "github.com/featureform/helpers"
	"github.com/featureform/metadata"
	dm "github.com/featureform/metadata/dashboard"
	"github.com/featureform/metadata/search"
	"github.com/featureform/storage"
	"go.uber.org/zap"
)

func main() {
	logger := zap.NewExample().Sugar()
	metadataHost := help.GetEnv("METADATA_HOST", "localhost")
	metadataPort := help.GetEnv("METADATA_PORT", "8080")
	searchHost := help.GetEnv("MEILISEARCH_HOST", "localhost")
	searchPort := help.GetEnv("MEILISEARCH_PORT", "7700")
	searchEndpoint := fmt.Sprintf("http://%s:%s", searchHost, searchPort)
	searchApiKey := help.GetEnv("MEILISEARCH_APIKEY", "xyz")
	logger.Infof("Connecting to typesense at: %s\n", searchEndpoint)
	sc, err := search.NewMeilisearch(&search.MeilisearchParams{
		Host:   searchHost,
		Port:   searchPort,
		ApiKey: searchApiKey,
	})
	if err != nil {
		logger.Panicw("Failed to create new meil search", err)
	}
	dm.SearchClient = sc
	metadataAddress := fmt.Sprintf("%s:%s", metadataHost, metadataPort)
	logger.Infof("Looking for metadata at: %s\n", metadataAddress)
	client, err := metadata.NewClient(metadataAddress, logger)
	if err != nil {
		logger.Panicw("Failed to connect", "error", err)
	}

	etcdHost := help.GetEnv("ETCD_HOST", "localhost")
	etcdPort := help.GetEnv("ETCD_PORT", "2379")

	etcdStore, err := storage.NewETCDStorageImplementation(etcdHost, etcdPort)
	if err != nil {
		logger.Panicw("Failed to create storage implementation", "error", err)
	}
	locker, err := ffsync.NewETCDLocker(etcdHost, etcdPort)
	if err != nil {
		logger.Panicw("Failed to create locker implementation", "error", err)
	}
	store := storage.MetadataStorage{
		Locker:  locker,
		Storage: etcdStore,
	}

	metadataServer, err := dm.NewMetadataServer(logger, client, store)
	if err != nil {
		logger.Panicw("Failed to create server", "error", err)
	}
	metadataHTTPPort := help.GetEnv("METADATA_HTTP_PORT", "3001")
	metadataServingPort := fmt.Sprintf(":%s", metadataHTTPPort)
	logger.Infof("Serving HTTP Metadata on port: %s\n", metadataServingPort)
	err = metadataServer.Start(metadataServingPort, false)
	if err != nil {
		panic(err)
	}
}
