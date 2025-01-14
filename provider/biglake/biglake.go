// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package biglake

import (
	"context"
	"fmt"
	"io/ioutil"

	"github.com/featureform/fferr"
	"github.com/featureform/filestore"
	"github.com/featureform/logging"
	"github.com/featureform/provider/location"
	"github.com/featureform/provider/types"
	"github.com/featureform/provider/spark"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	biglakeapi "cloud.google.com/go/bigquery/biglake/apiv1"
	// biglakepb "cloud.google.com/go/bigquery/biglake/apiv1/biglakepb"
)

type BiglakeSparkFileStore struct {
	basePath   filestore.Filepath
	gcsClient  *storage.Client
	blClient   *biglakeapi.MetastoreClient
	projectID  string
	region     string
	ctx        context.Context
	logger     logging.Logger
}

type SparkFileStoreConfig struct {
	// ProjectID that contains the biglake metastore
	ProjectID string
	// Region in Google Cloud to use for the biglake catalog.
	Region    string
	// Bucket to read and write from.
	Bucket    string
	// BaseDir is the directory to work in. paths in CreateFilePath will append to this base.
	BaseDir   string
	// CredsPath is optional. It should point to a file with the JSON creds at this path,
	// otherwise defaults to the default auth chain.
	CredsPath string
	// Logger to use.
	Logger    logging.Logger
}

func NewSparkFileStore(ctx context.Context, cfg SparkFileStoreConfig) (
	*BiglakeSparkFileStore, error,
) {
	logger := cfg.Logger.With(
		"bucket", cfg.Bucket,
		"base-dir",  cfg.BaseDir,
		"creds-path", cfg.CredsPath,
	)
	logger.Debug("Creating biglake spark filestore")
	path, err := filestore.NewGCSFilepath(cfg.Bucket, cfg.BaseDir, true)
	if err != nil {
		msg := "Failed to create base GCS filestore filepath"
		logger.Errorw(msg, "err", err)
		return nil, fferr.NewInternalErrorf("%s: %w", msg, err)
	}
	opts := []option.ClientOption{}
	if cfg.CredsPath != "" {
		logger.Debug("Adding creds path option")
		opts = append(opts, option.WithCredentialsFile(cfg.CredsPath))
	}
	gcsClient, err := storage.NewClient(ctx, opts...)
	if err != nil {
		msg := "Failed to create new storage client"
		logger.Errorw(msg, "err", err)
		return nil, fferr.NewInternalErrorf("%s: %w", msg, err)
	}
	logger.Info("Succesfully created biglake spark filestore")
	return &BiglakeSparkFileStore{
		ctx: ctx,
		gcsClient:     gcsClient,
		logger: logger,
		basePath: path,
	}, nil
}

func (bl *BiglakeSparkFileStore) CreateFilePath(path string, isDir bool) (filestore.Filepath, error) {
	logger := bl.logger.With("append-path", path, "is-dir", isDir)
	logger.Debug("Creating filepath")
	filepath := bl.basePath.Clone()
	if err := filepath.AppendPathString(path, isDir); err != nil {
		msg := "Failed to append path string"
		logger.Errorw(msg, "err", err)
		return nil, fferr.NewInternalErrorf("%s: %w", msg, err)
	}
	return filepath, nil
}
 
func (bl *BiglakeSparkFileStore) Write(path filestore.Filepath, data []byte) error {
	logger := bl.logger.With("write-path", path.ToURI())
	logger.Info("Writing to GCS file store")
	wc := bl.objectHandle(path).NewWriter(bl.ctx)
	defer func() {
		// If we don't put this in a func then wc.Close will be applied before
		// the defer is called.
		logger.LogIfErr("Failed to close GCS writer", wc.Close())
	}()

	if _, err := wc.Write(data); err != nil {
		msg := "Failed to write to GCS file store"
		logger.Errorw(msg, "err", err)
		return fferr.NewInternalErrorf("%s: %w", msg, err)
	}
	logger.Infow("Successfully wrote file")
	return nil
}

func (bl *BiglakeSparkFileStore) Read(path filestore.Filepath) ([]byte, error) {
	logger := bl.logger.With("read-path", path.ToURI())
	rc, err := bl.objectHandle(path).NewReader(bl.ctx)
	if err != nil {
		msg := "Failed to open GCS reader"
		logger.Errorw(msg, "err", err)
		return nil, fferr.NewInternalErrorf("%s: %w", msg, err)
	}
	defer logger.LogIfErr("Failed to close GCS reader", rc.Close())

	logger.Debug("Beginning to read all of file")
	data, err := ioutil.ReadAll(rc)
	if err != nil {
		msg := "Failed to read from GCS file store"
		logger.Errorw(msg, "err", err)
		return nil, fferr.NewInternalErrorf("%s: %w", msg, err)
	}
	logger.Infow("Successfully read file")
	return data, nil
}

func (bl *BiglakeSparkFileStore) Delete(path filestore.Filepath) error {
	logger := bl.logger.With("delete-path", path.ToURI())
	if err := bl.objectHandle(path).Delete(bl.ctx); err != nil {
		msg := "Failed to delete GCS file"
		logger.Errorw(msg, "err", err)
		return fferr.NewInternalErrorf("%s: %w", msg, err)
	}
	return nil
}

func (bl *BiglakeSparkFileStore) Close() error {
	err := bl.gcsClient.Close()
	bl.logger.LogIfErr("Failed to close client", err)
	return err
}

func (bl *BiglakeSparkFileStore) Exists(loc location.Location) (bool, error) {
	logger := bl.logger.With("exists-location", loc)
	logger.Info("Checking if path exists")
	switch typedLoc := loc.(type) {
	case *location.CatalogLocation:
		panic("TODO")
	case *location.FileStoreLocation:
		return bl.existsInGCS(typedLoc.Filepath())
	default:
		panic("TODO")
	}
	return true, nil
}

func (bl *BiglakeSparkFileStore) existsInBigLake(loc location.Location) (bool, error) {
	panic("TODO")
}

func (bl *BiglakeSparkFileStore) existsInGCS(path filestore.Filepath) (bool, error) {
	logger := bl.logger.With("exists-gcs-path", path.ToURI())
	logger.Debugw("Checking if path exists in GCS")
	_, err := bl.objectHandle(path).Attrs(bl.ctx)
	if err == storage.ErrObjectNotExist {
		logger.Info("Path does not exist in GCS")
		return false, nil
	}
	if err != nil {
		msg := "Failed to check if path exists in GCS"
		logger.Errorw(msg, "err", err)
		return false, fferr.NewInternalErrorf("%s: %w", msg, err)
	}
	logger.Info("Path exists in GCS")
	return true, nil
}

// // CreateCatalog creates a BigLake catalog if it doesn't already exist.
// func (bl *BiglakeSparkFileStore) createCatalog(loc location.Location) error {
// 	catalogPath := fmt.Sprintf("projects/%s/locations/%s/catalogs/%s", g.projectID, location, catalogName)
// 
// 	// Request to create the catalog
// 	req := &metastore.Catalog{
// 		Name: catalogPath,
// 	}
// 
// 	_, err := g.metastore.Projects.Locations.Catalogs.Create(fmt.Sprintf("projects/%s/locations/%s", g.projectID, location), req).Do()
// 	if err != nil {
// 		return fmt.Errorf("failed to create catalog %s: %w", catalogName, err)
// 	}
// 
// 	return nil
// }
// 
// // DeleteCatalog deletes a BigLake catalog by its name.
// func (bl *BiglakeSparkFileStore) deleteCatalog(loc location.Location) error {
// 	catalogPath := fmt.Sprintf("projects/%s/locations/%s/catalogs/%s", g.projectID, location, catalogName)
// 
// 	err := g.metastore.Projects.Locations.Catalogs.Delete(catalogPath).Do()
// 	if err != nil {
// 		return fmt.Errorf("failed to delete catalog %s: %w", catalogName, err)
// 	}
// 
// 	return nil
// }
// 
// // CatalogExists checks if a BigLake catalog exists.
// func (bl *BiglakeSparkFileStore) catalogExists(catalogName, location string) (bool, error) {
// 	catalogPath := fmt.Sprintf("projects/%s/locations/%s/catalogs/%s", g.projectID, location, catalogName)
// 
// 	_, err := g.metastore.Projects.Locations.Catalogs.Get(catalogPath).Do()
// 	if err != nil {
// 		if isNotFoundErr(err) {
// 			return false, nil
// 		}
// 		return false, fmt.Errorf("failed to check if catalog exists: %w", err)
// 	}
// 
// 	return true, nil
// }

// formatCatalogPath returns a string in the format that the biglake expects for pointing at a catalog.
func (bl *BiglakeSparkFileStore) formatCatalogPath(catalog string) string {
	return fmt.Sprintf("projects/%s/locations/%s/catalogs/%s", bl.projectID, bl.region, catalog)
}

// Helper function to identify a "not found" error
func (bl *BiglakeSparkFileStore) isGoogleNotFoundErr(err error) bool {
	if apiErr, ok := err.(*googleapi.Error); ok && apiErr.Code == 404 {
		return true
	}
	return false
}

func (bl *BiglakeSparkFileStore) bucketHandle() *storage.BucketHandle {
	return bl.gcsClient.Bucket(bl.basePath.Bucket())
}

func (bl *BiglakeSparkFileStore) objectHandle(path filestore.Filepath) *storage.ObjectHandle {
	return bl.bucketHandle().Object(path.Key())
}

func (bl *BiglakeSparkFileStore) SparkConfigs() spark.Configs {
	return spark.Configs{}
}

func (bl *BiglakeSparkFileStore) Type() types.SparkFileStoreType {
	return types.SFS_BIGLAKE
}

func (bl *BiglakeSparkFileStore) FilestoreType() filestore.FileStoreType {
	return filestore.GCS
}
