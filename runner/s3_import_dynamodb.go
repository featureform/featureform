// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package runner

import (
	"encoding/json"
	"fmt"

	"time"

	"github.com/featureform/filestore"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/types"
	"go.uber.org/zap"
)

type S3ImportMaterializationOption struct {
	storeType  pt.Type
	outputType filestore.FileType
}

func (o S3ImportMaterializationOption) Output() filestore.FileType {
	return o.outputType
}

func (o S3ImportMaterializationOption) StoreType() pt.Type {
	return o.storeType
}

type S3ImportDynamoDBRunner struct {
	Online      provider.ImportableOnlineStore
	Offline     provider.OfflineStore
	OfflineType pt.Type
	ID          provider.ResourceID
	VType       provider.ValueType
	IsUpdate    bool // Not currently useable
	Logger      *zap.SugaredLogger
}

func (r S3ImportDynamoDBRunner) Resource() metadata.ResourceID {
	return metadata.ResourceID{
		Name:    r.ID.Name,
		Variant: r.ID.Variant,
		Type:    provider.ProviderToMetadataResourceType[r.ID.Type],
	}
}

func (r S3ImportDynamoDBRunner) IsUpdateJob() bool {
	return r.IsUpdate
}

func (r S3ImportDynamoDBRunner) Run() (types.CompletionWatcher, error) {
	r.Logger.Infow("Staring S3 import to DynamoDB materialization runner", "name", r.ID.Name, "variant", r.ID.Variant)

	if r.IsUpdate {
		return nil, fmt.Errorf("materialization updates are not implemented for S3 import to DynamoDB")
	}

	option := S3ImportMaterializationOption{
		storeType:  pt.SparkOffline,
		outputType: filestore.CSV,
	}

	mat, err := r.Offline.CreateMaterialization(r.ID, option)
	if err != nil {
		r.Logger.Errorf("failed to create materialization: %v", err)
		return nil, err
	}

	sparkOffline, ok := r.Offline.(*provider.SparkOfflineStore)
	if !ok {
		r.Logger.Errorf("offline store is not a SparkOfflineStore")
		return nil, fmt.Errorf("offline store is not a SparkOfflineStore")
	}

	// **NOTE:** Unlike ResourceID, which has methods to convert the name, variant and type of resource to and from a path,
	//  MaterializationID is a string that is already in the form of `/Materialization/<name>/<variant>`. We currently need
	// to append `featureform/` to the materialization ID to get the source dir path, but this is not ideal. We should
	// probably change the type of MaterializationID to be ResourceID.
	sourceDirPath, err := sparkOffline.Store.CreateDirPath(fmt.Sprintf("featureform/%s", mat.ID()))
	if err != nil {
		r.Logger.Errorf("failed to create source dir path for resource %s: %v", r.ID.ToFilestorePath(), err)
		return nil, err
	}

	files, err := sparkOffline.Store.List(sourceDirPath, filestore.CSV)
	if err != nil {
		r.Logger.Errorf("failed to list files in source dir path %s: %v", sourceDirPath, err)
		return nil, err
	}

	if len(files) == 0 {
		r.Logger.Errorf("no files found in source dir path %s", sourceDirPath)
		return nil, fmt.Errorf("no files found in source dir path %s", sourceDirPath)
	}

	sourceFile := files[0]

	r.Logger.Debugw("Source file", "URI", sourceFile.ToURI(), "prefix", sourceFile.KeyPrefix(), "ext", sourceFile.Ext())

	r.Logger.Debugw("Importing table to DynamoDB", "name", r.ID.Name, "variant", r.ID.Variant, "vtype", r.VType, "file", files[0])
	importArn, err := r.Online.ImportTable(r.ID.Name, r.ID.Variant, r.VType, files[0])
	if err != nil {
		r.Logger.Errorf("failed to import table: %v", err)
		return nil, err
	}

	r.Logger.Debugw("Waiting for import to complete", "importID", importArn)

	watcher := &S3ImportCompletionWatcher{
		status:    "PENDING",
		store:     r.Online,
		importArn: importArn,
		logger:    logging.NewLogger("s3importWatcher"),
	}

	watcher.Poll()

	return watcher, nil
}

type S3ImportCompletionWatcher struct {
	status    string
	err       error
	store     provider.ImportableOnlineStore
	importArn provider.ImportID
	logger    *zap.SugaredLogger
}

func (w *S3ImportCompletionWatcher) Poll() {
	go func() {
		for {
			s3Import, err := w.store.GetImport(w.importArn)
			if err != nil {
				w.logger.Errorf("failed to get import status: %v", err)
				return
			}
			w.logger.Debugw("Import status", "status", s3Import.Status())
			if s3Import.Status() == "COMPLETED" {
				w.logger.Infow("Import completed", "importID", w.importArn)
				w.status = "COMPLETED"
			}
			if s3Import.Status() == "FAILED" {
				w.logger.Infow("Import failed", "importID", w.importArn, "error", s3Import.ErrorMessage())
				w.status = "FAILED"
				w.err = fmt.Errorf("import %s failed: %s", w.importArn, s3Import.ErrorMessage())
			}
			time.Sleep(90 * time.Second)
		}
	}()
}

func (w *S3ImportCompletionWatcher) Wait() error {
	for {
		switch w.status {
		case "COMPLETED":
			w.logger.Infow("Changing S3 import watcher status to COMPLETED", "importID", w.importArn)
			return nil
		case "FAILED":
			w.logger.Infow("Changing S3 import watcher status to FAILED", "importID", w.importArn)
			return w.err
		default:
			// Sleep for a short duration to prevent busy waiting
			time.Sleep(120 * time.Second)
		}
	}
}

func (w *S3ImportCompletionWatcher) Err() error {
	return w.err
}

func (w *S3ImportCompletionWatcher) String() string {
	return fmt.Sprintf("S3 import %s: %s", w.importArn, w.status)
}

func (w *S3ImportCompletionWatcher) Complete() bool {
	return w.status == "COMPLETED"
}

type S3ImportDynamoDBRunnerConfig struct {
	OnlineType    pt.Type
	OfflineType   pt.Type
	OnlineConfig  pc.SerializedConfig
	OfflineConfig pc.SerializedConfig
	ResourceID    provider.ResourceID
	VType         provider.ValueTypeJSONWrapper
	Cloud         JobCloud
	IsUpdate      bool
}

func (cfg *S3ImportDynamoDBRunnerConfig) Serialize() (Config, error) {
	config, err := json.Marshal(cfg)
	if err != nil {
		panic(err)
	}
	return config, nil
}

func (cfg *S3ImportDynamoDBRunnerConfig) Deserialize(config Config) error {
	err := json.Unmarshal(config, cfg)
	if err != nil {
		return err
	}
	return nil
}

func S3ImportDynamoDBRunnerFactory(config Config) (types.Runner, error) {
	runnerConfig := &S3ImportDynamoDBRunnerConfig{}
	if err := runnerConfig.Deserialize(config); err != nil {
		return nil, fmt.Errorf("failed to deserialize materialize runner config: %v", err)
	}
	onlineProvider, err := provider.Get(runnerConfig.OnlineType, runnerConfig.OnlineConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to configure %s provider: %v", runnerConfig.OnlineType, err)
	}
	offlineProvider, err := provider.Get(runnerConfig.OfflineType, runnerConfig.OfflineConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to configure %s provider: %v", runnerConfig.OfflineType, err)
	}
	onlineStore, err := onlineProvider.AsOnlineStore()
	if err != nil {
		return nil, fmt.Errorf("failed to convert provider to online store: %v", err)
	}
	importableOfflineStore, ok := onlineStore.(provider.ImportableOnlineStore)
	if !ok {
		return nil, fmt.Errorf("online store is not importable")
	}
	offlineStore, err := offlineProvider.AsOfflineStore()
	if err != nil {
		return nil, fmt.Errorf("failed to convert provider to offline store: %v", err)
	}
	return &S3ImportDynamoDBRunner{
		Online:   importableOfflineStore,
		Offline:  offlineStore,
		ID:       runnerConfig.ResourceID,
		VType:    runnerConfig.VType.ValueType,
		IsUpdate: runnerConfig.IsUpdate,
		Logger:   logging.NewLogger("s3importer"),
	}, nil
}
