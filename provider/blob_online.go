package provider

import (
	"fmt"
	"strconv"
	"time"

	"github.com/featureform/fferr"
	"github.com/featureform/filestore"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/provider/types"
)

const STORE_PREFIX = ".featureform/inferencestore"

type OnlineFileStore struct {
	FileStore
	Prefix string
	BaseProvider
}

func blobOnlineStoreFactory(serialized pc.SerializedConfig) (Provider, error) {
	onlineBlobConfig := &pc.OnlineBlobConfig{}
	if err := onlineBlobConfig.Deserialize(serialized); err != nil {
		return nil, err
	}
	return NewOnlineFileStore(onlineBlobConfig)
}

func NewOnlineFileStore(config *pc.OnlineBlobConfig) (*OnlineFileStore, error) {
	serializedBlob, err := config.Config.Serialize()
	if err != nil {
		return nil, err
	}

	FileStore, err := CreateFileStore(string(config.Type), Config(serializedBlob))
	if err != nil {
		return nil, err
	}
	return &OnlineFileStore{
		FileStore,
		config.Config.Path,
		BaseProvider{
			ProviderType:   pt.BlobOnline,
			ProviderConfig: config.Serialized(),
		},
	}, nil
}

func (store *OnlineFileStore) AsOnlineStore() (OnlineStore, error) {
	return store, nil
}

func blobTableKey(prefix, feature, variant string) string {
	return fmt.Sprintf("%s/%s/tables/%s/%s", prefix, STORE_PREFIX, feature, variant)
}

func (store OnlineFileStore) tableExists(feature, variant string) (bool, error) {
	tableKey := blobTableKey(store.Prefix, feature, variant)
	filepath := filestore.AzureFilepath{}
	if err := filepath.ParseFilePath(tableKey); err != nil {
		return false, err
	}
	return store.Exists(&filepath)
}

func (store OnlineFileStore) readTableValue(feature, variant string) (types.ValueType, error) {
	tableKey := blobTableKey(store.Prefix, feature, variant)
	filepath := filestore.AzureFilepath{}
	if err := filepath.ParseFilePath(tableKey); err != nil {
		return nil, err
	}
	value, err := store.Read(&filepath)
	if err != nil {
		return types.NilType, err
	}
	return types.ScalarType(string(value)), nil
}

func (store OnlineFileStore) writeTableValue(feature, variant string, valueType types.ValueType) error {
	tableKey := blobTableKey(store.Prefix, feature, variant)
	filepath := filestore.AzureFilepath{}
	if err := filepath.ParseFilePath(tableKey); err != nil {
		return err
	}
	return store.Write(&filepath, []byte(valueType.Scalar()))
}

func (store OnlineFileStore) deleteTable(feature, variant string) error {
	tableKey := blobTableKey(store.Prefix, feature, variant)
	entityDirectory := entityDirectory(store.Prefix, feature, variant)
	filepath := filestore.AzureFilepath{}
	if err := filepath.ParseFilePath(tableKey); err != nil {
		return err
	}
	if err := store.Delete(&filepath); err != nil {
		return err
	}
	filepath = filestore.AzureFilepath{}
	if err := filepath.ParseFilePath(entityDirectory); err != nil {
		return err
	}
	if err := store.DeleteAll(&filepath); err != nil {
		return err
	}
	return nil
}

func (store OnlineFileStore) GetTable(feature, variant string) (OnlineStoreTable, error) {
	exists, err := store.tableExists(feature, variant)
	if err != nil {
		return nil, err
	}
	if !exists {
		wrapped := fferr.NewDatasetNotFoundError(feature, variant, nil)
		wrapped.AddDetail("provider", store.ProviderType.String())
		return nil, wrapped
	}
	tableType, err := store.readTableValue(feature, variant)
	if err != nil {
		return nil, err
	}
	return OnlineFileStoreTable{store, feature, variant, store.Prefix, tableType}, nil
}

func (store OnlineFileStore) CreateTable(feature, variant string, valueType types.ValueType) (OnlineStoreTable, error) {
	exists, err := store.tableExists(feature, variant)
	if err != nil {
		return nil, err
	}
	if exists {
		wrapped := fferr.NewDatasetAlreadyExistsError(feature, variant, nil)
		wrapped.AddDetail("provider", store.ProviderType.String())
		return nil, wrapped
	}
	if err := store.writeTableValue(feature, variant, valueType); err != nil {
		return nil, err
	}
	return OnlineFileStoreTable{store, feature, variant, store.Prefix, valueType}, nil
}

func (store OnlineFileStore) CheckHealth() (bool, error) {
	return false, fferr.NewInternalError(fmt.Errorf("provider health check not implemented"))
}

type OnlineFileStoreTable struct {
	store     FileStore
	feature   string
	variant   string
	prefix    string
	valueType types.ValueType
}

func (store OnlineFileStore) DeleteTable(feature, variant string) error {
	exists, err := store.tableExists(feature, variant)
	if err != nil {
		return err
	}
	if !exists {
		wrapped := fferr.NewDatasetNotFoundError(feature, variant, nil)
		wrapped.AddDetail("provider", store.ProviderType.String())
		return wrapped
	}
	return store.deleteTable(feature, variant)
}

func entityDirectory(prefix, feature, variant string) string {
	return fmt.Sprintf("%s/%s/values/%s/%s", prefix, STORE_PREFIX, feature, variant)
}

func entityValueKey(prefix, feature, variant, entity string) string {
	return fmt.Sprintf("%s/%s", entityDirectory(prefix, feature, variant), entity)
}

func (table OnlineFileStoreTable) setEntityValue(feature, variant, entity string, value interface{}) error {
	entityValueKey := entityValueKey(table.prefix, feature, variant, entity)
	filepath := filestore.AzureFilepath{}
	if err := filepath.ParseFilePath(entityValueKey); err != nil {
		return err
	}
	valueBytes := []byte(fmt.Sprintf("%v", value.(interface{})))
	return table.store.Write(&filepath, valueBytes)
}

func (table OnlineFileStoreTable) getEntityValue(feature, variant, entity string) (interface{}, error) {
	entityValueKey := entityValueKey(table.prefix, feature, variant, entity)
	filepath := filestore.AzureFilepath{}
	if err := filepath.ParseFilePath(entityValueKey); err != nil {
		return nil, err
	}
	exists, err := table.store.Exists(&filepath)
	if err != nil {
		return nil, err
	}
	if !exists {
		wrapped := fferr.NewEntityNotFoundError(feature, variant, entity, nil)
		wrapped.AddDetail("provider", string(table.store.FilestoreType()))
		return nil, wrapped
	}

	return table.store.Read(&filepath)
}

func (table OnlineFileStoreTable) Set(entity string, value interface{}) error {
	return table.setEntityValue(table.feature, table.variant, entity, value)
}

func (table OnlineFileStoreTable) Get(entity string) (interface{}, error) {
	value, err := table.getEntityValue(table.feature, table.variant, entity)
	entityNotFoundError, ok := err.(*fferr.EntityNotFoundError)
	if ok {
		return nil, entityNotFoundError
	} else if err != nil {
		return nil, err
	}
	return castBytesToValue(value.([]byte), table.valueType)
}

func castBytesToValue(value []byte, valueType types.ValueType) (interface{}, error) {
	valueString := string(value)
	var val interface{}
	var err error
	switch valueType {
	case types.NilType, types.String:
		return valueString, nil
	case types.Int, types.Int32:
		val, err = strconv.ParseInt(valueString, 10, 32)
		return int(val.(int64)), err
	case types.Int64:
		val, err = strconv.ParseInt(valueString, 10, 64)
		return int64(val.(int64)), err
	case types.Float32:
		val, err = strconv.ParseFloat(valueString, 32)
		return float32(val.(float64)), err
	case types.Float64:
		val, err = strconv.ParseFloat(valueString, 64)
		return float64(val.(float64)), err
	case types.Bool:
		val, err = strconv.ParseBool(valueString)
		return bool(val.(bool)), err
	case types.Timestamp:
		return time.Parse(time.ANSIC, valueString)
	default:
		return nil, fferr.NewDataTypeNotFoundErrorf(valueType, "cannot cast unknown value type")
	}

}
