package provider

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

type OnlineBlobConfig struct {
	Type   BlobStoreType
	Config BlobStoreConfig
}

func (online OnlineBlobConfig) Serialized() SerializedConfig {
	config, err := json.Marshal(online)
	if err != nil {
		panic(err)
	}
	return config
}

func (online OnlineBlobConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, online)
	if err != nil {
		return err
	}
	return nil
}

type OnlineBlobStore struct {
	BlobStore
	BaseProvider
}

func blobOnlineStoreFactory(serialized SerializedConfig) (Provider, error) {
	onlineBlobConfig := &OnlineBlobConfig{}
	if err := onlineBlobConfig.Deserialize(serialized); err != nil {
		return nil, err
	}
	return NewOnlineBlobStore(onlineBlobConfig)
}

func NewOnlineBlobStore(config *OnlineBlobConfig) (*OnlineBlobStore, error) {
	blobStore, err := CreateBlobStore(string(config.Type), Config(config.Config))
	if err != nil {
		return nil, fmt.Errorf("could not create blob store: %v", err)
	}
	return &OnlineBlobStore{
		blobStore,
		BaseProvider{
			ProviderType:   BlobOnline,
			ProviderConfig: config.Serialized(),
		},
	}, nil
}

func (store *OnlineBlobStore) AsOnlineStore() (OnlineStore, error) {
	return store, nil
}

func blobTableKey(feature, variant string) string {
	return fmt.Sprintf("tables/%s/%s", feature, variant)
}

func (store OnlineBlobStore) tableExists(feature, variant string) (bool, error) {
	tableKey := blobTableKey(feature, variant)
	return store.Exists(tableKey)
}

func (store OnlineBlobStore) readTableValue(feature, variant string) (ValueType, error) {
	tableKey := blobTableKey(feature, variant)
	value, err := store.Read(tableKey)
	if err != nil {
		return NilType, err
	}
	return ValueType(string(value)), nil
}

func (store OnlineBlobStore) writeTableValue(feature, variant string, valueType ValueType) error {
	tableKey := blobTableKey(feature, variant)
	return store.Write(tableKey, []byte(valueType))
}

func (store OnlineBlobStore) deleteTable(feature, variant string) error {
	tableKey := blobTableKey(feature, variant)
	return store.Delete(tableKey)
}

func (store OnlineBlobStore) GetTable(feature, variant string) (OnlineStoreTable, error) {
	exists, err := store.tableExists(feature, variant)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, &TableNotFound{feature, variant}
	}
	tableType, err := store.readTableValue(feature, variant)
	if err != nil {
		return nil, err
	}
	return OnlineBlobStoreTable{store, feature, variant, tableType}, nil
}

func (store OnlineBlobStore) CreateTable(feature, variant string, valueType ValueType) (OnlineStoreTable, error) {
	exists, err := store.tableExists(feature, variant)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, &TableAlreadyExists{feature, variant}
	}
	if err := store.writeTableValue(feature, variant, valueType); err != nil {
		return nil, err
	}
	return OnlineBlobStoreTable{store, feature, variant, valueType}, nil
}

type OnlineBlobStoreTable struct {
	store     BlobStore
	feature   string
	variant   string
	valueType ValueType
}

func (store OnlineBlobStore) DeleteTable(feature, variant string) error {
	exists, err := store.tableExists(feature, variant)
	if err != nil {
		return err
	}
	if !exists {
		return &TableNotFound{feature, variant}
	}
	return store.deleteTable(feature, variant)
	//TODO should this also cycle through all values and delete them too?
}

func entityValueKey(feature, variant, entity string) string {
	return fmt.Sprintf("values/%s/%s/%s", feature, variant, entity)
}

func (table OnlineBlobStoreTable) setEntityValue(feature, variant, entity string, value interface{}) error {
	entityValueKey := entityValueKey(feature, variant, entity)
	valueBytes := []byte(fmt.Sprintf("%v", value.(interface{})))
	return table.store.Write(entityValueKey, valueBytes)
}

func (table OnlineBlobStoreTable) getEntityValue(feature, variant, entity string) (interface{}, error) {
	entityValueKey := entityValueKey(feature, variant, entity)
	exists, err := table.store.Exists(entityValueKey)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, &EntityNotFound{entity}
	}

	return table.store.Read(entityValueKey)
}

func (table OnlineBlobStoreTable) Set(entity string, value interface{}) error {
	return table.setEntityValue(table.feature, table.variant, entity, value)
}

func (table OnlineBlobStoreTable) Get(entity string) (interface{}, error) {
	value, err := table.getEntityValue(table.feature, table.variant, entity)
	entityNotFoundError, ok := err.(*EntityNotFound)
	if ok {
		return nil, entityNotFoundError
	} else if err != nil {
		return nil, err
	}
	valueBytes := []byte(fmt.Sprintf("%v", value))
	return castBytesToValue(valueBytes, table.valueType)
}

func castBytesToValue(value []byte, valueType ValueType) (interface{}, error) {
	valueString := string(value)
	var val interface{}
	var err error
	switch valueType {
	case NilType, String:
		return valueString, nil
	case Int, Int32:
		val, err = strconv.ParseInt(valueString, 10, 32)
		return int(val.(int)), err
	case Int64:
		val, err = strconv.ParseInt(valueString, 10, 64)
		return int64(val.(int64)), err
	case Float32:
		val, err = strconv.ParseFloat(valueString, 32)
		return float32(val.(float32)), err
	case Float64:
		val, err = strconv.ParseFloat(valueString, 64)
		return float64(val.(float64)), err
	case Bool:
		val, err = strconv.ParseBool(valueString)
		return bool(val.(bool)), err
	case Timestamp:
		return time.Parse(time.ANSIC, valueString)
	default:
		return nil, fmt.Errorf("undefined value type: %v", valueType)
	}

}
