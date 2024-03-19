package provider

import (
	"fmt"
	"strconv"
	"time"

	"github.com/featureform/fferr"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	ikv "github.com/inlinedio/ikv-store/ikv-go-client"
	"github.com/redis/rueidis"
)

// IKV primary-key for storing entity key values
const pkey_field_name string = "ff_primary_key"

// primary-key of the row storing table metadata
const metadata_pkey string = "ff_table_metadata"

// Integrates IKV: https://docs.inlined.io as an Online Store in Featureform.
// Wraps a single IKV store instance.
// Inner fields act as individual OnlineTableStore instances.
type ikvOnlineStore struct {
	// write through cache over ikv field columns
	tables map[string]*ikvOnlineStoreTable
	reader ikv.IKVReader
	writer ikv.IKVWriter
	BaseProvider
}

// Create a Provider instance backed by IKV.
func ikvOnlineStoreFactory(serialized pc.SerializedConfig) (Provider, error) {
	ikvConfig := &pc.IKVConfig{}
	if err := ikvConfig.Deserialize(serialized); err != nil {
		return nil, err
	}
	return NewInlinedOnlineStore(ikvConfig)
}

func NewInlinedOnlineStore(config *pc.IKVConfig) (*ikvOnlineStore, error) {
	// convert to ikv client options
	options, err := config.ToClientOptions()
	if err != nil {
		return nil, err
	}

	// instantiate reader and writer clients
	factory := &ikv.IKVClientFactory{}

	writer, err := factory.CreateNewWriter(options)
	if err != nil {
		return nil, err
	}

	reader, err := factory.CreateNewReader(options)
	if err != nil {
		return nil, err
	}

	// Startup writer and reader. Blocking operation.
	err = writer.Startup()
	if err != nil {
		return nil, fferr.NewConnectionError(pt.IKVOnline.String(), err)
	}
	err = reader.Startup()
	if err != nil {
		return nil, fferr.NewConnectionError(pt.IKVOnline.String(), err)
	}

	return &ikvOnlineStore{
		tables: make(map[string]*ikvOnlineStoreTable),
		reader: reader,
		writer: writer,
		BaseProvider: BaseProvider{
			ProviderType:   pt.IKVOnline,
			ProviderConfig: config.Serialized(),
		},
	}, nil
}

func (i *ikvOnlineStore) AsOnlineStore() (OnlineStore, error) {
	return i, nil
}

func (i *ikvOnlineStore) CheckHealth() (bool, error) {
	status, err := i.writer.HealthCheck()
	if !status || err != nil {
		return false, err
	}

	status, err = i.reader.HealthCheck()
	if !status || err != nil {
		return false, err
	}

	return true, nil
}

// GetTable implements OnlineStore.
func (i *ikvOnlineStore) GetTable(feature string, variant string) (OnlineStoreTable, error) {
	fieldName := constructFieldName(feature, variant)

	if table, exists := i.tables[fieldName]; exists {
		return table, nil
	}

	// fetch metadata row from ikv
	exists, valueTypeString, err := i.reader.GetStringValue(metadata_pkey, fieldName)
	if err != nil {
		return nil, fferr.NewResourceInternalError(feature, variant, fferr.INTERNAL_ERROR, err)
	}

	// feature + variant pair not created
	if !exists {
		return nil, fferr.NewDatasetNotFoundError(feature, variant, nil)
	}

	// populate cache
	table := ikvOnlineStoreTable{
		featureName: feature,
		variant:     variant,
		fieldName:   fieldName,
		valueType:   ScalarType(valueTypeString),
		reader:      i.reader,
		writer:      i.writer,
	}
	i.tables[fieldName] = &table
	return &table, nil
}

// CreateTable implements OnlineStore.
func (i *ikvOnlineStore) CreateTable(feature string, variant string, valueType ValueType) (OnlineStoreTable, error) {
	// check if already exists
	if maybe_table, _ := i.GetTable(feature, variant); maybe_table != nil {
		return nil, fferr.NewDatasetAlreadyExistsError(feature, variant, nil)
	}

	// Still possible that the field column already exists
	// since GetTable has eventual consistency.
	// Ok to re-create.
	fieldName := constructFieldName(feature, variant)
	document, _ := ikv.NewIKVDocumentBuilder().PutStringField(pkey_field_name, metadata_pkey).PutStringField(fieldName, string(valueType.Scalar().Scalar())).Build()
	if err := i.writer.UpsertFields(&document); err != nil {
		return nil, fferr.NewResourceInternalError(feature, variant, fferr.INTERNAL_ERROR, err)
	}

	table := ikvOnlineStoreTable{
		featureName: feature,
		variant:     variant,
		fieldName:   fieldName,
		valueType:   valueType,
		reader:      i.reader,
		writer:      i.writer,
	}
	i.tables[fieldName] = &table
	return &table, nil
}

// DeleteTable implements OnlineStore.
func (i *ikvOnlineStore) DeleteTable(feature string, variant string) error {
	fieldName := constructFieldName(feature, variant)

	delete(i.tables, fieldName)

	// drop for all documents (including metadata row)
	return i.writer.DropFieldsByName([]string{fieldName})
}

func (i *ikvOnlineStore) Close() error {
	if err := i.reader.Shutdown(); err != nil {
		return err
	}

	if err := i.writer.Shutdown(); err != nil {
		return err
	}

	return nil
}

// Wraps one field/attribute/column of an IKV store,
// and provides set/get functionality for an entity.
type ikvOnlineStoreTable struct {
	featureName string
	variant     string
	fieldName   string
	valueType   ValueType
	reader      ikv.IKVReader
	writer      ikv.IKVWriter
}

func (i *ikvOnlineStoreTable) Set(entity string, value interface{}) error {
	// stringify "value"
	var valueAsString string

	switch v := value.(type) {
	case nil:
		valueAsString = "nil"
	case string:
		valueAsString = v
	case int:
		valueAsString = strconv.Itoa(v)
	case int8:
		valueAsString = strconv.FormatInt(int64(v), 10)
	case int16:
		valueAsString = strconv.FormatInt(int64(v), 10)
	case int32:
		valueAsString = strconv.FormatInt(int64(v), 10)
	case int64:
		valueAsString = strconv.FormatInt(v, 10)
	case float32:
		valueAsString = strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		valueAsString = strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		if v {
			valueAsString = "1"
		} else {
			valueAsString = "0"
		}
	case time.Time:
		valueAsString = v.Format(time.RFC3339Nano)
	case []float32:
		valueAsString = rueidis.VectorString32(v)
	default:
		return fferr.NewDataTypeNotFoundError(fmt.Sprintf("%T", value), fmt.Errorf("unsupported data type"))
	}

	// create IKVDocument
	document, err := ikv.NewIKVDocumentBuilder().PutStringField(pkey_field_name, entity).PutStringField(i.fieldName, valueAsString).Build()
	if err != nil {
		wrapped := fferr.NewResourceExecutionError(pt.RedisOnline.String(), i.featureName, i.variant, fferr.ENTITY, err)
		wrapped.AddDetail("entity", entity)
		return wrapped
	}

	// upsert operation
	if err := i.writer.UpsertFields(&document); err != nil {
		wrapped := fferr.NewResourceExecutionError(pt.RedisOnline.String(), i.featureName, i.variant, fferr.ENTITY, err)
		wrapped.AddDetail("entity", entity)
		return wrapped
	}

	return nil
}

func (i *ikvOnlineStoreTable) Get(entity string) (interface{}, error) {
	exists, valueAsString, err := i.reader.GetStringValue(entity, i.fieldName)
	if err != nil {
		return nil, fferr.NewResourceExecutionError(pt.RedisOnline.String(), i.featureName, i.variant, fferr.ENTITY, err)
	}
	if !exists {
		return nil, fferr.NewEntityNotFoundError(i.featureName, i.variant, entity, nil)
	}

	// convert back to type
	var value interface{}
	switch i.valueType {
	case NilType, String:
		value, err = valueAsString, nil
	case Int:
		value, err = strconv.Atoi(valueAsString)
	case Int8:
		if value, err = strconv.ParseInt(valueAsString, 10, 64); err == nil {
			value = int8(value.(int64))
		}
	case Int16:
		if value, err = strconv.ParseInt(valueAsString, 10, 64); err == nil {
			value = int16(value.(int64))
		}
	case Int32:
		if value, err = strconv.ParseInt(valueAsString, 10, 64); err == nil {
			value = int32(value.(int64))
		}
	case Int64:
		value, err = strconv.ParseInt(valueAsString, 10, 64)
	case Float32:
		if value, err = strconv.ParseFloat(valueAsString, 64); err == nil {
			value, err = float32(value.(float64)), nil
		}
	case Float64:
		value, err = strconv.ParseFloat(valueAsString, 64)
	case Bool:
		value, err = strconv.ParseBool(valueAsString)
	case Timestamp, Datetime:
		value, err = time.Parse(time.RFC3339Nano, valueAsString)
	default:
		value, err = valueAsString, nil
	}

	if err != nil {
		wrapped := fferr.NewInternalError(fmt.Errorf("could not cast value: %v to %s: %w", valueAsString, i.valueType, err))
		wrapped.AddDetail("entity", entity)
		return nil, wrapped
	}

	return value, nil
}

func constructFieldName(feature string, variant string) string {
	return fmt.Sprintf("%s$_$%s", feature, variant)
}
