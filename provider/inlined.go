package provider

import (
	"fmt"
	"strconv"
	"time"

	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	ikv "github.com/inlinedio/ikv-store/ikv-go-client"
	"github.com/redis/rueidis"
)

var pkey_field_name string = "entity_primary_key"

// Controls a single IKV store instance.
// Inner document fields act as individual OnlineTableStore instances.
type inlinedOnlineStore struct {
	tables map[string]*inlinedOnlineStoreTable
	reader ikv.IKVReader
	writer ikv.IKVWriter
	BaseProvider
}

// Create a Provider instance backed by IKV.
func inlinedOnlineStoreFactory(serialized pc.SerializedConfig) (Provider, error) {
	inlinedConfig := &pc.IKVConfig{}
	if err := inlinedConfig.Deserialize(serialized); err != nil {
		return nil, NewProviderError(Runtime, pt.InlinedKVOnline, ConfigDeserialize, err.Error())
	}
	return NewInlinedOnlineStore(inlinedConfig)
}

func NewInlinedOnlineStore(config *pc.IKVConfig) (*inlinedOnlineStore, error) {
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

	// Startup writer and reader. Blocks.
	err = writer.Startup()
	if err != nil {
		return nil, err
	}
	err = reader.Startup()
	if err != nil {
		return nil, err
	}

	return &inlinedOnlineStore{
		tables: make(map[string]*inlinedOnlineStoreTable),
		reader: reader,
		writer: writer,
		BaseProvider: BaseProvider{
			ProviderType:   pt.InlinedKVOnline,
			ProviderConfig: config.Serialized(),
		},
	}, nil
}

func (i *inlinedOnlineStore) AsOnlineStore() (OnlineStore, error) {
	return i, nil
}

func (i *inlinedOnlineStore) CheckHealth() (bool, error) {
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
func (i *inlinedOnlineStore) GetTable(feature string, variant string) (OnlineStoreTable, error) {
	fieldName := constructFieldName(feature, variant)

	if table, exists := i.tables[fieldName]; exists {
		return table, nil
	}

	return nil, fmt.Errorf("table for feature: %s variant: %s has not been created.", feature, variant)
}

// CreateTable implements OnlineStore.
func (i *inlinedOnlineStore) CreateTable(feature string, variant string, valueType ValueType) (OnlineStoreTable, error) {
	fieldName := constructFieldName(feature, variant)

	// already exists?
	if table, exists := i.tables[fieldName]; exists {
		return table, nil
	}

	table := inlinedOnlineStoreTable{
		fieldName: fieldName,
		valueType: valueType,
		reader:    i.reader,
		writer:    i.writer,
	}
	i.tables[fieldName] = &table
	return &table, nil
}

func (*inlinedOnlineStore) DeleteTable(feature string, variant string) error {
	// Not supported. Field cannot be deleted from all entities.
	// Deletes keyed on particular entity are ok.
	return nil
}

func (i *inlinedOnlineStore) Close() error {
	// Shutdown clients.

	if err := i.reader.Shutdown(); err != nil {
		return err
	}

	if err := i.writer.Shutdown(); err != nil {
		return err
	}

	return nil
}

func constructFieldName(feature, variant string) string {
	return fmt.Sprintf("%s_%s", feature, variant)
}

// encapsulates get/set operations
// over a strongly typed particular entity-type (feature/column)
type inlinedOnlineStoreTable struct {
	fieldName string
	valueType ValueType
	reader    ikv.IKVReader
	writer    ikv.IKVWriter
}

func (i *inlinedOnlineStoreTable) Set(entity string, value interface{}) error {
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
		return fmt.Errorf("type %T of value %v is unsupported", value, value)
	}

	// create IKVDocument
	document, err := ikv.NewIKVDocumentBuilder().PutStringField(pkey_field_name, entity).PutStringField(i.fieldName, valueAsString).Build()
	if err != nil {
		return err
	}

	// upsert operation
	return i.writer.UpsertFields(&document)
}

func (i *inlinedOnlineStoreTable) Get(entity string) (interface{}, error) {
	valueAsString, err := i.reader.GetStringValue(entity, i.fieldName)
	if err != nil {
		return nil, err
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
		return nil, fmt.Errorf("could not cast value: %s to %s: %w", valueAsString, i.valueType, err)
	}

	return value, nil
}
