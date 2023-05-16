package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"

	"github.com/redis/rueidis"
)

type redisTableKey struct {
	Prefix, Feature, Variant string
}

func (t redisTableKey) String() string {
	marshalled, _ := json.Marshal(t)
	return string(marshalled)
}

type redisOnlineStore struct {
	client *rueidis.Client
	prefix string
	BaseProvider
}

func redisOnlineStoreFactory(serialized pc.SerializedConfig) (Provider, error) {
	redisConfig := &pc.RedisConfig{}
	if err := redisConfig.Deserialize(serialized); err != nil {
		return nil, err
	}
	if redisConfig.Prefix == "" {
		redisConfig.Prefix = "Featureform_table__"
	}
	return NewRedisOnlineStore(redisConfig)
}

func NewRedisOnlineStore(options *pc.RedisConfig) (*redisOnlineStore, error) {
	redisOptions := rueidis.ClientOption{
		InitAddress: []string{options.Addr},
		Password:    options.Password,
		SelectDB:    options.DB,
	}
	redisClient, err := rueidis.NewClient(redisOptions)
	if err != nil {
		return nil, err
	}
	return &redisOnlineStore{&redisClient, options.Prefix, BaseProvider{
		ProviderType:   pt.RedisOnline,
		ProviderConfig: options.Serialized(),
	},
	}, nil
}

func (store *redisOnlineStore) AsOnlineStore() (OnlineStore, error) {
	return store, nil
}

func (store *redisOnlineStore) Close() error {
	c := *store.client
	c.Close()
	return nil
}

func (store *redisOnlineStore) GetTable(feature, variant string) (OnlineStoreTable, error) {
	client := *store.client
	key := redisTableKey{store.prefix, feature, variant}
	cmd := client.B().
		Hget().
		Key(fmt.Sprintf("%s__tables", store.prefix)).
		Field(key.String()).
		Build()
	vType, err := client.Do(context.TODO(), cmd).ToString()
	if err != nil {
		return nil, &TableNotFound{feature, variant}
	}
	table := &redisOnlineTable{client: &client, key: key, valueType: ValueType(vType)}
	return table, nil
}

func (store *redisOnlineStore) CreateTable(feature, variant string, valueType ValueType) (OnlineStoreTable, error) {
	client := *store.client
	key := redisTableKey{store.prefix, feature, variant}
	cmd := client.B().
		Hexists().
		Key(fmt.Sprintf("%s__tables", store.prefix)).
		Field(key.String()).
		Build()
	exists, err := client.Do(context.TODO(), cmd).AsBool()
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, &TableAlreadyExists{feature, variant}
	}
	cmd = client.B().
		Hset().
		Key(fmt.Sprintf("%s__tables", store.prefix)).
		FieldValue().
		FieldValue(key.String(), string(valueType)).
		Build()
	if resp := client.Do(context.TODO(), cmd); resp.Error() != nil {
		return nil, resp.Error()
	}
	table := &redisOnlineTable{client: &client, key: key, valueType: valueType}
	return table, nil
}

func (store *redisOnlineStore) DeleteTable(feature, variant string) error {
	return nil
}

func (table redisOnlineTable) Set(entity string, value interface{}) error {
	client := *table.client
	switch v := value.(type) {
	case nil:
		value = "nil"
	case string:
		value = v
	case int:
		value = strconv.Itoa(v)
	case int32:
		value = strconv.FormatInt(int64(v), 10)
	case int64:
		value = strconv.FormatInt(v, 10)
	case float32:
		value = strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		value = strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		// The previous Redis client stored booleans as 1 or 0; to maintain backwards compatibility
		// we do the same here, stringifying the value to satisfy the interface. See redis_test.go
		// lines 59-66 for more reasons why we do this.
		if v {
			value = "1"
		} else {
			value = "0"
		}
	case time.Time:
		value = v.Format(time.RFC3339)
	// Vector parsed from parquet files will have the type map[string]interface{}
	// and require further processing to be stored in Redis as a string.
	case map[string]interface{}:
		serialized, err := table.formatVector(v)
		if err != nil {
			return err
		}
		value = serialized
	default:
		return fmt.Errorf("type %T of value %v is unsupported", value, value)
	}
	cmd := client.B().
		Hset().
		Key(table.key.String()).
		FieldValue().
		FieldValue(entity, value.(string)).
		Build()
	res := client.Do(context.TODO(), cmd)
	if res.Error() != nil {
		return res.Error()
	}
	return nil
}

func (table redisOnlineTable) Get(entity string) (interface{}, error) {
	client := *table.client
	cmd := client.B().
		Hget().
		Key(table.key.String()).
		Field(entity).
		Build()
	resp := client.Do(context.TODO(), cmd)
	if resp.Error() != nil {
		return nil, &EntityNotFound{entity}
	}
	var result interface{}
	var err error
	val, err := resp.ToString()
	if err != nil {
		return nil, err
	}
	switch table.valueType {
	case NilType, String:
		result, err = val, nil
	case Int:
		result, err = strconv.Atoi(val)
	case Int32:
		if result, err = strconv.ParseInt(val, 10, 32); err == nil {
			result = int32(result.(int64))
		}
	case Int64:
		result, err = strconv.ParseInt(val, 10, 64)
	case Float32:
		if result, err = strconv.ParseFloat(val, 32); err == nil {
			result = float32(result.(float64))
		}
	case Float64:
		result, err = strconv.ParseFloat(val, 64)
	case Bool:
		result, err = strconv.ParseBool(val)
	case Timestamp, Datetime: // Maintains compatibility with previously create timestamp tables
		// maintains compatibility with go-redis implementation:
		// https://github.com/redis/go-redis/blob/v8.11.5/command.go#L939
		result, err = time.Parse(time.RFC3339Nano, val)
	case Vector32:
		result, err = rueidis.ToVector32(val), nil
	default:
		result, err = val, nil
	}
	if err != nil {
		return nil, fmt.Errorf("could not cast value: %v to %s: %w", resp, table.valueType, err)
	}
	return result, nil
}

// formatVector takes a map[string]interface{} and returns a string representation of the vector
// using the VectorString32 method provided by the rueidis Redis package.
func (table redisOnlineTable) formatVector(value map[string]interface{}) (string, error) {
	list, ok := value["list"]
	if !ok {
		return "", fmt.Errorf("expected to find field 'list' value (type %T)", value)
	}
	// To iterate over the list and create a we need to cast it to []interface{}
	elementsSlice, ok := list.([]interface{})
	if !ok {
		return "", fmt.Errorf("could not cast type: %T to []interface{}", list)
	}
	vector32 := make([]float32, len(elementsSlice))
	for i, e := range elementsSlice {
		// To access the 'element' field, which holds the float value,
		// we need to cast it to map[string]interface{}
		m, ok := e.(map[string]interface{})
		if !ok {
			return "", fmt.Errorf("could not cast type: %T to map[string]interface{}", e)
		}
		switch element := m["element"].(type) {
		case float32:
			vector32[i] = element
		// Given floats in Python are typically 64-bit, it's possible we'll receive
		// a vector of float64
		case float64:
			vector32[i] = float32(element)
		default:
			return "", fmt.Errorf("unexpected type in parquet vector list: %T", element)
		}
	}
	return rueidis.VectorString32(vector32), nil
}
