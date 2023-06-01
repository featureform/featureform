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

const vectorField = "vector_field"

type redisTableKey struct {
	Prefix, Feature, Variant string
}

func (t redisTableKey) String() string {
	marshalled, _ := json.Marshal(t)
	return string(marshalled)
}

type redisOnlineStore struct {
	client rueidis.Client
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
	return &redisOnlineStore{redisClient, options.Prefix, BaseProvider{
		ProviderType:   pt.RedisOnline,
		ProviderConfig: options.Serialized(),
	},
	}, nil
}

func (store *redisOnlineStore) AsOnlineStore() (OnlineStore, error) {
	return store, nil
}

func (store *redisOnlineStore) AsVectorStore() (VectorStore, error) {
	return store, nil
}

func (store *redisOnlineStore) Close() error {
	store.client.Close()
	return nil
}

func (store *redisOnlineStore) GetTable(feature, variant string) (OnlineStoreTable, error) {
	key := redisTableKey{store.prefix, feature, variant}
	cmd := store.client.B().
		Hget().
		Key(fmt.Sprintf("%s__tables", store.prefix)).
		Field(key.String()).
		Build()
	vType, err := store.client.Do(context.TODO(), cmd).ToString()
	if err != nil {
		return nil, &TableNotFound{feature, variant}
	}
	var valueType ValueType
	var table OnlineStoreTable
	// This maintains backwards compatibility with the previous implementation,
	// which wrote the scalar type string as the value to the field under the
	// tables hash.
	if _, isScalarString := ScalarTypes[ScalarType(vType)]; isScalarString {
		return &redisOnlineTable{
			client:    store.client,
			key:       key,
			valueType: ScalarType(vType),
		}, nil
	}
	valueTypeJSON := &ValueTypeJSONWrapper{}
	err = json.Unmarshal([]byte(vType), valueTypeJSON)
	if err != nil {
		return nil, err
	}
	switch valueTypeJSON.ValueType.(type) {
	case VectorType:
		table = &redisOnlineIndex{
			client:    store.client,
			key:       key,
			valueType: valueType,
		}
	case ScalarType:
		table = &redisOnlineTable{
			client:    store.client,
			key:       key,
			valueType: valueType,
		}
	default:
		return nil, fmt.Errorf("unknown value type: %T", valueTypeJSON.ValueType)
	}
	return table, nil
}

func (store *redisOnlineStore) CreateTable(feature, variant string, valueType ValueType) (OnlineStoreTable, error) {
	key := redisTableKey{store.prefix, feature, variant}
	cmd := store.client.B().
		Hexists().
		Key(fmt.Sprintf("%s__tables", store.prefix)).
		Field(key.String()).
		Build()
	exists, err := store.client.Do(context.TODO(), cmd).AsBool()
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, &TableAlreadyExists{feature, variant}
	}
	serialized, err := json.Marshal(ValueTypeJSONWrapper{valueType})
	if err != nil {
		return nil, err
	}
	cmd = store.client.B().
		Hset().
		Key(fmt.Sprintf("%s__tables", store.prefix)).
		FieldValue().
		FieldValue(key.String(), string(serialized)).
		Build()
	if resp := store.client.Do(context.TODO(), cmd); resp.Error() != nil {
		return nil, resp.Error()
	}
	table := &redisOnlineTable{client: store.client, key: key, valueType: valueType}
	return table, nil
}

func (store *redisOnlineStore) DeleteTable(feature, variant string) error {
	return nil
}

func (store *redisOnlineStore) CreateIndex(feature, variant, entity string, vectorType VectorType) (VectorStoreTable, error) {
	key := redisTableKey{store.prefix, feature, variant}
	cmd := store.createIndexCmd(key, entity, vectorType)
	resp := store.client.Do(context.Background(), cmd)
	if resp.Error() != nil {
		return &redisOnlineIndex{}, resp.Error()
	}
	table := &redisOnlineIndex{client: store.client, key: key, valueType: vectorType}
	return table, nil
}

// TODO: write unit tests for command creation
func (store *redisOnlineStore) createIndexCmd(key redisTableKey, entity string, vectorType VectorType) rueidis.Completed {
	requiredParams := []string{
		"TYPE", "FLOAT32",
		"DIM", strconv.FormatUint(uint64(vectorType.Dimension), 10),
		"DISTANCE_METRIC", "COSINE",
	}
	return store.client.B().
		FtCreate().
		Index(key.String()).
		Schema().
		FieldName(vectorField).
		Vector("HNSW", int64(len(requiredParams)), requiredParams...).
		Build()
}

func (store *redisOnlineStore) GetIndex(feature, variant string) (string, error) {
	return "", nil
}

func (store *redisOnlineStore) DeleteIndex(feature, variant string) error {
	return nil
}

type redisOnlineTable struct {
	client    rueidis.Client
	key       redisTableKey
	valueType ValueType
}

func (table redisOnlineTable) Set(entity string, value interface{}) error {
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
	case []float32:
		value = rueidis.VectorString32(v)
	default:
		return fmt.Errorf("type %T of value %v is unsupported", value, value)
	}
	cmd := table.client.B().
		Hset().
		Key(table.key.String()).
		FieldValue().
		FieldValue(entity, value.(string)).
		Build()
	res := table.client.Do(context.TODO(), cmd)
	if res.Error() != nil {
		return res.Error()
	}
	return nil
}

func (table redisOnlineTable) Get(entity string) (interface{}, error) {
	cmd := table.client.B().
		Hget().
		Key(table.key.String()).
		Field(entity).
		Build()
	resp := table.client.Do(context.TODO(), cmd)
	if resp.Error() != nil {
		return nil, &EntityNotFound{entity}
	}
	var result interface{}
	var err error
	val, err := resp.ToString()
	if err != nil {
		return nil, err
	}
	if table.valueType.IsVector() {
		return rueidis.ToVector32(val), nil
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
			result, err = float32(result.(float64)), nil
		}
	case Float64:
		result, err = strconv.ParseFloat(val, 64)
	case Bool:
		result, err = strconv.ParseBool(val)
	case Timestamp, Datetime: // Maintains compatibility with previously create timestamp tables
		// maintains compatibility with go-redis implementation:
		// https://github.com/redis/go-redis/blob/v8.11.5/command.go#L939
		result, err = time.Parse(time.RFC3339Nano, val)
	default:
		result, err = val, nil
	}
	if err != nil {
		return nil, fmt.Errorf("could not cast value: %v to %s: %w", resp, table.valueType, err)
	}
	return result, nil
}

type redisOnlineIndex struct {
	client    rueidis.Client
	key       redisTableKey
	valueType ValueType
}

func (table redisOnlineIndex) Set(entity string, value interface{}) error {
	vector, ok := value.([]float32)
	if !ok {
		return fmt.Errorf("value %v is not a vector", value)
	}
	cmd := table.client.B().
		Hset().
		Key(table.key.String()).
		FieldValue().
		FieldValue("entity_id", entity).
		FieldValue(vectorField, rueidis.VectorString32(vector)).
		Build()
	res := table.client.Do(context.TODO(), cmd)
	if res.Error() != nil {
		return res.Error()
	}
	return nil
}

func (table redisOnlineIndex) Get(entity string) (interface{}, error) {
	cmd := table.client.B().
		Hget().
		Key(table.key.String()).
		Field(vectorField).
		Build()
	resp := table.client.Do(context.TODO(), cmd)
	if resp.Error() != nil {
		return nil, &EntityNotFound{entity}
	}
	val, err := resp.ToString()
	if err != nil {
		return nil, err
	}
	return rueidis.ToVector32(val), nil
}

func (table redisOnlineIndex) Nearest(feature, variant, entity string, vector []float32, k int32) ([]string, error) {
	cmd := table.createNearestCmd(vector, k)
	_, docs, err := table.client.Do(context.Background(), cmd).AsFtSearch()
	if err != nil {
		return nil, err
	}
	entities := make([]string, len(docs))
	for idx, doc := range docs {
		entities[idx] = doc.Doc["entity_id"]
	}
	return entities, nil
}

func (table redisOnlineIndex) createNearestCmd(vector []float32, k int32) rueidis.Completed {
	return table.client.B().
		FtSearch().
		Index(table.key.String()).
		Query("*=>[KNN $K @vector_field $BLOB]").
		Return("1").
		Identifier("entity_id").
		Sortby("__vector_field_score").
		Params().
		Nargs(4).
		NameValue().
		NameValue("K", strconv.Itoa(int(k))).
		NameValue("BLOB", rueidis.VectorString32(vector)).
		Dialect(2).
		Build()
}
