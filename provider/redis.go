package provider

import (
	"context"
	"encoding/base64"
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
		/*
			The rueidis client opts-in to server-assisted client-side caching by default.
			Given we're not making use of this feature (i.e. via the commands `DoCach` or
			`DoMultiCache`), and given this is normally a feature that a user would have to
			explicitly opt into for a connection via the Redis command `CLIENT TRACKING ON`,
			we disable this feature by default to avoid any issues with Redis servers versions
			under 6.0.

			See details here:
				* rueidis client-side caching: https://github.com/redis/rueidis#client-side-caching
				* Redis CLIENT TRACKING command: https://redis.io/commands/client-tracking/
		*/
		DisableCache: true,
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
			client: store.client,
			key: redisIndexKey{
				Prefix:  store.prefix,
				Feature: feature,
				Variant: variant,
			},
			valueType: valueTypeJSON.ValueType,
		}
	case ScalarType:
		table = &redisOnlineTable{
			client:    store.client,
			key:       key,
			valueType: valueTypeJSON.ValueType,
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
	var table OnlineStoreTable
	switch valueType.(type) {
	case VectorType:
		table = &redisOnlineIndex{
			client: store.client,
			key: redisIndexKey{
				Prefix:  store.prefix,
				Feature: feature,
				Variant: variant,
			},
			valueType: valueType,
		}
	case ScalarType:
		table = &redisOnlineTable{
			client:    store.client,
			key:       key,
			valueType: valueType,
		}
	default:
		return nil, fmt.Errorf("unknown value type: %T", valueType)
	}
	return table, nil
}

func (store *redisOnlineStore) DeleteTable(feature, variant string) error {
	return nil
}

func (store *redisOnlineStore) Check() (bool, error) {
	cmd := store.client.B().Ping().Build()
	resp, err := store.client.Do(context.Background(), cmd).ToString()
	if err != nil {
		return false, err
	}
	if resp != "PONG" {
		return false, fmt.Errorf("unexpected response from Redis server: %s", resp)
	}
	return true, nil
}

func (store *redisOnlineStore) CreateIndex(feature, variant string, vectorType VectorType) (VectorStoreTable, error) {
	key := redisIndexKey{Prefix: store.prefix, Feature: feature, Variant: variant}
	cmd, err := store.createIndexCmd(key, vectorType)
	if err != nil {
		return nil, err
	}
	resp := store.client.Do(context.Background(), cmd)
	if resp.Error() != nil {
		return &redisOnlineIndex{}, resp.Error()
	}
	table := &redisOnlineIndex{client: store.client, key: key, valueType: vectorType}
	return table, nil
}

func (store *redisOnlineStore) DeleteIndex(feature, variant string) error {
	return nil
}

func (store *redisOnlineStore) createIndexCmd(key redisIndexKey, vectorType VectorType) (rueidis.Completed, error) {
	serializedKey, err := key.serialize("")
	if err != nil {
		return rueidis.Completed{}, err
	}
	requiredParams := []string{
		"TYPE", "FLOAT32",
		"DIM", strconv.FormatUint(uint64(vectorType.Dimension), 10),
		"DISTANCE_METRIC", "COSINE",
	}
	return store.client.B().
		FtCreate().
		Index(string(serializedKey)).
		Schema().
		FieldName(key.getVectorField()).
		Vector("HNSW", int64(len(requiredParams)), requiredParams...).
		Build(), nil
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
	case Timestamp, Datetime: // Including `Datetime` here maintains compatibility with previously create timestamp tables
		// Maintains compatibility with go-redis implementation:
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
	key       redisIndexKey
	valueType ValueType
}

type redisIndexKey struct {
	Prefix, Feature, Variant, Entity string
}

func (k *redisIndexKey) serialize(entity string) ([]byte, error) {
	k.Entity = entity
	return json.Marshal(k)
}

func (k *redisIndexKey) deserialize(key []byte) error {
	return json.Unmarshal(key, &k)
}

func (k redisIndexKey) getVectorField() string {
	name_variant := fmt.Sprintf("%s_%s", k.Feature, k.Variant)
	// RawStdEncoding is necessary given the padding character is considered
	// a token character in RediSearch
	encoded := base64.RawStdEncoding.EncodeToString([]byte(name_variant))
	return fmt.Sprintf("vector_field_%s", encoded)
}

func (table redisOnlineIndex) Set(entity string, value interface{}) error {
	vector, ok := value.([]float32)
	if !ok {
		return fmt.Errorf("value %v is not a vector", value)
	}
	serializedKey, err := table.key.serialize(entity)
	if err != nil {
		return err
	}
	cmd := table.client.B().
		Hset().
		Key(string(serializedKey)).
		FieldValue().
		FieldValue(table.key.getVectorField(), rueidis.VectorString32(vector)).
		Build()
	res := table.client.Do(context.TODO(), cmd)
	if res.Error() != nil {
		return res.Error()
	}
	return nil
}

func (table redisOnlineIndex) Get(entity string) (interface{}, error) {
	serializedKey, err := table.key.serialize(entity)
	if err != nil {
		return nil, err
	}
	cmd := table.client.B().
		Hget().
		Key(string(serializedKey)).
		Field(table.key.getVectorField()).
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

func (table redisOnlineIndex) Nearest(feature, variant string, vector []float32, k int32) ([]string, error) {
	cmd, err := table.createNearestCmd(vector, k)
	if err != nil {
		return nil, err
	}
	_, docs, err := table.client.Do(context.Background(), cmd).AsFtSearch()
	if err != nil {
		return nil, err
	}
	entities := make([]string, len(docs))
	for idx, doc := range docs {
		key := redisIndexKey{}
		err := key.deserialize([]byte(doc.Key))
		if err != nil {
			return nil, err
		}
		entities[idx] = key.Entity
	}
	return entities, nil
}

func (table redisOnlineIndex) createNearestCmd(vector []float32, k int32) (rueidis.Completed, error) {
	vectorField := table.key.getVectorField()
	serializedKey, err := table.key.serialize("")
	if err != nil {
		return rueidis.Completed{}, err
	}
	return table.client.B().
		FtSearch().
		Index(string(serializedKey)).
		Query(fmt.Sprintf("*=>[KNN $K @%s $BLOB]", vectorField)).
		Sortby(fmt.Sprintf("__%s_score", vectorField)).
		Params().
		Nargs(4).
		NameValue().
		NameValue("K", strconv.Itoa(int(k))).
		NameValue("BLOB", rueidis.VectorString32(vector)).
		Dialect(2).
		Build(), nil
}
