package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/google/uuid"
	"github.com/milvus-io/milvus-sdk-go/milvus"
	"github.com/redis/rueidis"
)

const (
	/*
		Milvus index names can only be 45 characters long and can only contain
		lowercase letters, numbers, and hyphens. To save space, we use a shorter
		prefix than those used by other online providers.
	*/
	prefixTemplate    = "ff-idx--%s"
	namespaceTemplate = "ff-namespace--%s-%s"
	/*
		The subdomain for index operations only need the environment.
	*/
	indexOperationURLTemplate = "controller.%s"
	/*
		The subdomain for vector operations is composed of the following, in order:
		1. The index name
		2. The project ID
		3. The environment
	*/
	vectorOperationURLTemplate = "%s-%s.svc.%s"
	nameVariantTemplate        = "%s-%s"
)

type MilvusConfig struct {
	Address     string `json:"address"`
	Port        int32  `json:"port"`
	ApiKey      string `json:"api_key"`
	Environment string `json:"environment"`
	ProjectID   string `json:"project_id"`
}

type milvusOnlineStore struct {
	client *milvusAPI
	prefix string
	BaseProvider
}

func milvusOnlineStoreFactory(serialized pc.SerializedConfig) (Provider, error) {
	milvusConfig := &pc.MilvusConfig{}
	if err := milvusConfig.Deserialize(serialized); err != nil {
		return nil, err
	}
	if milvusConfig.Prefix == "" {
		milvusConfig.Prefix = "Featureform_table__"
	}
	return NewMilvusOnlineStore(milvusConfig)
}

func NewMilvusOnlineStore(options *pc.MilvusConfig) (*milvusOnlineStore, error) {
	return &milvusOnlineStore{
		client: NewMilvusAPI(options),
		prefix: prefixTemplate,
		BaseProvider: BaseProvider{
			ProviderType:   pt.MilvusOnline,
			ProviderConfig: options.Serialize(),
		},
	}, nil
}

func (store *milvusOnlineStore) AsOnlineStore() (OnlineStore, error) {
	return store, nil
}

func (store *milvusOnlineStore) Close() error {
	store.client.Close()
	return nil
}

func (store *milvusOnlineStore) GetTable(feature, variant string) (OnlineStoreTable, error) {
	key := milvusTableKey{store.prefix, feature, variant}
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
		return &milvusOnlineTable{
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
		table = &milvusOnlineIndex{
			client: store.client,
			key: milvusIndexKey{
				Prefix:  store.prefix,
				Feature: feature,
				Variant: variant,
			},
			valueType: valueTypeJSON.ValueType,
		}
	case ScalarType:
		table = &milvusOnlineTable{
			client:    store.client,
			key:       key,
			valueType: valueTypeJSON.ValueType,
		}
	default:
		return nil, fmt.Errorf("unknown value type: %T", valueTypeJSON.ValueType)
	}
	return table, nil
}

func (store *milvusOnlineStore) CreateTable(feature, variant string, valueType ValueType) (OnlineStoreTable, error) {
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
		table = &milvusOnlineIndex{
			client: store.client,
			key: milvusIndexKey{
				Prefix:  store.prefix,
				Feature: feature,
				Variant: variant,
			},
			valueType: valueType,
		}
	case ScalarType:
		table = &milvusOnlineTable{
			client:    store.client,
			key:       key,
			valueType: valueType,
		}
	default:
		return nil, fmt.Errorf("unknown value type: %T", valueType)
	}
	return table, nil
}

func (store *milvusOnlineStore) DeleteTable(feature, variant string) error {
	return nil
}

func (store *milvusOnlineStore) CreateIndex(feature, variant string, vectorType VectorType) (VectorStoreTable, error) {
	indexName := store.createIndexName(feature, variant)
	if err := store.client.createIndex(indexName, vectorType.Dimension); err != nil {
		return nil, err
	}
	// Given Milvus indexes are cloud-based clusters of compute resources, they take
	// some time to spin up and become fully available. To ensure users don't encounter
	// unnecessary errors while registering a new index, we wait for the index to be
	// ready.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("timed out waiting for milvus index %s to be ready", indexName)
		case <-ticker.C:
			table, err := store.getTableForReadyIndex(indexName, feature, variant)
			if err != nil {
				return nil, err
			}
			if table != nil {
				return table, nil
			}
		}
	}
}

func (store *milvusOnlineStore) getTableForReadyIndex(indexName, feature, variant string) (VectorStoreTable, error) {
	dimension, state, err := store.client.describeIndex(indexName)
	if err != nil {
		return nil, err
	}
	if state == Ready {
		return milvusOnlineTable{
			api:       store.client,
			indexName: indexName,
			namespace: fmt.Sprintf(namespaceTemplate, feature, variant),
			valueType: VectorType{
				Dimension:   dimension,
				ScalarType:  Float32,
				IsEmbedding: true,
			},
		}, nil
	} else {
		return nil, nil
	}
}

func (store *milvusOnlineStore) DeleteIndex(feature, variant string) error {
	indexName := store.createIndexName(feature, variant)
	return store.client.deleteIndex(indexName)
}

func (store *milvusOnlineStore) createIndexName(feature, variant string) string {
	uuid := uuid.NewSHA1(uuid.NameSpaceDNS, []byte(fmt.Sprintf(nameVariantTemplate, feature, variant)))
	return fmt.Sprintf(store.prefix, uuid.String())
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

type milvusOnlineTable struct {
	client    milvus.Client
	key       milvusTableKey
	valueType ValueType
}

func (table milvusOnlineTable) Set(entity string, value interface{}) error {
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
		value = milvus.VectorString32(v)
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

func (table milvusOnlineTable) Get(entity string) (interface{}, error) {
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
		return milvus.ToVector32(val), nil
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

type milvusAPI struct {
	client          *http.Client
	baseURLTemplate string
	config          *mc.MilvusConfig
}

func NewMilvusAPI(config *mc.MilvusConfig) *milvusAPI {
	return &milvusAPI{
		client:          &http.Client{},
		baseURLTemplate: "https://%s.milvus.io/%s",
		config:          config,
	}
}

// https://milvus.io/docs/v2.0.0/api_reference.md#create_collection
func (api milvusAPI) createCollection(name string, dimension int32) error {
	base := api.getCollectionOperationURL("collections")
	payload := &createCollectionRequest{
		CollectionName: name,
		Dimension:      dimension,
		IndexFileSize:  1024,
		MetricType:     "L2",
	}
	_, err := api.request(http.MethodPost, base, payload, http.StatusCreated)
	if err != nil {
		return err
	}
	return nil
}

// https://milvus.io/docs/v2.0.0/api_reference.md#describe_collection
func (api milvusAPI) describeCollection(name string) (dimension int32, state MilvusCollectionState, err error) {
	base := api.getCollectionOperationURL(fmt.Sprintf("collections/%s", name))
	body, err := api.request(http.MethodGet, base, nil, http.StatusOK)
	if err != nil {
		return
	}
	var response describeCollectionResponse
	err = json.Unmarshal(body, &response)
	if err != nil {
		return
	}
	dimension = int32(response.Collection.Dimension)
	state = response.Status.State
	return
}

// https://milvus.io/docs/v2.0.0/api_reference.md#drop_collection
func (api milvusAPI) dropCollection(name string) error {
	base := api.getCollectionOperationURL(fmt.Sprintf("collections/%s", name))
	_, err := api.request("DELETE", base, nil, http.StatusAccepted)
	return err
}

// https://milvus.io/docs/v2.0.0/api_reference.md#insert
func (api milvusAPI) insert(collectionName string, vector []float32) error {
	base := api.getVectorOperationURL(collectionName, "vectors/insert")
	payload := &insertRequest{
		CollectionName: collectionName,
		Vectors: []vectorElement{
			{
				ID:     api.generateDeterministicID(id),
				Values: vector,
				Metadata: metadataElement{
					"id": id,
				},
			},
		},
	}
	body, err := api.request(http.MethodPost, base, payload, http.StatusOK)
	if err != nil {
		return err
	}
	var response insertResponse
	err = json.Unmarshal(body, &response)
	if err != nil {
		return err
	}
	if response.InsertCount != 1 {
		return fmt.Errorf("expected 1 insert count, got %d", response.InsertCount)
	}
	return nil
}

// https://milvus.io/docs/v2.0.0/api_reference.md#search
func (api milvusAPI) search(collectionName string, vector []float32, k int64) ([]string, error) {
	base := api.getVectorOperationURL(collectionName, "vectors/search")
	payload := &searchRequest{
		CollectionName: collectionName,
		Vectors: []vectorElement{
			{
				ID:     api.generateDeterministicID(id),
				Values: vector,
				Metadata: metadataElement{
					"id": id,
				},
			},
		},
		TopK: k,
	}
	body, err := api.request(http.MethodPost, base, payload, http.StatusOK)
	if err != nil {
		return nil, err
	}
	var response searchResponse
	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, err
	}
	results := make([]string, k)
	for i, result := range response.Matches {
		results[i] = result.Metadata["id"]
	}
	return results, nil
}

func (api milvusAPI) getCollectionOperationURL(operation string) string {
	subdomain := fmt.Sprintf(collectionOperationURLTemplate, api.config.Environment)
	return fmt.Sprintf(api.baseURLTemplate, subdomain, operation)
}

func (api milvusAPI) getVectorOperationURL(collectionName, operation string) string {
	subdomain := fmt.Sprintf(vectorOperationURLTemplate, collectionName, api.config.ProjectID, api.config.Environment)
	return fmt.Sprintf(api.baseURLTemplate, subdomain, operation)
}

func (api milvusAPI) request(method, url string, payload interface{}, expectedStatus int) ([]byte, error) {
	var reader io.Reader
	switch method {
	case http.MethodPost:
		jsonPayload, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewBuffer(jsonPayload)
	case http.MethodGet, http.MethodDelete:
		reader = nil
	default:
		return nil, fmt.Errorf("unsupported method %s", method)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Api-Key", api.config.ApiKey)
	resp, err := api.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != expectedStatus {
		return nil, fmt.Errorf("request failed with status code %d: %s", resp.StatusCode, string(body))
	}
	return body, nil
}

func (api milvusAPI) generateDeterministicID(id string) string {
	uuid := uuid.NewSHA1(uuid.NameSpaceDNS, []byte(id))
	return uuid.String()
}

type createCollectionRequest struct {
	CollectionName string `json:"collection_name"`
	Dimension      int32  `json:"dimension"`
	IndexFileSize  int32  `json:"index_file_size"`
	MetricType     string `json:"metric_type"`
}

type describeCollectionResponse struct {
	Collection struct {
		CollectionName string `json:"collection_name"`
		Dimension      int32  `json:"dimension"`
		IndexFileSize  int32  `json:"index_file_size"`
		MetricType     string `json:"metric_type"`
	} `json:"collection"`
	Status struct {
		State   MilvusCollectionState `json:"state"`
		Message string                `json:"message"`
	} `json:"status"`
}

type insertRequest struct {
	CollectionName string          `json:"collection_name"`
	Vectors        []vectorElement `json:"vectors"`
}

type insertResponse struct {
	InsertCount int64 `json:"insert_count"`
}

type searchRequest struct {
	CollectionName string          `json:"collection_name"`
	Vectors        []vectorElement `json:"vectors"`
	TopK           int64           `json:"top_k"`
}

type searchResponse struct {
	Matches []match `json:"matches"`
}
