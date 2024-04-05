package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/featureform/fferr"
	"github.com/featureform/filestore"
	"github.com/featureform/logging"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	sn "github.com/mrz1836/go-sanitize"
	"go.uber.org/zap"
)

func init() {
	if _, ok := serializers[dynamoSerializationVersion]; !ok {
		panic("Dynamo serializer not implemented")
	}
}

const (
	// Serialization version to use for new tables
	dynamoSerializationVersion = serializeV1
)

const (
	// Default timeout when waiting for dynamoDB tables to be ready
	defaultDynamoTableTimeout = 30 * time.Second
)

type dynamodbTableKey struct {
	Prefix, Feature, Variant string
}

func (t dynamodbTableKey) String() string {
	marshalled, err := json.Marshal(t)
	if err != nil {
		panic(err)
	}
	return string(marshalled)
}

type dynamodbOnlineStore struct {
	client *dynamodb.Client
	prefix string
	BaseProvider
	timeout time.Duration
	logger  *zap.SugaredLogger
}

type dynamodbOnlineTable struct {
	client    *dynamodb.Client
	key       dynamodbTableKey
	valueType ValueType
	version   serializeVersion
}

// dynamodbMetadataEntry is the format of each row in the Metadata table.
type dynamodbMetadataEntry struct {
	Tablename string `dynamodbav:"Tablename"`
	Valuetype string `dynamodbav:"ValueType"`
	Version   int    `dynamodbav:"SerializeVersion"`
}

// ToTableMetadata converts a dynamodb entry from the Metadata table to a struct
// with all its fields properly casted and type checked.
func (entry dynamodbMetadataEntry) ToTableMetadata() (*dynamodbTableMetadata, error) {
	version := serializeVersion(entry.Version)
	if _, ok := serializers[version]; !ok {
		wrapped := fferr.NewInternalErrorf("serialization version not implemented")
		wrapped.AddDetail("dynamo_serialize_version", fmt.Sprintf("%d", entry.Version))
		wrapped.AddDetail("dynamo_metadata_entry_name", entry.Tablename)
		return nil, wrapped
	}
	t, err := deserializeType(entry.Valuetype)
	if err != nil {
		wrapped := fferr.NewInternalError(err)
		wrapped.AddDetail("type_string", entry.Valuetype)
		wrapped.AddDetail("dynamo_metadata_entry_name", entry.Tablename)
		return nil, wrapped
	}
	return &dynamodbTableMetadata{t, version}, nil
}

// dynamodbTableMetadata is created by taking an entry from the Metadata table and
// casting and validating its values.
type dynamodbTableMetadata struct {
	Valuetype ValueType
	Version   serializeVersion
}

func dynamodbOnlineStoreFactory(serialized pc.SerializedConfig) (Provider, error) {
	dynamodbConfig := &pc.DynamodbConfig{}
	if err := dynamodbConfig.Deserialize(serialized); err != nil {
		return nil, err
	}
	if dynamodbConfig.Prefix == "" {
		dynamodbConfig.Prefix = "Featureform_table__"
	}
	return NewDynamodbOnlineStore(dynamodbConfig)
}

func NewDynamodbOnlineStore(options *pc.DynamodbConfig) (*dynamodbOnlineStore, error) {
	args := []func(*config.LoadOptions) error{
		config.WithRegion(options.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(options.AccessKey, options.SecretKey, "")),
		config.WithRetryer(func() aws.Retryer {
		    return retry.AddWithMaxBackoffDelay(retry.NewStandard(func(o *retry.StandardOptions) {
				o.RateLimiter = ratelimit.None
			    }), defaultDynamoTableTimeout)
		}),
	}
	// If we are using a custom endpoint, such as when running localstack, we should point at it. We'd never set this when
	// directly accessing DynamoDB on AWS.
	if options.Endpoint != "" {
		args = append(args,
			config.WithEndpointResolver(aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:           options.Endpoint,
					SigningRegion: options.Region,
				}, nil
			})))
	}
	cfg, err := config.LoadDefaultConfig(context.TODO(), args...)
	if err != nil {
		return nil, err
	}
	client := dynamodb.NewFromConfig(cfg)
	if err := waitForDynamoDB(client); err != nil {
		return nil, fferr.NewConnectionError("DynamoDB", err)
	}
	logger := logging.NewLogger("dynamodb")
	if err := CreateMetadataTable(client, logger); err != nil {
		return nil, err
	}
	return &dynamodbOnlineStore{client, options.Prefix, BaseProvider{
		ProviderType:   pt.DynamoDBOnline,
		ProviderConfig: options.Serialized(),
	}, defaultDynamoTableTimeout, logger,
	}, nil
}

func (store *dynamodbOnlineStore) AsOnlineStore() (OnlineStore, error) {
	return store, nil
}

func (store *dynamodbOnlineStore) Close() error {
	// dynamoDB client does not implement an equivalent to Close
	return nil
}

func CreateMetadataTable(client *dynamodb.Client, logger *zap.SugaredLogger) error {
	tableName := "Metadata"
	params := &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("Tablename"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("Tablename"),
				KeyType:       types.KeyTypeHash,
			},
		},
		BillingMode: types.BillingModePayPerRequest,
	}
	describeMetadataTableParams := &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}
	_, err := client.DescribeTable(context.TODO(), describeMetadataTableParams)
	if err == nil {
		return nil
	}
	logger.Infow("Could not describe dynamo metadata table, attempting to create...", "Error", err)
	if _, err := client.CreateTable(context.TODO(), params); err != nil {
		return fferr.NewExecutionError(pt.DynamoDBOnline.String(), err)
	}
	if err := waitForDynamoTable(client, tableName, defaultDynamoTableTimeout); err != nil {
		return fferr.NewExecutionError(pt.DynamoDBOnline.String(), err)
	}
	return nil
}

func (store *dynamodbOnlineStore) updateMetadataTable(tablename string, valueType ValueType, version serializeVersion) error {
	input := &dynamodb.UpdateItemInput{
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":valtype": &types.AttributeValueMemberS{
				Value: serializeType(valueType),
			},
			":serializeVersion": &types.AttributeValueMemberN{
				Value: fmt.Sprintf("%d", version),
			},
		},
		TableName: aws.String("Metadata"),
		Key: map[string]types.AttributeValue{
			"Tablename": &types.AttributeValueMemberS{
				Value: tablename,
			},
		},
		UpdateExpression: aws.String("set ValueType = :valtype, SerializeVersion = :serializeVersion"),
	}
	_, err := store.client.UpdateItem(context.TODO(), input)
	if err != nil {
		wrappedErr := fferr.NewExecutionError(pt.DynamoDBOnline.String(), err)
		wrappedErr.AddDetail("tablename", tablename)
		return wrappedErr
	}
	return nil
}

func (store *dynamodbOnlineStore) getFromMetadataTable(tablename string) (*dynamodbTableMetadata, error) {
	input := &dynamodb.GetItemInput{
		TableName: aws.String("Metadata"),
		Key: map[string]types.AttributeValue{
			"Tablename": &types.AttributeValueMemberS{
				Value: tablename,
			},
		},
	}
	output_val, err := store.client.GetItem(context.TODO(), input)
	if len(output_val.Item) == 0 {
		return nil, fferr.NewDatasetNotFoundError("", "", fmt.Errorf("table %s not found", tablename))
	}
	if err != nil {
		wrappedErr := fferr.NewExecutionError(pt.DynamoDBOnline.String(), err)
		wrappedErr.AddDetail("tablename", tablename)
		return nil, wrappedErr
	}
	var entry dynamodbMetadataEntry
	if err = attributevalue.UnmarshalMap(output_val.Item, &entry); err != nil {
		wrappedErr := fferr.NewExecutionError(pt.DynamoDBOnline.String(), err)
		wrappedErr.AddDetail("tablename", tablename)
		return nil, wrappedErr
	}
	tableMeta, err := entry.ToTableMetadata()
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	return tableMeta, nil
}

func formatDynamoTableName(prefix, feature, variant string) string {
	tablename := fmt.Sprintf("%s__%s__%s", sn.Custom(prefix, "[^a-zA-Z0-9_]"), sn.Custom(feature, "[^a-zA-Z0-9_]"), sn.Custom(variant, "[^a-zA-Z0-9_]"))
	return sn.Custom(tablename, "[^a-zA-Z0-9_.\\-]")
}

func (store *dynamodbOnlineStore) GetTable(feature, variant string) (OnlineStoreTable, error) {
	key := dynamodbTableKey{store.prefix, feature, variant}
	meta, err := store.getFromMetadataTable(formatDynamoTableName(store.prefix, feature, variant))
	if err != nil {
		return nil, fferr.NewDatasetNotFoundError(feature, variant, err)
	}
	table := &dynamodbOnlineTable{client: store.client, key: key, valueType: meta.Valuetype, version: meta.Version}
	return table, nil
}

func (store *dynamodbOnlineStore) CreateTable(feature, variant string, valueType ValueType) (OnlineStoreTable, error) {
	key := dynamodbTableKey{store.prefix, feature, variant}
	tableName := formatDynamoTableName(store.prefix, feature, variant)
	_, err := store.getFromMetadataTable(tableName)
	if err == nil {
		wrapped := fferr.NewDatasetAlreadyExistsError(feature, variant, nil)
		wrapped.AddDetail("tablename", formatDynamoTableName(store.prefix, feature, variant))
		return nil, wrapped
	}
	params := &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String(feature),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		BillingMode: types.BillingModePayPerRequest,
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(feature),
				KeyType:       types.KeyTypeHash,
			},
		},
	}
	err = store.updateMetadataTable(formatDynamoTableName(store.prefix, feature, variant), valueType, dynamoSerializationVersion)
	if err != nil {
		return nil, err
	}
	_, err = store.client.CreateTable(context.TODO(), params)
	if err != nil {
		return nil, fferr.NewResourceExecutionError(pt.DynamoDBOnline.String(), feature, variant, fferr.FEATURE_VARIANT, err)
	}
	if err := waitForDynamoTable(store.client, tableName, store.timeout); err != nil {
		return nil, fferr.NewResourceExecutionError(pt.DynamoDBOnline.String(), feature, variant, fferr.FEATURE_VARIANT, err)
	}
	return &dynamodbOnlineTable{store.client, key, valueType, dynamoSerializationVersion}, nil
}

func (store *dynamodbOnlineStore) DeleteTable(feature, variant string) error {
	params := &dynamodb.DeleteTableInput{
		TableName: aws.String(formatDynamoTableName(store.prefix, feature, variant)),
	}
	_, err := store.client.DeleteTable(context.TODO(), params)
	if err != nil {
		return fferr.NewExecutionError(pt.DynamoDBOnline.String(), err)
	}
	return nil
}

func (store *dynamodbOnlineStore) CheckHealth() (bool, error) {
	_, err := store.client.ListTables(context.TODO(), &dynamodb.ListTablesInput{Limit: aws.Int32(1)})
	if err != nil {
		return false, fferr.NewExecutionError(pt.DynamoDBOnline.String(), err)
	}
	return true, nil
}

func (store *dynamodbOnlineStore) ImportTable(feature, variant string, valueType ValueType, source filestore.Filepath) (ImportID, error) {
	tableName := formatDynamoTableName(store.prefix, feature, variant)
	store.logger.Infof("Checking metadata table for existing table %s\n", tableName)
	_, err := store.getFromMetadataTable(tableName)
	if err == nil {
		return "", err
	}

	store.logger.Infof("Updating metadata table %s\n", tableName)
	err = store.updateMetadataTable(tableName, valueType, dynamoSerializationVersion)
	if err != nil {
		return "", fferr.NewResourceExecutionError(pt.DynamoDBOnline.String(), feature, variant, fferr.FEATURE_VARIANT, err)
	}

	store.logger.Infof("Building import table input for %s\n", tableName)
	// https://pkg.go.dev/github.com/aws/aws-sdk-go@v1.47.7/service/dynamodb#ImportTableInput
	importInput := &dynamodb.ImportTableInput{
		// This is optional but it ensures idempotency within an 8-hour window,
		// so it seems prudent to include it to avoid triggering a duplicate import.
		ClientToken: aws.String(fmt.Sprintf("%s-%s", feature, variant)),

		InputCompressionType: types.InputCompressionTypeNone,

		InputFormat: types.InputFormatCsv,

		// https://pkg.go.dev/github.com/aws/aws-sdk-go@v1.47.7/service/dynamodb#InputFormatOptions
		InputFormatOptions: &types.InputFormatOptions{
			Csv: &types.CsvOptions{
				Delimiter:  aws.String(","),
				HeaderList: []string{feature, "FeatureValue", "ts"},
			},
		},

		// https://pkg.go.dev/github.com/aws/aws-sdk-go@v1.47.7/service/dynamodb#S3BucketSource
		S3BucketSource: &types.S3BucketSource{
			S3Bucket: aws.String(source.Bucket()),
			// To avoid importing Spark's _committed/_SUCCESS files, we use a prefix that contains the beginning of the
			// part-file naming conventions (e.g. `part-`). This ensures we only import the actual data files.
			S3KeyPrefix: aws.String(fmt.Sprintf("%s/part-", source.KeyPrefix())),
		},

		// https://pkg.go.dev/github.com/aws/aws-sdk-go@v1.47.7/service/dynamodb#TableCreationParameters
		TableCreationParameters: &types.TableCreationParameters{
			TableName: aws.String(tableName),
			AttributeDefinitions: []types.AttributeDefinition{
				{
					AttributeName: aws.String(feature),
					AttributeType: types.ScalarAttributeTypeS,
				},
			},
			BillingMode: types.BillingModePayPerRequest,
			KeySchema: []types.KeySchemaElement{
				{
					AttributeName: aws.String(feature),
					KeyType:       types.KeyTypeHash,
				},
			},
		},
	}

	store.logger.Infof("Importing table %s from source %s\n", tableName, source.KeyPrefix())
	output, err := store.client.ImportTable(context.TODO(), importInput)
	if err != nil {
		return "", fferr.NewResourceExecutionError(pt.DynamoDBOnline.String(), feature, variant, fferr.FEATURE_VARIANT, err)
	}

	store.logger.Infof("Import table response: %v\n", output)
	return ImportID(*output.ImportTableDescription.ImportArn), nil
}

type S3Import struct {
	id           ImportID
	status       string
	errorMessage string
}

func (i S3Import) Status() string {
	return i.status
}

func (i S3Import) ErrorMessage() string {
	return i.errorMessage
}

func (store *dynamodbOnlineStore) GetImport(id ImportID) (Import, error) {
	input := &dynamodb.DescribeImportInput{
		ImportArn: aws.String(string(id)),
	}
	output, err := store.client.DescribeImport(context.TODO(), input)
	if err != nil {
		wrapped := fferr.NewExecutionError(pt.DynamoDBOnline.String(), err)
		wrapped.AddDetail("import_id", string(id))
		return S3Import{id: id}, wrapped
	}
	var errorMessage string
	if output.ImportTableDescription.FailureCode != nil {
		errorMessage = *output.ImportTableDescription.FailureCode
	}
	return S3Import{id: id, status: string(output.ImportTableDescription.ImportStatus), errorMessage: errorMessage}, nil
}

func (table dynamodbOnlineTable) Set(entity string, value interface{}) error {
	dynamoValue, err := serializers[table.version].Serialize(table.valueType, value)
	if err != nil {
		wrap := fferr.NewInternalError(err)
		wrap.AddDetail("entity", entity)
		wrap.AddDetail("value", fmt.Sprintf("%v", value))
		return wrap
	}
	input := &dynamodb.UpdateItemInput{
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":val": dynamoValue,
		},
		TableName: aws.String(formatDynamoTableName(table.key.Prefix, table.key.Feature, table.key.Variant)),
		Key: map[string]types.AttributeValue{
			table.key.Feature: &types.AttributeValueMemberS{
				Value: entity,
			},
		},
		UpdateExpression: aws.String("set FeatureValue = :val"),
	}
	if _, err := table.client.UpdateItem(context.TODO(), input); err != nil {
		wrapped := fferr.NewResourceExecutionError(pt.DynamoDBOnline.String(), table.key.Feature, table.key.Variant, "FEATURE_VARIANT", fmt.Errorf("error setting entity: %w", err))
		wrapped.AddDetail("entity", entity)
		wrapped.AddDetail("value", fmt.Sprintf("%v", value))
		return wrapped
	}
	return nil
}

func (table dynamodbOnlineTable) Get(entity string) (interface{}, error) {
	input := &dynamodb.GetItemInput{
		TableName: aws.String(formatDynamoTableName(table.key.Prefix, table.key.Feature, table.key.Variant)),
		Key: map[string]types.AttributeValue{
			table.key.Feature: &types.AttributeValueMemberS{
				Value: entity,
			},
		},
	}
	output_val, err := table.client.GetItem(context.TODO(), input)
	if len(output_val.Item) == 0 {
		wrapped := fferr.NewEntityNotFoundError(table.key.Feature, table.key.Variant, entity, nil)
		wrapped.AddDetail("entity", entity)
		return nil, wrapped
	}
	if err != nil {
		return nil, err
	}
	item := output_val.Item
	value, ok := item["FeatureValue"]
	if !ok {
		wrapped := fferr.NewInternalErrorf("dynamoDB item does not have FeatureValue column")
		wrapped.AddDetail("entity", entity)
		return nil, wrapped
	}
	return serializers[table.version].Deserialize(table.valueType, value)
}

// waitForDynamoDB waits for DynamoDB to return a valid response with exponential backoff.
// We can't use waitForDynamoTable since we need to ignore most tcp and network errors and
// continue to retry.
func waitForDynamoDB(client *dynamodb.Client) error {
	waitTime := time.Second
	totalWait := time.Duration(0)
	for attempts := 0; attempts < 3; attempts++ {
		_, err := client.DescribeTable(context.TODO(), &dynamodb.DescribeTableInput{
			TableName: aws.String("PING"), // Arbitrary name
		})
		if err != nil {
			var resourceNotFoundErr *types.ResourceNotFoundException
			if errors.As(err, &resourceNotFoundErr) {
				// The table doesn't exist, but DynamoDB responded, meaning it's ready.
				return nil
			}
		} else {
			// DescribeTable succeeded, indicating DynamoDB is ready and the table exists.
			return nil
		}
		time.Sleep(waitTime)
		totalWait += waitTime
		// Exponential backoff
		waitTime = waitTime * 2
		// Don't wait longer than the max
		if totalWait+waitTime > defaultDynamoTableTimeout {
			waitTime = defaultDynamoTableTimeout - totalWait
		}
	}
	return fmt.Errorf("Failed to connect to DynamoDB")
}

// waitForDynamoDB waits for a DynamoDB table.
func waitForDynamoTable(client *dynamodb.Client, table string, maxWait time.Duration) error {
	waiter := dynamodb.NewTableExistsWaiter(client)
	waitParams := &dynamodb.DescribeTableInput{TableName: aws.String(table)}
	return waiter.Wait(context.TODO(), waitParams, maxWait)
}

// serializeVersion is used to specify what method of serializing and deserializing values
// into Dynamo columns that we're using.
type serializeVersion int

// The serializer versions. If adding a new one make sure to add to the serializers map variable
const (
	// serializeV0 serializes everything as strings, including numbers
	serializeV0 serializeVersion = iota
	// serializeV1 serializes everything into native dynamo types and handles lists as well
	serializeV1
)

func (v serializeVersion) String() string {
	return strconv.Itoa(int(v))
}

// serializer provides methods to serialize and deserialize values into DynamoDB columns
type serializer interface {
	Version() serializeVersion
	Serialize(t ValueType, value any) (types.AttributeValue, error)
	Deserialize(t ValueType, value types.AttributeValue) (any, error)
}

// serializers is the map of all serializers. If a new version is added it should be added
// into this map as well.
var serializers = map[serializeVersion]serializer{
	serializeV0: serializerV0{},
	serializeV1: serializerV1{},
}

// serializerV0 serializes everything as strings, including numbers
type serializerV0 struct{}

func (ser serializerV0) Version() serializeVersion {
	return serializeV0
}

func (ser serializerV0) Serialize(t ValueType, value any) (types.AttributeValue, error) {
	if value == nil {
		return &types.AttributeValueMemberNULL{
			Value: true,
		}, nil
	} else {
		return &types.AttributeValueMemberS{
			Value: fmt.Sprintf("%v", value),
		}, nil
	}
}

func (ser serializerV0) Deserialize(t ValueType, value types.AttributeValue) (any, error) {
	if _, isNil := value.(*types.AttributeValueMemberNULL); isNil {
		return nil, nil
	}
	typed, ok := value.(*types.AttributeValueMemberS)
	if !ok {
		return nil, fferr.NewInternalErrorf(
			"unable to deserialize dynamodb value into string, is %T", value).
			AddDetail("version", ser.Version().String())
	}
	valString := typed.Value
	var result interface{}
	var err error
	switch t {
	case NilType, String:
		result, err = valString, nil
	case Int:
		result, err = strconv.Atoi(valString)
	case Int64:
		result, err = strconv.ParseInt(valString, 0, 64)
	case Float32:
		var result_float float64
		result_float, err = strconv.ParseFloat(valString, 32)
		result = float32(result_float)
	case Float64:
		result, err = strconv.ParseFloat(valString, 64)
	case Bool:
		result, err = strconv.ParseBool(valString)
	}
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	return result, nil
}

// serializerV1 serializes everything into native dynamo types and handles lists as well
type serializerV1 struct{}

func (ser serializerV1) Version() serializeVersion {
	return serializeV1
}

func (ser serializerV1) Serialize(t ValueType, value any) (types.AttributeValue, error) {
	if value == nil {
		return &types.AttributeValueMemberNULL{Value: true}, nil
	}
	if !t.IsVector() {
		return ser.serializeScalar(t, value)
	}
	vecT := t.(VectorType)
	scalar := vecT.Scalar()

	list := reflect.ValueOf(value)
	if list.Kind() != reflect.Slice {
		return nil, fferr.NewTypeError(vecT.String(), value, nil).
			AddDetail("version", ser.Version().String())
	}
	length := list.Len()
	if int32(length) != vecT.Dimension {
		errMsg := "Type error. Wrong length.\nFound %d\nExpected %d"
		return nil, fferr.NewTypeErrorf(vecT.String(), value, errMsg, vecT.Dimension, length).
			AddDetail("version", ser.Version().String())
	}
	vals := make([]types.AttributeValue, length)
	for i := 0; i < length; i++ {
		elem := list.Index(i).Interface()
		val, err := ser.serializeScalar(scalar, elem)
		if err != nil {
			if typed, ok := err.(fferr.Error); ok {
				typed.AddDetail("list_element", strconv.Itoa(i))
			}
			return nil, err
		}
		vals[i] = val
	}
	return &types.AttributeValueMemberL{
		Value: vals,
	}, nil
}

func (ser serializerV1) serializeScalar(t ValueType, value any) (types.AttributeValue, error) {
	if value == nil {
		return &types.AttributeValueMemberNULL{Value: true}, nil
	}
	// Dynamo teats all numerical types as strings, so we have to serialize.
	switch t {
	case NilType:
		return &types.AttributeValueMemberNULL{Value: true}, nil
	case Int:
		// This rounds via Go if needed
		intVal, err := castNumberToInt(value)
		if err != nil {
			return nil, fferr.NewTypeError(t.String(), value, err).
				AddDetail("version", ser.Version().String())
		}
		intStr := strconv.FormatInt(int64(intVal), 10)
		return &types.AttributeValueMemberN{Value: intStr}, nil
	case Int32:
		// This rounds via Go if needed
		intVal, err := castNumberToInt32(value)
		if err != nil {
			return nil, fferr.NewTypeError(t.String(), value, err).
				AddDetail("version", ser.Version().String())
		}
		intStr := strconv.FormatInt(int64(intVal), 10)
		return &types.AttributeValueMemberN{Value: intStr}, nil
	case Int64:
		intVal, err := castNumberToInt64(value)
		if err != nil {
			return nil, fferr.NewTypeError(t.String(), value, err).
				AddDetail("version", ser.Version().String())
		}
		intStr := strconv.FormatInt(intVal, 10)
		return &types.AttributeValueMemberN{Value: intStr}, nil
	case Float32:
		floatVal, err := castNumberToFloat32(value)
		if err != nil {
			return nil, fferr.NewTypeError(t.String(), value, err).
				AddDetail("version", ser.Version().String())
		}
		floatStr := strconv.FormatFloat(float64(floatVal), 'e', -1, 32)
		return &types.AttributeValueMemberN{Value: floatStr}, nil
	case Float64:
		floatVal, err := castNumberToFloat64(value)
		if err != nil {
			return nil, fferr.NewTypeError(t.String(), value, err).
				AddDetail("version", ser.Version().String())
		}
		floatStr := strconv.FormatFloat(floatVal, 'e', -1, 64)
		return &types.AttributeValueMemberN{Value: floatStr}, nil
	case Bool:
		casted, ok := value.(bool)
		if !ok {
			return nil, fferr.NewTypeError(t.String(), value, nil).
				AddDetail("version", ser.Version().String())
		}
		return &types.AttributeValueMemberBOOL{Value: casted}, nil
	case String:
		casted, ok := value.(string)
		if !ok {
			return nil, fferr.NewTypeError(t.String(), value, nil).
				AddDetail("version", ser.Version().String())
		}
		return &types.AttributeValueMemberS{Value: casted}, nil
	default:
		return nil, fferr.NewInternalErrorf("dynamo doesn't support type").
			AddDetail("type", serializeType(t))
	}
}

func castNumberToFloat32(value any) (float32, error) {
	// I have to do one type per case for this to work properly.
	switch typed := value.(type) {
	case int:
		return float32(typed), nil
	case int32:
		return float32(typed), nil
	case int64:
		return float32(typed), nil
	case int8:
		return float32(typed), nil
	case int16:
		return float32(typed), nil
	case float32:
		return typed, nil
	case float64:
		return float32(typed), nil
	case string:
		f64, err := strconv.ParseFloat(typed, 32)
		if err != nil {
			return 0, fmt.Errorf("Type error: Expected numerical type and got %T", typed)
		}
		return float32(f64), nil
	default:
		return 0, fmt.Errorf("Type error: Expected numerical type and got %T", typed)
	}
}

func castNumberToFloat64(value any) (float64, error) {
	// I have to do one type per case for this to work properly.
	switch typed := value.(type) {
	case int:
		return float64(typed), nil
	case int32:
		return float64(typed), nil
	case int64:
		return float64(typed), nil
	case int8:
		return float64(typed), nil
	case int16:
		return float64(typed), nil
	case float32:
		return float64(typed), nil
	case float64:
		return typed, nil
	case string:
		f64, err := strconv.ParseFloat(typed, 64)
		if err != nil {
			return 0, fmt.Errorf("Type error: Expected numerical type and got %T", typed)
		}
		return f64, nil
	default:
		return 0, fmt.Errorf("Type error: Expected numerical type and got %T", typed)
	}
}

func castNumberToInt(value any) (int, error) {
	// I have to do one type per case for this to work properly.
	switch typed := value.(type) {
	case int:
		return typed, nil
	case int32:
		return int(typed), nil
	case int64:
		return int(typed), nil
	case int8:
		return int(typed), nil
	case int16:
		return int(typed), nil
	case float32:
		return int(typed), nil
	case float64:
		return int(typed), nil
	case string:
		i64, err := strconv.ParseInt(typed, 64)
		if err != nil {
			return 0, fmt.Errorf("Type error: Expected numerical type and got %T", typed)
		}
		return int(i64), nil
	default:
		return 0, fmt.Errorf("Type error: Expected numerical type and got %T", typed)
	}
}

func castNumberToInt32(value any) (int32, error) {
	// I have to do one type per case for this to work properly.
	switch typed := value.(type) {
	case int:
		return int32(typed), nil
	case int32:
		return typed, nil
	case int64:
		return int32(typed), nil
	case int8:
		return int32(typed), nil
	case int16:
		return int32(typed), nil
	case float32:
		return int32(typed), nil
	case float64:
		return int32(typed), nil
	case string:
		i64, err := strconv.ParseInt(typed, 32)
		if err != nil {
			return 0, fmt.Errorf("Type error: Expected numerical type and got %T", typed)
		}
		return int32(i64), nil
	default:
		return 0, fmt.Errorf("Type error: Expected numerical type and got %T", typed)
	}
}

func castNumberToInt64(value any) (int64, error) {
	// I have to do one type per case for this to work properly.
	switch typed := value.(type) {
	case int:
		return int64(typed), nil
	case int32:
		return int64(typed), nil
	case int64:
		return typed, nil
	case int8:
		return int64(typed), nil
	case int16:
		return int64(typed), nil
	case float32:
		return int64(typed), nil
	case float64:
		return int64(typed), nil
	case string:
		i64, err := strconv.ParseInt(typed, 64)
		if err != nil {
			return 0, fmt.Errorf("Type error: Expected numerical type and got %T", typed)
		}
		return int64(i64), nil
	default:
		return 0, fmt.Errorf("Type error: Expected numerical type and got %T", typed)
	}
}

func (ser serializerV1) Deserialize(t ValueType, value types.AttributeValue) (any, error) {
	// Dynamo teats all numerical types as strings, so we have to deserialize.
	version := ser.Version().String()
	_, ok := value.(*types.AttributeValueMemberNULL)
	if ok {
		return nil, nil
	}
	if !t.IsVector() {
		return deserializeScalar(t.Scalar(), value, version)
	}
	list, ok := value.(*types.AttributeValueMemberL)
	if !ok {
		return nil, fferr.NewInternalErrorf("unable to deserialize dynamodb value into list, is %T", value).
			AddDetail("version", ser.Version().String())
	}
	values := list.Value
	scalar := t.Scalar()
	switch scalar {
	case Int:
		return deserializeList[int](scalar, values, version)
	case Int32:
		return deserializeList[int32](scalar, values, version)
	case Int64:
		return deserializeList[int64](scalar, values, version)
	case Float32:
		return deserializeList[float32](scalar, values, version)
	case Float64:
		return deserializeList[float64](scalar, values, version)
	case Bool:
		return deserializeList[bool](scalar, values, version)
	case String:
		return deserializeList[string](scalar, values, version)
	default:
		return nil, fferr.NewInternalErrorf("dynamo doesn't support type").
			AddDetail("type", serializeType(t))
	}
}

func deserializeList[T any](scalar ScalarType, values []types.AttributeValue, version string) ([]T, error) {
	deserList := make([]T, len(values))
	for i, value := range values {
		deser, err := deserializeScalar(scalar, value, version)
		if err != nil {
			if typed, ok := err.(fferr.Error); ok {
				typed.AddDetail("list_element", strconv.Itoa(i))
			}
			return nil, err
		}
		casted, ok := deser.(T)
		if !ok {
			return nil, fferr.NewInternalErrorf("Deserialize failed due to wrong generic").
				AddDetail("found_type", fmt.Sprintf("%T", casted)).
				AddDetail("expected_type", scalar.String()).
				AddDetail("list_element", strconv.Itoa(i)).
				AddDetail("version", version)
		}
		deserList[i] = deser.(T)
	}
	return deserList, nil
}

func deserializeScalar(t ScalarType, value types.AttributeValue, version string) (any, error) {
	// Dynamo teats all numerical types as strings, so we have to deserialize.
	switch t {
	case Int:
		castedValue, ok := value.(*types.AttributeValueMemberN)
		if !ok {
			return nil, fferr.NewInternalErrorf("unable to deserialize dynamodb value into numerical, is %T", value).
				AddDetail("version", version)
		}
		val, err := strconv.ParseInt(castedValue.Value, 10, 0)
		return int(val), err
	case Int32:
		castedValue, ok := value.(*types.AttributeValueMemberN)
		if !ok {
			return nil, fferr.NewInternalErrorf("unable to deserialize dynamodb value into numerical, is %T", value).
				AddDetail("version", version)
		}
		val, err := strconv.ParseInt(castedValue.Value, 10, 32)
		return int32(val), err
	case Int64:
		castedValue, ok := value.(*types.AttributeValueMemberN)
		if !ok {
			return nil, fferr.NewInternalErrorf("unable to deserialize dynamodb value into numerical, is %T", value).
				AddDetail("version", version)
		}
		return strconv.ParseInt(castedValue.Value, 10, 64)
	case Float32:
		castedValue, ok := value.(*types.AttributeValueMemberN)
		if !ok {
			return nil, fferr.NewInternalErrorf("unable to deserialize dynamodb value into numerical, is %T", value).
				AddDetail("version", version)
		}
		val, err := strconv.ParseFloat(castedValue.Value, 32)
		return float32(val), err
	case Float64:
		castedValue, ok := value.(*types.AttributeValueMemberN)
		if !ok {
			return nil, fferr.NewInternalErrorf("unable to deserialize dynamodb value into numerical, is %T", value).
				AddDetail("version", version)
		}
		return strconv.ParseFloat(castedValue.Value, 64)
	case Bool:
		castedValue, ok := value.(*types.AttributeValueMemberBOOL)
		if !ok {
			return nil, fferr.NewInternalErrorf("unable to deserialize dynamodb value into bool, is %T", value).
				AddDetail("version", version)
		}
		return castedValue.Value, nil
	case String:
		castedValue, ok := value.(*types.AttributeValueMemberS)
		if !ok {
			return nil, fferr.NewInternalErrorf("unable to deserialize dynamodb value into string, is %T", value).
				AddDetail("version", version)
		}
		return castedValue.Value, nil
	default:
		return nil, fferr.NewInternalErrorf("Dynamo doesn't support type").
			AddDetail("version", version)
	}
}
