package provider

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	sn "github.com/mrz1836/go-sanitize"
	"strconv"
	"time"
)

type dynamodbTableKey struct {
	Prefix, Feature, Variant string
}

func (t dynamodbTableKey) String() string {
	marshalled, _ := json.Marshal(t)
	return string(marshalled)
}

type dynamodbOnlineStore struct {
	client *dynamodb.DynamoDB
	prefix string
	BaseProvider
}

type dynamodbOnlineTable struct {
	client    *dynamodb.DynamoDB
	key       dynamodbTableKey
	valueType ValueType
}

type dynamodbItem struct {
	Entity string `dynamodbav:"Entity"`
	Value  string `dynamodbav:"FeatureValue"`
}

type Metadata struct {
	Tablename string `dynamodbav:"Tablename"`
	Valuetype string `dynamodbav:"ValueType"`
}

func dynamodbOnlineStoreFactory(serialized SerializedConfig) (Provider, error) {
	dynamodbConfig := &DynamodbConfig{}
	if err := dynamodbConfig.Deserialize(serialized); err != nil {
		return nil, err
	}
	if dynamodbConfig.Prefix == "" {
		dynamodbConfig.Prefix = "Featureform_table__"
	}
	return NewDynamodbOnlineStore(dynamodbConfig)
}

func NewDynamodbOnlineStore(options *DynamodbConfig) (*dynamodbOnlineStore, error) {
	config := &aws.Config{
		// Endpoint: &options.Addr,
		Region:      aws.String(options.Region),
		Credentials: credentials.NewStaticCredentials(options.AccessKey, options.SecretKey, ""),
	}
	sess := session.Must(session.NewSession(config))
	dynamodbClient := dynamodb.New(sess)
	if err := CreateMetadataTable(dynamodbClient); err != nil {
		return nil, err
	}
	return &dynamodbOnlineStore{dynamodbClient, options.Prefix, BaseProvider{
		ProviderType:   DynamoDBOnline,
		ProviderConfig: options.Serialized(),
	},
	}, nil
}

func getDynamodbConfig(options *DynamodbConfig) (*aws.Config){
	config := &aws.Config{
		// Endpoint: &options.Addr,
		Region:      aws.String(options.Region),
		Credentials: credentials.NewStaticCredentials(options.AccessKey, options.SecretKey, ""),
	}
	return config
}

func (store *dynamodbOnlineStore) AsOnlineStore() (OnlineStore, error) {
	return store, nil
}

func CreateMetadataTable(dynamodbClient *dynamodb.DynamoDB) error {
	params := &dynamodb.CreateTableInput{
		TableName: aws.String("Metadata"),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("Tablename"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("Tablename"),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(5),
		},
	}
	describeMetadataTableParams := &dynamodb.DescribeTableInput{
		TableName: aws.String("Metadata"),
	}
	describeMetadataTable, err := dynamodbClient.DescribeTable(describeMetadataTableParams)
	if err != nil {
		return err
	}
	if describeMetadataTable == nil {
		_, err := dynamodbClient.CreateTable(params)
		if err != nil {
			return err
		}
	}
	return nil
}

func (store *dynamodbOnlineStore) UpdateMetadataTable(tablename string, valueType ValueType) error {
	input := &dynamodb.UpdateItemInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":valtype": {
				S: aws.String(string(valueType)),
			},
		},
		TableName: aws.String("Metadata"),
		Key: map[string]*dynamodb.AttributeValue{
			"Tablename": {
				S: aws.String(tablename),
			},
		},
		UpdateExpression: aws.String("set ValueType = :valtype"),
	}
	_, err := store.client.UpdateItem(input)
	return err
}

func (store *dynamodbOnlineStore) GetFromMetadataTable(tablename string) (ValueType, error) {
	input := &dynamodb.GetItemInput{
		TableName: aws.String("Metadata"),
		Key: map[string]*dynamodb.AttributeValue{
			"Tablename": {
				S: aws.String(tablename),
			},
		},
	}
	output_val, err := store.client.GetItem(input)
	if len(output_val.Item) == 0 {
		return NilType, &CustomError{"Table not found"}
	}
	if err != nil {
		return NilType, err
	}
	metadata_item := Metadata{}
	err = dynamodbattribute.UnmarshalMap(output_val.Item, &metadata_item)

	if err != nil {
		return NilType, err
	}
	return ValueType(metadata_item.Valuetype), err
}

func (store *dynamodbOnlineStore) GetTable(feature, variant string) (OnlineStoreTable, error) {
	key := dynamodbTableKey{store.prefix, feature, variant}
	typeOfValue, err := store.GetFromMetadataTable(sn.Custom(key.String(), "[^a-zA-Z0-9_.\\-]"))
	if err != nil {
		return nil, &TableNotFound{feature, variant}
	}
	table := &dynamodbOnlineTable{client: store.client, key: key, valueType: typeOfValue}
	return table, nil
}

func (store *dynamodbOnlineStore) CreateTable(feature, variant string, valueType ValueType) (OnlineStoreTable, error) {
	key := dynamodbTableKey{store.prefix, feature, variant}
	_, err := store.GetFromMetadataTable(sn.Custom(key.String(), "[^a-zA-Z0-9_.\\-]"))
	if err == nil {
		return nil, &TableAlreadyExists{feature, variant}
	}
	params := &dynamodb.CreateTableInput{
		TableName: aws.String(sn.Custom(key.String(), "[^a-zA-Z0-9_.\\-]")),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(feature),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(feature),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(5),
		},
	}
	err = store.UpdateMetadataTable(sn.Custom(key.String(), "[^a-zA-Z0-9_.\\-]"), valueType)
	if err != nil {
		return nil, err
	}
	_, err = store.client.CreateTable(params)
	if err != nil {
		return nil, err
	}
	describeTableParams := &dynamodb.DescribeTableInput{TableName: aws.String(sn.Custom(key.String(), "[^a-zA-Z0-9_.\\-]"))}
	describeTableOutput, err := store.client.DescribeTable(describeTableParams)
	if err != nil {
		return nil, err
	}
	for describeTableOutput == nil || *describeTableOutput.Table.TableStatus != "ACTIVE" {
		describeTableOutput, err = store.client.DescribeTable(describeTableParams)
		if err != nil {
			return nil, err
		}
		time.Sleep(5 * time.Second)
	}
	return &dynamodbOnlineTable{store.client, key, valueType}, nil
}

func (table dynamodbOnlineTable) Set(entity string, value interface{}) error {
	input := &dynamodb.UpdateItemInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":val": {
				S: aws.String(fmt.Sprintf("%v", value)),
			},
		},
		TableName: aws.String(sn.Custom(table.key.String(), "[^a-zA-Z0-9_.\\-]")),
		Key: map[string]*dynamodb.AttributeValue{
			table.key.Feature: {
				S: aws.String(entity),
			},
		},
		UpdateExpression: aws.String("set FeatureValue = :val"),
	}
	_, err := table.client.UpdateItem(input)
	return err
}

func (table dynamodbOnlineTable) Get(entity string) (interface{}, error) {
	input := &dynamodb.GetItemInput{
		TableName: aws.String(sn.Custom(table.key.String(), "[^a-zA-Z0-9_.\\-]")),
		Key: map[string]*dynamodb.AttributeValue{
			table.key.Feature: {
				S: aws.String(entity),
			},
		},
	}
	output_val, err := table.client.GetItem(input)
	if len(output_val.Item) == 0 {
		return nil, &EntityNotFound{entity}
	}
	if err != nil {
		return nil, err
	}
	dynamodb_item := dynamodbItem{}
	err = dynamodbattribute.UnmarshalMap(output_val.Item, &dynamodb_item)
	if err != nil {
		return nil, &EntityNotFound{entity}
	}
	var result interface{}
	var result_float float64
	switch table.valueType {
	case NilType, String:
		result, err = dynamodb_item.Value, nil
	case Int:
		result, err = strconv.Atoi(dynamodb_item.Value)
	case Int64:
		result, err = strconv.ParseInt(dynamodb_item.Value, 0, 64)
	case Float32:
		result_float, err = strconv.ParseFloat(dynamodb_item.Value, 32)
		result = float32(result_float)
	case Float64:
		result, err = strconv.ParseFloat(dynamodb_item.Value, 64)
	case Bool:
		result, err = strconv.ParseBool(dynamodb_item.Value)
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}
