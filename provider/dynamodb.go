package provider

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

//Added by Riddhi
type dynamodbTableKey struct {
	Prefix, Feature, Variant string
}

//Added by Riddhi
func (t dynamodbTableKey) String() string {
	marshalled, _ := json.Marshal(t)
	return string(marshalled)
}

//added by Riddhi
type dynamodbOnlineStore struct {
	client *dynamodb.DynamoDB
	prefix string
	BaseProvider
}

//added by Riddhi
func dynamodbOnlineStoreFactory(serialized SerializedConfig) (Provider, error) {
	dynamodbConfig := &DynamodbConfig{}
	if err := dynamodbConfig.Deserialize(serialized); err != nil {
		return nil, err
	}
	if dynamodbConfig.Prefix == "" {
		dynamodbConfig.Prefix = "Featureform_table__"
	}
	return NewDyanamodbOnlineStore(dynamodbConfig), nil
}

// DONE
// added by Riddhi
func NewDyanamodbOnlineStore(options *DynamodbConfig) *dynamodbOnlineStore {
	mySession := session.Must(session.NewSession())
	dynamodbClient := dynamodb.New(mySession)

	// create metadata table
	CreateMetadataTable(dynamodbClient)

	return &dynamodbOnlineStore{dynamodbClient, options.Prefix, BaseProvider{
		ProviderType:   DynamoDBOnline,
		ProviderConfig: options.Serialized(),
	},
	}
}

func CreateMetadataTable(dynamodbClient *dynamodb.DynamoDB) {
	params := &dynamodb.CreateTableInput{
		TableName: aws.String("Metadata"),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("table_name"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("table_name"),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(5),
		},
	}

	dynamodbClient.CreateTable(params)
}

// Dymanodb createtable
func (store *dynamodbOnlineStore) CreateTable(feature, variant string, valueType ValueType) (OnlineStoreTable, error) {
	key := dynamodbTableKey{store.prefix, feature, variant}
	params := &dynamodb.CreateTableInput{
		TableName: aws.String(key.String()),
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

	store.client.CreateTable(params)
	store.UpdateMetadataTable(key.String(), valueType)
	return &dynamodbOnlineTable{store.client, key, valueType}, nil
}

func (store *dynamodbOnlineStore) UpdateMetadataTable(tablename string, valueType ValueType) error {
	primaryKey := map[string]string{
		"table_name": tablename,
	}
	feature, err := dynamodbattribute.MarshalMap(primaryKey)
	if err != nil {
		return err
	}

	upd := expression.
		Set(expression.Name("value_type"), expression.Value(valueType))
	expr, err := expression.NewBuilder().WithUpdate(upd).Build()
	if err != nil {
		return err
	}

	input := &dynamodb.UpdateItemInput{
		TableName:                 aws.String("Metadata"),
		Key:                       feature,
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
	}

	_, err = store.client.UpdateItemWithContext(ctx, input)
	return err
}

type Metadata struct {
	tablename string `dynamodbav:"table_name"`
	valuetype string `dynamodbav:"value_type"`
}

func (store *dynamodbOnlineStore) GetFromMetadataTable(tablename string) (ValueType, error) {
	primaryKey := map[string]string{
		"table_name": tablename,
	}
	table, err := dynamodbattribute.MarshalMap(primaryKey)
	if err != nil {
		return NilType, err
	}

	proj := expression.NamesList(expression.Name("value_type"))
	expr, err := expression.NewBuilder().WithProjection(proj).Build()
	if err != nil {
		return NilType, err
	}

	input := &dynamodb.GetItemInput{
		TableName:                aws.String("Metadata"),
		Key:                      table,
		ExpressionAttributeNames: expr.Names(),
		ProjectionExpression:     expr.Projection(),
	}

	output_val, err := store.client.GetItemWithContext(ctx, input)
	if err != nil {
		return NilType, err
	}
	metadata_item := Metadata{}
	err = dynamodbattribute.UnmarshalMap(output_val.Item, &metadata_item)

	if err != nil {
		panic(fmt.Sprintf("Failed to unmarshal Record, %v", err))
	}

	return ValueType(metadata_item.valuetype), err
}

func (store *dynamodbOnlineStore) GetTable(feature, variant string) (OnlineStoreTable, error) {
	key := dynamodbTableKey{store.prefix, feature, variant}
	typeOfValue, err := store.GetFromMetadataTable(key.String())
	// vType, err := store.client.HGet(ctx, fmt.Sprintf("%s__tables", store.prefix), key.String()).Result()
	if err != nil {
		return nil, &TableNotFound{feature, variant}
	}
	table := &dynamodbOnlineTable{client: store.client, key: key, valueType: typeOfValue}
	return table, nil
}

type dynamodbOnlineTable struct {
	client    *dynamodb.DynamoDB
	key       dynamodbTableKey
	valueType ValueType
}

type dynamodbItem struct {
	entity string      `dynamodbav:"entity"`
	value  interface{} `dynamodbav:"value"`
}

func (table dynamodbOnlineTable) Set(entity string, value interface{}) error {

	primaryKey := map[string]string{
		table.key.Feature: entity,
	}
	feature, err := dynamodbattribute.MarshalMap(primaryKey)
	if err != nil {
		return err
	}

	upd := expression.
		Set(expression.Name("value"), expression.Value(value))
	expr, err := expression.NewBuilder().WithUpdate(upd).Build()
	if err != nil {
		return err
	}

	input := &dynamodb.UpdateItemInput{
		TableName:                 aws.String(table.key.String()),
		Key:                       feature,
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
	}

	_, err = table.client.UpdateItemWithContext(ctx, input)
	return err
}

func (table dynamodbOnlineTable) Get(entity string) (interface{}, error) {
	primaryKey := map[string]string{
		table.key.Feature: entity,
	}
	feature, err := dynamodbattribute.MarshalMap(primaryKey)
	if err != nil {
		return nil, err
	}

	proj := expression.NamesList(expression.Name("value"))
	expr, err := expression.NewBuilder().WithProjection(proj).Build()
	if err != nil {
		return nil, err
	}

	input := &dynamodb.GetItemInput{
		TableName:                aws.String(table.key.String()),
		Key:                      feature,
		ExpressionAttributeNames: expr.Names(),
		ProjectionExpression:     expr.Projection(),
	}

	output_val, err := table.client.GetItemWithContext(ctx, input)
	if err != nil {
		return nil, err
	}
	dynamodb_item := dynamodbItem{}
	err = dynamodbattribute.UnmarshalMap(output_val.Item, &dynamodb_item)

	if err != nil {
		panic(fmt.Sprintf("Failed to unmarshal Record, %v", err))
	}

	return dynamodb_item.value, err
}
