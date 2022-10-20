package provider

import (
	"context"
	"fmt"

	sn "github.com/mrz1836/go-sanitize"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type mongodbTableKey struct {
	Prefix, Feature, Variant string
}

func (t mongodbTableKey) String() string {
	tablename := fmt.Sprintf("%s_%s_%s", sn.Custom(t.Prefix, "[^a-zA-Z0-9_]"), sn.Custom(t.Feature, "[^a-zA-Z0-9_]"), sn.Custom(t.Variant, "[^a-zA-Z0-9_]"))
	return sn.Custom(tablename, "[^a-zA-Z0-9_.\\-]")
}

type mongodbOnlineStore struct {
	client *mongo.Client
	prefix string
	BaseProvider
}

type mongodbOnlineTable struct {
	client    *mongo.Client
	key       mongodbTableKey
	valueType ValueType
}

func mongodbOnlineStoreFactory(serialized SerializedConfig) (Provider, error) {
	mongodbConfig := &MongodbConfig{}
	if err := mongodbConfig.Deserialize(serialized); err != nil {
		return nil, err
	}
	if mongodbConfig.Prefix == "" {
		mongodbConfig.Prefix = "Featureform_table__"
	}
	return NewMongoDBOnlineStore(mongodbConfig), nil
}

func NewMongoDBOnlineStore(mongodbConfig *MongodbConfig) *mongodbOnlineStore {
	mongodbClient, err := mongo.Connect(
		context.TODO(),
		options.Client().ApplyURI(mongodbConfig.MongodbUri),
	)

	if err != nil {
		panic(err)
	}

	return &mongodbOnlineStore{mongodbClient, mongodbConfig.Prefix, BaseProvider{
		ProviderType:   MongoDBOnline,
		ProviderConfig: mongodbConfig.Serialized(),
	}}
}

func (store *mongodbOnlineStore) CreateTable(feature, variant string, valueType ValueType) (OnlineStoreTable, error) {
	// TODO: get database name from user
	key := mongodbTableKey{store.prefix, feature, variant}
	fmt.Println(key)

	cNames, err := store.client.Database("featureform").ListCollectionNames(context.TODO(), bson.D{})
	fmt.Println(cNames)

	if err != nil {
		return nil, err
	}

	for _, n := range cNames {
		if key.String() == n {
			fmt.Println(n)
			return nil, &TableAlreadyExists{feature, variant}
		}
	}

	return &mongodbOnlineTable{store.client, key, valueType}, nil
}

func (table mongodbOnlineTable) Set(entity string, value interface{}) error {
	coll := table.client.Database("featureform").Collection(table.key.String())
	doc := bson.D{{"entity", entity}, {"value", value}}

	var foundEntity bson.M
	err := coll.FindOne(context.TODO(), bson.M{"entity": entity}).Decode(&foundEntity)

	if err != nil {
		result, err := coll.InsertOne(context.TODO(), doc)

		fmt.Println(result)

		if err != nil {
			return err
		}
	} else {
		// TODO: Implement update value for entity
		//  Note: entity is primary key so if entity exists then update the value
	}

	return nil
}

func (table mongodbOnlineTable) Get(entity string) (interface{}, error) {
	// TODO: Implement Get
	return nil, nil
}
