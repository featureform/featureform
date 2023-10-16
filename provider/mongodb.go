// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import (
	"context"
	"fmt"

	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	sn "github.com/mrz1836/go-sanitize"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

type mongoDBMetadataRow struct {
	Name string
	T    string
}

type mongoDBOnlineStore struct {
	client          *mongo.Client
	database        string
	tableThroughput int
	BaseProvider
}

type mongoDBOnlineTable struct {
	client    *mongo.Client
	database  string
	name      string
	valueType ValueType
}

func mongoOnlineStoreFactory(serialized pc.SerializedConfig) (Provider, error) {
	mongoConfig := &pc.MongoDBConfig{}
	if err := mongoConfig.Deserialize(serialized); err != nil {
		return nil, err
	}

	return NewMongoDBOnlineStore(mongoConfig)
}

func NewMongoDBOnlineStore(config *pc.MongoDBConfig) (*mongoDBOnlineStore, error) {
	uri := fmt.Sprintf("mongodb://%s:%s@%s:%s/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000", config.Username, config.Password, config.Host, config.Port)
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("could not connect to mongodb: %w", err)
	}
	cur, err := client.Database(config.Database).ListCollections(context.TODO(), bson.D{{"name", "featureform__metadata"}})
	if err != nil {
		return nil, fmt.Errorf("could not create check if metadata exists: %w", err)
	}
	var res []interface{}
	err = cur.All(context.TODO(), &res)
	if err != nil {
		return nil, fmt.Errorf("could not get metadata results: %w", err)
	}
	if len(res) == 0 {
		command := bson.D{{"customAction", "CreateCollection"}, {"collection", "featureform__metadata"}, {"autoScaleSettings", bson.D{{"maxThroughput", 1000}}}}
		var cmdResult interface{}
		wConcern := writeconcern.New(writeconcern.J(true), writeconcern.WMajority())
		err := client.Database(config.Database, &options.DatabaseOptions{
			WriteConcern: wConcern,
		}).RunCommand(context.TODO(), command).Decode(&cmdResult)
		if err != nil {
			return nil, fmt.Errorf("could not set metadata table throughput: %w", err)
		}
	}

	return &mongoDBOnlineStore{
		client:          client,
		database:        config.Database,
		tableThroughput: config.Throughput,
		BaseProvider: BaseProvider{
			ProviderType:   pt.MongoDBOnline,
			ProviderConfig: config.Serialized(),
		},
	}, nil
}

func (store *mongoDBOnlineStore) AsOnlineStore() (OnlineStore, error) {
	return store, nil
}

func (store *mongoDBOnlineStore) Close() error {
	err := store.client.Disconnect(context.TODO())
	if err != nil {
		return fmt.Errorf("could not close mongoDB online store session: %w", err)
	}
	return nil
}

func (store *mongoDBOnlineStore) GetTableName(feature, variant string) string {
	tableName := fmt.Sprintf("featureform__%s__%s", sn.Custom(feature, "[^a-zA-Z0-9_]"), sn.Custom(variant, "[^a-zA-Z0-9_]"))
	return tableName
}

func (store *mongoDBOnlineStore) GetMetadataTableName() string {
	metadataTableName := fmt.Sprintf("featureform__metadata")
	return metadataTableName
}

func (store *mongoDBOnlineStore) CreateTable(feature, variant string, valueType ValueType) (OnlineStoreTable, error) {
	tableName := store.GetTableName(feature, variant)
	vType := string(valueType.Scalar())
	getTable, _ := store.GetTable(feature, variant)
	if getTable != nil {
		return nil, &TableAlreadyExists{feature, variant}
	}

	metadataTableName := store.GetMetadataTableName()
	wConcern := writeconcern.New(writeconcern.J(true), writeconcern.WMajority())
	_, err := store.client.Database(store.database, &options.DatabaseOptions{
		WriteConcern: wConcern,
	}).Collection(metadataTableName).InsertOne(context.TODO(), mongoDBMetadataRow{tableName, vType})
	if err != nil {
		return nil, fmt.Errorf("could not insert metadata table name: %w", err)
	}

	command := bson.D{{"customAction", "CreateCollection"}, {"collection", tableName}, {"autoScaleSettings", bson.D{{"maxThroughput", store.tableThroughput}}}}
	var cmdResult interface{}
	err = store.client.Database(store.database).RunCommand(context.TODO(), command).Decode(&cmdResult)
	if err != nil {
		return nil, fmt.Errorf("could not set table throughput: %s, %w", tableName, err)
	}

	table := &mongoDBOnlineTable{
		client:    store.client,
		database:  store.database,
		name:      tableName,
		valueType: valueType,
	}

	return table, nil

}

func (store *mongoDBOnlineStore) GetTable(feature, variant string) (OnlineStoreTable, error) {
	tableName := store.GetTableName(feature, variant)
	cur, err := store.client.Database(store.database).ListCollections(context.TODO(), bson.D{{"name", tableName}})
	if err != nil {
		return nil, fmt.Errorf("could not create check if metadata exists: %w", err)
	}
	var res []interface{}
	err = cur.All(context.TODO(), &res)
	if err != nil {
		return nil, fmt.Errorf("could not get metadata results: %w", err)
	}
	if len(res) == 0 {
		return nil, &TableNotFound{feature, variant}
	}

	var row mongoDBMetadataRow
	err = store.client.Database(store.database).Collection(store.GetMetadataTableName()).FindOne(context.TODO(), bson.D{{"name", tableName}}).Decode(&row)
	if err != nil {
		return nil, fmt.Errorf("could not get metadata table value: %s, %w", tableName, err)
	}
	if err != nil {
		return nil, fmt.Errorf("could not get metadata table value type: %s, %w", tableName, err)
	}
	table := &mongoDBOnlineTable{
		client:    store.client,
		database:  store.database,
		name:      tableName,
		valueType: ScalarType(row.T),
	}
	return table, nil
}

func (store *mongoDBOnlineStore) DeleteTable(feature, variant string) error {
	tableName := store.GetTableName(feature, variant)
	err := store.client.Database(store.database).Collection(tableName).Drop(context.TODO())
	if err != nil {
		return fmt.Errorf("could not drop collection: %s: %w", tableName, err)
	}
	_, err = store.client.Database(store.database).Collection(store.GetMetadataTableName()).DeleteOne(context.TODO(), bson.D{{"name", tableName}})
	if err != nil {
		return fmt.Errorf("could not drop collection: %s: %w", tableName, err)
	}
	return nil
}

func (store *mongoDBOnlineStore) Check() (bool, error) {
	return false, fmt.Errorf("provider health check not implemented")
}

func (table mongoDBOnlineTable) Set(entity string, value interface{}) error {
	upsert := true
	_, err := table.client.Database(table.database).
		Collection(table.name).
		UpdateOne(
			context.TODO(),
			bson.D{{"entity", entity}},
			bson.D{{"$set", bson.D{{"entity", entity}, {"value", value}}}},
			&options.UpdateOptions{
				Upsert: &upsert,
			},
		)
	if err != nil {
		return fmt.Errorf("could not set values: (entity: %s, value: %v): %w", entity, value, err)
	}
	return nil
}

func (table mongoDBOnlineTable) Get(entity string) (interface{}, error) {

	type tableRow struct {
		ID     primitive.ObjectID `bson:"_id"`
		Entity string             `bson:"entity"`
		Value  interface{}        `bson:"value"`
	}
	var row tableRow
	err := table.client.Database(table.database).Collection(table.name).FindOne(context.TODO(), bson.D{{"entity", entity}}).Decode(&row)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			fmt.Printf("could not get table value: %s: %s: %s", table.name, entity, err.Error())
			return nil, &EntityNotFound{entity}
		}
		return nil, fmt.Errorf("could not get table value: %s: %s: %w", table.name, entity, err)
	}

	switch table.valueType {
	case Int:
		return int(row.Value.(int32)), nil
	case Int64:
		return row.Value.(int64), nil
	case Float32:
		return float32(row.Value.(float64)), nil
	case Float64:
		return row.Value.(float64), nil
	case Bool:
		return row.Value.(bool), nil
	case String, NilType:
		return row.Value.(string), nil
	default:
		return nil, fmt.Errorf("given data type not recognized: %v", table.valueType)
	}

}
