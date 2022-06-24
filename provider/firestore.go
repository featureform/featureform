// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import (
	"cloud.google.com/go/firestore"
	"encoding/json"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func (t firestoreTableKey) String() string {
	marshalled, err := json.Marshal(t)
	if err != nil {
		return err.Error()
	}
	return string(marshalled)
}

type firestoreOnlineStore struct {
	client     *firestore.Client
	collection *firestore.CollectionRef
	BaseProvider
}

type firestoreOnlineTable struct {
	document  *firestore.DocumentRef
	key       firestoreTableKey
	valueType ValueType
}

type firestoreTableKey struct {
	Collection, Feature, Variant string
}

func firestoreOnlineStoreFactory(serialized SerializedConfig) (Provider, error) {
	firestoreConfig := &FirestoreConfig{}
	if err := firestoreConfig.Deserialize(serialized); err != nil {
		return nil, err
	}
	if firestoreConfig.Collection == "" {
		firestoreConfig.Collection = "Featureform_table__"
	}
	return NewFirestoreOnlineStore(firestoreConfig)
}

func NewFirestoreOnlineStore(options *FirestoreConfig) (*firestoreOnlineStore, error) {
	firestoreClient, err := firestore.NewClient(ctx, options.ProjectID, option.WithCredentialsJSON(options.Credentials))
	if err != nil {
		return nil, err
	}

	firestoreCollection := firestoreClient.Collection(options.Collection)
	_, err = firestoreCollection.Doc("firestoreMetadata").Set(ctx, map[string]interface{}{})

	return &firestoreOnlineStore{firestoreClient, firestoreCollection, BaseProvider{
		ProviderType:   FirestoreOnline,
		ProviderConfig: options.Serialized(),
	},
	}, nil
}

func (store *firestoreOnlineStore) AsOnlineStore() (OnlineStore, error) {
	return store, nil
}

func (store *firestoreOnlineStore) GetTable(feature, variant string) (OnlineStoreTable, error) {

	key := firestoreTableKey{store.collection.ID, feature, variant}
	tableName := key.String()

	table, err := store.collection.Doc(tableName).Get(ctx)
	if grpc.Code(err) == codes.NotFound {
		return nil, &TableNotFound{feature, variant}
	}
	if err != nil {
		return nil, err
	}

	metadata, err := store.collection.Doc("firestoreMetadata").Get(ctx)
	if err != nil {
		return nil, err
	}
	valueType, err := metadata.DataAt(tableName)
	if err != nil {
		return nil, err
	}

	return &firestoreOnlineTable{
		document:  table.Ref,
		key:       key,
		valueType: ValueType(valueType.(string)),
	}, nil
}

func (store *firestoreOnlineStore) CreateTable(feature, variant string, valueType ValueType) (OnlineStoreTable, error) {

	getTable, _ := store.GetTable(feature, variant)
	if getTable != nil {
		return nil, &TableAlreadyExists{feature, variant}
	}

	key := firestoreTableKey{store.collection.ID, feature, variant}
	tableName := key.String()
	_, err := store.collection.Doc(tableName).Set(ctx, map[string]interface{}{})
	if err != nil {
		return nil, err
	}

	_, err = store.collection.Doc("firestoreMetadata").Set(ctx, map[string]interface{}{
		tableName: valueType,
	}, firestore.MergeAll)

	return &firestoreOnlineTable{
		document:  store.collection.Doc(tableName),
		key:       key,
		valueType: valueType,
	}, nil

}

func (table firestoreOnlineTable) Set(entity string, value interface{}) error {
	_, err := table.document.Set(ctx, map[string]interface{}{
		entity: value,
	}, firestore.MergeAll)

	return err
}

func (table firestoreOnlineTable) Get(entity string) (interface{}, error) {

	dataSnap, err := table.document.Get(ctx)
	if err != nil {
		return nil, err
	}
	value, err := dataSnap.DataAt(entity)
	if err != nil {
		return nil, &EntityNotFound{entity}
	}

	switch table.valueType {
	case Int:
		var intVal int64 = value.(int64)
		return int(intVal), nil
	case Float32:
		var floatVal float64 = value.(float64)
		return float32(floatVal), nil
	}

	return value, nil
}
