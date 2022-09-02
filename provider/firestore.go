// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import (
	"encoding/json"
	"fmt"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func (t firestoreTableKey) String() string {
	return fmt.Sprintf("%s__%s__%s", t.Collection, t.Feature, t.Variant)
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

	return NewFirestoreOnlineStore(firestoreConfig)
}

func NewFirestoreOnlineStore(options *FirestoreConfig) (*firestoreOnlineStore, error) {
	credBytes, err := json.Marshal(options.Credentials)
	if err != nil {
		return nil, fmt.Errorf("could not serialized firestore config, %v", err)
	}
	firestoreClient, err := firestore.NewClient(ctx, options.ProjectID, option.WithCredentialsJSON(credBytes))
	if err != nil {
		return nil, fmt.Errorf("could not create firestore connection, %v", err)
	}

	firestoreCollection := firestoreClient.Collection(options.Collection)
	_, err = firestoreCollection.Doc(GetMetadataTable()).Set(ctx, map[string]interface{}{}, firestore.MergeAll)
	if err != nil {
		return nil, fmt.Errorf("could not create firestore document, %v", err)
	}
	return &firestoreOnlineStore{firestoreClient, firestoreCollection, BaseProvider{
		ProviderType:   FirestoreOnline,
		ProviderConfig: options.Serialize(),
	},
	}, nil
}

func (store *firestoreOnlineStore) AsOnlineStore() (OnlineStore, error) {
	return store, nil
}

func GetMetadataTable() string {
	return "featureform_firestore_metadata"
}

func (store *firestoreOnlineStore) GetTable(feature, variant string) (OnlineStoreTable, error) {
	fmt.Println("Firestore GetTable()")
	key := firestoreTableKey{store.collection.ID, feature, variant}
	tableName := key.String()

	table, err := store.collection.Doc(tableName).Get(ctx)
	if grpc.Code(err) == codes.NotFound {
		return nil, &TableNotFound{feature, variant}
	}
	if err != nil {
		return nil, fmt.Errorf("could not get table: %v", err)
	}

	metadata, err := store.collection.Doc(GetMetadataTable()).Get(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not get metadata table: %v", err)
	}
	valueType, err := metadata.DataAt(tableName)
	if err != nil {
		return nil, fmt.Errorf("could not get data at: %v", err)
	}

	return &firestoreOnlineTable{
		document:  table.Ref,
		key:       key,
		valueType: ValueType(valueType.(string)),
	}, nil
}

func (store *firestoreOnlineStore) CreateTable(feature, variant string, valueType ValueType) (OnlineStoreTable, error) {
	fmt.Println("Firestore CreateTable()")
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

	_, err = store.collection.Doc(GetMetadataTable()).Set(ctx, map[string]interface{}{
		tableName: valueType,
		"test":    "test",
	}, firestore.MergeAll)
	if err != nil {
		return nil, fmt.Errorf("could not insert into metadata table: %v", err)
	}
	return &firestoreOnlineTable{
		document:  store.collection.Doc(tableName),
		key:       key,
		valueType: valueType,
	}, nil

}

func (store *firestoreOnlineStore) DeleteTable(feature, variant string) error {
	fmt.Println("Firestore DeleteTable()")
	key := firestoreTableKey{store.collection.ID, feature, variant}
	tableName := key.String()
	_, err := store.collection.Doc(tableName).Delete(ctx)
	if err != nil {
		return err
	}

	_, err = store.collection.Doc(GetMetadataTable()).Update(ctx, []firestore.Update{
		{
			Path:  tableName,
			Value: firestore.Delete,
		},
	})

	if err != nil {
		return err
	}

	return nil
}

func (table firestoreOnlineTable) Set(entity string, value interface{}) error {
	fmt.Println("Firestore Set()")
	_, err := table.document.Set(ctx, map[string]interface{}{
		entity: value,
	}, firestore.MergeAll)

	return err
}

func (table firestoreOnlineTable) Get(entity string) (interface{}, error) {
	fmt.Println("Firestore Get()")
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
