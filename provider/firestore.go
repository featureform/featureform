// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import (
	"encoding/json"

	"cloud.google.com/go/firestore"
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
	client     *firestore.Client
	collection *firestore.CollectionRef
	document   *firestore.DocumentRef
	key        firestoreTableKey
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

	firestoreClient, err := firestore.NewClient(ctx, options.Collection)
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

	//Check if table exists
	table, err := store.collection.Doc(tableName).Get(ctx)
	if !table.Exists() {
		return nil, &TableNotFound{feature, variant}
	}
	if err != nil {
		return nil, err
	}

	return &firestoreOnlineTable{
		client:     store.client,
		collection: store.collection,
		document:   table.Ref,
		key:        key,
	}, nil
}

func (store *firestoreOnlineStore) CreateTable(feature, variant string, valueType ValueType) (OnlineStoreTable, error) {
	//Check if table exists
	getTable, _ := store.GetTable(feature, variant)
	if getTable != nil {
		return nil, &TableAlreadyExists{feature, variant}
	}

	//Create table
	key := firestoreTableKey{store.collection.ID, feature, variant}
	tableName := key.String()
	_, err := store.collection.Doc(tableName).Set(ctx, map[string]interface{}{})
	if err != nil {
		return nil, err
	}

	//update metadata
	_, err = store.collection.Doc("firestoreMetadata").Set(ctx, map[string]interface{}{
		tableName: valueType,
	}, firestore.MergeAll)

	return &firestoreOnlineTable{
		client:     store.client,
		collection: store.collection,
		document:   store.collection.Doc(tableName),
		key:        key,
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
		return nil, err
	}

	return value, nil
	//https: pkg.go.dev/cloud.google.com/go/firestore#DocumentSnapshot.DataTo
}
