// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import (
	"context"
	"encoding/json"
	"fmt"

	"google.golang.org/grpc/status"

	"cloud.google.com/go/firestore"
	"github.com/featureform/fferr"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"google.golang.org/api/option"
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

func firestoreOnlineStoreFactory(serialized pc.SerializedConfig) (Provider, error) {
	firestoreConfig := &pc.FirestoreConfig{}
	if err := firestoreConfig.Deserialize(serialized); err != nil {
		return nil, err
	}

	return NewFirestoreOnlineStore(firestoreConfig)
}

func NewFirestoreOnlineStore(options *pc.FirestoreConfig) (*firestoreOnlineStore, error) {
	credBytes, err := json.Marshal(options.Credentials)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	firestoreClient, err := firestore.NewClient(context.TODO(), options.ProjectID, option.WithCredentialsJSON(credBytes))
	if err != nil {
		return nil, fferr.NewExecutionError(pt.FirestoreOnline.String(), err)
	}

	firestoreCollection := firestoreClient.Collection(options.Collection)
	_, err = firestoreCollection.Doc(GetMetadataTable()).Set(context.TODO(), map[string]interface{}{}, firestore.MergeAll)
	if err != nil {
		return nil, fferr.NewExecutionError(pt.FirestoreOnline.String(), err)
	}
	return &firestoreOnlineStore{
		firestoreClient,
		firestoreCollection, BaseProvider{
			ProviderType:   pt.FirestoreOnline,
			ProviderConfig: options.Serialize(),
		},
	}, nil
}

func (store *firestoreOnlineStore) AsOnlineStore() (OnlineStore, error) {
	return store, nil
}

func (store *firestoreOnlineStore) Close() error {
	if err := store.client.Close(); err != nil {
		fferr.NewExecutionError(pt.FirestoreOnline.String(), err)
	}
	return nil
}

func GetMetadataTable() string {
	return "featureform_firestore_metadata"
}

func (store *firestoreOnlineStore) GetTable(feature, variant string) (OnlineStoreTable, error) {
	key := firestoreTableKey{store.collection.ID, feature, variant}
	tableName := key.String()

	table, err := store.collection.Doc(tableName).Get(context.TODO())
	if status.Code(err) == codes.NotFound {
		wrapped := fferr.NewDatasetNotFoundError(feature, variant, err)
		wrapped.AddDetail("table_name", tableName)
		return nil, wrapped
	}
	if err != nil {
		wrapped := fferr.NewResourceExecutionError(pt.FirestoreOnline.String(), feature, variant, fferr.FEATURE_VARIANT, err)
		wrapped.AddDetail("table_name", tableName)
		return nil, wrapped
	}

	metadata, err := store.collection.Doc(GetMetadataTable()).Get(context.TODO())
	if err != nil {
		wrapped := fferr.NewResourceExecutionError(pt.FirestoreOnline.String(), feature, variant, fferr.FEATURE_VARIANT, err)
		wrapped.AddDetail("table_name", tableName)
		return nil, wrapped
	}
	valueType, err := metadata.DataAt(tableName)
	if err != nil {
		wrapped := fferr.NewResourceExecutionError(pt.FirestoreOnline.String(), feature, variant, fferr.FEATURE_VARIANT, err)
		wrapped.AddDetail("table_name", tableName)
		return nil, wrapped
	}
	return &firestoreOnlineTable{
		document:  table.Ref,
		key:       key,
		valueType: ScalarType(valueType.(string)),
	}, nil
}

func (store *firestoreOnlineStore) CreateTable(feature, variant string, valueType ValueType) (OnlineStoreTable, error) {
	table, err := store.GetTable(feature, variant)
	if table != nil {
		return nil, fferr.NewDatasetAlreadyExistsError(feature, variant, nil)
	}
	if err != nil {
		return nil, fferr.NewResourceExecutionError(pt.FirestoreOnline.String(), feature, variant, fferr.FEATURE_VARIANT, err)
	}

	key := firestoreTableKey{store.collection.ID, feature, variant}
	tableName := key.String()
	_, err = store.collection.Doc(tableName).Set(context.TODO(), map[string]interface{}{})
	if err != nil {
		return nil, fferr.NewResourceExecutionError(pt.FirestoreOnline.String(), feature, variant, fferr.FEATURE_VARIANT, err)
	}

	_, err = store.collection.Doc(GetMetadataTable()).Set(context.TODO(), map[string]interface{}{
		tableName: valueType,
	}, firestore.MergeAll)
	if err != nil {
		return nil, fferr.NewResourceExecutionError(pt.FirestoreOnline.String(), feature, variant, fferr.FEATURE_VARIANT, err)
	}
	return &firestoreOnlineTable{
		document:  store.collection.Doc(tableName),
		key:       key,
		valueType: valueType,
	}, nil

}

func (store *firestoreOnlineStore) DeleteTable(feature, variant string) error {
	key := firestoreTableKey{store.collection.ID, feature, variant}
	tableName := key.String()
	_, err := store.collection.Doc(tableName).Delete(context.TODO())
	if err != nil {
		return fferr.NewResourceExecutionError(pt.FirestoreOnline.String(), feature, variant, fferr.FEATURE_VARIANT, err)
	}

	_, err = store.collection.Doc(GetMetadataTable()).Update(context.TODO(), []firestore.Update{
		{
			Path:  tableName,
			Value: firestore.Delete,
		},
	})

	if err != nil {
		return fferr.NewResourceExecutionError(pt.FirestoreOnline.String(), feature, variant, fferr.FEATURE_VARIANT, err)
	}

	return nil
}

func (store *firestoreOnlineStore) CheckHealth() (bool, error) {
	return false, fferr.NewInternalError(fmt.Errorf("provider health check not implemented"))
}

func (table firestoreOnlineTable) Set(entity string, value interface{}) error {
	if _, err := table.document.Set(context.TODO(), map[string]interface{}{
		entity: value,
	}, firestore.MergeAll); err != nil {
		wrapped := fferr.NewResourceExecutionError(pt.FirestoreOnline.String(), table.key.Feature, table.key.Variant, fferr.ENTITY, err)
		wrapped.AddDetail("entity", entity)
		return wrapped
	}
	return nil
}

func (table firestoreOnlineTable) Get(entity string) (interface{}, error) {
	dataSnap, err := table.document.Get(context.TODO())
	if err != nil {
		wrapped := fferr.NewResourceExecutionError(pt.FirestoreOnline.String(), table.key.Feature, table.key.Variant, fferr.ENTITY, err)
		wrapped.AddDetail("entity", entity)
		return nil, wrapped
	}
	value, err := dataSnap.DataAt(entity)
	if err != nil {
		return nil, fferr.NewEntityNotFoundError(table.key.Feature, table.key.Variant, entity, err)
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
