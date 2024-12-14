// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/featureform/logging"
	se "github.com/featureform/provider/serialization"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/status"

	"cloud.google.com/go/firestore"
	"github.com/featureform/fferr"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	vt "github.com/featureform/provider/types"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
)

// The serializer versions. If adding a new one make sure to add to the serializers map variable
const (
	// firestoreSerializeV0 serializes everything as strings, including numbers
	firestoreSerializeV0 se.SerializeVersion = iota
)

type firestoreSerializerV0 struct{}

func (ser firestoreSerializerV0) Version() se.SerializeVersion { return firestoreSerializeV0 }

func (ser firestoreSerializerV0) Serialize(_ vt.ValueType, value any) (interface{}, error) {
	// The client automatically handles this for us.
	return value, nil
}

func (ser firestoreSerializerV0) Deserialize(t vt.ValueType, value interface{}) (any, error) {
	// Firestore only has one integer and float type, each being converted into int64 and float64 respectively.
	// For conversions, see https://pkg.go.dev/cloud.google.com/go/firestore@v1.15.0#DocumentSnapshot.DataTo
	switch t {
	case vt.Int:
		return int(value.(int64)), nil
	case vt.Int8:
		return int8(value.(int64)), nil
	case vt.Int16:
		return int16(value.(int64)), nil
	case vt.Int32:
		return int32(value.(int64)), nil
	case vt.Int64:
		return value.(int64), nil
	case vt.Float32:
		return float32(value.(float64)), nil
	case vt.Float64:
		return value.(float64), nil
	case vt.NilType, vt.Bool, vt.String, vt.Timestamp:
		// These are all converted to the same types we use by the client
		return value, nil
	default:
		err := fferr.NewInternalErrorf("Unsupported casting value %v of type %T into %v", value, value, t)
		err.AddDetail("version", ser.Version().String())
		return nil, err
	}
}

func (t firestoreTableKey) String() string {
	return fmt.Sprintf("%s__%s__%s", t.Collection, t.Feature, t.Variant)
}

type firestoreOnlineStore struct {
	client     *firestore.Client
	collection *firestore.CollectionRef
	logger     *zap.SugaredLogger
	BaseProvider
}

type firestoreOnlineTable struct {
	document   *firestore.DocumentRef
	key        firestoreTableKey
	valueType  vt.ValueType
	serializer se.Serializer[interface{}]
	logger     *zap.SugaredLogger
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

	logger := logging.NewLogger("firestore")

	return &firestoreOnlineStore{
		firestoreClient,
		firestoreCollection,
		logger.SugaredLogger,
		BaseProvider{
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
	logger := store.logger.With("table", tableName)
	return &firestoreOnlineTable{
		document:   table.Ref,
		key:        key,
		valueType:  vt.ScalarType(valueType.(string)),
		serializer: firestoreSerializerV0{},
		logger:     logger,
	}, nil
}

func (store *firestoreOnlineStore) CreateTable(feature, variant string, valueType vt.ValueType) (OnlineStoreTable, error) {
	table, _ := store.GetTable(feature, variant)
	if table != nil {
		return nil, fferr.NewDatasetAlreadyExistsError(feature, variant, nil)
	}

	key := firestoreTableKey{store.collection.ID, feature, variant}
	tableName := key.String()
	_, err := store.collection.Doc(tableName).Set(context.TODO(), map[string]interface{}{})
	if err != nil {
		return nil, fferr.NewResourceExecutionError(pt.FirestoreOnline.String(), feature, variant, fferr.FEATURE_VARIANT, err)
	}

	_, err = store.collection.Doc(GetMetadataTable()).Set(context.TODO(), map[string]interface{}{
		tableName: valueType,
	}, firestore.MergeAll)
	if err != nil {
		return nil, fferr.NewResourceExecutionError(pt.FirestoreOnline.String(), feature, variant, fferr.FEATURE_VARIANT, err)
	}
	logger := store.logger.With("table", tableName)
	return &firestoreOnlineTable{
		document:   store.collection.Doc(tableName),
		key:        key,
		valueType:  valueType,
		serializer: firestoreSerializerV0{},
		logger:     logger,
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
	_, err := store.client.Collections(context.TODO()).Next()
	if err != nil && !errors.Is(err, iterator.Done) {
		store.logger.Error("Health check failed, unable to connect to firestore")
		return false, fferr.NewExecutionError(pt.FirestoreOnline.String(), err)
	}
	store.logger.Info("Health check successful")
	return true, nil
}

func (table firestoreOnlineTable) Set(entity string, value interface{}) error {
	serializedValue, err := table.serializer.Serialize(table.valueType, value)

	if err != nil {
		table.logger.Error("Error serializing value", "entity", entity, "value", value, "err", err)
		return err
	}

	if _, err := table.document.Set(context.TODO(), map[string]interface{}{
		entity: serializedValue,
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
	case types.Int:
		var intVal int64 = value.(int64)
		return int(intVal), nil
	case types.Float32:
		var floatVal float64 = value.(float64)
		return float32(floatVal), nil
	}

	return value, nil
}
