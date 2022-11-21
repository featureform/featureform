// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import (
	"cloud.google.com/go/firestore"
	"encoding/json"
	"fmt"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
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
		return nil, fmt.Errorf("could not create firestore document: %v", err)
	}
	return &firestoreOnlineStore{
		firestoreClient,
		firestoreCollection, BaseProvider{
			ProviderType:   FirestoreOnline,
			ProviderConfig: options.Serialize(),
		},
	}, nil
}

func (store *firestoreOnlineStore) AsOnlineStore() (OnlineStore, error) {
	return store, nil
}

func (store *firestoreOnlineStore) Close() error {
	return store.client.Close()
}

func GetMetadataTable() string {
	return "featureform_firestore_metadata"
}

func (store *firestoreOnlineStore) GetTable(feature, variant string) (OnlineStoreTable, error) {
	key := firestoreTableKey{store.collection.ID, feature, variant}
	tableName := key.String()

	table, err := store.collection.Doc(tableName).Get(ctx)
	if status.Code(err) == codes.NotFound {
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
	if err := valueType.isValid(); err != nil {
		return nil, err
	}
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
	if !table.valueType.doesMatch(value) {
		return fmt.Errorf("value does not match table type: given %T, table type: %s", value, table.valueType)
	}
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
	return table.castValue(value, table.valueType)
}

func (table firestoreOnlineTable) castValue(value interface{}, vType ValueType) (interface{}, error) {
	switch vType {
	case NilType, String:
		str := fmt.Sprintf("%v", value)
		return str, nil
	case Int:
		if val, ok := value.(int64); !ok {
			return nil, fmt.Errorf("could not cast value %v to %s", value, Int)
		} else {
			return int(val), nil
		}

	case Int32:
		if val, ok := value.(int64); !ok {
			return nil, fmt.Errorf("could not cast value %v to %s", value, Int32)
		} else {
			return int32(val), nil
		}
	case Int64:
		if val, ok := value.(int64); !ok {
			return nil, fmt.Errorf("could not cast value %v to %s", value, Int64)
		} else {
			return int64(val), nil
		}
	case Float32:
		if val, ok := value.(float64); !ok {
			return nil, fmt.Errorf("could not cast value %v to %s", value, Float32)
		} else {
			return float32(val), nil
		}
	case Float64:
		if val, ok := value.(float64); !ok {
			return nil, fmt.Errorf("could not cast value %v to %s", value, Float64)
		} else {
			return float64(val), nil
		}
	case Bool:
		if val, ok := value.(bool); !ok {
			return nil, fmt.Errorf("could not cast value %v to %s", value, Bool)
		} else {
			return val, nil
		}
	case Timestamp, Datetime:
		if val, ok := value.(time.Time); !ok {
			return nil, fmt.Errorf("could not cast value %v to %s", value, Datetime)
		} else {
			return time.Time(val), nil
		}
	default:
		val := fmt.Sprintf("%v", value)
		return val, nil
	}
}
