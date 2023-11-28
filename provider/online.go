// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import (
	"fmt"

	fs "github.com/featureform/filestore"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
)

var cassandraTypeMap = map[string]string{
	"string":  "text",
	"int":     "int",
	"int64":   "bigint",
	"float32": "float",
	"float64": "double",
	"bool":    "boolean",
}

type OnlineStore interface {
	GetTable(feature, variant string) (OnlineStoreTable, error)
	CreateTable(feature, variant string, valueType ValueType) (OnlineStoreTable, error)
	DeleteTable(feature, variant string) error
	Close() error
	Provider
}

type ImportID string

type Import interface {
	Status() string
	ErrorMessage() string
}

// This interface provides an abstraction for online stores that offer
// bulk creation via import and was created to avoid having to make specific
// online store implementation public for the purpose of calling specialized
// methods on them. Currently, DynamoDB is the only online store that implements
// this interface for the purpose of support the S3 import feature.
type ImportableOnlineStore interface {
	OnlineStore
	ImportTable(feature, variant string, valueType ValueType, source fs.Filepath) (ImportID, error)
	GetImport(id ImportID) (Import, error)
}

type OnlineStoreTable interface {
	Set(entity string, value interface{}) error
	Get(entity string) (interface{}, error)
}

type VectorStore interface {
	CreateIndex(feature, variant string, vectorType VectorType) (VectorStoreTable, error)
	DeleteIndex(feature, variant string) error
	OnlineStore
}

type VectorStoreTable interface {
	OnlineStoreTable
	Nearest(feature, variant string, vector []float32, k int32) ([]string, error)
}

type TableNotFound struct {
	Feature, Variant string
}

func (err *TableNotFound) Error() string {
	return fmt.Sprintf("Table %s Variant %s not found.", err.Feature, err.Variant)
}

type TableAlreadyExists struct {
	Feature, Variant string
}

func (err *TableAlreadyExists) Error() string {
	return fmt.Sprintf("Table %s Variant %s already exists.", err.Feature, err.Variant)
}

type EntityNotFound struct {
	Entity string
}

func (err *EntityNotFound) Error() string {
	return fmt.Sprintf("Entity %s not found.", err.Entity)
}

type tableKey struct {
	feature, variant string
}

type CustomError struct {
	ErrorMessage string
}

func (err *CustomError) Error() string {
	return err.ErrorMessage
}

func localOnlineStoreFactory(pc.SerializedConfig) (Provider, error) {
	return NewLocalOnlineStore(), nil
}

type localOnlineStore struct {
	tables map[tableKey]localOnlineTable
	BaseProvider
}

func NewLocalOnlineStore() *localOnlineStore {
	return &localOnlineStore{
		make(map[tableKey]localOnlineTable),
		BaseProvider{
			ProviderType:   pt.LocalOnline,
			ProviderConfig: []byte{},
		},
	}
}

func (store *localOnlineStore) AsOnlineStore() (OnlineStore, error) {
	return store, nil
}

func (store *localOnlineStore) GetTable(feature, variant string) (OnlineStoreTable, error) {
	table, has := store.tables[tableKey{feature, variant}]
	if !has {
		return nil, &TableNotFound{feature, variant}
	}
	return table, nil
}

func (store *localOnlineStore) CreateTable(feature, variant string, valueType ValueType) (OnlineStoreTable, error) {
	key := tableKey{feature, variant}
	if _, has := store.tables[key]; has {
		return nil, &TableAlreadyExists{feature, variant}
	}
	table := make(localOnlineTable)
	store.tables[key] = table
	return table, nil
}

func (store *localOnlineStore) DeleteTable(feaute, variant string) error {
	return nil
}

func (store *localOnlineStore) Close() error {
	return nil
}

func (store *localOnlineStore) CheckHealth() (bool, error) {
	return false, fmt.Errorf("provider health check not implemented")
}

type localOnlineTable map[string]interface{}

func (table localOnlineTable) Set(entity string, value interface{}) error {
	table[entity] = value
	return nil
}

func (table localOnlineTable) Get(entity string) (interface{}, error) {
	val, has := table[entity]
	if !has {
		return nil, &EntityNotFound{entity}
	}
	return val, nil
}
