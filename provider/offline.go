// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"golang.org/x/sync/syncmap"

	"github.com/mitchellh/mapstructure"

	"github.com/featureform/metadata"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/google/uuid"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

type OfflineResourceType int

const (
	NoType OfflineResourceType = iota
	Label
	Feature
	TrainingSet
	Primary
	Transformation
	FeatureMaterialization
)

var ProviderToMetadataResourceType = map[OfflineResourceType]metadata.ResourceType{
	Feature:        metadata.FEATURE_VARIANT,
	TrainingSet:    metadata.TRAINING_SET_VARIANT,
	Primary:        metadata.SOURCE_VARIANT,
	Transformation: metadata.SOURCE_VARIANT,
}

func (offlineType OfflineResourceType) String() string {
	typeMap := map[OfflineResourceType]string{
		Label:                  "Label",
		Feature:                "Feature",
		TrainingSet:            "TrainingSet",
		Primary:                "Primary",
		Transformation:         "Transformation",
		FeatureMaterialization: "Materialization",
	}
	return typeMap[offlineType]
}

type FeatureLabelColumnType string

const (
	Entity FeatureLabelColumnType = "entity"
	Value  FeatureLabelColumnType = "value"
	TS     FeatureLabelColumnType = "ts"
)

type ResourceID struct {
	Name, Variant string
	Type          OfflineResourceType
}

func (id *ResourceID) ToFilestorePath() string {
	return fmt.Sprintf("featureform/%s/%s/%s", id.Type, id.Name, id.Variant)
}

// TODO: add unit tests
func (id *ResourceID) FromFilestorePath(path string) error {
	featureformRootPathPart := "featureform/"
	idx := strings.Index(path, featureformRootPathPart)
	if idx == -1 {
		return fmt.Errorf("expected \"featureform\" root path part in path %s", path)
	}
	resourceParts := strings.Split(path[idx+len(featureformRootPathPart):], "/")
	if len(resourceParts) < 3 {
		return fmt.Errorf("expected path %s to contain OfflineResourceType/Name/Variant", strings.Join(resourceParts, "/"))
	}
	switch resourceParts[0] {
	case "Label":
		id.Type = OfflineResourceType(1)
	case "Feature":
		id.Type = OfflineResourceType(2)
	case "TrainingSet":
		id.Type = OfflineResourceType(3)
	case "Primary":
		id.Type = OfflineResourceType(4)
	case "Transformation":
		id.Type = OfflineResourceType(5)
	case "Materialization":
		id.Type = OfflineResourceType(6)
	default:
		return fmt.Errorf("unrecognized OfflineResourceType: %s", resourceParts[0])
	}
	id.Name = resourceParts[1]
	id.Variant = resourceParts[2]
	return nil
}

func (id *ResourceID) check(expectedType OfflineResourceType, otherTypes ...OfflineResourceType) error {
	if id.Name == "" {
		return errors.New("ResourceID must have Name set")
	}
	// If there is one expected type, we will default to it.
	if id.Type == NoType && len(otherTypes) == 0 {
		id.Type = expectedType
		return nil
	}
	possibleTypes := append(otherTypes, expectedType)
	for _, t := range possibleTypes {
		if id.Type == t {
			return nil
		}
	}
	return fmt.Errorf("unexpected ResourceID Type: %v", id.Type)
}

type LagFeatureDef struct {
	FeatureName    string
	FeatureVariant string
	LagName        string
	LagDelta       time.Duration
}

type TrainingSetDef struct {
	ID          ResourceID
	Label       ResourceID
	Features    []ResourceID
	LagFeatures []LagFeatureDef
}

func (def *TrainingSetDef) check() error {
	if err := def.ID.check(TrainingSet); err != nil {
		return err
	}
	if err := def.Label.check(Label); err != nil {
		return err
	}
	if len(def.Features) == 0 {
		return errors.New("training set must have atleast one feature")
	}
	for i := range def.Features {
		// We use features[i] to make sure that the Type value is updated to
		// Feature if it's unset.
		if err := def.Features[i].check(Feature); err != nil {
			return err
		}
	}
	return nil
}

type TransformationType int

const (
	NoTransformationType TransformationType = iota
	SQLTransformation
	DFTransformation
)

type SourceMapping struct {
	Template string
	Source   string
}

type TransformationConfig struct {
	Type          TransformationType
	TargetTableID ResourceID
	Query         string
	Code          []byte
	SourceMapping []SourceMapping
	Args          metadata.TransformationArgs
	ArgType       metadata.TransformationArgType
}

func (m *TransformationConfig) MarshalJSON() ([]byte, error) {
	var argType metadata.TransformationArgType
	if m.Args != nil {
		argType = m.Args.Type()
	} else {
		argType = metadata.NoArgs
	}
	m.ArgType = argType

	// Prevents recursion in marshal
	type config TransformationConfig
	c := config(*m)
	marshal, err := json.Marshal(&c)
	if err != nil {
		return nil, err
	}
	return marshal, nil
}

func (m *TransformationConfig) UnmarshalJSON(data []byte) error {
	type tempConfig struct {
		Type          TransformationType
		TargetTableID ResourceID
		Query         string
		Code          []byte
		SourceMapping []SourceMapping
		Args          map[string]interface{}
		ArgType       metadata.TransformationArgType
	}

	var temp tempConfig
	err := json.Unmarshal(data, &temp)
	if err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}

	m.Type = temp.Type
	m.TargetTableID = temp.TargetTableID
	m.Query = temp.Query
	m.Code = temp.Code
	m.SourceMapping = temp.SourceMapping

	err = m.decodeArgs(temp.ArgType, temp.Args)
	if err != nil {
		return fmt.Errorf("decode: %w", err)
	}

	return nil
}

func (m *TransformationConfig) decodeArgs(t metadata.TransformationArgType, argMap map[string]interface{}) error {

	var args metadata.TransformationArgs
	switch t {
	case metadata.K8sArgs:
		args = metadata.KubernetesArgs{}
	case metadata.NoArgs:
		m.Args = nil
		return nil
	default:
		return fmt.Errorf("invalid transformation arg type")
	}
	err := mapstructure.Decode(argMap, &args)
	if err != nil {
		return fmt.Errorf("could not decode map: %w", err)
	}
	m.Args = args
	return nil
}

type OfflineStore interface {
	RegisterResourceFromSourceTable(id ResourceID, schema ResourceSchema) (OfflineTable, error)
	RegisterPrimaryFromSourceTable(id ResourceID, sourceName string) (PrimaryTable, error)
	CreateTransformation(config TransformationConfig) error
	GetTransformationTable(id ResourceID) (TransformationTable, error)
	UpdateTransformation(config TransformationConfig) error
	CreatePrimaryTable(id ResourceID, schema TableSchema) (PrimaryTable, error)
	GetPrimaryTable(id ResourceID) (PrimaryTable, error)
	CreateResourceTable(id ResourceID, schema TableSchema) (OfflineTable, error)
	GetResourceTable(id ResourceID) (OfflineTable, error)
	CreateMaterialization(id ResourceID) (Materialization, error)
	GetMaterialization(id MaterializationID) (Materialization, error)
	UpdateMaterialization(id ResourceID) (Materialization, error)
	DeleteMaterialization(id MaterializationID) error
	CreateTrainingSet(TrainingSetDef) error
	UpdateTrainingSet(TrainingSetDef) error
	GetTrainingSet(id ResourceID) (TrainingSetIterator, error)
	Close() error
	Provider
}

type MaterializationID string

type TrainingSetIterator interface {
	Next() bool
	Features() []interface{}
	Label() interface{}
	Err() error
}

type GenericTableIterator interface {
	Next() bool
	Values() GenericRecord
	Columns() []string
	Err() error
	Close() error
}

type Materialization interface {
	ID() MaterializationID
	NumRows() (int64, error)
	IterateSegment(begin, end int64) (FeatureIterator, error)
}

type FeatureIterator interface {
	Next() bool
	Value() ResourceRecord
	Err() error
	Close() error
}

// Used to implement sort.Interface
type ResourceRecords []ResourceRecord

func (recs ResourceRecords) Swap(i, j int) {
	recs[i], recs[j] = recs[j], recs[i]
}

func (recs ResourceRecords) Less(i, j int) bool {
	return recs[j].TS.After(recs[i].TS)
}

func (recs ResourceRecords) Len() int {
	return len(recs)
}

type ResourceRecord struct {
	Entity string
	Value  interface{}
	// Defaults to 00:00 on 01-01-0001, technically if a user sets a time
	// in a BC year for some reason, our default time would not be the
	// earliest time in the feature store.
	TS time.Time
}

// This generic version of ResourceRecord is only used for converting
// ResourceRecord to a type that's interpretable by parquet-go. See
// BlobOfflineTable.writeRecordsToParquetBytes for more details.
// In addition to using generics to aid in parquet-go's encoding, int64
// is used for the timestamp due to a Spark issue relating to time.Time:
// org.apache.spark.sql.AnalysisException: Illegal Parquet type: INT64 (TIMESTAMP(NANOS,true))
type GenericResourceRecord[T any] struct {
	Entity string
	Value  T
	// TS     int64
	TS time.Time `parquet:"TS,timestamp"`
}

type GenericRecord []interface{}

func (rec ResourceRecord) check() error {
	if rec.Entity == "" {
		return errors.New("resourceRecord must have Entity set")
	}
	return nil
}

// This interface represents the contract for implementations that
// write feature and label tables, which have a knowable schema.
type OfflineTable interface {
	Write(ResourceRecord) error
	WriteBatch([]ResourceRecord) error
}

// The "primary" in the name here might be misleading.
// This interface is meant to support generic tables,
// such as those created by transformations.
type PrimaryTable interface {
	Write(GenericRecord) error
	WriteBatch([]GenericRecord) error
	// TODO: Consider renaming this to GetSchema, which is more
	// descriptive and general purpose. The SourceTable string
	// could be used by callers that are only interested in the
	// absolute path to the source table (i.e. the "name" in our
	// current lexicon).
	GetName() string
	IterateSegment(n int64) (GenericTableIterator, error)
	NumRows() (int64, error)
}

type TransformationTable interface {
	PrimaryTable
}

type ResourceSchema struct {
	Entity      string
	Value       string
	TS          string
	SourceTable string
}

func (schema *ResourceSchema) Serialize() ([]byte, error) {
	config, err := json.Marshal(schema)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize resource schema due to: %w", err)
	}
	return config, nil
}

func (schema *ResourceSchema) Deserialize(config []byte) error {
	err := json.Unmarshal(config, schema)
	if err != nil {
		return fmt.Errorf("failed to deserialize resource schema due to: %w", err)
	}
	return nil
}

type TableSchema struct {
	Columns []TableColumn
	// The complete URL that points to the location of the data file
	SourceTable string
}

type TableSchemaJSONWrapper struct {
	Columns     []TableColumnJSONWrapper
	SourceTable string
}

// This method converts the list of columns into a struct type that can be
// serialized by parquet-go. This is necessary because GenericRecord, which
// is of type []interface{}, does not hold the necessary metadata information
// to create a valid parquet-go schema.
func (schema *TableSchema) Interface() interface{} {
	return schema.Value().Interface()
}

func (schema *TableSchema) Value() reflect.Value {
	fields := make([]reflect.StructField, len(schema.Columns))
	for i, col := range schema.Columns {
		caser := cases.Title(language.English)
		colType := col.Scalar().Type()

		f := reflect.StructField{
			// We need to title case the column name to ensure the fields are public
			// in the struct we create.
			Name: caser.String(col.Name),
			Type: colType,
			// At a minimum, we need to set the parquet tag to the column name so that when
			// we read from the file, the field names match up with the column names as they
			// are defined; additionally, we set the optional tag to ensure that the field
			// is nullable in the parquet file.
			Tag: reflect.StructTag(fmt.Sprintf(`parquet:"%s,optional"`, col.Name)),
		}

		// TODO: use a better way of determining if a column is a timestamp
		if colType.Name() == "Time" {
			f.Tag = reflect.StructTag(fmt.Sprintf(`parquet:"%s,optional,timestamp"`, col.Name))
		}

		fields[i] = f
	}
	structType := reflect.StructOf(fields)
	return reflect.New(structType)
}

func (schema *TableSchema) Serialize() ([]byte, error) {
	wrapper := &TableSchemaJSONWrapper{
		SourceTable: schema.SourceTable,
		Columns:     make([]TableColumnJSONWrapper, len(schema.Columns)),
	}
	for i, col := range schema.Columns {
		wrapper.Columns[i] = TableColumnJSONWrapper{
			Name:      col.Name,
			ValueType: ValueTypeJSONWrapper{col.ValueType},
		}
	}
	config, err := json.Marshal(wrapper)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize table schema due to: %w", err)
	}
	return config, nil
}

func (schema *TableSchema) Deserialize(config []byte) error {
	wrapper := &TableSchemaJSONWrapper{}
	err := json.Unmarshal(config, wrapper)
	if err != nil {
		return fmt.Errorf("failed to deserialize table schema due to: %w", err)
	}
	schema.Columns = make([]TableColumn, len(wrapper.Columns))
	for i, col := range wrapper.Columns {
		schema.Columns[i] = TableColumn{
			Name:      col.Name,
			ValueType: col.ValueType.ValueType,
		}
	}
	schema.SourceTable = wrapper.SourceTable
	return nil
}

// *NOTE:* pointer types are used for all the scalar types to ensure they
// can be nullable in the parquet file.
func (schema *TableSchema) ToParquetRecords(records []GenericRecord) []any {
	parquetRecords := make([]any, len(records))
	caser := cases.Title(language.English)
	for i, record := range records {
		parquetRecord := schema.Value()
		for j, value := range record {
			// if a value is nil, we skip it so that the zero value for the pointer
			// type is used instead, which will preserve the null value when the parquet
			// file is read back.
			if value == nil {
				continue
			}
			// To ensure the struct fields are public and accessible to other methods,
			// we need to title case them when setting them.
			colName := caser.String(schema.Columns[j].Name)
			switch v := value.(type) {
			case int:
				parquetRecord.Elem().FieldByName(colName).Set(reflect.ValueOf(&v))
			case int32:
				parquetRecord.Elem().FieldByName(colName).Set(reflect.ValueOf(&v))
			case int64:
				parquetRecord.Elem().FieldByName(colName).Set(reflect.ValueOf(&v))
			case float32:
				parquetRecord.Elem().FieldByName(colName).Set(reflect.ValueOf(&v))
			case float64:
				parquetRecord.Elem().FieldByName(colName).Set(reflect.ValueOf(&v))
			case string:
				parquetRecord.Elem().FieldByName(colName).Set(reflect.ValueOf(&v))
			case bool:
				parquetRecord.Elem().FieldByName(colName).Set(reflect.ValueOf(&v))
			default:
				parquetRecord.Elem().FieldByName(colName).Set(reflect.ValueOf(value))
			}
		}
		parquetRecords[i] = parquetRecord.Interface()
	}
	return parquetRecords
}

type TableColumnJSONWrapper struct {
	Name      string
	ValueType ValueTypeJSONWrapper
}

type TableColumn struct {
	Name string
	ValueType
}

type memoryOfflineStore struct {
	tables           syncmap.Map
	materializations syncmap.Map
	trainingSets     syncmap.Map
	BaseProvider
}

func memoryOfflineStoreFactory(serializedConfig pc.SerializedConfig) (Provider, error) {
	return NewMemoryOfflineStore(), nil
}

func NewMemoryOfflineStore() *memoryOfflineStore {
	return &memoryOfflineStore{
		tables:           syncmap.Map{},
		materializations: syncmap.Map{},
		trainingSets:     syncmap.Map{},
		BaseProvider: BaseProvider{
			ProviderType:   pt.MemoryOffline,
			ProviderConfig: []byte{},
		},
	}
}

func (store *memoryOfflineStore) AsOfflineStore() (OfflineStore, error) {
	return store, nil
}

func (store *memoryOfflineStore) RegisterResourceFromSourceTable(id ResourceID, schema ResourceSchema) (OfflineTable, error) {
	return nil, fmt.Errorf("Snowflake RegisterResourceFromSourceTable not implemented")
}

func (store *memoryOfflineStore) RegisterPrimaryFromSourceTable(id ResourceID, sourceName string) (PrimaryTable, error) {
	return nil, fmt.Errorf("Snowflake RegisterPrimaryFromSourceTable not implemented")
}

func (store *memoryOfflineStore) CreatePrimaryTable(id ResourceID, schema TableSchema) (PrimaryTable, error) {
	return nil, errors.New("primary table unsupported for this provider")
}

func (store *memoryOfflineStore) GetPrimaryTable(id ResourceID) (PrimaryTable, error) {
	return nil, errors.New("primary table unsupported for this provider")
}

func (store *memoryOfflineStore) CreateTransformation(config TransformationConfig) error {
	return errors.New("CreateTransformation unsupported for this provider")
}

func (store *memoryOfflineStore) UpdateTransformation(config TransformationConfig) error {
	return errors.New("UpdateTransformation unsupported for this provider")
}

func (store *memoryOfflineStore) GetTransformationTable(id ResourceID) (TransformationTable, error) {
	return nil, errors.New("GetTransformationTable unsupported for this provider")
}

func (store *memoryOfflineStore) CreateResourceTable(id ResourceID, schema TableSchema) (OfflineTable, error) {
	if err := id.check(Feature, Label); err != nil {
		return nil, err
	}
	if _, has := store.tables.Load(id); has {
		return nil, &TableAlreadyExists{id.Name, id.Variant}
	}
	table := newMemoryOfflineTable()
	store.tables.Store(id, table)
	return table, nil
}

func (store *memoryOfflineStore) GetResourceTable(id ResourceID) (OfflineTable, error) {
	return store.getMemoryResourceTable(id)
}

func (store *memoryOfflineStore) getMemoryResourceTable(id ResourceID) (*memoryOfflineTable, error) {
	table, has := store.tables.Load(id)
	if !has {
		return nil, &TableNotFound{id.Name, id.Variant}
	}
	return table.(*memoryOfflineTable), nil
}

// Used to implement sort.Interface for sorting.
type materializedRecords []ResourceRecord

func (recs materializedRecords) Len() int {
	return len(recs)
}

func (recs materializedRecords) Less(i, j int) bool {
	return recs[i].Entity < recs[j].Entity
}

func (recs materializedRecords) Swap(i, j int) {
	recs[i], recs[j] = recs[j], recs[i]
}

func (store *memoryOfflineStore) CreateMaterialization(id ResourceID) (Materialization, error) {
	if id.Type != Feature {
		return nil, errors.New("only features can be materialized")
	}
	table, err := store.getMemoryResourceTable(id)
	if err != nil {
		return nil, err
	}
	var matData materializedRecords
	table.entityMap.Range(func(key, value interface{}) bool {
		records := value.([]ResourceRecord)
		matRec := latestRecord(records)
		matData = append(matData, matRec)
		return true
	})
	sort.Sort(matData)
	matId := MaterializationID(uuid.NewString())
	mat := &memoryMaterialization{
		id:   matId,
		data: matData,
	}
	store.materializations.Store(matId, mat)
	return mat, nil
}

type MaterializationNotFound struct {
	id MaterializationID
}

func (err *MaterializationNotFound) Error() string {
	return fmt.Sprintf("Materialization %s not found", err.id)
}

func (store *memoryOfflineStore) GetMaterialization(id MaterializationID) (Materialization, error) {
	mat, has := store.materializations.Load(id)
	if !has {
		return nil, &MaterializationNotFound{id}
	}
	return mat.(Materialization), nil
}

func (store *memoryOfflineStore) UpdateMaterialization(id ResourceID) (Materialization, error) {
	return store.CreateMaterialization(id)
}

func (store *memoryOfflineStore) DeleteMaterialization(id MaterializationID) error {
	if _, has := store.materializations.Load(id); !has {
		return &MaterializationNotFound{id}
	}
	store.materializations.Delete(id)
	return nil
}

func latestRecord(recs []ResourceRecord) ResourceRecord {
	latest := recs[0]
	for _, rec := range recs {
		if latest.TS.Before(rec.TS) {
			latest = rec
		}
	}
	return latest
}

func (store *memoryOfflineStore) CreateTrainingSet(def TrainingSetDef) error {
	if err := def.check(); err != nil {
		return err
	}
	label, err := store.getMemoryResourceTable(def.Label)
	if err != nil {
		return err
	}
	features := make([]*memoryOfflineTable, len(def.Features))
	for i, id := range def.Features {
		feature, err := store.getMemoryResourceTable(id)
		if err != nil {
			return err
		}
		features[i] = feature
	}
	labelRecs := label.records()
	trainingData := make(trainingRows, len(labelRecs))
	for i, rec := range labelRecs {
		featureVals := make([]interface{}, len(features))
		for i, feature := range features {
			featureVals[i] = feature.getLastValueBefore(rec.Entity, rec.TS)
		}
		labelVal := rec.Value
		trainingData[i] = trainingRow{
			Features: featureVals,
			Label:    labelVal,
		}
	}
	store.trainingSets.Store(def.ID, trainingData)
	return nil
}

func (store *memoryOfflineStore) UpdateTrainingSet(def TrainingSetDef) error {
	return store.CreateTrainingSet(def)
}

func (store *memoryOfflineStore) GetTrainingSet(id ResourceID) (TrainingSetIterator, error) {
	if err := id.check(TrainingSet); err != nil {
		return nil, err
	}
	data, has := store.trainingSets.Load(id)
	if !has {
		return nil, &TrainingSetNotFound{id}
	}
	return data.(trainingRows).Iterator(), nil
}
func (store *memoryOfflineStore) Close() error {
	return nil
}

type TrainingSetNotFound struct {
	ID ResourceID
}

func (err *TrainingSetNotFound) Error() string {
	return fmt.Sprintf("TrainingSet with ID %v not found", err.ID)
}

type trainingRows []trainingRow

func (rows trainingRows) Iterator() TrainingSetIterator {
	return newMemoryTrainingSetIterator(rows)
}

type trainingRow struct {
	Features []interface{}
	Label    interface{}
}

type memoryTrainingRowsIterator struct {
	data trainingRows
	idx  int
}

func newMemoryTrainingSetIterator(data trainingRows) TrainingSetIterator {
	return &memoryTrainingRowsIterator{
		data: data,
		idx:  -1,
	}
}

func (it *memoryTrainingRowsIterator) Next() bool {
	lastIdx := len(it.data) - 1
	if it.idx == lastIdx {
		return false
	}
	it.idx++
	return true
}

func (it *memoryTrainingRowsIterator) Err() error {
	return nil
}

func (it *memoryTrainingRowsIterator) Close() error {
	return nil
}

func (it *memoryTrainingRowsIterator) Features() []interface{} {
	return it.data[it.idx].Features
}

func (it *memoryTrainingRowsIterator) Label() interface{} {
	return it.data[it.idx].Label
}

type memoryOfflineTable struct {
	entityMap syncmap.Map
}

func newMemoryOfflineTable() *memoryOfflineTable {
	return &memoryOfflineTable{
		entityMap: syncmap.Map{},
	}
}

func (table *memoryOfflineTable) records() []ResourceRecord {
	allRecs := make([]ResourceRecord, 0)
	table.entityMap.Range(func(key, value interface{}) bool {
		allRecs = append(allRecs, value.([]ResourceRecord)...)
		return true
	})
	return allRecs
}

func (table *memoryOfflineTable) getLastValueBefore(entity string, ts time.Time) interface{} {
	recs, has := table.entityMap.Load(entity)
	if !has {
		return nil
	}
	sortedRecs := ResourceRecords(recs.([]ResourceRecord))
	sort.Sort(sortedRecs)
	lastIdx := len(sortedRecs) - 1
	for i, rec := range sortedRecs {
		if rec.TS.After(ts) {
			// Entity was not yet set at timestamp, don't return a record.
			if i == 0 {
				return nil
			}
			// Use the record before this, since it would have been before TS.
			return sortedRecs[i-1].Value
		} else if i == lastIdx {
			// Every record happened before the TS, use the last record.
			return rec.Value
		}
	}
	// This line should never be able to be reached.
	panic("Unable to getLastValue before timestamp")
}

func (table *memoryOfflineTable) Write(rec ResourceRecord) error {
	rec = checkTimestamp(rec)
	if err := rec.check(); err != nil {
		return err
	}

	if records, has := table.entityMap.Load(rec.Entity); has {
		// Replace any record with the same timestamp/entity pair.
		recs := records.([]ResourceRecord)
		for i, existingRec := range recs {
			if existingRec.TS == rec.TS {
				recs[i] = rec
				return nil
			}
		}
		table.entityMap.Store(rec.Entity, append(recs, rec))
	} else {
		table.entityMap.Store(rec.Entity, []ResourceRecord{rec})
	}
	return nil
}

func (table *memoryOfflineTable) WriteBatch(recs []ResourceRecord) error {
	for _, rec := range recs {
		if err := table.Write(rec); err != nil {
			return err
		}
	}
	return nil
}

type memoryMaterialization struct {
	id   MaterializationID
	data []ResourceRecord
}

func (mat *memoryMaterialization) ID() MaterializationID {
	return mat.id
}

func (mat *memoryMaterialization) NumRows() (int64, error) {
	return int64(len(mat.data)), nil
}

func (mat *memoryMaterialization) IterateSegment(start, end int64) (FeatureIterator, error) {
	segment := mat.data[start:end]
	return newMemoryFeatureIterator(segment), nil
}

type memoryFeatureIterator struct {
	data []ResourceRecord
	idx  int64
}

func newMemoryFeatureIterator(recs []ResourceRecord) FeatureIterator {
	return &memoryFeatureIterator{
		data: recs,
		idx:  -1,
	}
}

func (iter *memoryFeatureIterator) Next() bool {
	if isLastIdx := iter.idx == int64(len(iter.data)-1); isLastIdx {
		return false
	}
	iter.idx++
	return true
}

func (iter *memoryFeatureIterator) Value() ResourceRecord {
	return iter.data[iter.idx]
}

func (iter *memoryFeatureIterator) Err() error {
	return nil
}

func (iter *memoryFeatureIterator) Close() error {
	return nil
}

// checkTimestamp checks the timestamp of a record.
// If the record has the default initialization value of 0001-01-01 00:00:00 +0000 UTC, it is changed
// to the start of unix epoch time, since snowflake cannot handle values before 1582
func checkTimestamp(rec ResourceRecord) ResourceRecord {
	checkRecord := ResourceRecord{}
	if rec.TS == checkRecord.TS {
		rec.TS = time.UnixMilli(0).UTC()
	}
	return rec
}

type sanitization func(string) string

func replaceSourceName(query string, mapping []SourceMapping, sanitize sanitization) (string, error) {
	replacements := make([]string, len(mapping)*2) // It's times 2 because each replacement will be a pair; (original, replacedValue)

	for _, m := range mapping {
		replacements = append(replacements, m.Template)
		replacements = append(replacements, sanitize(m.Source))
	}

	replacer := strings.NewReplacer(replacements...)
	replacedQuery := replacer.Replace(query)

	if strings.Contains(replacedQuery, "{{") {
		return "", fmt.Errorf("could not replace all the templates with the current mapping. Mapping: %v; Replaced Query: %s", mapping, replacedQuery)
	}

	return replacedQuery, nil
}
