package provider

import (
	"context"
	"fmt"
	"time"

	types "github.com/featureform/fftypes"
	"github.com/featureform/provider/dataset"
	pl "github.com/featureform/provider/location"
)

const (
	entityColIdx = 0
	valueColIdx  = 1
	tsColIdx     = 2
)

type LegacyMaterializationAdapter struct {
	legacy        Materialization
	featureSchema types.FeaturesSchema
	dataset.SizedSegmentableChunkedDataset
}

func NewLegacyMaterializationAdapterWithEmptySchema(legacy Materialization) dataset.Materialization {
	adapter := &LegacyMaterializationAdapter{
		legacy:        legacy,
		featureSchema: types.FeaturesSchema{},
	}

	return dataset.NewMaterialization(adapter, dataset.MaterializationID(legacy.ID()), types.FeaturesSchema{})
}

func NewLegacyMaterializationAdapter(legacy Materialization, schema ResourceSchema) dataset.Materialization {
	featureSchema := createFeatureSchemaFromResourceSchema(schema)

	adapter := &LegacyMaterializationAdapter{
		legacy:        legacy,
		featureSchema: featureSchema,
	}

	return dataset.NewMaterialization(adapter, dataset.MaterializationID(legacy.ID()), featureSchema)
}

func (adapter *LegacyMaterializationAdapter) Location() pl.Location {
	return adapter.legacy.Location()
}

func (adapter *LegacyMaterializationAdapter) Schema() types.Schema {
	return types.Schema{
		Fields: extractFieldsFromFeatureSchema(adapter.featureSchema),
	}
}

func (adapter *LegacyMaterializationAdapter) Iterator(ctx context.Context, limit int64) (dataset.Iterator, error) {
	numRows, err := adapter.legacy.NumRows()
	if err != nil {
		return nil, err
	}

	if limit > 0 && limit < numRows {
		numRows = limit
	}

	legacyIter, err := adapter.legacy.IterateSegment(0, numRows)
	if err != nil {
		return nil, err
	}

	return NewLegacyIteratorAdapter(legacyIter, adapter.Schema()), nil
}

func (adapter *LegacyMaterializationAdapter) IterateSegment(ctx context.Context, begin, end int64) (dataset.Iterator, error) {
	legacyIter, err := adapter.legacy.IterateSegment(begin, end)
	if err != nil {
		return nil, err
	}

	return NewLegacyIteratorAdapter(legacyIter, adapter.Schema()), nil
}

func (adapter *LegacyMaterializationAdapter) NumChunks() (int, error) {
	return adapter.legacy.NumChunks()
}

func (adapter *LegacyMaterializationAdapter) ChunkIterator(ctx context.Context, idx int) (dataset.SizedIterator, error) {
	legacyIter, err := adapter.legacy.IterateChunk(idx)
	if err != nil {
		return nil, err
	}

	iteratorAdapter := NewLegacyIteratorAdapter(legacyIter, adapter.Schema())

	chunks, err := adapter.legacy.NumChunks()
	if err != nil {
		return nil, err
	}
	return &dataset.GenericSizedIterator{
		Iterator: iteratorAdapter,
		Length:   int64(chunks),
	}, nil
}

func (adapter *LegacyMaterializationAdapter) Len() (int64, error) {
	return adapter.legacy.NumRows()
}

type LegacyIteratorAdapter struct {
	legacy     FeatureIterator
	schema     types.Schema
	currentRow types.Row
}

func NewLegacyIteratorAdapter(legacy FeatureIterator, schema types.Schema) dataset.Iterator {
	return &LegacyIteratorAdapter{
		legacy:     legacy,
		schema:     schema,
		currentRow: nil,
	}
}

func (adapter *LegacyIteratorAdapter) Next() bool {
	if !adapter.legacy.Next() {
		return false
	}

	record := adapter.legacy.Value()
	adapter.currentRow = convertResourceRecordToRow(record)
	return true
}

func (adapter *LegacyIteratorAdapter) Values() types.Row {
	return adapter.currentRow
}

func (adapter *LegacyIteratorAdapter) Schema() types.Schema {
	return adapter.schema
}

func (adapter *LegacyIteratorAdapter) Err() error {
	return adapter.legacy.Err()
}

func (adapter *LegacyIteratorAdapter) Close() error {
	return adapter.legacy.Close()
}

func createFeatureSchemaFromResourceSchema(schema ResourceSchema) types.FeaturesSchema {
	entityCol := types.ColumnSchema{
		Name:       types.ColumnName(schema.Entity),
		NativeType: types.NativeTypeLiteral("string"),
		Type:       types.String,
	}

	featureCols := make([]types.FeatureColumn, 1)
	featureCols[0] = types.FeatureColumn{
		FeatureColumn: types.ColumnSchema{
			Name:       types.ColumnName(schema.Value),
			NativeType: types.NativeTypeLiteral("string"),
			Type:       types.String,
		},
	}

	if schema.TS != "" {
		featureCols[0].TimestampColumn = types.ColumnSchema{
			Name:       types.ColumnName(schema.TS),
			NativeType: types.NativeTypeLiteral("timestamp"),
			Type:       types.Timestamp,
		}
	}

	// Handle entity mappings if present
	if len(schema.EntityMappings.Mappings) > 0 {
		// Implementation-specific mapping logic would go here
	}

	return types.FeaturesSchema{
		EntityColumn:   entityCol,
		FeatureColumns: featureCols,
	}
}

func extractFieldsFromFeatureSchema(featureSchema types.FeaturesSchema) []types.ColumnSchema {
	fields := make([]types.ColumnSchema, 0)
	fields = append(fields, featureSchema.EntityColumn)

	for _, featureCol := range featureSchema.FeatureColumns {
		fields = append(fields, featureCol.FeatureColumn)
		if featureCol.TimestampColumn.Name != "" {
			fields = append(fields, featureCol.TimestampColumn)
		}
	}

	return fields
}

func convertResourceRecordToRow(record ResourceRecord) types.Row {
	values := make(types.Row, 3)

	values[entityColIdx] = types.Value{Value: record.Entity}
	values[valueColIdx] = types.Value{Value: record.Value}

	if !record.TS.IsZero() {
		values[tsColIdx] = types.Value{Value: record.TS}
	} else {
		values[tsColIdx] = types.Value{Value: nil}
	}

	return values
}

func RowToResourceRecord(row types.Row) (ResourceRecord, error) {
	rec := ResourceRecord{}

	if len(row) < 2 {
		return rec, fmt.Errorf("row has insufficient columns: expected at least 2, got %d", len(row))
	}

	if row[entityColIdx].Value == nil {
		return rec, fmt.Errorf("entity column has nil value")
	}

	entityVal, ok := row[entityColIdx].Value.(string)
	if !ok {
		return rec, fmt.Errorf("entity column is not a string: %T", row[entityColIdx].Value)
	}
	rec.Entity = entityVal
	rec.Value = row[valueColIdx].Value

	if len(row) > 2 && row[tsColIdx].Value != nil {
		tsVal, ok := row[tsColIdx].Value.(time.Time)
		if ok {
			rec.TS = tsVal
		} else {
			return rec, fmt.Errorf("timestamp column is not a time.Time: %T", row[tsColIdx].Value)
		}
	}

	return rec, nil
}
