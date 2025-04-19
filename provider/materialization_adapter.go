package provider

import (
	"context"
	"fmt"
	"time"

	types "github.com/featureform/fftypes"
	"github.com/featureform/provider/dataset"
	pl "github.com/featureform/provider/location"
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

	//Get total rows and chunks
	totalRows, err := adapter.legacy.NumRows()
	if err != nil {
		return nil, err
	}
	//
	numChunks, err := adapter.legacy.NumChunks()
	if err != nil {
		return nil, err
	}
	//Calculate chunk size
	chunkSize := totalRows / int64(numChunks)
	remainder := totalRows % int64(numChunks)
	//Calculate the size of this specific chunk
	var thisChunkSize int64
	if idx < int(remainder) {
		//First 'remainder' chunks get an extra row
		thisChunkSize = chunkSize + 1
	} else {
		thisChunkSize = chunkSize
	}

	return &dataset.GenericSizedIterator{
		Iterator: iteratorAdapter,
		Length:   thisChunkSize,
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

func extractFieldsFromFeatureSchema(featureSchema types.FeaturesSchema) []types.ColumnSchema {
	fields := make([]types.ColumnSchema, 0)
	fields = append(fields, featureSchema.EntityCol)

	for _, featureCol := range featureSchema.FeatureCols {
		fields = append(fields, featureCol.FeatureCol)
		if featureCol.TimestampCol.Name != "" {
			fields = append(fields, featureCol.TimestampCol)
		}
	}

	return fields
}

func convertResourceRecordToRow(record ResourceRecord) types.Row {
	values := make(types.Row, 3)

	values[0] = types.Value{Value: record.Entity}
	values[1] = types.Value{Value: record.Value}

	if !record.TS.IsZero() {
		values[2] = types.Value{Value: record.TS}
	} else {
		values[2] = types.Value{Value: nil}
	}

	return values
}

func RowToResourceRecord(row types.Row) (ResourceRecord, error) {
	rec := ResourceRecord{}

	if len(row) < 2 {
		return rec, fmt.Errorf("row has insufficient columns: expected at least 2, got %d", len(row))
	}

	if row[0].Value == nil {
		return rec, fmt.Errorf("entity column has nil value")
	}

	entityVal, ok := row[0].Value.(string)
	if !ok {
		return rec, fmt.Errorf("entity column is not a string: %T", row[0].Value)
	}
	rec.Entity = entityVal
	rec.Value = row[1].Value

	if len(row) > 2 && row[2].Value != nil {
		tsVal, ok := row[2].Value.(time.Time)
		if ok {
			rec.TS = tsVal
		} else {
			return rec, fmt.Errorf("timestamp column is not a time.Time: %T", row[2].Value)
		}
	}

	return rec, nil
}
