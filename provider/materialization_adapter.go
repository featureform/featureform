package provider

import (
	"context"

	types "github.com/featureform/fftypes"
	"github.com/featureform/provider/dataset"
	pl "github.com/featureform/provider/location"
)

// LegacyMaterializationAdapter adapts the old Materialization interface to the new MaterializationDataset interface
type LegacyMaterializationAdapter struct {
	legacy        Materialization // This is the old sqlMaterialization
	featureSchema types.FeaturesSchema
}

// NewLegacyMaterializationAdapter creates a new adapter for a legacy Materialization
func NewLegacyMaterializationAdapter(legacy Materialization, schema ResourceSchema) dataset.MaterializationDataset {
	// Convert the ResourceSchema to a FeaturesSchema
	featureSchema := createFeatureSchemaFromResourceSchema(schema)

	return &LegacyMaterializationAdapter{
		legacy:        legacy,
		featureSchema: featureSchema,
	}
}

// ID returns the materialization ID
func (adapter *LegacyMaterializationAdapter) ID() dataset.MaterializationID {
	return dataset.MaterializationID(adapter.legacy.ID())
}

// Location returns the materialization location
func (adapter *LegacyMaterializationAdapter) Location() pl.Location {
	return adapter.legacy.Location()
}

// Schema returns the dataset schema
func (adapter *LegacyMaterializationAdapter) Schema() types.Schema {
	// Construct schema from feature schema
	return types.Schema{
		Fields: extractFieldsFromFeatureSchema(adapter.featureSchema),
	}
}

// Iterator returns a standard iterator for the entire dataset
func (adapter *LegacyMaterializationAdapter) Iterator(ctx context.Context, limit int64) (dataset.Iterator, error) {
	// Determine the total size
	numRows, err := adapter.legacy.NumRows()
	if err != nil {
		return nil, err
	}

	// Apply limit if specified
	if limit > 0 && limit < numRows {
		numRows = limit
	}

	// Use legacy IterateSegment to get all rows
	legacyIter, err := adapter.legacy.IterateSegment(0, numRows)
	if err != nil {
		return nil, err
	}

	return NewLegacyIteratorAdapter(legacyIter, adapter.Schema()), nil
}

// IterateSegment returns a standard iterator for a segment
func (adapter *LegacyMaterializationAdapter) IterateSegment(ctx context.Context, begin, end int64) (dataset.Iterator, error) {
	legacyIter, err := adapter.legacy.IterateSegment(begin, end)
	if err != nil {
		return nil, err
	}

	return NewLegacyIteratorAdapter(legacyIter, adapter.Schema()), nil
}

// NumChunks returns the number of chunks
func (adapter *LegacyMaterializationAdapter) NumChunks() (int, error) {
	return adapter.legacy.NumChunks()
}

// ChunkIterator returns a standard iterator for a chunk
func (adapter *LegacyMaterializationAdapter) ChunkIterator(ctx context.Context, idx int) (dataset.SizedIterator, error) {
	legacyIter, err := adapter.legacy.IterateChunk(idx)
	if err != nil {
		return nil, err
	}

	length, err := adapter.Len()
	if err != nil {
		return nil, err

	}
	return &dataset.GenericSizedIterator{
		Iterator: NewLegacyIteratorAdapter(legacyIter, adapter.Schema()),
		Length:   length,
	}, nil
}

// Len returns the number of rows
func (adapter *LegacyMaterializationAdapter) Len() (int64, error) {
	return adapter.legacy.NumRows()
}

// FeatureSchema returns the feature schema
func (adapter *LegacyMaterializationAdapter) FeatureSchema() types.FeaturesSchema {
	return adapter.featureSchema
}

// FeatureIterator returns a feature-specific iterator for the entire dataset
func (adapter *LegacyMaterializationAdapter) FeatureIterator(ctx context.Context, limit int64) (dataset.FeatureIterator, error) {
	iter, err := adapter.Iterator(ctx, limit)
	if err != nil {
		return nil, err
	}

	return dataset.NewFeatureIterator(iter, adapter.featureSchema), nil
}

// FeatureIterateSegment returns a feature-specific iterator for a segment
func (adapter *LegacyMaterializationAdapter) FeatureIterateSegment(ctx context.Context, begin, end int64) (dataset.FeatureIterator, error) {
	iter, err := adapter.IterateSegment(ctx, begin, end)
	if err != nil {
		return nil, err
	}

	return dataset.NewFeatureIterator(iter, adapter.featureSchema), nil
}

// FeatureChunkIterator returns a feature-specific iterator for a chunk
func (adapter *LegacyMaterializationAdapter) FeatureChunkIterator(ctx context.Context, idx int) (dataset.FeatureIterator, error) {
	iter, err := adapter.ChunkIterator(ctx, idx)
	if err != nil {
		return nil, err
	}

	return dataset.NewFeatureIterator(iter, adapter.featureSchema), nil
}

// LegacyIteratorAdapter adapts the old FeatureIterator to the new Iterator interface
type LegacyIteratorAdapter struct {
	legacy     FeatureIterator
	schema     types.Schema
	currentRow types.Row
}

// NewLegacyIteratorAdapter creates a new adapter for a legacy iterator
func NewLegacyIteratorAdapter(legacy FeatureIterator, schema types.Schema) dataset.Iterator {
	return &LegacyIteratorAdapter{
		legacy:     legacy,
		schema:     schema,
		currentRow: nil,
	}
}

// Next advances to the next row
func (adapter *LegacyIteratorAdapter) Next() bool {
	if !adapter.legacy.Next() {
		return false
	}

	// Convert the legacy ResourceRecord to a Row
	record := adapter.legacy.Value()
	adapter.currentRow = convertResourceRecordToRow(record, adapter.schema)
	return true
}

// Values returns the current row
func (adapter *LegacyIteratorAdapter) Values() types.Row {
	return adapter.currentRow
}

// Schema returns the iterator schema
func (adapter *LegacyIteratorAdapter) Schema() types.Schema {
	return adapter.schema
}

// Err returns any error encountered during iteration
func (adapter *LegacyIteratorAdapter) Err() error {
	return adapter.legacy.Err()
}

// Close closes the iterator
func (adapter *LegacyIteratorAdapter) Close() error {
	return adapter.legacy.Close()
}

// createFeatureSchemaFromResourceSchema converts a ResourceSchema to a FeaturesSchema
func createFeatureSchemaFromResourceSchema(schema ResourceSchema) types.FeaturesSchema {
	// Create entity column schema
	entityCol := types.ColumnSchema{
		Name:       types.ColumnName(schema.Entity),
		NativeType: types.NativeType("string"), // Adjust based on your actual types
		Type:       types.String,               // Adjust based on your actual types
	}

	// Create feature column schemas
	featureCols := make([]types.FeatureCol, 1) // Adjust if you have multiple features
	featureCols[0] = types.FeatureCol{
		FeatureCol: types.ColumnSchema{
			Name:       types.ColumnName(schema.Value),
			NativeType: types.NativeType("string"), // Adjust based on your actual types
			Type:       types.String,               // Adjust based on your actual types
		},
	}

	// Add timestamp column if available
	if schema.TS != "" {
		featureCols[0].TimestampCol = types.ColumnSchema{
			Name:       types.ColumnName(schema.TS),
			NativeType: types.NativeType("timestamp"),
			Type:       types.Timestamp,
		}
	}

	// For cases with EntityMappings, you'll need to expand this logic
	if len(schema.EntityMappings.Mappings) > 0 {
		// Handle entity mappings based on your requirements
		// This is a simplification - adjust based on your actual semantics
		// for entity mappings
	}

	return types.FeaturesSchema{
		EntityCol:   entityCol,
		FeatureCols: featureCols,
	}
}

// extractFieldsFromFeatureSchema extracts column schemas from a feature schema
func extractFieldsFromFeatureSchema(featureSchema types.FeaturesSchema) []types.ColumnSchema {
	fields := make([]types.ColumnSchema, 0)

	// Add entity column
	fields = append(fields, featureSchema.EntityCol)

	// Add feature columns
	for _, featureCol := range featureSchema.FeatureCols {
		fields = append(fields, featureCol.FeatureCol)
		// Add timestamp column if it exists
		if featureCol.TimestampCol.Name != "" {
			fields = append(fields, featureCol.TimestampCol)
		}
	}

	return fields
}

// convertResourceRecordToRow converts a ResourceRecord to a Row
func convertResourceRecordToRow(record ResourceRecord, schema types.Schema) types.Row {
	// Create a row with values for each field in the schema
	values := make(types.Row, len(schema.Fields))

	// Populate values based on the schema
	for i, field := range schema.Fields {
		fieldName := string(field.Name)

		// Match field name to determine which value to use
		switch {
		case fieldName == string(schema.Fields[0].Name): // Assuming first field is entity
			values[i] = types.Value{
				NativeType: field.NativeType,
				Type:       field.Type,
				Value:      record.Entity,
			}
		case i == 1: // Assuming second field is the feature value
			values[i] = types.Value{
				NativeType: field.NativeType,
				Type:       field.Type,
				Value:      record.Value,
			}
		case i == 2 && !record.TS.IsZero(): // Assuming third field is timestamp if present
			values[i] = types.Value{
				NativeType: field.NativeType,
				Type:       field.Type,
				Value:      record.TS,
			}
		default:
			// Handle any other fields or set to nil
			values[i] = types.Value{
				NativeType: field.NativeType,
				Type:       field.Type,
				Value:      nil,
			}
		}
	}

	return values
}
