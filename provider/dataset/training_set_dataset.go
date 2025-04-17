package dataset

import (
	"context"

	types "github.com/featureform/fftypes"
)

type TrainingSet struct {
	Dataset
	tsSchema types.TrainingSetSchema
}

func NewTrainingSet(dataset Dataset, schema types.TrainingSetSchema) *TrainingSet {
	return &TrainingSet{
		Dataset:  dataset,
		tsSchema: schema,
	}
}

func (ts *TrainingSet) TrainingSetSchema() types.TrainingSetSchema {
	return ts.tsSchema
}

func (ts *TrainingSet) TrainingSetIterator(ctx context.Context, limit int64) (*TrainingSetIterator, error) {
	baseIterator, err := ts.Dataset.Iterator(ctx, limit)
	if err != nil {
		return nil, err
	}
	return NewTrainingSetIterator(baseIterator, ts.tsSchema), nil
}

type TrainingSetIterator struct {
	Iterator
	tsSchema        types.TrainingSetSchema
	currentFeatures []interface{}
	currentLabel    interface{}
}

func NewTrainingSetIterator(it Iterator, schema types.TrainingSetSchema) *TrainingSetIterator {
	return &TrainingSetIterator{
		Iterator: it,
		tsSchema: schema,
	}
}

func (it *TrainingSetIterator) TrainingSetSchema() types.TrainingSetSchema {
	return it.tsSchema
}

func (it *TrainingSetIterator) TrainingSetValues() types.TrainingSetRow {
	return types.TrainingSetRow{
		Schema: it.tsSchema,
		Row:    it.Values(),
	}
}

func (it *TrainingSetIterator) Next() bool {
	if !it.Iterator.Next() {
		return false
	}

	row := it.Values()
	schema := it.Schema()

	// Extract features
	features := make([]interface{}, len(it.tsSchema.FeatureColumns))
	for i, featureCol := range it.tsSchema.FeatureColumns {
		colIndex := findColumnIndex(schema.Fields, featureCol.FeatureCol.Name)
		if colIndex >= 0 && colIndex < len(row) {
			features[i] = row[colIndex].Value
		}
	}

	// Extract label
	var label any
	labelIndex := findColumnIndex(schema.Fields, it.tsSchema.LabelColumn.Name)
	if labelIndex >= 0 && labelIndex < len(row) {
		label = row[labelIndex].Value
	}

	it.currentFeatures = features
	it.currentLabel = label

	return true
}

func (it *TrainingSetIterator) Features() []interface{} {
	return it.currentFeatures
}

func (it *TrainingSetIterator) Label() interface{} {
	return it.currentLabel
}

// Helper function to find column index by name
func findColumnIndex(columns []types.ColumnSchema, name types.ColumnName) int {
	for i, col := range columns {
		if col.Name == name {
			return i
		}
	}
	return -1
}
