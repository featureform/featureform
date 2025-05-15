package dataset

import (
	"context"

	"github.com/featureform/fferr"
	types "github.com/featureform/fftypes"
)

type Training struct {
	Dataset
	tsSchema types.TrainingSetSchema
}

func NewTrainingSet(dataset Dataset, schema types.TrainingSetSchema) *Training {
	return &Training{
		Dataset:  dataset,
		tsSchema: schema,
	}
}

func (ts *Training) TrainingSetSchema() types.TrainingSetSchema {
	return ts.tsSchema
}

func (ts *Training) TrainingSetIterator(ctx context.Context, limit int64) (TrainingSetIterator, error) {
	baseIterator, err := ts.Dataset.Iterator(ctx, limit)
	if err != nil {
		return nil, err
	}
	return NewTrainingSetIterator(baseIterator, ts.tsSchema)
}

// TODO Remove this and just use the Impl Struct, going to add this for now to help adapt
type TrainingSetIterator interface {
	Iterator
	TrainingSetSchema() types.TrainingSetSchema
	Features() types.FeatureRow
	Label() types.Value
}

type TrainingSetIteratorImpl struct {
	Iterator
	tsSchema       types.TrainingSetSchema
	featureIndices []int
	labelIndex     int

	currentFeatures types.FeatureRow
	currentLabel    types.Value

	error error
}

func NewTrainingSetIterator(it Iterator, schema types.TrainingSetSchema) (TrainingSetIterator, error) {
	featureIndices := make([]int, len(schema.FeatureColumns))
	for i := range featureIndices {
		featureIndices[i] = -1
	}
	labelIndex := -1

	// Create a map of column names to indices in the base schema
	baseSchema := it.Schema()
	columnNameToIdx := make(map[types.ColumnName]int, len(baseSchema.Fields))
	for i, field := range baseSchema.Fields {
		columnNameToIdx[field.Name] = i
	}

	// Optional validation of schema lengths
	if len(schema.FeatureColumns)+1 != len(baseSchema.Fields) {
		return nil, fferr.NewInternalErrorf("schema length mismatch: %d features + 1 label != %d base schema fields", len(schema.FeatureColumns), len(baseSchema.Fields))
	}

	// Map feature columns to their indices in the base schema
	for i, featureCol := range schema.FeatureColumns {
		if idx, ok := columnNameToIdx[featureCol.FeatureColumn.Name]; ok {
			featureIndices[i] = idx
		} else {
			return nil, fferr.NewInternalErrorf("feature column %s not found in base schema", featureCol.FeatureColumn.Name)
		}
	}

	// Map label column to its index in the base schema
	if idx, ok := columnNameToIdx[schema.LabelColumn.Name]; ok {
		labelIndex = idx
	} else {
		return nil, fferr.NewInternalErrorf("label column %s not found in base schema", schema.LabelColumn.Name)
	}

	return &TrainingSetIteratorImpl{
		Iterator:       it,
		tsSchema:       schema,
		featureIndices: featureIndices,
		labelIndex:     labelIndex,
	}, nil
}

func (it *TrainingSetIteratorImpl) TrainingSetSchema() types.TrainingSetSchema {
	return it.tsSchema
}

func (it *TrainingSetIteratorImpl) Next() bool {
	if !it.Iterator.Next() {
		return false
	}

	row := it.Values()

	features := make([]types.Value, len(it.featureIndices))
	for i, idx := range it.featureIndices {
		if idx >= 0 && idx < len(row) {
			features[i] = row[idx]
		}
	}

	it.currentFeatures = types.FeatureRow{
		Schema: it.tsSchema.GetFeatureSchema(),
		Row:    features,
	}

	if it.labelIndex >= 0 && it.labelIndex < len(row) {
		it.currentLabel = row[it.labelIndex]
	} else {
		it.error = fferr.NewInternalErrorf("label index %d out of range for row of length %d", it.labelIndex, len(row))
	}

	return true
}

func (it *TrainingSetIteratorImpl) Err() error {
	if it.error != nil {
		return it.error
	}
	return it.Iterator.Err()
}

func (it *TrainingSetIteratorImpl) Features() types.FeatureRow {
	return it.currentFeatures
}

func (it *TrainingSetIteratorImpl) Label() types.Value {
	row := it.Values()

	if it.labelIndex >= 0 && it.labelIndex < len(row) {
		return row[it.labelIndex]
	}

	return it.currentLabel
}
