// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2025 FeatureForm Inc.
//

package dataset

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	types "github.com/featureform/fftypes"
	"github.com/featureform/logging"
)

func TestTrainingSet_TrainingSetSchema(t *testing.T) {
	// Create base schema with feature and label columns
	baseSchema := types.Schema{
		Fields: []types.ColumnSchema{
			{Name: "feature1", Type: types.Float64, NativeType: types.NativeTypeLiteral("float")},
			{Name: "feature2", Type: types.Int, NativeType: types.NativeTypeLiteral("int")},
			{Name: "label", Type: types.String, NativeType: types.NativeTypeLiteral("string")},
		},
	}

	// Create empty dataset with the schema
	inMemoryDataset := NewInMemoryDataset(nil, baseSchema, nil)

	// Create training set schema
	tsSchema := types.TrainingSetSchema{
		FeatureColumns: []types.FeatureColumn{
			{FeatureColumn: baseSchema.Fields[0]}, // feature1
			{FeatureColumn: baseSchema.Fields[1]}, // feature2
		},
		LabelColumn: baseSchema.Fields[2], // label
	}

	// Create the training set
	trainingSet := NewTrainingSet(inMemoryDataset, tsSchema)

	// Verify that the training set schema matches what we provided
	assert.Equal(t, tsSchema, trainingSet.TrainingSetSchema())
}

func TestTrainingSetIterator_Basic(t *testing.T) {
	// Create base schema with feature and label columns
	baseSchema := types.Schema{
		Fields: []types.ColumnSchema{
			{Name: "feature1", Type: types.Float64, NativeType: types.NativeTypeLiteral("float")},
			{Name: "feature2", Type: types.Int, NativeType: types.NativeTypeLiteral("int")},
			{Name: "label", Type: types.String, NativeType: types.NativeTypeLiteral("string")},
		},
	}

	// Create test data
	testData := []types.Row{
		{
			{Type: types.Float64, NativeType: types.NativeTypeLiteral("float"), Value: 1.0},
			{Type: types.Int, NativeType: types.NativeTypeLiteral("int"), Value: 10},
			{Type: types.String, NativeType: types.NativeTypeLiteral("string"), Value: "class_a"},
		},
		{
			{Type: types.Float64, NativeType: types.NativeTypeLiteral("float"), Value: 2.0},
			{Type: types.Int, NativeType: types.NativeTypeLiteral("int"), Value: 20},
			{Type: types.String, NativeType: types.NativeTypeLiteral("string"), Value: "class_b"},
		},
	}

	// Create in-memory dataset with the schema and data
	inMemoryDataset := NewInMemoryDataset(testData, baseSchema, nil)

	// Create training set schema
	tsSchema := types.TrainingSetSchema{
		FeatureColumns: []types.FeatureColumn{
			{FeatureColumn: baseSchema.Fields[0]}, // feature1
			{FeatureColumn: baseSchema.Fields[1]}, // feature2
		},
		LabelColumn: baseSchema.Fields[2], // label
	}

	// Create the training set
	trainingSet := NewTrainingSet(inMemoryDataset, tsSchema)

	// Get the training set iterator
	ctx := logging.NewTestContext(t)
	tsIterator, err := trainingSet.TrainingSetIterator(ctx, -1) // No limit
	require.NoError(t, err)

	// Iterate and verify data
	rowCount := 0
	for tsIterator.Next() {
		// Check that we get the expected feature values
		features := tsIterator.Features()
		require.Len(t, features.Row, 2, "Should have 2 feature values")

		// Verify feature values match the original data
		assert.Equal(t, testData[rowCount][0], features.Row[0], "First feature should match")
		assert.Equal(t, testData[rowCount][1], features.Row[1], "Second feature should match")

		// Verify label value matches the original data
		labelValue := tsIterator.Label()
		assert.Equal(t, testData[rowCount][2], labelValue, "Label should match")

		rowCount++
	}

	// Verify no errors and we read all rows
	require.NoError(t, tsIterator.Err())
	assert.Equal(t, len(testData), rowCount, "Should have read all test rows")
}

func TestTrainingSetIterator_WithLimit(t *testing.T) {
	// Create base schema with feature and label columns
	baseSchema := types.Schema{
		Fields: []types.ColumnSchema{
			{Name: "feature1", Type: types.Float64, NativeType: types.NativeTypeLiteral("float")},
			{Name: "feature2", Type: types.Int, NativeType: types.NativeTypeLiteral("int")},
			{Name: "label", Type: types.String, NativeType: types.NativeTypeLiteral("string")},
		},
	}

	// Create test data with multiple rows
	testData := []types.Row{
		{
			{Type: types.Float64, NativeType: types.NativeTypeLiteral("float"), Value: 1.0},
			{Type: types.Int, NativeType: types.NativeTypeLiteral("int"), Value: 10},
			{Type: types.String, NativeType: types.NativeTypeLiteral("string"), Value: "class_a"},
		},
		{
			{Type: types.Float64, NativeType: types.NativeTypeLiteral("float"), Value: 2.0},
			{Type: types.Int, NativeType: types.NativeTypeLiteral("int"), Value: 20},
			{Type: types.String, NativeType: types.NativeTypeLiteral("string"), Value: "class_b"},
		},
		{
			{Type: types.Float64, NativeType: types.NativeTypeLiteral("float"), Value: 3.0},
			{Type: types.Int, NativeType: types.NativeTypeLiteral("int"), Value: 30},
			{Type: types.String, NativeType: types.NativeTypeLiteral("string"), Value: "class_c"},
		},
	}

	// Create in-memory dataset with the schema and data
	inMemoryDataset := NewInMemoryDataset(testData, baseSchema, nil)

	// Create training set schema
	tsSchema := types.TrainingSetSchema{
		FeatureColumns: []types.FeatureColumn{
			{FeatureColumn: baseSchema.Fields[0]}, // feature1
			{FeatureColumn: baseSchema.Fields[1]}, // feature2
		},
		LabelColumn: baseSchema.Fields[2], // label
	}

	// Create the training set
	trainingSet := NewTrainingSet(inMemoryDataset, tsSchema)

	// Get the training set iterator with a limit of 2 rows
	ctx := logging.NewTestContext(t)
	limit := int64(2)
	tsIterator, err := trainingSet.TrainingSetIterator(ctx, limit)
	require.NoError(t, err)

	// Iterate and verify we only get two rows
	rowCount := 0
	for tsIterator.Next() {
		// Just count rows and verify we don't exceed limit
		rowCount++
	}

	// Verify no errors and we read the limited number of rows
	require.NoError(t, tsIterator.Err())
	assert.Equal(t, int(limit), rowCount, "Should have read only up to the limit")
}

func TestTrainingSetIterator_ErrorHandling(t *testing.T) {
	// Create base schema
	baseSchema := types.Schema{
		Fields: []types.ColumnSchema{
			{Name: "feature1", Type: types.Float64, NativeType: types.NativeTypeLiteral("float")},
			{Name: "feature2", Type: types.Int, NativeType: types.NativeTypeLiteral("int")},
		},
	}

	// Create test data
	testData := []types.Row{
		{
			{Type: types.Float64, NativeType: types.NativeTypeLiteral("float"), Value: 1.0},
			{Type: types.Int, NativeType: types.NativeTypeLiteral("int"), Value: 10},
		},
	}

	// Create in-memory dataset with the schema and data
	inMemoryDataset := NewInMemoryDataset(testData, baseSchema, nil)

	// Create training set schema with a column that doesn't exist in the base schema
	tsSchema := types.TrainingSetSchema{
		FeatureColumns: []types.FeatureColumn{
			{FeatureColumn: baseSchema.Fields[0]}, // feature1
		},
		LabelColumn: types.ColumnSchema{Name: "nonexistent", Type: types.String, NativeType: types.NativeTypeLiteral("string")},
	}

	// Create the training set
	trainingSet := NewTrainingSet(inMemoryDataset, tsSchema)

	// Get the training set iterator - should error
	ctx := logging.NewTestContext(t)
	_, err := trainingSet.TrainingSetIterator(ctx, -1)
	assert.Error(t, err, "Should error with nonexistent column")
	assert.Contains(t, err.Error(), "not found in base schema", "Error should mention missing column")
}

func TestTrainingSetIterator_DifferentColumnOrder(t *testing.T) {
	// Create base schema with feature and label columns in one order
	baseSchema := types.Schema{
		Fields: []types.ColumnSchema{
			{Name: "feature2", Type: types.Int, NativeType: types.NativeTypeLiteral("int")},
			{Name: "label", Type: types.String, NativeType: types.NativeTypeLiteral("string")},
			{Name: "feature1", Type: types.Float64, NativeType: types.NativeTypeLiteral("float")},
		},
	}

	// Create test data matching the base schema order
	testData := []types.Row{
		{
			{Type: types.Int, NativeType: types.NativeTypeLiteral("int"), Value: 10},
			{Type: types.String, NativeType: types.NativeTypeLiteral("string"), Value: "class_a"},
			{Type: types.Float64, NativeType: types.NativeTypeLiteral("float"), Value: 1.0},
		},
	}

	// Create in-memory dataset with the schema and data
	inMemoryDataset := NewInMemoryDataset(testData, baseSchema, nil)

	// Create training set schema with different order
	tsSchema := types.TrainingSetSchema{
		FeatureColumns: []types.FeatureColumn{
			{FeatureColumn: baseSchema.Fields[2]}, // feature1
			{FeatureColumn: baseSchema.Fields[0]}, // feature2
		},
		LabelColumn: baseSchema.Fields[1], // label
	}

	// Create the training set
	trainingSet := NewTrainingSet(inMemoryDataset, tsSchema)

	// Get the training set iterator
	ctx := logging.NewTestContext(t)
	tsIterator, err := trainingSet.TrainingSetIterator(ctx, -1)
	require.NoError(t, err)

	// Iterate and verify data mapping works correctly regardless of order
	require.True(t, tsIterator.Next(), "Should have at least one row")

	// Check feature values - should be in the order specified by tsSchema, not baseSchema
	features := tsIterator.Features()
	require.Len(t, features.Row, 2)

	// First feature should be feature1 (from index 2 in base schema)
	assert.Equal(t, testData[0][2], features.Row[0])

	// Second feature should be feature2 (from index 0 in base schema)
	assert.Equal(t, testData[0][0], features.Row[1])

	// Label should be from index 1 in base schema
	assert.Equal(t, testData[0][1], tsIterator.Label())
}

func TestTrainingSetIterator_GetFeatureSchema(t *testing.T) {
	// Create base schema
	baseSchema := types.Schema{
		Fields: []types.ColumnSchema{
			{Name: "feature1", Type: types.Float64, NativeType: types.NativeTypeLiteral("float")},
			{Name: "feature2", Type: types.Int, NativeType: types.NativeTypeLiteral("int")},
			{Name: "label", Type: types.String, NativeType: types.NativeTypeLiteral("string")},
		},
	}

	// Create training set schema
	tsSchema := types.TrainingSetSchema{
		FeatureColumns: []types.FeatureColumn{
			{FeatureColumn: baseSchema.Fields[0]}, // feature1
			{FeatureColumn: baseSchema.Fields[1]}, // feature2
		},
		LabelColumn: baseSchema.Fields[2], // label
	}

	// Test GetFeatureSchema method
	featureSchema := tsSchema.GetFeatureSchema()

	// Verify the feature schema has the correct columns
	assert.Equal(t, tsSchema.FeatureColumns, featureSchema.FeatureColumns)
	assert.Equal(t, types.ColumnSchema{}, featureSchema.EntityColumn, "Entity column should be empty")
}

func TestTrainingSetIterator_SchemaMismatch(t *testing.T) {
	// Create base schema
	baseSchema := types.Schema{
		Fields: []types.ColumnSchema{
			{Name: "feature1", Type: types.Float64, NativeType: types.NativeTypeLiteral("float")},
			{Name: "feature2", Type: types.Int, NativeType: types.NativeTypeLiteral("int")},
		},
	}

	// Create test data
	testData := []types.Row{
		{
			{Type: types.Float64, NativeType: types.NativeTypeLiteral("float"), Value: 1.0},
			{Type: types.Int, NativeType: types.NativeTypeLiteral("int"), Value: 10},
		},
	}

	// Create in-memory dataset with the schema and data
	inMemoryDataset := NewInMemoryDataset(testData, baseSchema, nil)

	// Create training set schema that doesn't match base schema length
	tsSchema := types.TrainingSetSchema{
		FeatureColumns: []types.FeatureColumn{
			{FeatureColumn: baseSchema.Fields[0]}, // feature1
			{FeatureColumn: baseSchema.Fields[1]}, // feature2
		},
		LabelColumn: baseSchema.Fields[0], // Using feature1 as label too
	}

	// Create the training set
	trainingSet := NewTrainingSet(inMemoryDataset, tsSchema)

	// Get the training set iterator - should error due to schema length mismatch
	ctx := logging.NewTestContext(t)
	_, err := trainingSet.TrainingSetIterator(ctx, -1)
	assert.Error(t, err, "Should error with schema length mismatch")
	assert.Contains(t, err.Error(), "schema length mismatch", "Error should mention schema length")
}

func TestFeatureRow_GetRawValues(t *testing.T) {
	// Create a feature row
	featureValues := []types.Value{
		{Type: types.Float64, NativeType: types.NativeTypeLiteral("float"), Value: 1.0},
		{Type: types.Int, NativeType: types.NativeTypeLiteral("int"), Value: 10},
	}

	featureRow := types.FeatureRow{
		Schema: types.FeaturesSchema{
			FeatureColumns: []types.FeatureColumn{
				{FeatureColumn: types.ColumnSchema{Name: "feature1", Type: types.Float64}},
				{FeatureColumn: types.ColumnSchema{Name: "feature2", Type: types.Int}},
			},
		},
		Row: featureValues,
	}

	// Get raw values
	rawValues := featureRow.GetRawValues()

	// Verify the raw values match the expected values
	assert.Equal(t, []interface{}{1.0, 10}, rawValues, "Raw values should match")
}
