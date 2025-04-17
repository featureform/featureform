package dataset

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	fftypes "github.com/featureform/fftypes"
	pl "github.com/featureform/provider/location"
)

func TestTrainingSet(t *testing.T) {
	// Create a schema for the base dataset
	schema := fftypes.Schema{
		Fields: []fftypes.ColumnSchema{
			{Name: "age", Type: fftypes.Int},
			{Name: "income", Type: fftypes.Float64},
			{Name: "is_employed", Type: fftypes.Bool},
			{Name: "credit_score", Type: fftypes.Int},
		},
	}

	// Create sample data for the base dataset
	data := []fftypes.Row{
		{
			{Value: int64(25)},
			{Value: float64(50000)},
			{Value: true},
			{Value: int64(720)},
		},
		{
			{Value: int64(35)},
			{Value: float64(75000)},
			{Value: true},
			{Value: int64(680)},
		},
		{
			{Value: int64(45)},
			{Value: float64(90000)},
			{Value: false},
			{Value: int64(800)},
		},
		{
			{Value: int64(30)},
			{Value: float64(60000)},
			{Value: true},
			{Value: int64(750)},
		},
	}

	// Create a training set schema (3 features, 1 label)
	tsSchema := fftypes.TrainingSetSchema{
		FeatureColumns: []fftypes.FeatureColumn{
			{
				FeatureColumn: fftypes.ColumnSchema{
					Name: "age",
					Type: fftypes.Int,
				},
			},
			{
				FeatureColumn: fftypes.ColumnSchema{
					Name: "income",
					Type: fftypes.Float64,
				},
			},
			{
				FeatureColumn: fftypes.ColumnSchema{
					Name: "is_employed",
					Type: fftypes.Bool,
				},
			},
		},
		LabelColumn: fftypes.ColumnSchema{
			Name: "credit_score",
			Type: fftypes.Int,
		},
	}

	// Create base dataset and training set
	baseDataset := NewInMemoryDataset(data, schema, pl.NewSQLLocation("testtable"))
	ts := NewTrainingSet(baseDataset, tsSchema)

	t.Run("TestTrainingSetSchema", func(t *testing.T) {
		assert.Equal(t, tsSchema, ts.TrainingSetSchema())
	})

	t.Run("TestTrainingSetIterator", func(t *testing.T) {
		ctx := context.Background()
		iter, err := ts.TrainingSetIterator(ctx, 0)
		require.NoError(t, err)
		require.NotNil(t, iter)

		// Validate the first row
		require.True(t, iter.Next())

		features := iter.Features()
		require.Len(t, features, 3)
		assert.Equal(t, int64(25), features[0])
		assert.Equal(t, float64(50000), features[1])
		assert.Equal(t, true, features[2])

		label := iter.Label()
		assert.Equal(t, int64(720), label)

		// Validate the second row
		require.True(t, iter.Next())

		features = iter.Features()
		require.Len(t, features, 3)
		assert.Equal(t, int64(35), features[0])
		assert.Equal(t, float64(75000), features[1])
		assert.Equal(t, true, features[2])

		label = iter.Label()
		assert.Equal(t, int64(680), label)

		// Validate the third row
		require.True(t, iter.Next())

		features = iter.Features()
		require.Len(t, features, 3)
		assert.Equal(t, int64(45), features[0])
		assert.Equal(t, float64(90000), features[1])
		assert.Equal(t, false, features[2])

		label = iter.Label()
		assert.Equal(t, int64(800), label)

		// Validate the fourth row
		require.True(t, iter.Next())

		features = iter.Features()
		require.Len(t, features, 3)
		assert.Equal(t, int64(30), features[0])
		assert.Equal(t, float64(60000), features[1])
		assert.Equal(t, true, features[2])

		label = iter.Label()
		assert.Equal(t, int64(750), label)

		// No more rows
		assert.False(t, iter.Next())
		assert.NoError(t, iter.Err())
	})

	t.Run("TestTrainingSetWithLimit", func(t *testing.T) {
		ctx := context.Background()
		iter, err := ts.TrainingSetIterator(ctx, 2) // Limit to first 2 rows
		require.NoError(t, err)
		require.NotNil(t, iter)

		// Validate the first row
		require.True(t, iter.Next())

		features := iter.Features()
		require.Len(t, features, 3)
		assert.Equal(t, int64(25), features[0])

		// Validate the second row
		require.True(t, iter.Next())

		features = iter.Features()
		require.Len(t, features, 3)
		assert.Equal(t, int64(35), features[0])

		// No more rows due to limit
		assert.False(t, iter.Next())
	})

	t.Run("TestTrainingSetIteratorWithMismatchedSchema", func(t *testing.T) {
		// Create a schema with column names that don't match the dataset
		badSchema := fftypes.TrainingSetSchema{
			FeatureColumns: []fftypes.FeatureColumn{
				{
					FeatureColumn: fftypes.ColumnSchema{
						Name: "nonexistent_column",
						Type: fftypes.String,
					},
				},
				{
					FeatureColumn: fftypes.ColumnSchema{
						Name: "income",
						Type: fftypes.Float64,
					},
				},
			},
			LabelColumn: fftypes.ColumnSchema{
				Name: "credit_score",
				Type: fftypes.Int,
			},
		}

		badTs := NewTrainingSet(baseDataset, badSchema)

		ctx := context.Background()
		iter, err := badTs.TrainingSetIterator(ctx, 0)
		require.NoError(t, err)
		require.NotNil(t, iter)

		// Should still iterate but with nil values for nonexistent columns
		require.True(t, iter.Next())

		features := iter.Features()
		require.Len(t, features, 2)
		assert.Nil(t, features[0])                   // nonexistent column
		assert.Equal(t, float64(50000), features[1]) // income
	})

	t.Run("TestMultipleIterations", func(t *testing.T) {
		ctx := context.Background()

		// First iteration
		iter1, err := ts.TrainingSetIterator(ctx, 0)
		require.NoError(t, err)
		rowCount1 := 0
		for iter1.Next() {
			rowCount1++
		}
		assert.Equal(t, 4, rowCount1)

		// Second iteration should start fresh
		iter2, err := ts.TrainingSetIterator(ctx, 0)
		require.NoError(t, err)
		rowCount2 := 0
		for iter2.Next() {
			rowCount2++
		}
		assert.Equal(t, 4, rowCount2)
	})
}
