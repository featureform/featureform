package dataset

import (
	"context"

	types "github.com/featureform/fftypes"
)

type SizedSegmentableChunkedDataset interface {
	SizedSegmentableDataset
	ChunkedDataset
}

type MaterializationID string

type FeatureIterator struct {
	Iterator
	featureSchema types.FeaturesSchema
}

func (it *FeatureIterator) FeatureValues() types.FeatureRow {
	return types.FeatureRow{
		Schema: it.featureSchema,
		Row:    it.Values(),
	}
}

func (it *FeatureIterator) FeatureSchema() types.FeaturesSchema {
	return it.featureSchema
}

func NewFeatureIterator(it Iterator, featureSchema types.FeaturesSchema) *FeatureIterator {
	return &FeatureIterator{
		Iterator:      it,
		featureSchema: featureSchema,
	}
}

type Materialization struct {
	SizedSegmentableChunkedDataset
	materializationID MaterializationID
	featureSchema     types.FeaturesSchema
}

func (m *Materialization) ID() MaterializationID {
	return m.materializationID
}

func (m *Materialization) FeatureSchema() types.FeaturesSchema {
	return m.featureSchema
}

func (m *Materialization) FeatureIterator(ctx context.Context, limit int64) (*FeatureIterator, error) {
	baseIterator, err := m.SizedSegmentableChunkedDataset.Iterator(ctx, limit)
	if err != nil {
		return nil, err
	}
	return NewFeatureIterator(baseIterator, m.featureSchema), nil
}

func (m *Materialization) FeatureIterateSegment(ctx context.Context, begin, end int64) (*FeatureIterator, error) {
	baseIterator, err := m.SizedSegmentableChunkedDataset.IterateSegment(ctx, begin, end)
	if err != nil {
		return nil, err
	}
	return NewFeatureIterator(baseIterator, m.featureSchema), nil
}

func (m *Materialization) FeatureChunkIterator(ctx context.Context, idx int) (*FeatureIterator, error) {
	baseIterator, err := m.SizedSegmentableChunkedDataset.ChunkIterator(ctx, idx)
	if err != nil {
		return nil, err
	}
	return NewFeatureIterator(baseIterator, m.featureSchema), nil
}

func NewMaterialization(ds SizedSegmentableChunkedDataset, id MaterializationID, featureSchema types.FeaturesSchema) Materialization {
	return Materialization{
		SizedSegmentableChunkedDataset: ds,
		materializationID:              id,
		featureSchema:                  featureSchema,
	}
}
