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

type FeatureIterator interface {
	Iterator
	FeatureValues() types.FeatureRow
	FeatureSchema() types.FeaturesSchema
}

type MaterializationDataset interface {
	SizedSegmentableChunkedDataset
	ID() MaterializationID
	FeatureIterator(ctx context.Context, limit int64) (FeatureIterator, error)
	FeatureIterateSegment(ctx context.Context, begin, end int64) (FeatureIterator, error)
	FeatureChunkIterator(ctx context.Context, idx int) (FeatureIterator, error)
	FeatureSchema() types.FeaturesSchema
}

type FeatureIteratorImpl struct {
	Iterator
	featureSchema types.FeaturesSchema
}

func NewFeatureIterator(it Iterator, featureSchema types.FeaturesSchema) FeatureIterator {
	return &FeatureIteratorImpl{
		Iterator:      it,
		featureSchema: featureSchema,
	}
}

func (it *FeatureIteratorImpl) FeatureValues() types.FeatureRow {
	return types.FeatureRow{
		Schema: it.featureSchema,
		Row:    it.Values(),
	}
}

func (it *FeatureIteratorImpl) FeatureSchema() types.FeaturesSchema {
	return it.featureSchema
}

type MaterializationDatasetImpl struct {
	SizedSegmentableChunkedDataset
	materializationID MaterializationID
	featureSchema     types.FeaturesSchema
}

func NewMaterializationDataset(ds SizedSegmentableChunkedDataset, id MaterializationID, featureSchema types.FeaturesSchema) MaterializationDataset {
	return &MaterializationDatasetImpl{
		SizedSegmentableChunkedDataset: ds,
		materializationID:              id,
		featureSchema:                  featureSchema,
	}
}

func (md *MaterializationDatasetImpl) ID() MaterializationID {
	return md.materializationID
}

func (md *MaterializationDatasetImpl) FeatureSchema() types.FeaturesSchema {
	return md.featureSchema
}

func (md *MaterializationDatasetImpl) FeatureIterator(ctx context.Context, limit int64) (FeatureIterator, error) {
	baseIterator, err := md.SizedSegmentableChunkedDataset.Iterator(ctx, limit)
	if err != nil {
		return nil, err
	}
	return NewFeatureIterator(baseIterator, md.featureSchema), nil
}

func (md *MaterializationDatasetImpl) FeatureIterateSegment(ctx context.Context, begin, end int64) (FeatureIterator, error) {
	baseIterator, err := md.SizedSegmentableChunkedDataset.IterateSegment(ctx, begin, end)
	if err != nil {
		return nil, err
	}
	return NewFeatureIterator(baseIterator, md.featureSchema), nil
}

func (md *MaterializationDatasetImpl) FeatureChunkIterator(ctx context.Context, idx int) (FeatureIterator, error) {
	baseIterator, err := md.SizedSegmentableChunkedDataset.ChunkIterator(ctx, idx)
	if err != nil {
		return nil, err
	}
	return NewFeatureIterator(baseIterator, md.featureSchema), nil
}
