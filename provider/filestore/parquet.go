package provider

import (
	"context"
	"fmt"
	"github.com/featureform/provider"
	"github.com/segmentio/parquet-go"
	"io"
)

type ParquetIterator struct {
	reader *parquet.Reader
	index  int64
}

func (p *ParquetIterator) Next() (map[string]interface{}, error) {
	value := make(map[string]interface{})
	err := p.reader.Read(&value)
	if err != nil {
		if err == io.EOF {
			return nil, nil
		} else {
			return nil, err
		}
	}
	return value, nil
}

type ParquetIteratorMultipleFiles struct {
	fileList     []string
	currentFile  int64
	fileIterator provider.Iterator
	store        GenericFileStore
}

func parquetIteratorOverMultipleFiles(fileParts []string, store GenericFileStore) (provider.Iterator, error) {
	b, err := store.bucket.ReadAll(context.TODO(), fileParts[0])
	if err != nil {
		return nil, fmt.Errorf("could not read bucket: %w", err)
	}
	iterator, err := parquetIteratorFromBytes(b)
	if err != nil {
		return nil, fmt.Errorf("could not open first parquet file: %w", err)
	}
	return &ParquetIteratorMultipleFiles{
		fileList:     fileParts,
		currentFile:  int64(0),
		fileIterator: iterator,
		store:        store,
	}, nil
}

func (p *ParquetIteratorMultipleFiles) Next() (map[string]interface{}, error) {
	nextRow, err := p.fileIterator.Next()
	if err != nil {
		return nil, err
	}
	if nextRow == nil {
		if p.currentFile+1 == int64(len(p.fileList)) {
			return nil, nil
		}
		p.currentFile += 1
		b, err := p.store.bucket.ReadAll(context.TODO(), p.fileList[p.currentFile])
		if err != nil {
			return nil, err
		}
		iterator, err := parquetIteratorFromBytes(b)
		if err != nil {
			return nil, err
		}
		p.fileIterator = iterator
		return p.fileIterator.Next()
	}
	return nextRow, nil
}
