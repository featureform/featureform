// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"bytes"
	"fmt"
	"time"

	"github.com/featureform/fferr"
	"github.com/featureform/filestore"
	pl "github.com/featureform/provider/location"
	"github.com/featureform/provider/types"
	"github.com/parquet-go/parquet-go"
)

type FileStorePrimaryTable struct {
	store            FileStore
	source           filestore.Filepath
	schema           TableSchema
	isTransformation bool
	id               ResourceID
}

func (tbl *FileStorePrimaryTable) Write(record GenericRecord) error {
	return fferr.NewInternalErrorf("You cannot write to a primary table")
}

func (tbl *FileStorePrimaryTable) WriteBatch(records []GenericRecord) error {
	destination, err := filestore.NewEmptyFilepath(tbl.store.FilestoreType())
	if err != nil {
		return err
	}
	if err := destination.ParseFilePath(tbl.schema.SourceTable); err != nil {
		return err
	}
	exists, err := tbl.store.Exists(pl.NewFileLocation(destination))
	if err != nil {
		return err
	}
	if exists {
		iter, err := tbl.store.Serve([]filestore.Filepath{destination})
		if err != nil {
			return err
		}
		records, err = tbl.append(iter, records)
		if err != nil {
			return err
		}
	}
	buf := new(bytes.Buffer)
	schema := tbl.schema.AsParquetSchema()
	parquetRecords, err := tbl.schema.ToParquetRecords(records)
	if err != nil {
		return err
	}
	if err = parquet.Write[any](buf, parquetRecords, schema); err != nil {
		return err
	}
	return tbl.store.Write(destination, buf.Bytes())
}

// TODO: Add unit tests for this method
func (tbl *FileStorePrimaryTable) append(iter Iterator, newRecords []GenericRecord) ([]GenericRecord, error) {
	records := make([]GenericRecord, 0)
	for {
		val, err := iter.Next()
		if err != nil {
			return nil, err
		}
		if val == nil {
			break
		}
		record := GenericRecord{}
		for _, col := range tbl.schema.Columns {
			switch assertedVal := val[col.Name].(type) {
			case int32:
				record = append(record, int(assertedVal))
			case int64:
				// Given we're instructing Spark to output timestamps as int64 (microseconds),
				// we need to rely on the parquet schema's field metadata to determine whether
				// the field is a timestamp or not. If it is, we need to convert it to its
				// corresponding Go type (time.Time).
				if col.Scalar() == types.Timestamp {
					record = append(record, time.UnixMilli(assertedVal).UTC())
				} else {
					record = append(record, int(assertedVal))
				}
			default:
				record = append(record, assertedVal)
			}
		}
		records = append(records, record)
	}
	records = append(records, newRecords...)
	return records, nil
}

func (tbl *FileStorePrimaryTable) GetName() string {
	return tbl.source.ToURI()
}

func (tbl *FileStorePrimaryTable) IterateSegment(n int64) (GenericTableIterator, error) {
	sources := []filestore.Filepath{tbl.source}
	if tbl.source.IsDir() {
		// The key should only be a directory in the case of transformations.
		if !tbl.isTransformation {
			return nil, fferr.NewInternalErrorf("expected a file but got a directory: %s", tbl.source.Key())
		}
		// The file structure in cloud storage for transformations is /featureform/Transformation/<NAME>/<VARIANT>
		// but there is an additional directory that's named using a timestamp that contains the transformation file
		// we need to access. NewestFileOfType will recursively search for the newest file of the given type (i.e.
		// parquet) given a path (i.e. `key`).
		transformations, err := tbl.store.List(tbl.source, filestore.Parquet)
		if err != nil {
			return nil, err
		}
		groups, err := filestore.NewFilePathGroup(transformations, filestore.DateTimeDirectoryGrouping)
		if err != nil {
			return nil, err
		}
		newestFiles, err := groups.GetFirst()
		if err != nil {
			return nil, err
		}
		sources = newestFiles
	}
	fmt.Printf("Sources: %d found\n", len(sources))
	fmt.Printf("Source %s extension %s\n", sources[0].ToURI(), string(sources[0].Ext()))

	switch sources[0].Ext() {
	case filestore.Parquet:
		return newMultipleFileParquetIterator(sources, tbl.store, n)
	case filestore.CSV:
		if len(sources) > 1 {
			return nil, fferr.NewInternalErrorf("multiple CSV files found for table (%v)", tbl.id)
		}
		fmt.Printf("Reading file at key %s in file store type %s\n", sources[0].Key(), tbl.store.FilestoreType())
		src, err := tbl.store.Open(sources[0])
		if err != nil {
			return nil, err
		}
		return newCSVIterator(src, n)
	default:
		return nil, fferr.NewInvalidFileTypeError(string(sources[0].Ext()), nil)
	}
}

func (tbl *FileStorePrimaryTable) NumRows() (int64, error) {
	src, err := tbl.GetSource()
	if err != nil {
		return 0, err
	}
	return tbl.store.NumRows(src)
}

func (tbl *FileStorePrimaryTable) GetSource() (filestore.Filepath, error) {
	filepath, err := filestore.NewEmptyFilepath(tbl.store.FilestoreType())
	if err != nil {
		return nil, err
	}
	err = filepath.ParseFilePath(tbl.schema.SourceTable)
	return filepath, err
}
