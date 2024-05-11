package provider

import (
	"bytes"
	"time"

	"github.com/featureform/fferr"
	"github.com/featureform/filestore"
	"github.com/featureform/provider/types"
	"github.com/parquet-go/parquet-go"
	"go.uber.org/zap"
)

type FileStorePrimaryTable struct {
	store            FileStore
	source           filestore.Filepath
	schema           TableSchema
	isTransformation bool
	id               ResourceID
	logger           *zap.SugaredLogger
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
	exists, err := tbl.store.Exists(destination)
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
	logger := tbl.logger.With("resourceID", tbl.id, "source", tbl.source.ToURI())
	sources := make([]filestore.Filepath, 0)
	// Default to parquet unless we find CSV file(s)
	file_type := filestore.Parquet
	// Given we currently don't allow Spark to write the output of transformations in any other format
	// other than parquet, we can safely assume that the source file is a directory of parquet files.
	// NOTE: we'll have to change the "groups" logic here once we fetch the output of transformations,
	// which happens to be the directory path with the datetime directory part of the path, which is added
	// by offline_store_spark_runner.py
	if tbl.isTransformation {
		logger.Debugw("Reading transformation file(s)")
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
		sources = append(sources, newestFiles...)
	} else if tbl.schema.IsDir {
		logger.Debugw("Reading primary directory", "file_type", tbl.schema.FileType)
		file_type = tbl.schema.FileType
		primarySources, err := tbl.store.List(tbl.source, tbl.schema.FileType)
		if err != nil {
			return nil, err
		}
		sources = append(sources, primarySources...)
	} else {
		logger.Debugw("Reading primary file", "file_type", tbl.schema.FileType)
		sources = append(sources, tbl.source)
		file_type = tbl.schema.FileType
	}

	logger.Debugw("Sources found", "len", len(sources), "file_type", file_type)

	switch file_type {
	case filestore.Parquet:
		logger.Debugw("Getting parquet iterator", "len", len(sources), "store_type", tbl.store.FilestoreType(), "n", n)
		return newMultipleFileParquetIterator(sources, tbl.store, n)
	case filestore.CSV:
		logger.Debugw("Getting CSV iterator", "len", len(sources), "store_type", tbl.store.FilestoreType(), "n", n)
		return newCSVIterator(sources, tbl.store, n)
	default:
		logger.Errorw("Invalid file type", "file_type", file_type)
		return nil, fferr.NewInvalidFileTypeError(string(file_type), nil)
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
