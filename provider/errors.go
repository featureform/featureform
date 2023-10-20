package provider

import (
	"fmt"

	fs "github.com/featureform/filestore"
)

type InvalidQueryError struct {
	error string
}

func (e InvalidQueryError) Error() string {
	return e.error
}

type SQLError struct {
	error error
}

func (e SQLError) Error() string {
	return e.error.Error()
}

type TransformationTypeError struct {
	error string
}

func (e TransformationTypeError) Error() string {
	return e.error
}

type EmptyParquetFileError struct{}

func (e EmptyParquetFileError) Error() string {
	return "could not read empty parquet file"
}

type FileStoreError struct {
	Type    fs.FileStoreType
	Message string
}

func (e FileStoreError) Error() string {
	return fmt.Sprintf("(%s) %s", e.Type, e.Message)
}

type SparkExecutorError struct {
	Message string
}

func (e SparkExecutorError) Error() string {
	return e.Message
}
