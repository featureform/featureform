package provider

import (
	"bytes"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/featureform/filestore"
	"github.com/featureform/provider/types"
	"github.com/parquet-go/parquet-go"
)

func TestMultipleFileParquetIterator(t *testing.T) {
	type IteratorTest struct {
		Files    []filestore.Filepath
		Store    FileStore
		Limit    int64
		Expected []GenericRecord
	}

	tableSchema := TableSchema{
		Columns: []TableColumn{
			{Name: "entity", ValueType: types.String},
			{Name: "int", ValueType: types.Int},
			{Name: "flt", ValueType: types.Float64},
			{Name: "str", ValueType: types.String},
			{Name: "bool", ValueType: types.Bool},
			{Name: "ts", ValueType: types.Timestamp},
			{Name: "fltvec", ValueType: types.VectorType{types.Float32, 3, false}},
		},
	}

	allRecords := []GenericRecord{
		[]interface{}{nil, 1, 1.1, "test string", true, time.UnixMilli(0).UTC(), nil},
		[]interface{}{"b", nil, 1.2, "second string", false, time.UnixMilli(0).UTC(), []float32{1, 2, 3}},
		[]interface{}{"c", 3, nil, "third string", true, time.UnixMilli(0).UTC(), []float32{0, 0, 0}},
		[]interface{}{"d", -4, 1.4, nil, false, time.UnixMilli(0).UTC(), []float32{-1, -2, -3}},
		[]interface{}{"e", 5, 1.5, "fifth string", nil, time.UnixMilli(0).UTC(), []float32{1, 2, 3}},
		[]interface{}{"f", 6, 1.6, "sixth string", false, nil, []float32{1, 2, 3}},
		[]interface{}{"g", 7, 1.7, "seventh string", true, time.UnixMilli(0).UTC(), []float32{1, 2, 3}},
		[]interface{}{"h", 8, 1.8, "eighth string", false, time.UnixMilli(0).UTC(), []float32{1, 2, 3}},
		[]interface{}{"i", 9, 1.9, "ninth string", true, time.UnixMilli(0).UTC(), []float32{1, 2, 3}},
		[]interface{}{"j", 10, 2.0, "tenth string", false, time.UnixMilli(0).UTC(), []float32{1, 2, 3}},
		[]interface{}{"k", 11, 2.1, "eleventh string", true, time.UnixMilli(0).UTC(), []float32{1, 2, 3}},
		[]interface{}{"l", 12, 2.2, "twelfth string", false, time.UnixMilli(0).UTC(), []float32{1, 2, 3}},
		[]interface{}{"m", 13, 2.3, "thirteenth string", true, time.UnixMilli(0).UTC(), []float32{1, 2, 3}},
		[]interface{}{"n", 14, 2.4, "fourteenth string", false, time.UnixMilli(0).UTC(), []float32{1, 2, 3}},
		[]interface{}{"o", 15, 2.5, "fifteenth string", true, time.UnixMilli(0).UTC(), []float32{1, 2, 3}},
	}

	schema := tableSchema.AsParquetSchema()
	fileCount := 0
	records := make([]GenericRecord, 0)
	files := make([]filestore.Filepath, 0)
	file := &filestore.LocalFilepath{}
	if err := file.SetKey(fmt.Sprintf("%s/part-000%d.parquet", outputDir, fileCount)); err != nil {
		t.Fatalf("error setting key: %v", err)
	}
	for _, record := range allRecords {
		records = append(records, record)
		if len(records) == 5 {
			parquetRecords, err := tableSchema.ToParquetRecords(records)
			if err != nil {
				t.Fatalf("error writing parquet file: %v", err)
			}
			buf := new(bytes.Buffer)
			if err := parquet.Write[any](buf, parquetRecords, schema); err != nil {
				t.Fatalf("error writing parquet file: %v", err)
			}
			if err := os.MkdirAll(file.KeyPrefix(), 0755); err != nil {
				t.Fatalf("error creating directory: %v", err)
			}
			if err := os.WriteFile(file.Key(), buf.Bytes(), 0644); err != nil {
				t.Fatalf("error writing parquet file: %v", err)
			}
			files = append(files, file)
			fileCount++
			file = &filestore.LocalFilepath{}
			if err := file.SetKey(fmt.Sprintf("%s/part-000%d.parquet", outputDir, fileCount)); err != nil {
				t.Fatalf("error setting key: %v", err)
			}
			records = make([]GenericRecord, 0)
		}
	}

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("error getting working directory: %v", err)
	}
	dirPath := fmt.Sprintf("{\"DirPath\": \"file:///%s/\"}", wd)
	localFileStore, err := NewLocalFileStore([]byte(dirPath))
	if err != nil {
		t.Fatalf("error creating local file store: %v", err)
	}

	tests := map[string]IteratorTest{
		"SingleFileNoLimit": {
			Files:    files[:1],
			Store:    localFileStore,
			Limit:    -1,
			Expected: allRecords[:5],
		},
		"AllFilesNoLimit": {
			Files:    files,
			Store:    localFileStore,
			Limit:    -1,
			Expected: allRecords,
		},
		"SingleFileLimit": {
			Files:    files[:1],
			Store:    localFileStore,
			Limit:    3,
			Expected: allRecords[:3],
		},
		"AllFilesLimit": {
			Files:    files,
			Store:    localFileStore,
			Limit:    6,
			Expected: allRecords[:6],
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			iterator, err := newMultipleFileParquetIterator(test.Files, test.Store, test.Limit)
			if err != nil {
				t.Fatalf("error creating iterator: %v", err)
			}
			records := make([]GenericRecord, 0)
			for {
				if !iterator.Next() {
					break
				}
				records = append(records, iterator.Values())
			}
			if len(records) != len(test.Expected) {
				t.Fatalf("expected %d records, got %d", len(test.Expected), len(records))
			}
			if !reflect.DeepEqual(records, test.Expected) {
				t.Fatalf("expected %v, got %v", test.Expected, records)
			}
			if err := iterator.Err(); err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		})
	}
}
