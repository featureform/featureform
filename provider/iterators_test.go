package provider

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/featureform/filestore"
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
			{Name: "entity", ValueType: String},
			{Name: "int", ValueType: Int},
			{Name: "flt", ValueType: Float64},
			{Name: "str", ValueType: String},
			{Name: "bool", ValueType: Bool},
			{Name: "ts", ValueType: Timestamp},
		},
	}

	allRecords := []GenericRecord{
		[]interface{}{nil, 1, 1.1, "test string", true, time.UnixMilli(0).UTC()},
		[]interface{}{"b", nil, 1.2, "second string", false, time.UnixMilli(0).UTC()},
		[]interface{}{"c", 3, nil, "third string", true, time.UnixMilli(0).UTC()},
		[]interface{}{"d", 4, 1.4, nil, false, time.UnixMilli(0).UTC()},
		[]interface{}{"e", 5, 1.5, "fifth string", nil, time.UnixMilli(0).UTC()},
		[]interface{}{"f", 6, 1.6, "sixth string", false, nil},
		[]interface{}{"g", 7, 1.7, "seventh string", true, time.UnixMilli(0).UTC()},
		[]interface{}{"h", 8, 1.8, "eighth string", false, time.UnixMilli(0).UTC()},
		[]interface{}{"i", 9, 1.9, "ninth string", true, time.UnixMilli(0).UTC()},
		[]interface{}{"j", 10, 2.0, "tenth string", false, time.UnixMilli(0).UTC()},
		[]interface{}{"k", 11, 2.1, "eleventh string", true, time.UnixMilli(0).UTC()},
		[]interface{}{"l", 12, 2.2, "twelfth string", false, time.UnixMilli(0).UTC()},
		[]interface{}{"m", 13, 2.3, "thirteenth string", true, time.UnixMilli(0).UTC()},
		[]interface{}{"n", 14, 2.4, "fourteenth string", false, time.UnixMilli(0).UTC()},
		[]interface{}{"o", 15, 2.5, "fifteenth string", true, time.UnixMilli(0).UTC()},
	}

	schema := parquet.SchemaOf(tableSchema.Interface())
	fileCount := 0
	records := make([]GenericRecord, 0)
	files := make([]filestore.Filepath, 0)
	file := &filestore.LocalFilepath{}
	if err := file.SetKey(fmt.Sprintf("part-000%d.parquet", fileCount)); err != nil {
		t.Fatalf("error setting key: %v", err)
	}
	for _, record := range allRecords {
		records = append(records, record)
		if len(records) == 5 {
			parquetRecords := tableSchema.ToParquetRecords(records)
			buf := new(bytes.Buffer)
			if err := parquet.Write[any](buf, parquetRecords, schema); err != nil {
				t.Fatalf("error writing parquet file: %v", err)
			}
			if err := ioutil.WriteFile(file.Key(), buf.Bytes(), 0644); err != nil {
				t.Fatalf("error writing parquet file: %v", err)
			}
			files = append(files, file)
			fileCount++
			file = &filestore.LocalFilepath{}
			if err := file.SetKey(fmt.Sprintf("part-000%d.parquet", fileCount)); err != nil {
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
			for _, expected := range test.Expected {
				if !iterator.Next() {
					t.Fatalf("expected more records")
				}
				actual := iterator.Values()
				if !reflect.DeepEqual(actual, expected) {
					t.Fatalf("expected %v, got %v", expected, actual)
				}
			}
			if err := iterator.Err(); err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		})
	}
}
