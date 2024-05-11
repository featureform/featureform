package provider

import (
	"fmt"
	"os"
	"testing"
	"time"

	fs "github.com/featureform/filestore"
	"github.com/featureform/logging"
	pc "github.com/featureform/provider/provider_config"
	ps "github.com/featureform/provider/provider_schema"
	"github.com/featureform/provider/types"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

func TestFileStorePrimaryTable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	t.Logf("Testing PrimaryTable Write")

	_ = godotenv.Load("../.env")

	filestores, err := getFilestorePrimaryTables()
	if err != nil {
		t.Fatalf("Error getting filestores: %s", err)
	}

	testFuncMap := map[string]func(*testing.T, *FileStorePrimaryTable) error{
		"Write":          testWrite,
		"WriteBatch":     testWriteBatch,
		"Append":         testAppend,
		"IterateSegment": testIterateSegment,
		"GetSource":      testGetSource,
	}

	for _, filestore := range filestores {
		t.Logf("Testing filestore: %s", filestore.store.FilestoreType())

		for testName, testFunc := range testFuncMap {
			t.Logf("Running test: %s", testName)
			err := testFunc(t, filestore)
			if err != nil {
				t.Fatalf("Error in test %s: %s", testName, err)
			}
		}

	}
}

func testWrite(t *testing.T, store *FileStorePrimaryTable) error {
	t.Logf("Testing PrimaryTable Write")
	if err := store.Write(GenericRecord{}); err == nil {
		return fmt.Errorf("expected error, got nil")
	}
	return nil
}

func testWriteBatch(t *testing.T, store *FileStorePrimaryTable) error {
	t.Logf("Testing PrimaryTable WriteBatch")

	return store.WriteBatch(getGenericRecords())
}

func testAppend(t *testing.T, store *FileStorePrimaryTable) error {
	t.Logf("Testing PrimaryTable Append")

	return store.WriteBatch(getGenericRecords())
}

func testIterateSegment(t *testing.T, store *FileStorePrimaryTable) error {
	t.Logf("Testing PrimaryTable IterateSegment")

	iter, err := store.IterateSegment(50)
	if err != nil {
		return err
	}

	recordCount := 0
	for {
		hasNext := iter.Next()
		if iter.Err() != nil {
			return iter.Err()
		}
		if !hasNext {
			break
		}
		recordCount++
	}

	if recordCount < 5 && recordCount > 10 {
		return fmt.Errorf("expected record count between 5 and 10, got %d", recordCount)
	}

	return nil
}

func testGetSource(t *testing.T, store *FileStorePrimaryTable) error {
	t.Logf("Testing PrimaryTable GetSource")

	source, err := store.GetSource()
	if err != nil {
		return err
	}

	if source.Ext() != "parquet" {
		return fmt.Errorf("expected source to be parquet, got %s", source.Ext())
	}

	if source.ToURI() != store.source.ToURI() {
		return fmt.Errorf("expected source to be %s, got %s", store.source.ToURI(), source.ToURI())
	}

	return nil
}

func getFilestorePrimaryTables() ([]*FileStorePrimaryTable, error) {
	primaryTables := make([]*FileStorePrimaryTable, 0)

	s3Primary, err := getS3FilestorePrimaryTable()
	if err != nil {
		return nil, err
	}

	primaryTables = append(primaryTables, s3Primary)

	return primaryTables, nil
}

func getS3FilestorePrimaryTable() (*FileStorePrimaryTable, error) {
	id := ResourceID{
		Name:    uuid.NewString(),
		Variant: uuid.NewString(),
		Type:    Primary,
	}

	config := &pc.S3FileStoreConfig{
		Credentials: pc.AWSCredentials{
			AWSAccessKeyId: os.Getenv("AWS_ACCESS_KEY_ID"),
			AWSSecretKey:   os.Getenv("AWS_SECRET_KEY"),
		},
		BucketRegion: os.Getenv("S3_BUCKET_REGION"),
		BucketPath:   os.Getenv("S3_BUCKET_PATH"),
		Path:         "",
	}

	serialized, err := config.Serialize()
	if err != nil {
		return &FileStorePrimaryTable{}, err
	}

	filestore, err := NewS3FileStore(serialized)
	if err != nil {
		return &FileStorePrimaryTable{}, err
	}

	sourceKey := fmt.Sprintf("%s/%s/src.parquet", ps.ResourceToDirectoryPath(id.Type.String(), id.Name, id.Variant), time.Now().Format("2006-01-02-15-04-05-999999"))
	source, err := filestore.CreateFilePath(sourceKey, false)
	if err != nil {
		return &FileStorePrimaryTable{}, err
	}

	return &FileStorePrimaryTable{
		store:  filestore,
		source: source,
		schema: TableSchema{
			SourceTable: source.ToURI(),
			Columns: []TableColumn{
				{Name: "entity", ValueType: types.String},
				{Name: "int", ValueType: types.Int},
				{Name: "flt", ValueType: types.Float64},
				{Name: "str", ValueType: types.String},
				{Name: "bool", ValueType: types.Bool},
				{Name: "fltvec", ValueType: types.VectorType{types.Float64, 3, false}},
				{Name: "ts", ValueType: types.Timestamp},
			},
			FileType: fs.Parquet,
			IsDir:    false,
		},
		isTransformation: false,
		id:               id,
		logger:           logging.NewLogger("filestore_primary_table_test"),
	}, nil
}

func getGenericRecords() []GenericRecord {
	return []GenericRecord{
		[]interface{}{"a", 1, 1.1, "test string", true, []float64{1.0, 1.0, 1.0}, time.UnixMilli(0)},
		[]interface{}{"b", 2, 1.2, "second string", false, []float64{1.0, 2.0, 1.0}, time.UnixMilli(0)},
		[]interface{}{"c", 3, 1.3, "third string", nil, []float64{1.0, 3.0, 1.0}, time.UnixMilli(0)},
		[]interface{}{"d", 4, 1.4, "fourth string", false, []float64{1.0, 4.0, 1.0}, time.UnixMilli(0)},
		[]interface{}{"e", 5, 1.5, "fifth string", true, []float64{1.0, 5.0, 1.0}, time.UnixMilli(0)},
	}
}
