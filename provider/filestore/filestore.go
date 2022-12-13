package filestore

import (
	"bytes"
	"context"
	"fmt"
	"github.com/featureform/provider"
	"github.com/segmentio/parquet-go"
	"go.uber.org/zap"
	"gocloud.dev/blob"
	"io"
	"sort"
	"strings"
	"time"
)

type FileStoreConfig []byte

type FileStoreType string

const (
	Memory     FileStoreType = "MEMORY"
	FileSystem               = "FILE_SYSTEM"
	Azure                    = "AZURE"
	S3                       = "S3"
)

type FileStore interface {
	Write(key string, data []byte) error
	Read(key string) ([]byte, error)
	Serve(key string) (provider.Iterator, error)
	Exists(key string) (bool, error)
	Delete(key string) error
	DeleteAll(dir string) error
	NewestFile(prefix string) (string, error)
	PathWithPrefix(path string, remote bool) string
	NumRows(key string) (int64, error)
	Close() error
}

type GenericFileStore struct {
	bucket *blob.Bucket
	path   string
}

func (store GenericFileStore) PathWithPrefix(path string, remote bool) string {
	if len(store.path) > 4 && store.path[0:4] == "file" {
		return fmt.Sprintf("%s%s", store.path[len("file:///"):], path)
	} else {
		return path
	}
}

func (store GenericFileStore) NewestFile(prefix string) (string, error) {
	opts := blob.ListOptions{
		Prefix: prefix,
	}

	listIterator := store.bucket.List(&opts)
	mostRecentTime := time.UnixMilli(0)
	mostRecentKey := ""
	for {
		if listObj, err := listIterator.Next(context.TODO()); err == nil {
			pathParts := strings.Split(listObj.Key, ".")
			fileType := pathParts[len(pathParts)-1]
			if fileType == "parquet" && !listObj.IsDir && (listObj.ModTime.After(mostRecentTime) || listObj.ModTime.Equal(mostRecentTime)) {
				mostRecentTime = listObj.ModTime
				mostRecentKey = listObj.Key
			}
		} else if err == io.EOF {
			return mostRecentKey, nil
		} else {
			return "", err
		}
	}
}

func (store GenericFileStore) outputFileList(prefix string) []string {
	opts := blob.ListOptions{
		Prefix:    prefix,
		Delimiter: "/",
	}
	listIterator := store.bucket.List(&opts)
	mostRecentOutputPartTime := "0000-00-00 00:00:00.000000"
	mostRecentOutputPartPath := ""
	for listObj, err := listIterator.Next(context.TODO()); err == nil; listObj, err = listIterator.Next(context.TODO()) {
		if listObj == nil {
			return []string{}
		}
		dirParts := strings.Split(listObj.Key[:len(listObj.Key)-1], "/")
		timestamp := dirParts[len(dirParts)-1]
		if listObj.IsDir && timestamp > mostRecentOutputPartTime {
			mostRecentOutputPartTime = timestamp
			mostRecentOutputPartPath = listObj.Key
		}
	}
	opts = blob.ListOptions{
		Prefix: mostRecentOutputPartPath,
	}
	partsIterator := store.bucket.List(&opts)
	partsList := make([]string, 0)
	for listObj, err := partsIterator.Next(context.TODO()); err == nil; listObj, err = partsIterator.Next(context.TODO()) {
		pathParts := strings.Split(listObj.Key, ".")

		fileType := pathParts[len(pathParts)-1]
		if fileType == "parquet" {
			partsList = append(partsList, listObj.Key)
		}
	}
	sort.Strings(partsList)
	return partsList
}

func (store GenericFileStore) DeleteAll(dir string) error {
	opts := blob.ListOptions{
		Prefix: dir,
	}
	listIterator := store.bucket.List(&opts)
	for listObj, err := listIterator.Next(context.TODO()); err == nil; listObj, err = listIterator.Next(context.TODO()) {
		if !listObj.IsDir {
			if err := store.bucket.Delete(context.TODO(), listObj.Key); err != nil {
				return fmt.Errorf("failed to delete object %s in directory %s: %v", listObj.Key, dir, err)
			}
		}
	}
	return nil
}

func (store GenericFileStore) Write(key string, data []byte) error {
	err := store.bucket.WriteAll(context.TODO(), key, data, nil)
	if err != nil {
		return err
	}
	return nil
}

func (store GenericFileStore) Read(key string) ([]byte, error) {
	data, err := store.bucket.ReadAll(context.TODO(), key)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (store GenericFileStore) ServeDirectory(dir string) (provider.Iterator, error) {
	fileParts := store.outputFileList(dir)
	if len(fileParts) == 0 {
		return nil, fmt.Errorf("no files in given directory")
	}
	// assume file type is parquet
	return parquetIteratorOverMultipleFiles(fileParts, store)
}

func (store GenericFileStore) Serve(key string) (provider.Iterator, error) {
	keyParts := strings.Split(key, ".")
	if len(keyParts) == 1 {
		return store.ServeDirectory(key)
	}
	b, err := store.bucket.ReadAll(context.TODO(), key)
	if err != nil {
		return nil, fmt.Errorf("could not read file: %w", err)
	}
	switch fileType := keyParts[len(keyParts)-1]; fileType {
	case "parquet":
		return parquetIteratorFromBytes(b)
	case "csv":
		return nil, fmt.Errorf("could not find CSV reader")
	default:
		return nil, fmt.Errorf("unsupported file type")
	}

}

func (store GenericFileStore) NumRows(key string) (int64, error) {
	b, err := store.bucket.ReadAll(context.TODO(), key)
	if err != nil {
		return 0, err
	}
	keyParts := strings.Split(key, ".")
	switch fileType := keyParts[len(keyParts)-1]; fileType {
	case "parquet":
		return getParquetNumRows(b)
	default:
		return 0, fmt.Errorf("unsupported file type")
	}
}

func (store GenericFileStore) Exists(key string) (bool, error) {
	return store.bucket.Exists(context.TODO(), key)
}

func (store GenericFileStore) Delete(key string) error {
	return store.bucket.Delete(context.TODO(), key)
}

func (store GenericFileStore) Close() error {
	return store.bucket.Close()
}

type FilestoreOfflineTable struct {
	Schema provider.ResourceSchema
}

func (tbl *FilestoreOfflineTable) Write(provider.ResourceRecord) error {
	return fmt.Errorf("not yet implemented")
}

type PrimaryTable struct {
	Store            FileStore
	SourcePath       string
	IsTransformation bool
	Id               provider.ResourceID
}

func (tbl *PrimaryTable) Write(provider.GenericRecord) error {
	return fmt.Errorf("not implemented")
}

func (tbl *PrimaryTable) GetName() string {
	return tbl.SourcePath
}

func (tbl *PrimaryTable) IterateSegment(n int64) (provider.GenericTableIterator, error) {
	iterator, err := tbl.Store.Serve(tbl.SourcePath)
	if err != nil {
		return nil, fmt.Errorf("Could not create iterator from source table: %v", err)
	}
	return &FileStoreIterator{iter: iterator, curIdx: 0, maxIdx: n}, nil
}

func (tbl *PrimaryTable) NumRows() (int64, error) {
	return tbl.Store.NumRows(tbl.SourcePath)
}

type FileStoreIterator struct {
	iter    provider.Iterator
	err     error
	curIdx  int64
	maxIdx  int64
	records []interface{}
	columns []string
}

func (it *FileStoreIterator) Next() bool {
	it.curIdx += 1
	if it.curIdx > it.maxIdx {
		return false
	}
	values, err := it.iter.Next()
	if values == nil {
		return false
	}
	if err != nil {
		it.err = err
		return false
	}
	records := make([]interface{}, 0)
	columns := make([]string, 0)
	for k, v := range values {
		columns = append(columns, k)
		records = append(records, v)
	}
	it.columns = columns
	it.records = records
	return true
}

func (it *FileStoreIterator) Columns() []string {
	return it.columns
}

func (it *FileStoreIterator) Err() error {
	return it.err
}

func (it *FileStoreIterator) Values() provider.GenericRecord {
	return it.records
}

func (it *FileStoreIterator) Close() error {
	return nil
}

type Materialization struct {
	Id    provider.ResourceID
	Store FileStore
	Key   string
}

func (mat Materialization) ID() provider.MaterializationID {
	return provider.MaterializationID(fmt.Sprintf("%s/%s/%s", provider.FeatureMaterialization, mat.Id.Name, mat.Id.Variant))
}

func (mat Materialization) NumRows() (int64, error) {
	materializationPath := mat.Store.PathWithPrefix(ResourcePath(mat.Id), false)
	latestMaterializationPath, err := mat.Store.NewestFile(materializationPath)
	if err != nil {
		return 0, fmt.Errorf("Could not get materialization num rows; %v", err)
	}
	return mat.Store.NumRows(latestMaterializationPath)
}

func (mat Materialization) IterateSegment(begin, end int64) (provider.FeatureIterator, error) {
	materializationPath := mat.Store.PathWithPrefix(ResourcePath(mat.Id), false)
	latestMaterializationPath, err := mat.Store.NewestFile(materializationPath)
	if err != nil {
		return nil, fmt.Errorf("Could not get materialization iterate segment: %v", err)
	}
	iter, err := mat.Store.Serve(latestMaterializationPath)
	if err != nil {
		return nil, err
	}
	for i := int64(0); i < begin; i++ {
		_, _ = iter.Next()
	}
	return &FileStoreFeatureIterator{
		iter:   iter,
		curIdx: 0,
		maxIdx: end,
	}, nil
}

type FileStoreFeatureIterator struct {
	iter   provider.Iterator
	err    error
	cur    provider.ResourceRecord
	curIdx int64
	maxIdx int64
}

func (iter *FileStoreFeatureIterator) Next() bool {
	iter.curIdx += 1
	if iter.curIdx > iter.maxIdx {
		return false
	}
	nextVal, err := iter.iter.Next()
	if err != nil {
		iter.err = err
		return false
	}
	if nextVal == nil {
		return false
	}
	formatDate := "2006-01-02 15:04:05 UTC" // hardcoded golang format date
	timeString, ok := nextVal["ts"].(string)
	if !ok {
		iter.cur = provider.ResourceRecord{Entity: fmt.Sprintf("%s", nextVal["entity"]), Value: nextVal["value"]}
	} else {
		timestamp, err1 := time.Parse(formatDate, timeString)
		formatDateWithoutUTC := "2006-01-02 15:04:05"
		timestamp2, err2 := time.Parse(formatDateWithoutUTC, timeString)
		formatDateMilli := "2006-01-02 15:04:05 +0000 UTC" // hardcoded golang format date
		timestamp3, err3 := time.Parse(formatDateMilli, timeString)
		if err1 != nil && err2 != nil && err3 != nil {
			iter.err = fmt.Errorf("could not parse timestamp: %v: %v, %v", nextVal["ts"], err1, err2)
			return false
		}
		if err2 == nil {
			timestamp = timestamp2
		} else if err3 == nil {
			timestamp = timestamp3
		}
		iter.cur = provider.ResourceRecord{Entity: string(nextVal["entity"].(string)), Value: nextVal["value"], TS: timestamp}
	}

	return true
}

func (iter *FileStoreFeatureIterator) Value() provider.ResourceRecord {
	return iter.cur
}

func (iter *FileStoreFeatureIterator) Err() error {
	return iter.err
}

func (iter *FileStoreFeatureIterator) Close() error {
	return nil
}

type FileStoreTrainingSet struct {
	id       provider.ResourceID
	store    FileStore
	key      string
	iter     provider.Iterator
	Error    error
	features []interface{}
	label    interface{}
}

func (ts *FileStoreTrainingSet) Next() bool {
	row, err := ts.iter.Next()
	if err != nil {
		ts.Error = err
		return false
	}
	if row == nil {
		return false
	}
	feature_values := make([]interface{}, 0)
	for key, val := range row {
		columnSections := strings.Split(key, "__")
		if columnSections[0] == "Label" {
			ts.label = val
		} else {
			feature_values = append(feature_values, val)
		}
	}
	ts.features = feature_values
	return true
}

func (ts *FileStoreTrainingSet) Features() []interface{} {
	return ts.features
}

func (ts *FileStoreTrainingSet) Label() interface{} {
	return ts.label
}

func (ts *FileStoreTrainingSet) Err() error {
	return ts.Error
}

func convertToParquetBytes(list []any) ([]byte, error) {
	// TODO possibly accepts single struct instead of list, have to be able to accept either, or another function
	if len(list) == 0 {
		return nil, fmt.Errorf("list is empty")
	}
	schema := parquet.SchemaOf(list[0])
	buf := new(bytes.Buffer)
	err := parquet.Write[any](
		buf,
		list,
		schema,
	)
	if err != nil {
		return nil, fmt.Errorf("Could not write parquet file to bytes: %v", err)
	}
	return buf.Bytes(), nil
}

func getParquetNumRows(b []byte) (int64, error) {
	file := bytes.NewReader(b)
	r := parquet.NewReader(file)
	return r.NumRows(), nil
}

func parquetIteratorFromBytes(b []byte) (provider.Iterator, error) {
	file := bytes.NewReader(b)
	r := parquet.NewReader(file)
	return &ParquetIterator{
		reader: r,
		index:  int64(0),
	}, nil
}

func ResourcePath(id provider.ResourceID) string {
	return provider.ResourcePrefix(id)
}

func RegisterResource(id provider.ResourceID, schema provider.ResourceSchema, logger *zap.SugaredLogger, store FileStore) (provider.OfflineTable, error) {
	resourceKey := store.PathWithPrefix(ResourcePath(id), false)
	resourceExists, err := store.Exists(resourceKey)
	if err != nil {
		logger.Errorw("Error checking if resource exists", "error", err)
		return nil, fmt.Errorf("error checking if resource registry exists: %v", err)
	}
	if resourceExists {
		logger.Errorw("Resource already exists in blob store", "id", id, "ResourceKey", resourceKey)
		return nil, &provider.TableAlreadyExists{id.Name, id.Variant}
	}
	serializedSchema, err := schema.Serialize()
	if err != nil {
		return nil, fmt.Errorf("error serializing resource schema: %s: %s", schema, err)
	}
	if err := store.Write(resourceKey, serializedSchema); err != nil {
		return nil, fmt.Errorf("error writing resource schema: %s: %s", schema, err)
	}
	logger.Debugw("Registered resource table", "resourceID", id, "for source", schema.SourceTable)
	return &FilestoreOfflineTable{schema}, nil
}

func RegisterPrimary(id provider.ResourceID, sourceName string, logger *zap.SugaredLogger, store FileStore) (provider.PrimaryTable, error) {
	resourceKey := store.PathWithPrefix(ResourcePath(id), false)
	primaryExists, err := store.Exists(resourceKey)
	if err != nil {
		logger.Errorw("Error checking if primary exists", err)
		return nil, fmt.Errorf("error checking if primary exists: %v", err)
	}
	if primaryExists {
		logger.Errorw("Error checking if primary exists")
		return nil, fmt.Errorf("primary already exists")
	}

	logger.Debugw("Registering primary table", id, "for source", sourceName)
	if err := store.Write(resourceKey, []byte(sourceName)); err != nil {
		logger.Errorw("Could not write primary table", err)
		return nil, err
	}
	logger.Debugw("Succesfully registered primary table", id, "for source", sourceName)
	return &PrimaryTable{store, sourceName, false, id}, nil
}

func GetPrimaryTable(id provider.ResourceID, store FileStore, logger *zap.SugaredLogger) (provider.PrimaryTable, error) {
	resourceKey := store.PathWithPrefix(ResourcePath(id), false)
	logger.Debugw("Getting primary table", id)

	table, err := store.Read(resourceKey)
	if err != nil {
		return nil, fmt.Errorf("error fetching primary table: %v", err)
	}

	logger.Debugw("Succesfully retrieved primary table", id)
	return &PrimaryTable{store, string(table), false, id}, nil
}

func GetResourceTable(id provider.ResourceID, store FileStore, logger *zap.SugaredLogger) (provider.OfflineTable, error) {
	resourcekey := store.PathWithPrefix(ResourcePath(id), false)
	logger.Debugw("Getting resource table", id)
	serializedSchema, err := store.Read(resourcekey)
	if err != nil {
		return nil, fmt.Errorf("Error reading schema bytes from blob storage: %v", err)
	}
	resourceSchema := provider.ResourceSchema{}
	if err := resourceSchema.Deserialize(serializedSchema); err != nil {
		return nil, fmt.Errorf("Error deserializing resource table: %v", err)
	}
	logger.Debugw("Succesfully fetched resource table", "id", id)
	return &FilestoreOfflineTable{resourceSchema}, nil
}

func GetMaterialization(id provider.MaterializationID, store FileStore, logger *zap.SugaredLogger) (provider.Materialization, error) {
	s := strings.Split(string(id), "/")
	if len(s) != 3 {
		logger.Errorw("Invalid materialization id", id)
		return nil, fmt.Errorf("invalid materialization id")
	}
	materializationID := provider.ResourceID{s[1], s[2], provider.FeatureMaterialization}
	logger.Debugw("Getting materialization", "id", id)
	materializationPath := store.PathWithPrefix(ResourcePath(materializationID), false)
	materializationExactPath, err := store.NewestFile(materializationPath)
	if err != nil {
		logger.Errorw("Could not fetch materialization resource key", "error", err)
		return nil, fmt.Errorf("Could not fetch materialization resource key: %v", err)
	}
	logger.Debugw("Succesfully retrieved materialization", "id", id)
	return &Materialization{materializationID, store, materializationExactPath}, nil
}

func DeleteMaterialization(id provider.MaterializationID, store FileStore, logger *zap.SugaredLogger) error {
	s := strings.Split(string(id), "/")
	if len(s) != 3 {
		logger.Errorw("Invalid materialization id", id)
		return fmt.Errorf("invalid materialization id")
	}
	materializationID := provider.ResourceID{s[1], s[2], provider.FeatureMaterialization}
	materializationPath := store.PathWithPrefix(ResourcePath(materializationID), false)
	materializationExactPath, err := store.NewestFile(materializationPath)
	if err != nil {
		return fmt.Errorf("materialization does not exist: %v", err)
	}
	return store.Delete(materializationExactPath)
}

func GetTrainingSet(id provider.ResourceID, store FileStore, logger *zap.SugaredLogger) (provider.TrainingSetIterator, error) {
	resourceKeyPrefix := store.PathWithPrefix(ResourcePath(id), false)
	trainingSetExactPath, err := store.NewestFile(resourceKeyPrefix)
	if err != nil {
		return nil, fmt.Errorf("could not get training set: %v", err)
	}
	if trainingSetExactPath == "" {
		return nil, fmt.Errorf("the training set (%v at resource prefix: %s) does not exist", id, resourceKeyPrefix)
	}

	iterator, err := store.Serve(trainingSetExactPath)
	if err != nil {
		return nil, fmt.Errorf("could not serve training set: %w", err)
	}
	return &FileStoreTrainingSet{id: id, store: store, key: trainingSetExactPath, iter: iterator}, nil
}
