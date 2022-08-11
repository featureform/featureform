package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	//for compatability with parquet-go
	awsV1 "github.com/aws/aws-sdk-go/aws"
	credentialsV1 "github.com/aws/aws-sdk-go/aws/credentials"
	session "github.com/aws/aws-sdk-go/aws/session"
	s3manager "github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/emr"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	parquetGo "github.com/xitongsys/parquet-go-source/s3"
	reader "github.com/xitongsys/parquet-go/reader"
	source "github.com/xitongsys/parquet-go/source"
	writer "github.com/xitongsys/parquet-go/writer"
)

type SparkExecutorType string

const (
	EMR SparkExecutorType = "EMR"
)

type SparkStoreType string

const (
	S3 SparkStoreType = "S3"
)

type SelectReturnType string

const (
	CSV  SelectReturnType = "CSV"
	JSON                  = "JSON"
)

type SparkConfig struct {
	ExecutorType   SparkExecutorType
	ExecutorConfig string
	StoreType      SparkStoreType
	StoreConfig    string
}

func (s *SparkConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, s)
	if err != nil {
		return err
	}
	return nil
}

func (s *SparkConfig) Serialize() []byte {
	conf, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return conf
}

type EMRConfig struct {
	AWSAccessKeyId string
	AWSSecretKey   string
	ClusterRegion  string
	ClusterName    string
}

func (e *EMRConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, e)
	if err != nil {
		return err
	}
	return nil
}

func (e *EMRConfig) Serialize() []byte {
	conf, err := json.Marshal(e)
	if err != nil {
		panic(err)
	}
	return conf
}

type S3Config struct {
	AWSAccessKeyId string
	AWSSecretKey   string
	BucketRegion   string
	BucketPath     string
}

func (s *S3Config) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, s)
	if err != nil {
		return err
	}
	return nil
}

func (s *S3Config) Serialize() []byte {
	conf, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return conf
}

type sparkSQLQueries struct {
	defaultOfflineSQLQueries
}

type SparkOfflineStore struct {
	Executor SparkExecutor
	Store    SparkStore
	query    OfflineTableQueries
	BaseProvider
}

func (store *SparkOfflineStore) AsOfflineStore() (OfflineStore, error) {
	return store, nil
}

func SparkOfflineStoreFactory(config SerializedConfig) (Provider, error) {
	sc := SparkConfig{}
	if err := sc.Deserialize(config); err != nil {
		return nil, fmt.Errorf("invalid spark config: %v", config)
	}
	exec, err := NewSparkExecutor(sc.ExecutorType, SerializedConfig(sc.ExecutorConfig))
	if err != nil {
		return nil, err
	}
	store, err := NewSparkStore(sc.StoreType, SerializedConfig(sc.StoreConfig))
	if err != nil {
		return nil, err
	}
	if err := store.UploadSparkScript(); err != nil {
		return nil, err
	}
	queries := sparkSQLQueries{}
	sparkOfflineStore := SparkOfflineStore{
		Executor: exec,
		Store:    store,
		query:    &queries,
		BaseProvider: BaseProvider{
			ProviderType:   "SPARK_OFFLINE",
			ProviderConfig: config,
		},
	}
	return &sparkOfflineStore, nil
}

type SparkExecutor interface {
	RunSparkJob(args []string) error
}

type SparkStore interface {
	UploadSparkScript() error //initialization function
	ResourceKey(id ResourceID) (string, error)
	ResourceStream(id ResourceID) (chan []byte, error)
	ResourceColumns(id ResourceID) ([]string, error)
	ResourceRowCt(id ResourceID) (int, error)
	ResourcePath(id ResourceID) string
	SparkSubmitArgs(destPath string, cleanQuery string, sourceList []string) []string
	UploadParquetTable(path string, data interface{}) error
	CompareParquetTable(path string, data interface{}) error
	FileExists(path string) (bool, error)
	DeleteFile(path string) error
}

type EMRExecutor struct {
	client      *emr.Client
	clusterName string
}

type S3Store struct {
	client      *s3.Client
	uploader    *s3manager.Uploader
	credentials *credentialsV1.Credentials
	region      string
	bucketPath  string
}

func (s *S3Store) UploadSparkScript() error {
	var sparkScriptPath string
	sparkScriptPath, ok := os.LookupEnv("SPARK_SCRIPT_PATH")
	if !ok {
		sparkScriptPath = "./provider/scripts/offline_store_spark_runner.py"
	}
	scriptFile, err := os.Open(sparkScriptPath)
	if err != nil {
		return err
	}
	_, err = s.client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(s.bucketPath),
		Key:    aws.String("featureform/scripts/offline_store_spark_runner.py"),
		Body:   scriptFile,
	})
	if err != nil {
		return err
	}
	return nil
}

func NewSparkExecutor(execType SparkExecutorType, config SerializedConfig) (SparkExecutor, error) {
	if execType == EMR {
		emrConf := EMRConfig{}
		if err := emrConf.Deserialize(config); err != nil {
			return nil, fmt.Errorf("invalid emr config: %v", config)
		}
		client := emr.New(emr.Options{
			Region:      emrConf.ClusterRegion,
			Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(emrConf.AWSAccessKeyId, emrConf.AWSSecretKey, "")),
		})

		emrExecutor := EMRExecutor{
			client:      client,
			clusterName: emrConf.ClusterName,
		}
		return &emrExecutor, nil
	}
	return nil, nil
}

func NewSparkStore(storeType SparkStoreType, config SerializedConfig) (SparkStore, error) {
	if storeType == S3 {
		s3Conf := S3Config{}
		if err := s3Conf.Deserialize(config); err != nil {
			return nil, fmt.Errorf("invalid s3 config: %v", config)
		}
		client := s3.New(s3.Options{
			Region:      s3Conf.BucketRegion,
			Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(s3Conf.AWSAccessKeyId, s3Conf.AWSSecretKey, "")),
		})
		sess := session.Must(session.NewSession())
		uploader := s3manager.NewUploader(sess)
		s3Store := S3Store{
			client:      client,
			uploader:    uploader,
			credentials: credentialsV1.NewStaticCredentials(s3Conf.AWSAccessKeyId, s3Conf.AWSSecretKey, ""),
			region:      s3Conf.BucketRegion,
			bucketPath:  s3Conf.BucketPath,
		}
		return &s3Store, nil
	}
	return nil, nil
}

func (s *S3Store) resourcePrefix(id ResourceID) string {
	return fmt.Sprintf("featureform/%s/%s/%s/", id.Type, id.Name, id.Variant)
}

func (s *S3Store) ResourceKey(id ResourceID) (string, error) {
	return "", nil
}

func (s *S3Store) ResourceStream(id ResourceID) (chan []byte, error) {
	return nil, nil
}

func (s *S3Store) ResourceColumns(id ResourceID) ([]string, error) {
	return nil, nil
}

func (s *S3Store) ResourceRowCt(id ResourceID) (int, error) {
	return 0, nil
}

func (e *EMRExecutor) RunSparkJob(args []string) error {
	return nil
}

func (spark *SparkOfflineStore) RegisterResourceFromSourceTable(id ResourceID, schema ResourceSchema) (OfflineTable, error) {
	return nil, nil
}

func (s *S3Store) ResourcePath(id ResourceID) string {
	return fmt.Sprintf("s3://%s/%s", s.bucketPath, s.resourcePrefix(id))
}

func stringifyValue(value interface{}) string {
	switch reflect.TypeOf(value).String() {
	case "string":
		return fmt.Sprintf(`"%s"`, value.(string))
	case "int":
		return fmt.Sprintf(`%d`, value.(int))
	case "float":
		return strconv.FormatFloat(value.(float64), 'f', -1, 64)
	case "float64":
		return strconv.FormatFloat(value.(float64), 'f', -1, 64)
	case "float32":
		return strconv.FormatFloat(float64(value.(float32)), 'f', -1, 32)
	case "int32":
		return fmt.Sprintf(`%d`, value.(int32))
	case "bool":
		return fmt.Sprintf(`%t`, value.(bool))
	case "time.Time":
		//convert to int64
		return fmt.Sprintf(`%d`, value.(time.Time).UnixMilli())
	}
	return ""
}

func stringifyStruct(data interface{}) string {
	curStruct := reflect.ValueOf(data)
	structType := curStruct.Type()
	structString := `
	{`
	for j := 0; j < curStruct.NumField(); j++ {
		structString += fmt.Sprintf(`"%s":`, strings.ToLower(structType.Field(j).Name))
		value := curStruct.Field(j).Interface()
		structString += stringifyValue(value)
		if j != curStruct.NumField()-1 {
			structString += ",\n"
		} else {
			structString += "\n}"
		}
	}
	return structString
}

func stringifyStructArray(data interface{}) ([]string, error) {
	array := reflect.ValueOf(data)
	structStringArray := make([]string, array.Len())
	for i := 0; i < array.Len(); i++ {
		structStringArray[i] = stringifyStruct(array.Index(i).Interface())
	}
	return structStringArray, nil
}

func stringifyStructField(data interface{}, idx int) string {
	schemaStruct := reflect.ValueOf(data)
	typeOfS := schemaStruct.Type()
	fieldDataType := reflect.TypeOf(schemaStruct.Field(idx).Interface()).String()
	jsonFriendlyFieldName := strings.ToLower(typeOfS.Field(idx).Name)
	switch fieldDataType {
	case "string":
		return fmt.Sprintf(`{"Tag": "name=%s, type=BYTE_ARRAY, convertedtype=UTF8"}`, jsonFriendlyFieldName)
	case "int":
		return fmt.Sprintf(`{"Tag": "name=%s, type=INT32"}`, jsonFriendlyFieldName)
	case "float":
		return fmt.Sprintf(`{"Tag": "name=%s, type=FLOAT"}`, jsonFriendlyFieldName)
	case "float32":
		return fmt.Sprintf(`{"Tag": "name=%s, type=FLOAT"}`, jsonFriendlyFieldName)
	case "int32":
		return fmt.Sprintf(`{"Tag": "name=%s, type=INT32"}`, jsonFriendlyFieldName)
	case "bool":
		return fmt.Sprintf(`{"Tag": "name=%s, type=BOOLEAN"}`, jsonFriendlyFieldName)
	case "time.Time":
		return fmt.Sprintf(`{"Tag": "name=%s, type=INT64"}`, jsonFriendlyFieldName)
	}
	return ""
}

func parquetSchemaHeader() string {
	return `
    {
        "Tag":"name=parquet-go-root",
        "Fields":[
		    `
}

func generateSchemaFromInterface(data interface{}) (string, error) {
	array := reflect.ValueOf(data)
	if array.Len() < 1 {
		return "", fmt.Errorf("interface passed has no data")
	}
	schemaStruct := array.Index(0)
	schemaString := parquetSchemaHeader()
	for i := 0; i < schemaStruct.NumField(); i++ {
		schemaString += stringifyStructField(schemaStruct.Interface(), i)
		if i != schemaStruct.NumField()-1 {
			schemaString += `,
					`
		} else {
			schemaString += `
				]
			}`
		}
	}
	return schemaString, nil
}

func (s *S3Store) parquetJSONWriter(path string, schema string) (*writer.JSONWriter, source.ParquetFile, error) {
	file, err := parquetGo.NewS3FileWriter(context.TODO(), s.bucketPath, path, "bucket-owner-full-control", nil, &awsV1.Config{
		Credentials: s.credentials,
		Region:      awsV1.String(s.region),
	})
	if err != nil {
		return nil, nil, err
	}
	pw, err := writer.NewJSONWriter(schema, file, 4)
	if err != nil {
		return nil, nil, err
	}
	return pw, file, nil
}

func writeStringArrayToParquet(pw *writer.JSONWriter, dataString []string) error {
	for i := 0; i < len(dataString); i++ {
		if err := pw.Write(dataString[i]); err != nil {
			return err
		}
	}
	if err := pw.WriteStop(); err != nil {
		return err
	}
	return nil
}

func (s *S3Store) UploadParquetTable(path string, data interface{}) error {
	schemaString, err := generateSchemaFromInterface(data)
	if err != nil {
		return err
	}
	dataString, err := stringifyStructArray(data)
	if err != nil {
		return err
	}
	pw, file, err := s.parquetJSONWriter(path, schemaString)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := writeStringArrayToParquet(pw, dataString); err != nil {
		return err
	}
	return nil
}

func (s *S3Store) DeleteFile(path string) error {
	_, err := s.client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{Bucket: aws.String(s.bucketPath), Key: aws.String(path)})
	if err != nil {
		return err
	}
	return nil
}

func (s *S3Store) FileExists(path string) (bool, error) {
	output, err := s.client.GetObjectAttributes(context.TODO(), &s3.GetObjectAttributesInput{Bucket: aws.String(s.bucketPath), Key: aws.String(path), ObjectAttributes: []s3Types.ObjectAttributes{s3Types.ObjectAttributesChecksum}})
	if output == nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return false, nil
}

func (s *S3Store) S3ParquetReader(path string) (source.ParquetFile, error) {
	fr, err := parquetGo.NewS3FileReader(ctx, s.bucketPath, path, &awsV1.Config{
		Credentials: s.credentials,
		Region:      awsV1.String(s.region),
	})
	if err != nil {
		return nil, err
	}
	return fr, nil
}

func typeConvertValue(value interface{}) interface{} {
	switch reflect.TypeOf(value).String() {
	case "time.Time":
		return value.(time.Time).UnixMilli()
	case "int":
		return int32(value.(int))
	default:
		return value
	}
}

func compareStructs(local interface{}, fetched interface{}) error {
	localStruct := reflect.ValueOf(local)
	fetchedStruct := reflect.ValueOf(fetched)
	for i := 0; i < localStruct.NumField(); i++ {
		localValue := typeConvertValue(localStruct.Field(i).Interface())
		fetchedValue := typeConvertValue(fetchedStruct.Field(i).Interface())
		if !reflect.DeepEqual(fetchedValue, localValue) {
			return fmt.Errorf("%v does not equal %v", fetchedValue, localValue)
		}
	}
	return nil
}

func (s *S3Store) CompareParquetTable(path string, data interface{}) error {
	fr, err := s.S3ParquetReader(path)
	if err != nil {
		return err
	}
	defer fr.Close()
	pr, err := reader.NewParquetReader(fr, nil, 4)
	if err != nil {
		return err
	}
	fetchedArray, err := pr.ReadByNumber(int(pr.GetNumRows()))
	if err != nil {
		return err
	}
	compareArray := reflect.ValueOf(data)
	if len(fetchedArray) != compareArray.Len() {
		return fmt.Errorf("data do not have the same number of rows")
	}
	for i := 0; i < compareArray.Len(); i++ {
		localStruct := compareArray.Index(i).Interface()
		fetchedStruct := reflect.ValueOf(fetchedArray[i]).Interface()
		if err := compareStructs(localStruct, fetchedStruct); err != nil {
			return err
		}
	}
	pr.ReadStop()
	return nil
}

func (s *S3Store) SparkSubmitArgs(destPath string, cleanQuery string, sourceList []string) []string {
	argList := []string{
		"spark-submit",
		"--deploy-mode",
		"cluster",
		fmt.Sprintf("s3://%s/featureform/scripts/offline_store_spark_runner.py", s.bucketPath),
		"--output_uri",
		destPath,
		"--sql_query",
		cleanQuery,
		"--source_list",
	}
	argList = append(argList, sourceList...)
	return argList
}

func (spark *SparkOfflineStore) RegisterPrimaryFromSourceTable(id ResourceID, sourceName string) (PrimaryTable, error) {
	return nil, nil
}

func (spark *SparkOfflineStore) CreateTransformation(config TransformationConfig) error {
	return nil
}

func (spark *SparkOfflineStore) GetTransformationTable(id ResourceID) (TransformationTable, error) {
	return nil, nil
}

func (spark *SparkOfflineStore) UpdateTransformation(config TransformationConfig) error {
	return nil
}

func (spark *SparkOfflineStore) CreatePrimaryTable(id ResourceID, schema TableSchema) (PrimaryTable, error) {
	return nil, nil
}

func (spark *SparkOfflineStore) GetPrimaryTable(id ResourceID) (PrimaryTable, error) {
	return nil, nil
}

func (spark *SparkOfflineStore) CreateResourceTable(id ResourceID, schema TableSchema) (OfflineTable, error) {
	return nil, nil
}

func (spark *SparkOfflineStore) GetResourceTable(id ResourceID) (OfflineTable, error) {
	return nil, nil
}

func (spark *SparkOfflineStore) CreateMaterialization(id ResourceID) (Materialization, error) {
	return nil, nil
}

func (spark *SparkOfflineStore) GetMaterialization(id MaterializationID) (Materialization, error) {
	return nil, nil
}

func (spark *SparkOfflineStore) UpdateMaterialization(id ResourceID) (Materialization, error) {
	return nil, nil
}

func (spark *SparkOfflineStore) DeleteMaterialization(id MaterializationID) error {
	return nil
}

func (spark *SparkOfflineStore) CreateTrainingSet(TrainingSetDef) error {
	return nil
}

func (spark *SparkOfflineStore) UpdateTrainingSet(TrainingSetDef) error {
	return nil
}

func (spark *SparkOfflineStore) GetTrainingSet(id ResourceID) (TrainingSetIterator, error) {
	return nil, nil
}
