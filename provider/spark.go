package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
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

	emrTypes "github.com/aws/aws-sdk-go-v2/service/emr/types"
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

type JobType string

const (
	Materialize       JobType = "Materialization"
	Transform                 = "Transformation"
	CreateTrainingSet         = "Training Set"
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

type SparkOfflineQueries interface {
	materializationCreate(schema ResourceSchema) string
}

type defaultSparkOfflineQueries struct{}

func (q defaultSparkOfflineQueries) materializationCreate(schema ResourceSchema) string {
	timestampColumn := schema.TS
	if schema.TS == "" {
		timestampColumn = "ts"
	}
	return fmt.Sprintf(
		"SELECT entity, value, ts, ROW_NUMBER() over (ORDER BY (SELECT NULL)) AS row_number FROM "+
			"(SELECT entity, value, ts, rn FROM (SELECT %s AS entity, %s AS value, %s AS ts, "+
			"ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s DESC) AS rn FROM %s) t WHERE rn=1) t2",
		schema.Entity, schema.Value, timestampColumn, schema.Entity, timestampColumn, "source_0")
}

func featureColumnName(id ResourceID) string {
	return fmt.Sprintf("%s__%s__%s", id.Type, id.Name, id.Variant)
}

func (q defaultSparkOfflineQueries) trainingSetCreate(def TrainingSetDef, featureSchemas []ResourceSchema, labelSchema ResourceSchema) string {
	columns := make([]string, 0)
	joinQueries := make([]string, 0)
	for i, feature := range def.Features {
		fmt.Println(feature)
		featureColumnName := featureColumnName(feature)
		columns = append(columns, featureColumnName)
		featureWindowQuery := fmt.Sprintf("SELECT %s as entity, %s as %s, %s as ts FROM source_%d", featureSchemas[i].Entity, featureSchemas[i].Value, featureColumnName, featureSchemas[i].TS, i+1)
		featureJoinQuery := fmt.Sprintf("LEFT OUTER JOIN (%s) AS t%d ON (t%d.entity = labelTable.entity AND t%d.ts <= labelTable.ts)", featureWindowQuery, i+1, i+1, i+1)
		joinQueries = append(joinQueries, featureJoinQuery)
	}
	columnStr := strings.Join(columns, ", ")
	joinQueryString := strings.Join(joinQueries, " ")
	labelWindowQuery := fmt.Sprintf("SELECT %s as entity, %s as value, %s as ts FROM source_0", labelSchema.Entity, labelSchema.Value, labelSchema.TS)
	labelPartitionQuery := fmt.Sprintf("SELECT * FROM (SELECT *, ROW_NUMBER() over(PARTITION BY labelTable.entity, labelTable.value, labelTable.ts ORDER BY ts desc) as rn FROM (%s) AS labelTable) WHERE rn = 1) AS labelTable", labelWindowQuery)
	labelJoinQuery := fmt.Sprintf("%s %s", labelPartitionQuery, joinQueryString)
	fullQuery := fmt.Sprintf("SELECT %s, labelTable.value AS %s FROM (%s", columnStr, featureColumnName(def.Label), labelJoinQuery)
	return fullQuery
}

type SparkOfflineStore struct {
	Executor SparkExecutor
	Store    SparkStore
	query    *defaultSparkOfflineQueries
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
	queries := defaultSparkOfflineQueries{}
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
	ResourceStream(key string) (chan []byte, error)
	RowStreamFromSelectQuery(key string, query string) (chan []byte, error)
	ResourceColumns(key string) ([]string, error)
	ResourceRowCt(key string) (int, error)
	ResourcePath(id ResourceID) string
	BucketPrefix() string
	SparkSubmitArgs(destPath string, cleanQuery string, sourceList []string, jobType JobType) []string
	UploadParquetTable(path string, data interface{}) error
	DownloadParquetTable(path string) (interface{}, error)
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

func (s *S3Store) BucketPrefix() string {
	return fmt.Sprintf("s3://%s/", s.bucketPath)
}

func (s *S3Store) UploadSparkScript() error {
	var sparkScriptPath string
	sparkScriptPath, ok := os.LookupEnv("SPARK_SCRIPT_PATH")
	if !ok {
		sparkScriptPath = "./scripts/offline_store_spark_runner.py"
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

func ResourcePrefix(id ResourceID) string {
	return fmt.Sprintf("featureform/%s/%s/%s/", id.Type, id.Name, id.Variant)
}

func (s *S3Store) ResourceKey(id ResourceID) (string, error) {
	filePrefix := ResourcePrefix(id)
	objects, err := s.client.ListObjects(context.TODO(), &s3.ListObjectsInput{
		Bucket: aws.String(s.bucketPath),
		Prefix: aws.String(filePrefix),
	})
	if err != nil {
		return "", err
	}
	var resourceTimestamps = make(map[string]string)
	if len(objects.Contents) == 0 {
		return "", fmt.Errorf("no resource exists")
	}
	for _, object := range objects.Contents {
		suffix := (*object.Key)[len(filePrefix):]
		suffixParts := strings.Split(suffix, "/")
		if len(suffixParts) > 1 {
			resourceTimestamps[suffixParts[0]] = suffixParts[1]
		}
	}
	keys := make([]string, len(resourceTimestamps))
	for k := range resourceTimestamps {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	latestTimestamp := keys[len(keys)-1]
	lastSuffix := resourceTimestamps[latestTimestamp]
	return fmt.Sprintf("featureform/%s/%s/%s/%s/%s", id.Type, id.Name, id.Variant, latestTimestamp, lastSuffix), nil
}

func (e *EMRExecutor) RunSparkJob(args []string) error {
	params := &emr.AddJobFlowStepsInput{
		JobFlowId: aws.String(e.clusterName), //returned by listclusters
		Steps: []emrTypes.StepConfig{
			{
				Name: aws.String("Featureform execution step"),
				HadoopJarStep: &emrTypes.HadoopJarStepConfig{
					Jar:  aws.String("command-runner.jar"), //jar file for running pyspark scripts
					Args: args,
				},
				ActionOnFailure: emrTypes.ActionOnFailureCancelAndWait,
			},
		},
	}
	resp, err := e.client.AddJobFlowSteps(context.TODO(), params)
	if err != nil {
		return err
	}
	stepId := resp.StepIds[0]
	var waitDuration time.Duration = time.Second * 150
	time.Sleep(1 * time.Second)
	stepCompleteWaiter := emr.NewStepCompleteWaiter(e.client)
	_, err = stepCompleteWaiter.WaitForOutput(context.TODO(), &emr.DescribeStepInput{
		ClusterId: aws.String(e.clusterName),
		StepId:    aws.String(stepId),
	}, waitDuration)
	if err != nil {
		return err
	}
	return nil
}

func (s *S3Store) ResourcePath(id ResourceID) string {
	return fmt.Sprintf("s3://%s/%s", s.bucketPath, ResourcePrefix(id))
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

var parquetSchemaHeader = `
    {
        "Tag":"name=parquet-go-root",
        "Fields":[
		    `

func generateSchemaFromInterface(data interface{}) (string, error) {
	array := reflect.ValueOf(data)
	if array.Len() < 1 {
		return "", fmt.Errorf("interface passed has no data")
	}
	schemaStruct := array.Index(0)
	schemaString := parquetSchemaHeader
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

//not working?
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

func (s *S3Store) DownloadParquetTable(path string) (interface{}, error) {
	fr, err := s.S3ParquetReader(path)
	if err != nil {
		return nil, err
	}
	defer fr.Close()
	pr, err := reader.NewParquetReader(fr, nil, 4)
	if err != nil {
		return nil, err
	}
	fetchedArray, err := pr.ReadByNumber(int(pr.GetNumRows()))
	if err != nil {
		return nil, err
	}
	pr.ReadStop()
	return reflect.ValueOf(fetchedArray).Interface(), nil
}

func (s *S3Store) CompareParquetTable(path string, data interface{}) error {
	fetchedArrayInterface, err := s.DownloadParquetTable(path)
	if err != nil {
		return err
	}
	compareArray := reflect.ValueOf(data)
	fetchedArray := reflect.ValueOf(fetchedArrayInterface)
	if fetchedArray.Len() != compareArray.Len() {
		return fmt.Errorf("data do not have the same number of rows")
	}
	for i := 0; i < compareArray.Len(); i++ {
		localStruct := compareArray.Index(i).Interface()
		fetchedStruct := fetchedArray.Index(i).Interface()
		if err := compareStructs(localStruct, fetchedStruct); err != nil {
			return err
		}
	}
	return nil
}

func (s *S3Store) SparkSubmitArgs(destPath string, cleanQuery string, sourceList []string, jobType JobType) []string {
	argList := []string{
		"spark-submit",
		"--deploy-mode",
		"cluster",
		fmt.Sprintf("s3://%s/featureform/scripts/offline_store_spark_runner.py", s.bucketPath),
		"--output_uri",
		destPath,
		"--sql_query",
		cleanQuery,
		"--job_type",
		string(jobType),
		"--source_list",
	}
	argList = append(argList, sourceList...)
	return argList
}

type PrimarySchema struct {
	Source string
}

type S3PrimaryTable struct {
	store      SparkStore
	sourcePath string
}

type S3GenericTableIterator struct {
	store         SparkStore
	sourcePath    string
	rows          int64
	columns       []string
	valuesChannel chan []byte
	currentValue  []byte
	currentIndex  int64
}

func (s *S3GenericTableIterator) Next() bool {
	if s.rows == s.currentIndex {
		return false
	}
	s.currentValue = <-s.valuesChannel
	s.currentIndex += 1
	return true
}

func (s *S3GenericTableIterator) Values() GenericRecord {
	return []interface{}{string(s.currentValue)}
}

func (s *S3GenericTableIterator) Columns() []string {
	return s.columns
}

func (s *S3GenericTableIterator) Err() error {
	return nil
}

func (s *S3PrimaryTable) Write(GenericRecord) error {
	return fmt.Errorf("not implemented")
}

func (s *S3PrimaryTable) GetName() string {
	return s.sourcePath
}

func (s *S3PrimaryTable) IterateSegment(n int64) (GenericTableIterator, error) {
	columns, err := s.store.ResourceColumns(s.sourcePath)
	if err != nil {
		return nil, err
	}
	channel, err := s.store.ResourceStream(s.sourcePath)
	if err != nil {
		return nil, err
	}
	return &S3GenericTableIterator{s.store, s.sourcePath, n, columns, channel, nil, 0}, nil
}

func (s *S3PrimaryTable) NumRows() (int64, error) {
	num, err := s.store.ResourceRowCt(s.sourcePath)
	if err != nil {
		return 0, err
	}
	return int64(num), nil
}

func (spark *SparkOfflineStore) RegisterPrimaryFromSourceTable(id ResourceID, sourceName string) (PrimaryTable, error) {
	resourcePath := parquetResourcePath(id)
	exists, err := spark.Store.FileExists(resourcePath)
	if exists {
		return nil, fmt.Errorf("resource already exists")
	}
	if err != nil {
		return nil, err
	}
	primarySchema := PrimarySchema{sourceName}
	schemaList := []PrimarySchema{primarySchema}
	if err := spark.Store.UploadParquetTable(resourcePath, schemaList); err != nil {
		return nil, err
	}
	return &S3PrimaryTable{spark.Store, sourceName}, nil
}

type S3OfflineTable struct {
	schema ResourceSchema
}

func parquetResourcePath(id ResourceID) string {
	return fmt.Sprintf("%sresource.parquet", ResourcePrefix(id))
}

func (s *S3OfflineTable) Write(ResourceRecord) error {
	return fmt.Errorf("not implemented")
}

func (spark *SparkOfflineStore) RegisterResourceFromSourceTable(id ResourceID, schema ResourceSchema) (OfflineTable, error) {
	resourcePath := parquetResourcePath(id)
	exists, err := spark.Store.FileExists(resourcePath)
	if exists {
		return nil, fmt.Errorf("resource already exists")
	}
	if err != nil {
		return nil, err
	}
	schemaList := []ResourceSchema{schema}
	if err := spark.Store.UploadParquetTable(resourcePath, schemaList); err != nil {
		return nil, err
	}
	return &S3OfflineTable{schema}, nil
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
	path := parquetResourcePath(id)
	table, err := spark.Store.DownloadParquetTable(path)
	if err != nil {
		return nil, err
	}
	tableList := table.([]interface{})
	var sourcePath string
	for _, v := range tableList {
		sourcePath = reflect.ValueOf(v).FieldByName("Source").String()
	}
	return &S3PrimaryTable{spark.Store, sourcePath}, nil
}

func (spark *SparkOfflineStore) CreateResourceTable(id ResourceID, schema TableSchema) (OfflineTable, error) {
	return nil, nil
}

func (spark *SparkOfflineStore) GetResourceTable(id ResourceID) (OfflineTable, error) {
	path := parquetResourcePath(id)
	table, err := spark.Store.DownloadParquetTable(path)
	if err != nil {
		return nil, err
	}
	storedResourceData := reflect.ValueOf(table).Index(0)
	resourceTableStruct, ok := storedResourceData.Interface().(struct {
		Entity      string
		Value       string
		Ts          string
		Sourcetable string
	})
	if !ok {
		return nil, fmt.Errorf("cant convert downloaded resource table")
	}
	return &S3OfflineTable{ResourceSchema{
		Entity:      resourceTableStruct.Entity,
		Value:       resourceTableStruct.Value,
		TS:          resourceTableStruct.Ts,
		SourceTable: resourceTableStruct.Sourcetable,
	}}, nil
}

type S3Materialization struct {
	id    ResourceID
	store SparkStore
	Key   string
}

func (s *S3Materialization) ID() MaterializationID {
	return MaterializationID(fmt.Sprintf("%s/%s/%s", FeatureMaterialization, s.id.Name, s.id.Variant))
}

func (s *S3Materialization) NumRows() (int64, error) {
	numRows, err := s.store.ResourceRowCt(s.Key)
	if err != nil {
		return 0, err
	}
	return int64(numRows), nil
}

func (s *S3Materialization) IterateSegment(begin, end int64) (FeatureIterator, error) {
	stream, err := s.store.RowStreamFromSelectQuery(s.Key, fmt.Sprintf("SELECT * FROM S3Object WHERE row_number > %d AND row_number <= %d", begin, end))
	if err != nil {
		return nil, err
	}
	return &S3FeatureIterator{stream: stream, maxIdx: (end - begin)}, nil
}

type S3FeatureIterator struct {
	stream chan []byte
	cur    ResourceRecord
	err    error
	curIdx int64
	maxIdx int64
}

// expected format is "<entity(string)>,<value(interface{})>,<timestamp(int64)>"
func featureCSVToResource(csv string) (ResourceRecord, error) {
	values := strings.Split(csv, ",")
	entity := string(values[0])
	value := reflect.ValueOf(values[1]).Interface()
	timeStampMilli, err := strconv.Atoi(values[2])
	if err != nil {
		return ResourceRecord{}, err
	}
	timestamp := time.UnixMilli(int64(timeStampMilli))
	return ResourceRecord{entity, value, timestamp}, nil
}

func (s *S3FeatureIterator) Next() bool {
	if s.curIdx == s.maxIdx {
		return false
	}
	val := <-s.stream
	currentRecord, err := featureCSVToResource(string(val))
	if err != nil {
		s.err = err
		return false
	}
	s.cur = currentRecord
	s.curIdx += 1
	return true
}

func (s *S3FeatureIterator) Value() ResourceRecord {
	return s.cur
}

func (s *S3FeatureIterator) Err() error {
	return s.err
}

func (spark *SparkOfflineStore) CreateMaterialization(id ResourceID) (Materialization, error) {
	resourceTable, err := spark.GetResourceTable(id)
	if err != nil {
		return nil, fmt.Errorf("resource not registered: %v", err)
	}
	sparkResourceTable, ok := resourceTable.(*S3OfflineTable)
	if !ok {
		return nil, fmt.Errorf("could not convert offline table with id %v to sparkResourceTable", id)
	}
	materializationID := ResourceID{Name: id.Name, Variant: id.Variant, Type: FeatureMaterialization}
	destinationPath := spark.Store.ResourcePath(materializationID)
	exists, _ := spark.Store.FileExists(destinationPath)
	if exists {
		return nil, fmt.Errorf("materialization %v already exists", materializationID)
	}
	materializationQuery := spark.query.materializationCreate(sparkResourceTable.schema)
	sourcePath := fmt.Sprintf("%s%s", spark.Store.BucketPrefix(), sparkResourceTable.schema.SourceTable)
	sparkArgs := spark.Store.SparkSubmitArgs(destinationPath, materializationQuery, []string{sourcePath}, Materialize)
	if err := spark.Executor.RunSparkJob(sparkArgs); err != nil {
		return nil, fmt.Errorf("spark submit job for materialization %v failed to run: %v", materializationID, err)
	}
	key, err := spark.Store.ResourceKey(materializationID)
	if err != nil {
		return nil, fmt.Errorf("Materialization result does not exist in offline store: %v", err)
	}
	return &S3Materialization{materializationID, spark.Store, key}, nil
}

func (spark *SparkOfflineStore) GetMaterialization(id MaterializationID) (Materialization, error) {
	s := strings.Split(string(id), "/")
	materializationID := ResourceID{s[1], s[2], FeatureMaterialization}
	key, err := spark.Store.ResourceKey(materializationID)
	if err != nil {
		return nil, err
	}
	return &S3Materialization{materializationID, spark.Store, key}, nil
}

func (spark *SparkOfflineStore) UpdateMaterialization(id ResourceID) (Materialization, error) {
	return nil, nil
}

func (spark *SparkOfflineStore) DeleteMaterialization(id MaterializationID) error {
	return nil
}

type S3TrainingSet struct {
	id       ResourceID
	store    SparkStore
	Key      string
	err      error
	label    interface{}
	features []interface{}
	iter     chan []byte
}

func (s *S3TrainingSet) Next() bool {
	if s.iter == nil {
		iterator, err := s.store.ResourceStream(s.Key)
		if err != nil {
			s.err = err
			return false
		}
		s.iter = iterator
	}
	val := <-s.iter
	values := strings.Split(string(val), ",")
	features := make([]interface{}, len(values)-1)
	for i := 0; i < len(values)-1; i++ {
		features[i] = reflect.ValueOf(values[i]).Interface()
	}
	label := reflect.ValueOf(values[len(values)-1]).Interface()
	s.features = features
	s.label = label
	return true
}

func (s *S3TrainingSet) Features() []interface{} {
	return s.features
}

func (s *S3TrainingSet) Label() interface{} {
	return s.label
}

func (s *S3TrainingSet) Err() error {
	return s.err
}

func (spark *SparkOfflineStore) CreateTrainingSet(def TrainingSetDef) error {
	sourcePaths := make([]string, 0)
	featureSchemas := make([]ResourceSchema, 0)
	destinationPath := spark.Store.ResourcePath(def.ID)
	exists, _ := spark.Store.FileExists(destinationPath)
	if exists {
		return fmt.Errorf("training set already exists %v already exists", def.ID)
	}
	labelTable, err := spark.GetResourceTable(def.Label)
	if err != nil {
		return fmt.Errorf("resource not registered: %v", err)
	}
	sparkLabelResourceTable, ok := labelTable.(*S3OfflineTable)
	if !ok {
		return fmt.Errorf("could not convert offline table with id %v to sparkResourceTable", def.ID)
	}
	labelPath := fmt.Sprintf("%s%s", spark.Store.BucketPrefix(), sparkLabelResourceTable.schema.SourceTable)
	labelSchema := sparkLabelResourceTable.schema
	sourcePaths = append(sourcePaths, labelPath)
	for _, feature := range def.Features {
		featureTable, err := spark.GetResourceTable(feature)
		if err != nil {
			return fmt.Errorf("resource not registered: %v", err)
		}
		sparkFeatureResourceTable, ok := featureTable.(*S3OfflineTable)
		if !ok {
			return fmt.Errorf("could not convert offline table with id %v to sparkResourceTable", def.ID)
		}
		featurePath := fmt.Sprintf("%s%s", spark.Store.BucketPrefix(), sparkFeatureResourceTable.schema.SourceTable)
		sourcePaths = append(sourcePaths, featurePath)
		featureSchemas = append(featureSchemas, sparkFeatureResourceTable.schema)
	}
	trainingSetQuery := spark.query.trainingSetCreate(def, featureSchemas, labelSchema)
	sparkArgs := spark.Store.SparkSubmitArgs(destinationPath, trainingSetQuery, sourcePaths, CreateTrainingSet)
	if err := spark.Executor.RunSparkJob(sparkArgs); err != nil {
		return fmt.Errorf("spark submit job for training set %v failed to run: %v", def.ID, err)
	}
	_, err = spark.Store.ResourceKey(def.ID)
	if err != nil {
		return fmt.Errorf("Training Set result does not exist in offline store: %v", err)
	}
	return nil
}

func (spark *SparkOfflineStore) UpdateTrainingSet(TrainingSetDef) error {
	return nil
}

func (spark *SparkOfflineStore) GetTrainingSet(id ResourceID) (TrainingSetIterator, error) {
	key, err := spark.Store.ResourceKey(id)
	if err != nil {
		return nil, fmt.Errorf("Training Set result does not exist in offline store: %v", err)
	}
	return &S3TrainingSet{id: id, store: spark.Store, Key: key}, nil
}

func (s *S3Store) selectFromKey(key string, query string, returnType SelectReturnType) (*s3.SelectObjectContentEventStreamReader, error) {
	var outputSerialization s3Types.OutputSerialization
	if returnType == CSV {
		outputSerialization = s3Types.OutputSerialization{
			CSV: &s3Types.CSVOutput{},
		}
	} else if returnType == JSON {
		outputSerialization = s3Types.OutputSerialization{
			JSON: &s3Types.JSONOutput{},
		}
	}
	selectOutput, err := s.client.SelectObjectContent(context.TODO(), &s3.SelectObjectContentInput{
		Bucket:         aws.String(s.bucketPath),
		ExpressionType: "SQL",
		InputSerialization: &s3Types.InputSerialization{
			Parquet: &s3Types.ParquetInput{},
		},
		OutputSerialization: &outputSerialization,
		Expression:          &query,
		Key:                 aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	outputStream := selectOutput.GetStream().Reader
	return &outputStream, nil

}

func (s *S3Store) ResourceRowCt(key string) (int, error) {
	queryString := "SELECT COUNT(*) FROM S3Object"
	outputStream, err := s.selectFromKey(key, queryString, CSV)
	if err != nil {
		return 0, err
	}
	return streamResolveIntegerValue(outputStream)
}

func streamResolveIntegerValue(outputStream *s3.SelectObjectContentEventStreamReader) (int, error) {
	outputEvents := (*outputStream).Events()
	for i := range outputEvents {
		switch v := i.(type) {
		case *s3Types.SelectObjectContentEventStreamMemberRecords:
			return streamRecordReadInteger(v)
		}
	}
	return 0, nil
}

func streamRecordReadInteger(record *s3Types.SelectObjectContentEventStreamMemberRecords) (int, error) {
	intVar, err := strconv.Atoi(strings.TrimSuffix(string(record.Value.Payload), "\n"))
	if err != nil {
		return 0, err
	}
	return intVar, nil
}

func (s *S3Store) ResourceColumns(key string) ([]string, error) {
	queryString := "SELECT s.* from S3Object s limit 1"
	outputStream, err := s.selectFromKey(key, queryString, JSON)
	if err != nil {
		return nil, err
	}
	return streamResolveStringList(outputStream)

}

func streamResolveStringList(outputStream *s3.SelectObjectContentEventStreamReader) ([]string, error) {
	outputEvents := (*outputStream).Events()
	for i := range outputEvents {
		switch v := i.(type) {
		case *s3Types.SelectObjectContentEventStreamMemberRecords:
			return streamGetKeys(v)
		}
	}
	return nil, nil
}

func streamGetKeys(record *s3Types.SelectObjectContentEventStreamMemberRecords) ([]string, error) {
	var m map[string]interface{}
	if err := json.Unmarshal(record.Value.Payload, &m); err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys, nil
}

func (s *S3Store) ResourceStream(key string) (chan []byte, error) {
	queryString := "SELECT * from S3Object"
	outputStream, err := s.selectFromKey(key, queryString, CSV)
	if err != nil {
		return nil, err
	}
	out := make(chan []byte)
	go resolveByteChannel(outputStream, out)
	return out, nil
}

func (s *S3Store) RowStreamFromSelectQuery(key string, query string) (chan []byte, error) {
	outputStream, err := s.selectFromKey(key, query, CSV)
	if err != nil {
		return nil, err
	}
	out := make(chan []byte)
	go resolveByteChannel(outputStream, out)
	return out, nil
}

func resolveByteChannel(outputStream *s3.SelectObjectContentEventStreamReader, out chan []byte) {
	outputEvents := (*outputStream).Events()
	defer close(out)
	for i := range outputEvents {
		switch v := i.(type) {
		case *s3Types.SelectObjectContentEventStreamMemberRecords:
			splitRecordLinesOverStream(v, out)
		}
	}
}

func splitRecordLinesOverStream(record *s3Types.SelectObjectContentEventStreamMemberRecords, out chan []byte) {
	lines := strings.Split(string(record.Value.Payload), "\n")
	for _, line := range lines {
		out <- []byte(line)
	}
}
