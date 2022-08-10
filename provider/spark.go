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

	// "github.com/apache/arrow/go/arrow"
	// "github.com/apache/arrow/go/arrow/array"
	// "github.com/apache/arrow/go/arrow/memory"

	emrTypes "github.com/aws/aws-sdk-go-v2/service/emr/types"
	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	parquetGo "github.com/xitongsys/parquet-go-source/s3"
	// local "github.com/xitongsys/parquet-go-source/local"
	// parquet "github.com/xitongsys/parquet-go/parquet"
	reader "github.com/xitongsys/parquet-go/reader"
	writer "github.com/xitongsys/parquet-go/writer"
	// arrow "github.com/xitongsys/parquet-go/arrow"
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
	UploadTestTable(path string, data interface{}) error
	CompareTestTable(path string, data interface{}) error
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

func (s *S3Store) resourcePrefix(id ResourceID) string {
	return fmt.Sprintf("featureform/%s/%s/%s/", id.Type, id.Name, id.Variant)
}

func (s *S3Store) ResourceKey(id ResourceID) (string, error) {
	filePrefix := s.resourcePrefix(id)
	fmt.Println(filePrefix)
	objects, err := s.client.ListObjects(context.TODO(), &s3.ListObjectsInput{
		Bucket: aws.String(s.bucketPath),
		Prefix: aws.String(filePrefix),
	})
	if err != nil {
		return "", err
	}
	var resourceTimestamps = make(map[string]string)
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

func (s *S3Store) ResourceStream(id ResourceID) (chan []byte, error) {
	resourceKey, err := s.ResourceKey(id)
	if err != nil {
		return nil, err
	}
	queryString := "SELECT * from S3Object"
	outputStream, err := s.selectFromKey(resourceKey, queryString, "JSON")
	if err != nil {
		return nil, err
	}
	outputEvents := (*outputStream).Events()
	out := make(chan []byte)
	go func() {
		defer close(out)
		for i := range outputEvents {
			switch v := i.(type) {
			case *s3Types.SelectObjectContentEventStreamMemberRecords:
				lines := strings.Split(string(v.Value.Payload), "\n")
				for _, line := range lines {
					out <- []byte(line)
				}
			}
		}
	}()
	return out, nil
}

func (s *S3Store) ResourceColumns(id ResourceID) ([]string, error) {
	resourceKey, err := s.ResourceKey(id)
	if err != nil {
		return nil, err
	}
	queryString := "SELECT s.* from S3Object s limit 1"
	outputStream, err := s.selectFromKey(resourceKey, queryString, "JSON")
	if err != nil {
		return nil, err
	}
	outputEvents := (*outputStream).Events()
	for i := range outputEvents {
		switch v := i.(type) {
		case *s3Types.SelectObjectContentEventStreamMemberRecords:
			var m map[string]interface{}
			if err := json.Unmarshal(v.Value.Payload, &m); err != nil {
				return nil, err
			}
			keys := make([]string, 0, len(m))
			for k := range m {
				keys = append(keys, k)
			}
			return keys, nil
		}
	}
	return nil, nil
}

func (s *S3Store) ResourceRowCt(id ResourceID) (int, error) {
	resourceKey, err := s.ResourceKey(id)
	if err != nil {
		return 0, err
	}
	queryString := "SELECT COUNT(*) FROM S3Object"
	outputStream, err := s.selectFromKey(resourceKey, queryString, "CSV")
	if err != nil {
		return 0, err
	}
	outputEvents := (*outputStream).Events()
	for i := range outputEvents {
		switch v := i.(type) {
		case *s3Types.SelectObjectContentEventStreamMemberRecords:
			intVar, err := strconv.Atoi(strings.TrimSuffix(string(v.Value.Payload), "\n"))
			if err != nil {
				return 0, err
			}
			return intVar, nil
		}

	}
	return 0, nil
}

func (s *S3Store) selectFromKey(key string, query string, returnType SelectReturnType) (*s3.SelectObjectContentEventStreamReader, error) {
	fmt.Println(key)
	var outputSerialization s3Types.OutputSerialization
	if returnType == "CSV" {
		outputSerialization = s3Types.OutputSerialization{
			CSV: &s3Types.CSVOutput{},
		}
	} else if returnType == "JSON" {
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
	output, err := stepCompleteWaiter.WaitForOutput(context.TODO(), &emr.DescribeStepInput{
		ClusterId: aws.String(e.clusterName),
		StepId:    aws.String(stepId),
	}, waitDuration)
	if err != nil {
		return err
	}
	fmt.Println(output)
	return nil

}

//create a very simple temp table in the path that references the name sof the columns
func (spark *SparkOfflineStore) RegisterResourceFromSourceTable(id ResourceID, schema ResourceSchema) (OfflineTable, error) {

	return nil, nil
}

func (s *S3Store) ResourcePath(id ResourceID) string {
	return fmt.Sprintf("s3://%s/%s", s.bucketPath, s.resourcePrefix(id))
}

func (s *S3Store) cleanSparkSQLQuery(query string, sourceNameList []string) string {
	return ""
}

func (s *S3Store) generateJSONFromInterface(data interface{}) ([]string, error) {
	arr := reflect.ValueOf(data)
	dataList := make([]string, arr.Len())
	for i := 0; i < arr.Len(); i++ {
		dataString := `
		{`
		schemaStruct := arr.Index(i)
		typeOfS := schemaStruct.Type()
		for j := 0; j < schemaStruct.NumField(); j++ {
			dataString += fmt.Sprintf(`"%s":`, strings.ToLower(typeOfS.Field(j).Name))
			value := schemaStruct.Field(j).Interface()
			switch reflect.TypeOf(value).String() {
			case "string":
				dataString += fmt.Sprintf(`"%s"`, value.(string))
			case "int":
				dataString += fmt.Sprintf(`%d`, value.(int))
			case "float":
				dataString += fmt.Sprintf(`%f`, value.(float64))
			case "float32":
				dataString += fmt.Sprintf(`%f`, value.(float32))
			case "int32":
				dataString += fmt.Sprintf(`%d`, value.(int32))
			case "bool":
				dataString += fmt.Sprintf(`%t`, value.(bool))
			case "time.Time":
				//convert to int64
				dataString += fmt.Sprintf(`%d`, value.(time.Time).UnixMilli())
			}
			if j != schemaStruct.NumField()-1 {
				dataString += ",\n"
			} else {
				dataString += "\n}"
			}
		}
		dataList[i] = dataString
	}
	return dataList, nil
}

func (s *S3Store) generateSchemaFromInterface(data interface{}) (string, error) {
	arr := reflect.ValueOf(data)
	if arr.Len() < 1 {
		return "", fmt.Errorf("interface passed has no data")
	}
	schemaStruct := arr.Index(0)
	typeOfS := schemaStruct.Type()
	schemaString := `
    {
        "Tag":"name=parquet-go-root",
        "Fields":[
		    `
	for i := 0; i < schemaStruct.NumField(); i++ {
		fieldDataType := reflect.TypeOf(schemaStruct.Field(i).Interface()).String()
		jsonFriendlyFieldName := strings.ToLower(typeOfS.Field(i).Name)
		switch fieldDataType {
		case "string":
			schemaString = schemaString + fmt.Sprintf(`{"Tag": "name=%s, type=BYTE_ARRAY, convertedtype=UTF8"}`, jsonFriendlyFieldName)
		case "int":
			schemaString = schemaString + fmt.Sprintf(`{"Tag": "name=%s, type=INT32"}`, jsonFriendlyFieldName)
		case "float":
			schemaString = schemaString + fmt.Sprintf(`{"Tag": "name=%s, type=FLOAT"}`, jsonFriendlyFieldName)
		case "float32":
			schemaString = schemaString + fmt.Sprintf(`{"Tag": "name=%s, type=FLOAT"}`, jsonFriendlyFieldName)
		case "int32":
			schemaString = schemaString + fmt.Sprintf(`{"Tag": "name=%s, type=INT32"}`, jsonFriendlyFieldName)
		case "bool":
			schemaString = schemaString + fmt.Sprintf(`{"Tag": "name=%s, type=BOOLEAN"}`, jsonFriendlyFieldName)
		case "time.Time":
			schemaString = schemaString + fmt.Sprintf(`{"Tag": "name=%s, type=INT64"}`, jsonFriendlyFieldName)
		}
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

// //Uploads data to path in s3 bucket
func (s *S3Store) UploadTestTable(path string, data interface{}) error {
	ctx := context.Background()
	schemaString, err := s.generateSchemaFromInterface(data)
	if err != nil {
		return err
	}
	fmt.Println(schemaString)
	dataString, err := s.generateJSONFromInterface(data)
	if err != nil {
		return err
	}
	fmt.Println(dataString)
	file, err := parquetGo.NewS3FileWriter(ctx, s.bucketPath, path, "bucket-owner-full-control", nil, &awsV1.Config{
		Credentials: s.credentials,
		Region:      awsV1.String(s.region),
	})
	if err != nil {
		return err
	}
	pw, err := writer.NewJSONWriter(schemaString, file, 4)
	if err != nil {
		return err
	}
	for i := 0; i < len(dataString); i++ {
		if err = pw.Write(dataString[i]); err != nil {
			return err
		}
	}
	if err = pw.WriteStop(); err != nil {
		return err
	}
	file.Close()
	return nil
}

func (s *S3Store) CompareTestTable(path string, data interface{}) error {
	fr, err := parquetGo.NewS3FileReader(ctx, s.bucketPath, path, &awsV1.Config{
		Credentials: s.credentials,
		Region:      awsV1.String(s.region),
	})
	if err != nil {
		return err
	}
	pr, err := reader.NewParquetReader(fr, nil, 4)
	if err != nil {
		return err
	}
	num := int(pr.GetNumRows())
	res, err := pr.ReadByNumber(num)
	if err != nil {
		return err
	}
	compareArray := reflect.ValueOf(data)
	if len(res) != compareArray.Len() {
		return fmt.Errorf("data do not have the same number of rows")
	}
	for i := 0; i < compareArray.Len(); i++ {
		for j := 0; j < compareArray.Index(i).NumField(); j++ {
			var localValue interface{}
			localValue = compareArray.Index(i).Field(j).Interface()
			if reflect.TypeOf(localValue).String() == "time.Time" {
				localValue = localValue.(time.Time).UnixMilli()
			} else if reflect.TypeOf(localValue).String() == "int" {
				localValue = int32(localValue.(int))
			} else {
				localValue = compareArray.Index(i).Field(j).Interface()
			}
			fetchedValue := reflect.Indirect(reflect.ValueOf(res[i])).Field(j).Interface()
			if !reflect.DeepEqual(fetchedValue, localValue) {
				return fmt.Errorf("%v does not equal %v", fetchedValue, localValue)
			}
		}

	}
	pr.ReadStop()
	fr.Close()
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
	destPath := spark.Store.ResourcePath(id)
	sourceNameList := []string{sourceName}
	queryString := "SELECT * FROM source_0"
	sparkSubmitArgs := spark.Store.SparkSubmitArgs(destPath, queryString, sourceNameList)
	if err := spark.Executor.RunSparkJob(sparkSubmitArgs); err != nil {
		return nil, err
	}
	return spark.GetPrimaryTable(id)
}

//run the transformation on the source table
// type TransformationConfig struct {
// 	TargetTableID ResourceID
// 	Query         string
// 	ColumnMapping []ColumnMapping
// }
func (spark *SparkOfflineStore) CreateTransformation(config TransformationConfig) error {
	//transformationString := spark.Store.cleanTransformationString(sourceNameList)
	//check that it exists, if it does, retunr error
	return spark.UpdateTransformation(config)

	return nil
}

//just simple get request from the bucket
func (spark *SparkOfflineStore) GetTransformationTable(id ResourceID) (TransformationTable, error) {
	return nil, nil
}

//we run the same logic as create, just don't check that file exists
func (spark *SparkOfflineStore) UpdateTransformation(config TransformationConfig) error {
	// destPath := spark.Store.resourcePath(config.TargetTableID)
	return nil
}

//just create an empty parquet table with column names
func (spark *SparkOfflineStore) CreatePrimaryTable(id ResourceID, schema TableSchema) (PrimaryTable, error) {

	return spark.GetPrimaryTable(id)
}

//simple get request from the bucket
func (spark *SparkOfflineStore) GetPrimaryTable(id ResourceID) (PrimaryTable, error) {
	return nil, nil
}

//likewise make a parquet file with just le columns
func (spark *SparkOfflineStore) CreateResourceTable(id ResourceID, schema TableSchema) (OfflineTable, error) {
	return spark.GetResourceTable(id)
}

//simple get from le bucket
func (spark *SparkOfflineStore) GetResourceTable(id ResourceID) (OfflineTable, error) {
	return nil, nil
}

//this needs to be its own type and have a path
func (spark *SparkOfflineStore) CreateMaterialization(id ResourceID) (Materialization, error) {
	//check that it exists, if so, return error
	return spark.UpdateMaterialization(id)
}

//just bucket get from that path
func (spark *SparkOfflineStore) GetMaterialization(id MaterializationID) (Materialization, error) {
	return nil, nil
}

//same as create but don't check that it exists
func (spark *SparkOfflineStore) UpdateMaterialization(id ResourceID) (Materialization, error) {
	//do the work
	return nil, nil
}

//simple delete
func (spark *SparkOfflineStore) DeleteMaterialization(id MaterializationID) error {
	return nil
}

//simple query from resource tables together for fun
func (spark *SparkOfflineStore) CreateTrainingSet(def TrainingSetDef) error {
	//check that it exists, if so, return error
	return spark.UpdateTrainingSet(def)
	return nil
}

//same as create but don't check if it already exists
func (spark *SparkOfflineStore) UpdateTrainingSet(TrainingSetDef) error {
	return nil
}

//simple bucket query
func (spark *SparkOfflineStore) GetTrainingSet(id ResourceID) (TrainingSetIterator, error) {
	return nil, nil
}
