package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/emr"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	emrTypes "github.com/aws/aws-sdk-go-v2/service/emr/types"
	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
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
}

type EMRExecutor struct {
	client      *emr.Client
	clusterName string
}

type S3Store struct {
	client     *s3.Client
	region     string
	bucketPath string
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
		s3Store := S3Store{
			client:     client,
			region:     s3Conf.BucketRegion,
			bucketPath: s3Conf.BucketPath,
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

func (spark *SparkOfflineStore) RegisterResourceFromSourceTable(id ResourceID, schema ResourceSchema) (OfflineTable, error) {
	return nil, nil
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
