package provider

import (
	"context"
	"encoding/json"
	"fmt"
	// "io"
	// "io/ioutil"
	// "os"
	// "path"
	// "reflect"
	// "runtime"
	// "sort"
	// "strconv"
	"strings"
	"time"

	//for compatability with parquet-go
	// awsV1 "github.com/aws/aws-sdk-go/aws"
	// credentialsV1 "github.com/aws/aws-sdk-go/aws/credentials"
	// session "github.com/aws/aws-sdk-go/aws/session"
	// s3manager "github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsv2cfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/emr"
	s3v2 "github.com/aws/aws-sdk-go-v2/service/s3"
	"go.uber.org/zap"
	"gocloud.dev/blob/s3blob"

	databricks "github.com/Azure/databricks-sdk-golang"
	dbAzure "github.com/Azure/databricks-sdk-golang/azure"
	azureHTTPModels "github.com/Azure/databricks-sdk-golang/azure/jobs/httpmodels"
	azureModels "github.com/Azure/databricks-sdk-golang/azure/jobs/models"

	emrTypes "github.com/aws/aws-sdk-go-v2/service/emr/types"
	// s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	// parquetGo "github.com/xitongsys/parquet-go-source/s3"
	// reader "github.com/xitongsys/parquet-go/reader"
	// source "github.com/xitongsys/parquet-go/source"
	// writer "github.com/xitongsys/parquet-go/writer"
)

type SparkExecutorType string

const (
	EMR        SparkExecutorType = "EMR"
	Databricks                   = "DATABRICKS"
)

type JobType string

const (
	Materialize       JobType = "Materialization"
	Transform                 = "Transformation"
	CreateTrainingSet         = "Training Set"
)

const MATERIALIZATION_ID_SEGMENTS = 3
const ENTITY_INDEX = 0
const VALUE_INDEX = 1
const TIMESTAMP_INDEX = 2

type SparkExecutorConfig []byte

type SparkConfig struct {
	ExecutorType   SparkExecutorType
	ExecutorConfig SparkExecutorConfig
	StoreType      BlobStoreType
	StoreConfig    BlobStoreConfig
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

func ResourcePath(id ResourceID) string {
	return fmt.Sprintf("%s/%s/%s", id.Type, id.Name, id.Variant)
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

type DatabricksResultState string

const (
	Success   DatabricksResultState = "SUCCESS"
	Failed                          = "FAILED"
	Timedout                        = "TIMEDOUT"
	Cancelled                       = "CANCELLED"
)

type DatabricksConfig struct {
	Username string
	Password string
	Host     string
	Token    string
	Cluster  string
}

func (d *DatabricksConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, d)
	if err != nil {
		return err
	}
	return nil
}

func (d *DatabricksConfig) Serialize() []byte {
	conf, err := json.Marshal(d)
	if err != nil {
		panic(err)
	}
	return conf
}

type DatabricksExecutor struct {
	client  *dbAzure.DBClient
	cluster string
	config  DatabricksConfig
}

func (db *DatabricksExecutor) PythonFileURI() string {
	return "scripts/spark_executor.py"
}

func (db *DatabricksExecutor) InitializeExecutor(store BlobStore) error {
	return nil
}


func NewDatabricksExecutor(config Config) (SparkExecutor, error) {
	databricksConfig := DatabricksConfig{}
	if err := databricksConfig.Deserialize(SerializedConfig(config)); err != nil {
		return nil, fmt.Errorf("could not deserialize s3 store config: %v", err)
	}
	opt := databricks.NewDBClientOption(databricksConfig.Username, databricksConfig.Password, databricksConfig.Host, databricksConfig.Token, nil, false, 0)
	client := dbAzure.NewDBClient(opt)
	return &DatabricksExecutor{
		client:  client,
		cluster: databricksConfig.Cluster,
		config:  databricksConfig,
	}, nil
}

func (db *DatabricksExecutor) RunSparkJob(args *[]string) error {
	jobsClient := db.client.Jobs()
	pythonTask := azureModels.SparkPythonTask{
		PythonFile: db.PythonFileURI(),
		Parameters: args,
	}
	createJobRequest := azureHTTPModels.CreateReq{
		ExistingCluster: db.cluster,
		SparkPythonTask: &pythonTask,
		Name:            "databricks spark submit job",
	}
	createResp, err := jobsClient.Create(createJobRequest)
	if err != nil {
		return err
	}
	runJobRequest := azureHTTPModels.RunNowReq{
		JobID: createResp.JobID,
	}
	runNowResp, err := jobsClient.RunNow(runJobRequest)
	if err != nil {
		return err
	}
	runGetRequest := azureHTTPModels.RunsGetReq{
		RunID: runNowResp.RunID,
	}
	for {
		runsGetResp, err := jobsClient.RunsGet(runGetRequest)
		if err != nil {
			return err
		}
		if runsGetResp.EndTime != int64(0) {
			if string(runsGetResp.State.ResultState) != string(Success) {
				return fmt.Errorf("could not execute databricks spark job: %s", runsGetResp.State.StateMessage)
			}
			break
		}
	}
	return nil
}

type S3BlobStoreConfig struct {
	AWSAccessKeyId string
	AWSSecretKey   string
	BucketRegion   string
	BucketPath     string
	Path string
}

func (s *S3BlobStoreConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, s)
	if err != nil {
		return err
	}
	return nil
}

func (s *S3BlobStoreConfig) Serialize() []byte {
	conf, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return conf
}

type S3BlobStore struct {
	Bucket string
	Path   string
	genericBlobStore
}

func NewS3BlobStore(config Config) (BlobStore, error) {
	s3StoreConfig := S3BlobStoreConfig{}
	if err := s3StoreConfig.Deserialize(SerializedConfig(config)); err != nil {
		return nil, fmt.Errorf("could not deserialize s3 store config: %v", err)
	}
	cfg, err := awsv2cfg.LoadDefaultConfig(context.TODO(),
		awsv2cfg.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID: s3StoreConfig.AWSAccessKeyId, SecretAccessKey: s3StoreConfig.AWSSecretKey,
			},
		}))
	if err != nil {
		return nil, err
	}
	cfg.Region = s3StoreConfig.BucketRegion
	clientV2 := s3v2.NewFromConfig(cfg)
	bucket, err := s3blob.OpenBucketV2(ctx, clientV2, s3StoreConfig.BucketPath, nil)
	if err != nil {
		return nil, err
	}
	return &S3BlobStore{
		Bucket: s3StoreConfig.BucketPath,
		Path:   s3StoreConfig.Path,
		genericBlobStore: genericBlobStore{
			bucket: bucket,
		},
	}, nil
}

type SparkOfflineQueries interface {
	materializationCreate(schema ResourceSchema) string
}

type defaultSparkOfflineQueries struct{}

func (q defaultSparkOfflineQueries) materializationCreate(schema ResourceSchema) string {
	timestampColumn := schema.TS
	// without timestamp, assumes each entity only has single entry
	if schema.TS == "" {
		return fmt.Sprintf("SELECT %s AS entity, %s AS value, 0 as ts, ROW_NUMBER() over (ORDER BY (SELECT NULL)) AS row_number FROM source_0", schema.Entity, schema.Value)
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
		featureColumnName := featureColumnName(feature)
		columns = append(columns, featureColumnName)
		var featureWindowQuery string
		// if no timestamp column, set to default generated by resource registration
		if featureSchemas[i].TS == "" {
			featureWindowQuery = fmt.Sprintf("SELECT * FROM (SELECT %s as t%d_entity, %s as %s, 0 as t%d_ts FROM source_%d) ORDER BY t%d_ts ASC", featureSchemas[i].Entity, i+1, featureSchemas[i].Value, featureColumnName, i+1, i+1, i+1)
		} else {
			featureWindowQuery = fmt.Sprintf("SELECT * FROM (SELECT %s as t%d_entity, %s as %s, %s as t%d_ts FROM source_%d) ORDER BY t%d_ts ASC", featureSchemas[i].Entity, i+1, featureSchemas[i].Value, featureColumnName, featureSchemas[i].TS, i+1, i+1, i+1)
		}
		featureJoinQuery := fmt.Sprintf("LEFT OUTER JOIN (%s) t%d ON (t%d_entity = entity AND t%d_ts <= label_ts)", featureWindowQuery, i+1, i+1, i+1)
		joinQueries = append(joinQueries, featureJoinQuery)
	}
	columnStr := strings.Join(columns, ", ")
	joinQueryString := strings.Join(joinQueries, " ")
	var labelWindowQuery string
	if labelSchema.TS == "" {
		labelWindowQuery = fmt.Sprintf("SELECT %s AS entity, %s AS value, 0 AS label_ts FROM source_0", labelSchema.Entity, labelSchema.Value)
	} else {
		labelWindowQuery = fmt.Sprintf("SELECT %s AS entity, %s AS value, %s AS label_ts FROM source_0", labelSchema.Entity, labelSchema.Value, labelSchema.TS)
	}
	labelPartitionQuery := fmt.Sprintf("(SELECT * FROM (SELECT entity, value, label_ts FROM (%s) t ) t0)", labelWindowQuery)
	labelJoinQuery := fmt.Sprintf("%s %s", labelPartitionQuery, joinQueryString)
	fullQuery := fmt.Sprintf("SELECT %s, value AS %s, entity, label_ts, ROW_NUMBER() over (PARTITION BY entity, value, label_ts ORDER BY label_ts DESC) as row_number FROM (%s) tt", columnStr, featureColumnName(def.Label), labelJoinQuery)
	finalQuery := fmt.Sprintf("SELECT %s, %s FROM (SELECT * FROM (SELECT *, row_number FROM (%s) WHERE row_number=1 ))", columnStr, featureColumnName(def.Label), fullQuery)
	return finalQuery
}

type SparkOfflineStore struct {
	Executor SparkExecutor
	Store    BlobStore
	Logger   *zap.SugaredLogger
	query    *defaultSparkOfflineQueries
	BaseProvider
}

func (store *SparkOfflineStore) AsOfflineStore() (OfflineStore, error) {
	return store, nil
}

func (store *SparkOfflineStore) Close() error {
	return nil
}

func sparkOfflineStoreFactory(config SerializedConfig) (Provider, error) {
	sc := SparkConfig{}
	logger := zap.NewExample().Sugar()
	if err := sc.Deserialize(config); err != nil {
		logger.Errorw("Invalid config to initialize spark offline store", err)
		return nil, fmt.Errorf("invalid spark config: %v", config)
	}
	logger.Info("Creating Spark executor with type:", sc.ExecutorType)
	exec, err := NewSparkExecutor(sc.ExecutorType, sc.ExecutorConfig, logger)
	if err != nil {
		logger.Errorw("Failure initializing Spark executor with type", sc.ExecutorType, err)
		return nil, err
	}

	fmt.Sprintf("Executor type: %s, Executor config: %v", sc.ExecutorType, sc.ExecutorConfig)
	logger.Info("Creating Spark store with type:", sc.StoreType)
	store, err := CreateBlobStore(string(sc.StoreType), Config(sc.StoreConfig))
	if err != nil {
		logger.Errorw("Failure initializing blob store with type", sc.StoreType, err)
		return nil, err
	}
	fmt.Sprintf("Store type: %s, Store config: %v", sc.StoreType, sc.StoreConfig)
	logger.Info("Uploading Spark script to store")

	logger.Debugf("Store type: %s, Store config: %v", sc.StoreType, sc.StoreConfig)
	if err := exec.InitializeExecutor(store); err != nil {
		logger.Errorw("Failure initializing executor", err)
		return nil, err
	}
	logger.Info("Created Spark Offline Store")
	queries := defaultSparkOfflineQueries{}
	sparkOfflineStore := SparkOfflineStore{
		Executor: exec,
		Store:    store,
		Logger:   logger,
		query:    &queries,
		BaseProvider: BaseProvider{
			ProviderType:   "SPARK_OFFLINE",
			ProviderConfig: config,
		},
	}
	return &sparkOfflineStore, nil
}

type SparkExecutor interface {
	RunSparkJob(args *[]string) error
	InitializeExecutor(store BlobStore) error
	PythonFileURI() string
}

type EMRExecutor struct {
	client      *emr.Client
	clusterName string
	logger      *zap.SugaredLogger
}

func (e EMRExecutor) InitializeExecutor(store BlobStore) error {
	return nil
	// s3BlobStore, isS3 := store.(*S3BlobStore)
	// if isS3 {
	// 	// load spark script to buffer
	// 	// upload via the write function
	// }
	// here we handle the initialization for other blob stores (azure, etc)
}

func NewSparkExecutor(execType SparkExecutorType, config SparkExecutorConfig, logger *zap.SugaredLogger) (SparkExecutor, error) {
	// if execType == EMR {

	// 	client := emr.New(emr.Options{
	// 		Region:      config.ClusterRegion,
	// 		Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(config.AWSAccessKeyId, config.AWSSecretKey, "")),
	// 	})

	// 	emrExecutor := EMRExecutor{
	// 		client:      client,
	// 		logger:      logger,
	// 		clusterName: config.ClusterName,
	// 	}
	// 	return &emrExecutor, nil
	// }
	// return nil, nil
	return nil, nil
}

func (e *EMRExecutor) RunSparkJob(args *[]string) error {
	params := &emr.AddJobFlowStepsInput{
		JobFlowId: aws.String(e.clusterName), //returned by listclusters
		Steps: []emrTypes.StepConfig{
			{
				Name: aws.String("Featureform execution step"),
				HadoopJarStep: &emrTypes.HadoopJarStepConfig{
					Jar:  aws.String("command-runner.jar"), //jar file for running pyspark scripts
					Args: *args,
				},
				ActionOnFailure: emrTypes.ActionOnFailureContinue,
			},
		},
	}
	resp, err := e.client.AddJobFlowSteps(context.TODO(), params)
	if err != nil {
		e.logger.Errorw("Could not add job flow steps to EMR cluster", err)
		return err
	}
	stepId := resp.StepIds[0]
	var waitDuration time.Duration = time.Second * 500
	e.logger.Debugw("Waiting for EMR job to complete")
	stepCompleteWaiter := emr.NewStepCompleteWaiter(e.client)
	_, err = stepCompleteWaiter.WaitForOutput(context.TODO(), &emr.DescribeStepInput{
		ClusterId: aws.String(e.clusterName),
		StepId:    aws.String(stepId),
	}, waitDuration)
	if err != nil {
		e.logger.Errorw("Failure waiting for completion of EMR cluster", err)
		return err
	}
	return nil
}

func (s *S3BlobStore) sparkSubmitArgs(destPath string, cleanQuery string, sourceList []string, jobType JobType) []string {
	argList := []string{
		"spark-submit",
		"--deploy-mode",
		"client",
		fmt.Sprintf("s3://%s/%s/featureform/scripts/offline_store_spark_runner.py", s.Bucket, s.Path),
		"sql",
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

func (spark *SparkOfflineStore) pysparkArgs(destinationURI string, templatedQuery string, sourceList []string, jobType JobType) *[]string {
	args := []string{}
	return &args
}

func (spark *SparkOfflineStore) RegisterPrimaryFromSourceTable(id ResourceID, sourceName string) (PrimaryTable, error) {
	return blobRegisterPrimary(id, sourceName, spark.Logger, spark.Store)
}

func (spark *SparkOfflineStore) RegisterResourceFromSourceTable(id ResourceID, schema ResourceSchema) (OfflineTable, error) {
	return blobRegisterResource(id, schema, spark.Logger, spark.Store)
}

func (spark *SparkOfflineStore) CreateTransformation(config TransformationConfig) error {
	return spark.transformation(config, false)
}

func (spark *SparkOfflineStore) transformation(config TransformationConfig, isUpdate bool) error {
	if config.Type == SQLTransformation {
		return spark.sqlTransformation(config, isUpdate)
	} else if config.Type == DFTransformation {
		return spark.dfTransformation(config, isUpdate)
	} else {
		spark.Logger.Errorw("Unsupported transformation type", config.Type)
		return fmt.Errorf("the transformation type '%v' is not supported", config.Type)
	}
}

func (spark *SparkOfflineStore) sqlTransformation(config TransformationConfig, isUpdate bool) error {
	return nil
	// updatedQuery, sources, err := spark.updateQuery(config.Query, config.SourceMapping)
	// if err != nil {
	// 	spark.Logger.Errorw("Could not generate updated query for spark transformation", err)
	// 	return err
	// }

	// transformationExists := spark.Store.NewestBlob(ResourcePath(id)) != ""

	// if !isUpdate && transformationExists {
	// 	spark.Logger.Errorw("Creation when transformation already exists", config.TargetTableID, transformationDestination)
	// 	return fmt.Errorf("transformation %v already exists at %s", config.TargetTableID, transformationDestination)
	// } else if isUpdate && !transformationExists {
	// 	spark.Logger.Errorw("Update job attempted when transformation does not exist", config.TargetTableID, transformationDestination)
	// 	return fmt.Errorf("transformation %v doesn't exist at %s and you are trying to update", config.TargetTableID, transformationDestination)
	// }
	// spark.Logger.Debugw("Running SQL transformation", config)
	// sparkArgs := spark.Store.SparkSubmitArgs(transformationDestination, updatedQuery, sources, Transform)
	// if err := spark.Executor.RunSparkJob(sparkArgs); err != nil {
	// 	spark.Logger.Errorw("spark submit job for transformation failed to run", config.TargetTableID, err)
	// 	return fmt.Errorf("spark submit job for transformation %v failed to run: %v", config.TargetTableID, err)
	// }
	// spark.Logger.Debugw("Succesfully ran SQL transformation", config)
	// return nil
}
func (spark *SparkOfflineStore) dfTransformation(config TransformationConfig, isUpdate bool) error {
	return nil
	// transformationDestination := spark.Store.ResourcePath(config.TargetTableID)
	// exists, err := spark.Store.ResourceExists(config.TargetTableID)
	// if err != nil {
	// 	spark.Logger.Errorw("Error checking if resource exists", err)
	// 	return err
	// }

	// if !isUpdate && exists {
	// 	spark.Logger.Errorw("Transformation already exists", config.TargetTableID, transformationDestination)
	// 	return fmt.Errorf("transformation %v already exists at %s", config.TargetTableID, transformationDestination)
	// } else if isUpdate && !exists {
	// 	spark.Logger.Errorw("Transformation doesn't exists at destination and you are trying to update", config.TargetTableID, transformationDestination)
	// 	return fmt.Errorf("transformation %v doesn't exist at %s and you are trying to update", config.TargetTableID, transformationDestination)
	// }

	// transformationFilePath := spark.Store.GetTransformationFileLocation(config.TargetTableID)
	// fileName := "transformation.pkl"
	// transformationFileLocation := fmt.Sprintf("%s/%s", transformationFilePath, fileName)

	// f, err := os.Create(fileName)
	// if err != nil {
	// 	return fmt.Errorf("could not create file: %s", err)
	// }
	// defer f.Close()

	// err = ioutil.WriteFile(fileName, config.Code, 0644)
	// if err != nil {
	// 	return fmt.Errorf("could not write to file: %s", err)
	// }

	// // Write byte to file
	// f, err = os.Open(fileName)
	// if err != nil {
	// 	return fmt.Errorf("could not open file: %s", err)
	// }
	// err = spark.Store.UploadFile(transformationFileLocation, f)
	// if err != nil {
	// 	return fmt.Errorf("could not upload file: %s", err)
	// }

	// sparkArgs, err := spark.getDFArgs(transformationDestination, transformationFileLocation, spark.Store.Region(), config.SourceMapping)
	// if err != nil {
	// 	spark.Logger.Errorw("Problem creating spark dataframe arguments", err)
	// 	return fmt.Errorf("error with getting df arguments %v", sparkArgs)
	// }
	// spark.Logger.Debugw("Running DF transformation", config)
	// if err := spark.Executor.RunSparkJob(sparkArgs); err != nil {
	// 	spark.Logger.Errorw("Error running Spark dataframe job", err)
	// 	return fmt.Errorf("spark submit job for transformation %v failed to run: %v", config.TargetTableID, err)
	// }
	// spark.Logger.Debugw("Succesfully ran DF transformation", config)
	// return nil
}

func (spark *SparkOfflineStore) updateQuery(query string, mapping []SourceMapping) (string, []string, error) {
	sources := make([]string, len(mapping))
	replacements := make([]string, len(mapping)*2) // It's times 2 because each replacement will be a pair; (original, replacedValue)

	for i, m := range mapping {
		replacements = append(replacements, m.Template)
		replacements = append(replacements, fmt.Sprintf("source_%v", i))

		sourcePath, err := spark.getSourcePath(m.Source)
		if err != nil {
			spark.Logger.Errorw("Error getting source path of spark source", m.Source, err)
			return "", nil, fmt.Errorf("could not get the sourcePath for %s because %s", m.Source, err)
		}

		sources[i] = sourcePath
	}

	replacer := strings.NewReplacer(replacements...)
	updatedQuery := replacer.Replace(query)

	if strings.Contains(updatedQuery, "{{") {
		spark.Logger.Errorw("Template replace failed", updatedQuery)
		return "", nil, fmt.Errorf("could not replace all the templates with the current mapping. Mapping: %v; Replaced Query: %s", mapping, updatedQuery)
	}
	return updatedQuery, sources, nil
}

func (spark *SparkOfflineStore) getSourcePath(path string) (string, error) {
	fileType, fileName, fileVariant := spark.getResourceInformationFromFilePath(path)

	var filePath string
	if fileType == "primary" {
		fileResourceId := ResourceID{Name: fileName, Variant: fileVariant, Type: Primary}
		fileTable, err := spark.GetPrimaryTable(fileResourceId)
		if err != nil {
			spark.Logger.Errorw("Issue getting primary table", fileResourceId, err)
			return "", fmt.Errorf("could not get the primary table for {%v} because %s", fileResourceId, err)
		}
		filePath = spark.Store.PathWithPrefix(fileTable.GetName())
		return filePath, nil
	} else if fileType == "transformation" {
		fileResourceId := ResourceID{Name: fileName, Variant: fileVariant, Type: Transformation}
		transformationPath := spark.Store.NewestBlob(spark.Store.PathWithPrefix(ResourcePath(fileResourceId)))
		filePath = spark.Store.PathWithPrefix(transformationPath[:strings.LastIndex(transformationPath, "/")])
		return filePath, nil
	} else {
		return filePath, fmt.Errorf("could not find path for %s; fileType: %s, fileName: %s, fileVariant: %s", path, fileType, fileName, fileVariant)
	}
}

func (spark *SparkOfflineStore) getResourceInformationFromFilePath(path string) (string, string, string) {
	var fileType string
	var fileName string
	var fileVariant string
	if path[:5] == "s3://" {
		filePaths := strings.Split(path[len("s3://"):], "/")
		if len(filePaths) <= 4 {
			return "", "", ""
		}
		fileType, fileName, fileVariant = strings.ToLower(filePaths[2]), filePaths[3], filePaths[4]
	} else {
		filePaths := strings.Split(path[len("featureform_"):], "__")
		if len(filePaths) <= 2 {
			return "", "", ""
		}
		fileType, fileName, fileVariant = filePaths[0], filePaths[1], filePaths[2]
	}
	return fileType, fileName, fileVariant
}

func (spark *SparkOfflineStore) getDFArgs(outputURI string, code string, awsRegion string, mapping []SourceMapping) ([]string, error) {
	argList := []string{
		"spark-submit",
		"--deploy-mode",
		"client",
		spark.Executor.PythonFileURI(),
		"df",
		"--output_uri",
		outputURI,
		"--code",
		code,
		"--aws_region",
		awsRegion,
		"--source",
	}

	for _, m := range mapping {
		sourcePath, err := spark.getSourcePath(m.Source)
		if err != nil {
			spark.Logger.Errorw("Error getting source path for spark source", m.Source, err)
			return nil, fmt.Errorf("issue with retreiving the source path for %s because %s", m.Source, err)
		}

		argList = append(argList, sourcePath)
	}

	return argList, nil
}

func (spark *SparkOfflineStore) GetTransformationTable(id ResourceID) (TransformationTable, error) {
	// spark.Logger.Debugw("Getting transformation table", "ResourceID", id)
	// transformationPath, err := spark.Store.ResourceKeySinglePart(id)
	// if err != nil {
	// 	spark.Logger.Errorw("Could not get transformation table", "error", err)
	// 	return nil, fmt.Errorf("could not get transformation table (%v) because %s", id, err)
	// }
	// fixedPath := transformationPath[:strings.LastIndex(transformationPath, "/")+1]
	// spark.Logger.Debugw("Succesfully retrieved transformation table", "ResourceID", id)
	// return &S3PrimaryTable{spark.Store, fixedPath, true, id}, nil
	return nil, nil
}

func (spark *SparkOfflineStore) UpdateTransformation(config TransformationConfig) error {
	return spark.transformation(config, true)
}

func (spark *SparkOfflineStore) CreatePrimaryTable(id ResourceID, schema TableSchema) (PrimaryTable, error) {
	return nil, nil
}

func (spark *SparkOfflineStore) GetPrimaryTable(id ResourceID) (PrimaryTable, error) {
	return blobGetPrimary(id, spark.Store, spark.Logger)
}

func (spark *SparkOfflineStore) CreateResourceTable(id ResourceID, schema TableSchema) (OfflineTable, error) {
	return nil, nil
}

func (spark *SparkOfflineStore) GetResourceTable(id ResourceID) (OfflineTable, error) {
	return blobGetResourceTable(id, spark.Store, spark.Logger)
}

func (spark *SparkOfflineStore) CreateMaterialization(id ResourceID) (Materialization, error) {
	return nil, nil
	// if id.Type != Feature {
	// 	spark.Logger.Errorw("Attempted to create a materialization of a non feature resource", id.Type)
	// 	return nil, fmt.Errorf("only features can be materialized")
	// }
	// resourceTable, err := spark.GetResourceTable(id)
	// if err != nil {
	// 	spark.Logger.Errorw("Attempted to fetch resource table of non registered resource", err)
	// 	return nil, fmt.Errorf("resource not registered: %v", err)
	// }
	// sparkResourceTable, ok := resourceTable.(*S3OfflineTable)
	// if !ok {
	// 	spark.Logger.Errorw("Could not convert resource table to S3 offline table", id)
	// 	return nil, fmt.Errorf("could not convert offline table with id %v to sparkResourceTable", id)
	// }
	// materializationID := ResourceID{Name: id.Name, Variant: id.Variant, Type: FeatureMaterialization}
	// destinationPath := spark.Store.ResourcePath(materializationID)
	// materializationExists, err := spark.Store.ResourceExists(materializationID)
	// if err != nil {
	// 	spark.Logger.Errorw("Could not determine whether materialization exists", err)
	// 	return nil, fmt.Errorf("error checking if materialization exists: %v", err)
	// }
	// if materializationExists {
	// 	spark.Logger.Errorw("Attempted to materialize a materialization that already exists", id)
	// 	return nil, fmt.Errorf("materialization already exists")
	// }
	// materializationQuery := spark.query.materializationCreate(sparkResourceTable.schema)
	// sourcePath := spark.Store.KeyPath(sparkResourceTable.schema.SourceTable)
	// sparkArgs := spark.Store.SparkSubmitArgs(destinationPath, materializationQuery, []string{sourcePath}, Materialize)
	// spark.Logger.Debugw("Creating materialization", "id", id)
	// if err := spark.Executor.RunSparkJob(sparkArgs); err != nil {
	// 	spark.Logger.Errorw("Spark submit job failed to run", err)
	// 	return nil, fmt.Errorf("spark submit job for materialization %v failed to run: %v", materializationID, err)
	// }
	// key, err := spark.Store.ResourceKeySinglePart(materializationID)
	// if err != nil {
	// 	spark.Logger.Errorw("Created materialization not found in store", err)
	// 	return nil, fmt.Errorf("Materialization result does not exist in offline store: %v", err)
	// }
	// spark.Logger.Debugw("Succesfully created materialization", "id", id)
	// return &S3Materialization{materializationID, spark.Store, key}, nil
}

func (spark *SparkOfflineStore) GetMaterialization(id MaterializationID) (Materialization, error) {
	return blobGetMaterialization(id, spark.Store, spark.Logger)
}

func (spark *SparkOfflineStore) UpdateMaterialization(id ResourceID) (Materialization, error) {
	return nil, nil
	// if id.Type != Feature {
	// 	spark.Logger.Errorw("Attempted to materialize a non feature resource", id.Type)
	// 	return nil, fmt.Errorf("only features can be materialized")
	// }
	// resourceTable, err := spark.GetResourceTable(id)
	// if err != nil {
	// 	spark.Logger.Errorw("Attempted to materialize a non registered resource", err)
	// 	return nil, fmt.Errorf("resource not registered: %v", err)
	// }
	// sparkResourceTable, ok := resourceTable.(*S3OfflineTable)
	// if !ok {
	// 	spark.Logger.Errorw("could not convert offline table to sparkResourceTable", id)
	// 	return nil, fmt.Errorf("could not convert offline table with id %v to sparkResourceTable", id)
	// }
	// materializationID := ResourceID{Name: id.Name, Variant: id.Variant, Type: FeatureMaterialization}
	// destinationPath := spark.Store.ResourcePath(materializationID)
	// materializationQuery := spark.query.materializationCreate(sparkResourceTable.schema)
	// sourcePath := spark.Store.KeyPath(sparkResourceTable.schema.SourceTable)
	// sparkArgs := spark.Store.SparkSubmitArgs(destinationPath, materializationQuery, []string{sourcePath}, Materialize)
	// spark.Logger.Debugw("Updating materialization", "id", id)
	// if err := spark.Executor.RunSparkJob(sparkArgs); err != nil {
	// 	spark.Logger.Errorw("Could not run spark update materialization job", err)
	// 	return nil, fmt.Errorf("spark submit job for materialization %v failed to run: %v", materializationID, err)
	// }
	// key, err := spark.Store.ResourceKeySinglePart(materializationID)
	// if err != nil {
	// 	spark.Logger.Errorw("Could not fetch materialization resource key", err)
	// 	return nil, fmt.Errorf("Materialization result does not exist in offline store: %v", err)
	// }
	// spark.Logger.Debugw("Succesfully updated materialization", "id", id)
	// return &S3Materialization{materializationID, spark.Store, key}, nil
}

func (spark *SparkOfflineStore) DeleteMaterialization(id MaterializationID) error {
	return blobDeleteMaterialization(id, spark.Store, spark.Logger)
}

func (spark *SparkOfflineStore) registeredResourceSchema(id ResourceID) (ResourceSchema, error) {
	// spark.Logger.Debugw("Getting resource schema", "id", id)
	// table, err := spark.GetResourceTable(id)
	// if err != nil {
	// 	spark.Logger.Errorw("Resource not registered in spark store", id, err)
	// 	return ResourceSchema{}, fmt.Errorf("resource not registered: %v", err)
	// }
	// sparkResourceTable, ok := table.(*S3OfflineTable)
	// if !ok {
	// 	spark.Logger.Errorw("could not convert offline table to sparkResourceTable", id)
	// 	return ResourceSchema{}, fmt.Errorf("could not convert offline table with id %v to sparkResourceTable", id)
	// }
	// spark.Logger.Debugw("Succesfully retrieved resource schema", "id", id)
	// return sparkResourceTable.schema, nil
	return ResourceSchema{}, nil
}

func (spark *SparkOfflineStore) CreateTrainingSet(def TrainingSetDef) error {
	return nil
	// if err := def.check(); err != nil {
	// 	spark.Logger.Errorw("Training set definition not valid", def, err)
	// 	return err
	// }
	// sourcePaths := make([]string, 0)
	// featureSchemas := make([]ResourceSchema, 0)
	// destinationPath := spark.Store.ResourcePath(def.ID)
	// trainingSetExists, err := spark.Store.ResourceExists(def.ID)
	// if err != nil {
	// 	spark.Logger.Errorw("Error checking if training set exists", err)
	// 	return fmt.Errorf("error checking if training set exists: %v", err)
	// }
	// if trainingSetExists {
	// 	spark.Logger.Errorw("Training set already exists", def.ID)
	// 	return fmt.Errorf("training set already exists: %v", def.ID)
	// }
	// labelSchema, err := spark.registeredResourceSchema(def.Label)
	// if err != nil {
	// 	spark.Logger.Errorw("Could not get schema of label in spark store", def.Label, err)
	// 	return fmt.Errorf("Could not get schema of label %s: %v", def.Label, err)
	// }
	// labelPath := spark.Store.KeyPath(labelSchema.SourceTable)
	// sourcePaths = append(sourcePaths, labelPath)
	// for _, feature := range def.Features {
	// 	featureSchema, err := spark.registeredResourceSchema(feature)
	// 	if err != nil {
	// 		spark.Logger.Errorw("Could not get schema of feature in spark store", feature, err)
	// 		return fmt.Errorf("Could not get schema of feature %s: %v", feature, err)
	// 	}
	// 	featurePath := spark.Store.KeyPath(featureSchema.SourceTable)
	// 	sourcePaths = append(sourcePaths, featurePath)
	// 	featureSchemas = append(featureSchemas, featureSchema)
	// }
	// trainingSetQuery := spark.query.trainingSetCreate(def, featureSchemas, labelSchema)
	// sparkArgs := spark.Store.SparkSubmitArgs(destinationPath, trainingSetQuery, sourcePaths, CreateTrainingSet)
	// spark.Logger.Debugw("Creating training set", "definition", def)
	// if err := spark.Executor.RunSparkJob(sparkArgs); err != nil {
	// 	spark.Logger.Errorw("Spark submit training set job failed to run", "definition", def.ID, "error", err)
	// 	return fmt.Errorf("spark submit job for training set %v failed to run: %v", def.ID, err)
	// }
	// _, err = spark.Store.ResourceKeySinglePart(def.ID)
	// if err != nil {
	// 	spark.Logger.Errorw("Could not get training set resource key in offline store", "error", err)
	// 	return fmt.Errorf("Training Set result does not exist in offline store: %v", err)
	// }
	// spark.Logger.Debugw("Succesfully created training set:", "definition", def)
	// return nil
}

func (spark *SparkOfflineStore) UpdateTrainingSet(def TrainingSetDef) error {
	return nil
	// if err := def.check(); err != nil {
	// 	spark.Logger.Errorw("Training set definition not valid", def, err)
	// 	return err
	// }
	// sourcePaths := make([]string, 0)
	// featureSchemas := make([]ResourceSchema, 0)
	// destinationPath := spark.Store.ResourcePath(def.ID)
	// labelSchema, err := spark.registeredResourceSchema(def.Label)
	// if err != nil {
	// 	spark.Logger.Errorw("Could not get label schema", def.Label, err)
	// 	return fmt.Errorf("Could not get schema of label %s: %v", def.Label, err)
	// }
	// labelPath := labelSchema.SourceTable // spark.Store.KeyPath(labelSchema.SourceTable)
	// sourcePaths = append(sourcePaths, labelPath)
	// for _, feature := range def.Features {
	// 	featureSchema, err := spark.registeredResourceSchema(feature)
	// 	if err != nil {
	// 		spark.Logger.Errorw("Could not get feature schema", feature, err)
	// 		return fmt.Errorf("Could not get schema of feature %s: %v", feature, err)
	// 	}
	// 	featurePath := spark.Store.KeyPath(featureSchema.SourceTable)
	// 	sourcePaths = append(sourcePaths, featurePath)
	// 	featureSchemas = append(featureSchemas, featureSchema)
	// }
	// trainingSetQuery := spark.query.trainingSetCreate(def, featureSchemas, labelSchema)
	// spark.Logger.Debugw("Updating training set", "definition", def)
	// sparkArgs := spark.Store.SparkSubmitArgs(destinationPath, trainingSetQuery, sourcePaths, CreateTrainingSet)
	// if err := spark.Executor.RunSparkJob(sparkArgs); err != nil {
	// 	spark.Logger.Errorw("Spark submit job failed to run", "id", def.ID, "error", err)
	// 	return fmt.Errorf("spark submit job for training set %v failed to run: %v", def.ID, err)
	// }
	// _, err = spark.Store.ResourceKeySinglePart(def.ID)
	// if err != nil {
	// 	spark.Logger.Errorw("Created Training set does not exist", err)
	// 	return fmt.Errorf("Training Set result does not exist in offline store: %v", err)
	// }
	// spark.Logger.Debugw("Successfully updated training set:", def)
	// return nil
}

func (spark *SparkOfflineStore) GetTrainingSet(id ResourceID) (TrainingSetIterator, error) {
	return blobGetTrainingSet(id, spark.Store, spark.Logger)
}
