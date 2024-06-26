package provider

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"net/http"
	"path/filepath"

	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/pkg/errors"

	"github.com/featureform/logging"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/emr"
	databricks "github.com/databricks/databricks-sdk-go"
	dbClient "github.com/databricks/databricks-sdk-go/client"
	dbConfig "github.com/databricks/databricks-sdk-go/config"
	"github.com/databricks/databricks-sdk-go/retries"
	"github.com/databricks/databricks-sdk-go/service/compute"
	"github.com/databricks/databricks-sdk-go/service/jobs"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"golang.org/x/exp/slices"

	re "github.com/avast/retry-go/v4"
	emrTypes "github.com/aws/aws-sdk-go-v2/service/emr/types"
	"github.com/featureform/config"
	"github.com/featureform/fferr"
	filestore "github.com/featureform/filestore"
	"github.com/featureform/helpers/compression"
	pc "github.com/featureform/provider/provider_config"
	ps "github.com/featureform/provider/provider_schema"
	pt "github.com/featureform/provider/provider_type"
)

type JobType string

const (
	Materialize       JobType = "Materialization"
	Transform         JobType = "Transformation"
	CreateTrainingSet JobType = "Training Set"
	BatchFeatures     JobType = "Batch Features"
)

const MATERIALIZATION_ID_SEGMENTS = 3
const ENTITY_INDEX = 0
const VALUE_INDEX = 1
const TIMESTAMP_INDEX = 2
const SPARK_SUBMIT_PARAMS_BYTE_LIMIT = 10_240

type SparkExecutorConfig interface {
	Serialize() ([]byte, error)
	Deserialize(config pc.SerializedConfig) error
	IsExecutorConfig() bool
}

type SparkFileStore interface {
	SparkConfig() []string
	CredentialsConfig() []string
	Packages() []string
	Type() string // s3, azure_blob_store, google_cloud_storage, hdfs, local
	FileStore
}

type SparkFileStoreFactory func(config Config) (SparkFileStore, error)

var sparkFileStoreMap = map[filestore.FileStoreType]SparkFileStoreFactory{
	filestore.FileSystem: NewSparkLocalFileStore,
	filestore.Azure:      NewSparkAzureFileStore,
	filestore.S3:         NewSparkS3FileStore,
	filestore.GCS:        NewSparkGCSFileStore,
	filestore.HDFS:       NewSparkHDFSFileStore,
}

func CreateSparkFileStore(fsType filestore.FileStoreType, config Config) (SparkFileStore, error) {
	factory, exists := sparkFileStoreMap[fsType]
	if !exists {
		return nil, fferr.NewInternalError(fmt.Errorf("factory does not exist: %s", fsType))
	}
	fileStore, err := factory(config)
	if err != nil {
		return nil, err
	}
	return fileStore, nil
}

func NewSparkS3FileStore(config Config) (SparkFileStore, error) {
	fileStore, err := NewS3FileStore(config)
	if err != nil {
		return nil, err
	}
	s3, ok := fileStore.(*S3FileStore)
	if !ok {
		return nil, fferr.NewInternalError(fmt.Errorf("could not cast file store to *S3FileStore"))
	}

	return &SparkS3FileStore{s3}, nil
}

type SparkS3FileStore struct {
	*S3FileStore
}

func (s3 SparkS3FileStore) SparkConfig() []string {
	return []string{
		"--spark_config",
		fmt.Sprintf("\"fs.s3a.access.key=%s\"", s3.Credentials.AWSAccessKeyId),
		"--spark_config",
		fmt.Sprintf("\"fs.s3a.secret.key=%s\"", s3.Credentials.AWSSecretKey),
		"--spark_config",
		"\"fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider\"",
		"--spark_config",
		"\"spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem\"",
	}
}

func (s3 SparkS3FileStore) CredentialsConfig() []string {
	return []string{
		"--credential",
		fmt.Sprintf("\"aws_bucket_name=%s\"", s3.Bucket),
		"--credential",
		fmt.Sprintf("\"aws_region=%s\"", s3.BucketRegion),
		"--credential",
		fmt.Sprintf("\"aws_access_key_id=%s\"", s3.Credentials.AWSAccessKeyId),
		"--credential",
		fmt.Sprintf("\"aws_secret_access_key=%s\"", s3.Credentials.AWSSecretKey),
	}
}

func (s3 SparkS3FileStore) Packages() []string {
	return []string{
		"--packages",
		"org.apache.spark:spark-hadoop-cloud_2.12:3.2.0",
		"--exclude-packages",
		"com.google.guava:guava",
	}
}

func (s3 SparkS3FileStore) Type() string {
	return "s3"
}

func NewSparkAzureFileStore(config Config) (SparkFileStore, error) {
	fileStore, err := NewAzureFileStore(config)
	if err != nil {
		return nil, err
	}

	azure, ok := fileStore.(*AzureFileStore)
	if !ok {
		return nil, fferr.NewInternalError(fmt.Errorf("could not cast file store to *AzureFileStore"))
	}

	return &SparkAzureFileStore{azure}, nil
}

type SparkAzureFileStore struct {
	*AzureFileStore
}

func (store SparkAzureFileStore) configString() string {
	return fmt.Sprintf("fs.azure.account.key.%s.dfs.core.windows.net=%s", store.AccountName, store.AccountKey)
}

func (azureStore SparkAzureFileStore) SparkConfig() []string {
	return []string{
		"--spark_config",
		fmt.Sprintf("\"%s\"", azureStore.configString()),
	}
}

func (azureStore SparkAzureFileStore) CredentialsConfig() []string {
	return []string{
		"--credential",
		fmt.Sprintf("\"azure_connection_string=%s\"", azureStore.connectionString()),
		"--credential",
		fmt.Sprintf("\"azure_container_name=%s\"", azureStore.containerName()),
	}
}

func (azureStore SparkAzureFileStore) Packages() []string {
	return []string{
		"--packages",
		"\"org.apache.hadoop:hadoop-azure:3.2.0\"",
	}
}

func (azureStore SparkAzureFileStore) Type() string {
	return "azure_blob_store"
}

func NewSparkGCSFileStore(config Config) (SparkFileStore, error) {
	fileStore, err := NewGCSFileStore(config)
	if err != nil {
		return nil, err
	}
	gcs, ok := fileStore.(*GCSFileStore)
	if !ok {
		return nil, fferr.NewInternalError(fmt.Errorf("could not cast file store to *GCSFileStore"))
	}
	serializedCredentials, err := json.Marshal(gcs.Credentials.JSON)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}

	return &SparkGCSFileStore{SerializedCredentials: serializedCredentials, GCSFileStore: gcs}, nil
}

type SparkGCSFileStore struct {
	SerializedCredentials []byte
	*GCSFileStore
}

func (gcs SparkGCSFileStore) SparkConfig() []string {
	return []string{
		"--spark_config",
		"fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
		"--spark_config",
		"fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
		"--spark_config",
		"fs.gs.auth.service.account.enable=true",
		"--spark_config",
		"fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
		"--spark_config",
		"fs.gs.auth.type=SERVICE_ACCOUNT_JSON_KEYFILE",
	}
}

func (gcs SparkGCSFileStore) CredentialsConfig() []string {
	base64Credentials := base64.StdEncoding.EncodeToString(gcs.SerializedCredentials)

	return []string{
		"--credential",
		fmt.Sprintf("\"gcp_project_id=%s\"", gcs.Credentials.ProjectId),
		"--credential",
		fmt.Sprintf("\"gcp_bucket_name=%s\"", gcs.Bucket),
		"--credential",
		fmt.Sprintf("\"gcp_credentials=%s\"", base64Credentials),
	}
}

func (gcs SparkGCSFileStore) Packages() []string {
	return []string{
		"--packages",
		"com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.0",
		"--jars",
		"/app/provider/scripts/spark/jars/gcs-connector-hadoop2-2.2.11-shaded.jar",
	}
}

func (gcs SparkGCSFileStore) Type() string {
	return "google_cloud_storage"
}

func NewSparkHDFSFileStore(config Config) (SparkFileStore, error) {
	fileStore, err := NewHDFSFileStore(config)
	if err != nil {
		return nil, err
	}
	hdfs, ok := fileStore.(*HDFSFileStore)
	if !ok {
		return nil, fferr.NewInternalError(fmt.Errorf("could not cast file store to *HDFSFileStore"))
	}

	return &SparkHDFSFileStore{hdfs}, nil
}

type SparkHDFSFileStore struct {
	*HDFSFileStore
}

func (hdfs SparkHDFSFileStore) SparkConfig() []string {
	return []string{}
}

func (hdfs SparkHDFSFileStore) CredentialsConfig() []string {
	return []string{}
}

func (hdfs SparkHDFSFileStore) Packages() []string {
	return []string{}
}

func (hdfs SparkHDFSFileStore) Type() string {
	return "hdfs"
}

func NewSparkLocalFileStore(config Config) (SparkFileStore, error) {
	fileStore, err := NewLocalFileStore(config)
	if err != nil {
		return nil, err
	}
	local, ok := fileStore.(*LocalFileStore)
	if !ok {
		return nil, fferr.NewInternalError(fmt.Errorf("could not cast file store to *LocalFileStore"))
	}

	return &SparkLocalFileStore{local}, nil
}

type SparkLocalFileStore struct {
	*LocalFileStore
}

func (local SparkLocalFileStore) SparkConfig() []string {
	return []string{}
}

func (local SparkLocalFileStore) CredentialsConfig() []string {
	return []string{}
}

func (local SparkLocalFileStore) Packages() []string {
	return []string{}
}

func (local SparkLocalFileStore) Type() string {
	return "local"
}

type SparkFileStoreConfig interface {
	Serialize() ([]byte, error)
	Deserialize(config pc.SerializedConfig) error
	IsFileStoreConfig() bool
}

func ResourcePath(id ResourceID) string {
	return fmt.Sprintf("%s/%s/%s", id.Type, id.Name, id.Variant)
}

type DatabricksResultState string

const (
	Success   DatabricksResultState = "SUCCESS"
	Failed    DatabricksResultState = "FAILED"
	Timeout   DatabricksResultState = "TIMEOUT"
	Cancelled DatabricksResultState = "CANCELLED"
)

type DatabricksExecutor struct {
	client             *databricks.WorkspaceClient
	cluster            string
	config             pc.DatabricksConfig
	errorMessageClient *dbClient.DatabricksClient
}

func (e *EMRExecutor) PythonFileURI(store SparkFileStore) (filestore.Filepath, error) {
	return nil, nil
}

// Need the bucket from here
func (db *DatabricksExecutor) PythonFileURI(store SparkFileStore) (filestore.Filepath, error) {
	relativePath := config.GetSparkRemoteScriptPath()
	filePath, err := store.CreateFilePath(relativePath, false)
	if err != nil {
		return nil, fmt.Errorf("could not create file path: %v", err)
	}
	if store.FilestoreType() == filestore.S3 {
		if err := filePath.SetScheme(filestore.S3Prefix); err != nil {
			return nil, fmt.Errorf("could not set scheme: %v", err)
		}
	}
	return filePath, nil
}

func readAndUploadFile(filePath filestore.Filepath, storePath filestore.Filepath, store SparkFileStore) error {
	fileExists, _ := store.Exists(storePath)
	if fileExists {
		return nil
	}

	f, err := os.Open(filePath.Key())
	if err != nil {
		return fferr.NewInternalError(err)
	}

	fileStats, err := f.Stat()
	if err != nil {
		return fferr.NewInternalError(err)
	}

	pythonScriptBytes := make([]byte, fileStats.Size())
	_, err = f.Read(pythonScriptBytes)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	if err := store.Write(storePath, pythonScriptBytes); err != nil {
		return err
	}
	// TODO(simba) use filepath String method once implemented
	fmt.Printf("Uploaded %v to %v\n", filePath, storePath)
	return nil
}

func (db *DatabricksExecutor) InitializeExecutor(store SparkFileStore) error {
	// We can't use CreateFilePath here because it calls Validate under the hood,
	// which will always fail given it's a local file without a valid scheme or bucket, for example.
	sparkLocalScriptPath := &filestore.LocalFilepath{}
	if err := sparkLocalScriptPath.SetKey(config.GetSparkLocalScriptPath()); err != nil {
		return err
	}
	sparkRemoteScriptPath, err := store.CreateFilePath(config.GetSparkRemoteScriptPath(), false)
	if err != nil {
		return err
	}
	pythonLocalInitScriptPath := &filestore.LocalFilepath{}
	if err := pythonLocalInitScriptPath.SetKey(config.GetPythonLocalInitPath()); err != nil {
		return err
	}
	pythonRemoteInitScriptPath := config.GetPythonRemoteInitPath()

	err = readAndUploadFile(sparkLocalScriptPath, sparkRemoteScriptPath, store)
	if err != nil {
		return err
	}
	sparkExists, err := store.Exists(sparkRemoteScriptPath)
	if err != nil || !sparkExists {
		return err
	}
	remoteInitScriptPathWithPrefix, err := store.CreateFilePath(pythonRemoteInitScriptPath, false)
	if err != nil {
		return err
	}
	err = readAndUploadFile(pythonLocalInitScriptPath, remoteInitScriptPathWithPrefix, store)
	if err != nil {
		return err
	}
	initExists, err := store.Exists(remoteInitScriptPathWithPrefix)
	if err != nil || !initExists {
		return err
	}
	return nil
}

func NewDatabricksExecutor(databricksConfig pc.DatabricksConfig) (SparkExecutor, error) {
	client := databricks.Must(
		databricks.NewWorkspaceClient(&databricks.Config{
			Host:     databricksConfig.Host,
			Token:    databricksConfig.Token,
			Username: databricksConfig.Username,
			Password: databricksConfig.Password,
		}))

	if err := re.Do(
		func() error {
			// Creating a new workspace client doesn't actually test that the client is able to successfully connect and communicate with
			// the cluster given the provided credentials; to fail earlier in the process (i.e. _before_ submitting a job) we'll make a call
			// to Databricks's Clusters API to get information about the cluster with the provided ID.
			_, err := client.Clusters.Get(context.Background(), compute.GetClusterRequest{ClusterId: databricksConfig.Cluster})
			if err != nil {
				// The Databricks SDK uses Go's "net/url" under the hood for parsing the hostname; this _can_ result in error messages that
				// are not very helpful. For example, if the hostname is "_https://my-hostname" the error message will be:
				// parse '_https://my-hostname': first path segment in URL cannot contain colon
				// To direct users to a solution, we'll check for message prefix 'parse' and provide a more helpful error message that wraps
				// the original error message.
				if strings.Contains(err.Error(), "parse") {
					parsingError := strings.TrimPrefix(err.Error(), "parse ")
					return fferr.NewInternalError(fmt.Errorf("the hostname %s is invalid and resulted in a parsing error (%s); check that the hostname is correct before trying again", databricksConfig.Host, parsingError))
				}
			}
			return nil
		},
		re.DelayType(func(n uint, err error, config *re.Config) time.Duration {
			return re.BackOffDelay(n, err, config)
		}),
		re.Attempts(5),
	); err != nil {
		fmt.Printf("failed to get cluster information for %s due to error: %v\n", databricksConfig.Cluster, err)
		return nil, err
	}

	errorMessageClient, err := dbClient.New(&dbConfig.Config{
		Host:     databricksConfig.Host,
		Token:    databricksConfig.Token,
		Username: databricksConfig.Username,
		Password: databricksConfig.Password,
	})
	if err != nil {
		fmt.Println("could not create error message client: ", err)
		errorMessageClient = nil
	}

	return &DatabricksExecutor{
		client:             client,
		cluster:            databricksConfig.Cluster,
		config:             databricksConfig,
		errorMessageClient: errorMessageClient,
	}, nil
}

func (db *DatabricksExecutor) RunSparkJob(args []string, store SparkFileStore) error {
	pythonFilepath, err := db.PythonFileURI(store)
	if err != nil {
		return err
	}
	pythonTask := jobs.SparkPythonTask{
		PythonFile: pythonFilepath.ToURI(),
		Parameters: args,
	}
	ctx := context.Background()
	id := uuid.New().String()

	jobToRun, err := db.client.Jobs.Create(ctx, jobs.CreateJob{
		Name: fmt.Sprintf("featureform-job-%s", id),
		Tasks: []jobs.JobTaskSettings{
			{
				TaskKey:           fmt.Sprintf("featureform-task-%s", id),
				ExistingClusterId: db.cluster,
				SparkPythonTask:   &pythonTask,
			},
		},
	})
	if err != nil {
		wrapped := fferr.NewExecutionError(pt.SparkOffline.String(), err)
		wrapped.AddDetail("job_name", fmt.Sprintf("featureform-job-%s", id))
		wrapped.AddDetail("job_id", fmt.Sprint(jobToRun.JobId))
		wrapped.AddDetail("executor_type", "Databricks")
		wrapped.AddDetail("store_type", store.Type())
		return wrapped
	}

	// Making the timeout a week because we don't want to timeout on long-running jobs
	weekTimeout := retries.Timeout[jobs.Run](168 * time.Hour)
	_, err = db.client.Jobs.RunNowAndWait(ctx, jobs.RunNow{
		JobId: jobToRun.JobId,
	}, weekTimeout)
	if err != nil {
		errorMessage := err
		if db.errorMessageClient != nil {
			errorMessage, err = db.getErrorMessage(jobToRun.JobId)
			if err != nil {
				fmt.Printf("the '%v' job failed, could not get error message: %v\n", jobToRun.JobId, err)
			}
		}
		wrapped := fferr.NewExecutionError(pt.SparkOffline.String(), fmt.Errorf("job failed: %v", errorMessage))
		wrapped.AddDetail("job_name", fmt.Sprintf("featureform-job-%s", id))
		wrapped.AddDetail("job_id", fmt.Sprint(jobToRun.JobId))
		wrapped.AddDetail("executor_type", "Databricks")
		wrapped.AddDetail("store_type", store.Type())
		return wrapped
	}

	return nil
}

func (db *DatabricksExecutor) getErrorMessage(jobId int64) (error, error) {
	ctx := context.Background()

	runRequest := jobs.ListRunsRequest{
		JobId: jobId,
	}

	runs, err := db.client.Jobs.ListRunsAll(ctx, runRequest)
	if err != nil {
		wrapped := fferr.NewExecutionError(pt.SparkOffline.String(), fmt.Errorf("could not get runs for job: %v", err))
		wrapped.AddDetail("job_id", fmt.Sprint(jobId))
		wrapped.AddDetail("executor_type", "Databricks")
		return nil, wrapped
	}

	if len(runs) == 0 {
		wrapped := fferr.NewInternalError(fmt.Errorf("no runs found for job"))
		wrapped.AddDetail("job_id", fmt.Sprint(jobId))
		wrapped.AddDetail("executor_type", "Databricks")
		return nil, wrapped
	}
	runID := runs[0].RunId
	request := jobs.GetRunRequest{
		RunId: runID,
	}

	// in order to get the status of the run output, we need to
	// use API v2.0 instead of v2.1. The version 2.1 does not allow
	// for getting the run output for multiple tasks. The following code
	// leverages version 2.0 of the API to get the run output.
	// we have created a github issue on the databricks-sdk-go repo
	// https://github.com/databricks/databricks-sdk-go/issues/375
	var runOutput jobs.RunOutput
	path := "/api/2.0/jobs/runs/get-output"
	err = db.errorMessageClient.Do(ctx, http.MethodGet, path, request, &runOutput)
	if err != nil {
		wrapped := fferr.NewExecutionError(pt.SparkOffline.String(), fmt.Errorf("could not get run output for job: %v", err))
		wrapped.AddDetail("job_id", fmt.Sprint(jobId))
		wrapped.AddDetail("executor_type", "Databricks")
		return nil, wrapped
	}

	return fmt.Errorf("%s", runOutput.Error), nil
}

type PythonOfflineQueries interface {
	materializationCreate(schema ResourceSchema) string
	trainingSetCreate(def TrainingSetDef, featureSchemas []ResourceSchema, labelSchema ResourceSchema) string
}

type defaultPythonOfflineQueries struct{}

func (q defaultPythonOfflineQueries) materializationCreate(schema ResourceSchema) (string, error) {
	timestampColumn := schema.TS
	if schema.TS == "" {
		data, err := os.ReadFile(config.GetMaterializeNoTimestampQueryPath())
		if err != nil {
			return "", err
		}
		return fmt.Sprintf(string(data), schema.Entity, schema.Value, schema.Entity), nil
	}
	data, err := os.ReadFile(config.GetMaterializeWithTimestampQueryPath())
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(string(data), schema.Entity, schema.Value, timestampColumn, "source_0"), nil
}

// Spark SQL _seems_ to have some issues with double quotes in column names based on troubleshooting
// the offline tests. Given this, we will use backticks to quote column names in the queries.
func createQuotedIdentifier(id ResourceID) string {
	return fmt.Sprintf("`%s__%s__%s`", id.Type, id.Name, id.Variant)
}

func (q defaultPythonOfflineQueries) trainingSetCreate(def TrainingSetDef, featureSchemas []ResourceSchema, labelSchema ResourceSchema) string {
	columns := make([]string, 0)
	joinQueries := make([]string, 0)
	feature_timestamps := make([]string, 0)
	for i, feature := range def.Features {
		featureColumnName := createQuotedIdentifier(feature)
		columns = append(columns, featureColumnName)
		var featureWindowQuery string
		// if no timestamp column, set to default generated by resource registration
		if featureSchemas[i].TS == "" {
			featureWindowQuery = fmt.Sprintf("SELECT * FROM (SELECT %s as t%d_entity, %s as %s, CAST(0 AS TIMESTAMP) as t%d_ts FROM source_%d) ORDER BY t%d_ts ASC", featureSchemas[i].Entity, i+1, featureSchemas[i].Value, featureColumnName, i+1, i+1, i+1)
		} else {
			featureWindowQuery = fmt.Sprintf("SELECT * FROM (SELECT %s as t%d_entity, %s as %s, %s as t%d_ts FROM source_%d) ORDER BY t%d_ts ASC", featureSchemas[i].Entity, i+1, featureSchemas[i].Value, featureColumnName, featureSchemas[i].TS, i+1, i+1, i+1)
		}
		featureJoinQuery := fmt.Sprintf("LEFT OUTER JOIN (%s) t%d ON (t%d_entity = entity AND t%d_ts <= label_ts)", featureWindowQuery, i+1, i+1, i+1)
		joinQueries = append(joinQueries, featureJoinQuery)
		feature_timestamps = append(feature_timestamps, fmt.Sprintf("t%d_ts", i+1))
	}
	for i, lagFeature := range def.LagFeatures {
		lagFeaturesOffset := len(def.Features)
		idx := slices.IndexFunc(def.Features, func(id ResourceID) bool {
			return id.Name == lagFeature.FeatureName && id.Variant == lagFeature.FeatureVariant
		})
		lagSource := fmt.Sprintf("source_%d", idx+1)
		lagColumnName := sanitize(lagFeature.LagName)
		if lagFeature.LagName == "" {
			lagColumnName = fmt.Sprintf("`%s_%s_lag_%s`", lagFeature.FeatureName, lagFeature.FeatureVariant, lagFeature.LagDelta)
		}
		columns = append(columns, lagColumnName)
		timeDeltaSeconds := lagFeature.LagDelta.Seconds() //parquet stores time as microseconds
		curIdx := lagFeaturesOffset + i + 1
		var lagWindowQuery string
		if featureSchemas[idx].TS == "" {
			lagWindowQuery = fmt.Sprintf("SELECT * FROM (SELECT %s as t%d_entity, %s as %s, CAST(0 AS TIMESTAMP) as t%d_ts FROM %s) ORDER BY t%d_ts ASC", featureSchemas[idx].Entity, curIdx, featureSchemas[idx].Value, lagColumnName, curIdx, lagSource, curIdx)
		} else {
			lagWindowQuery = fmt.Sprintf("SELECT * FROM (SELECT %s as t%d_entity, %s as %s, %s as t%d_ts FROM %s) ORDER BY t%d_ts ASC", featureSchemas[idx].Entity, curIdx, featureSchemas[idx].Value, lagColumnName, featureSchemas[idx].TS, curIdx, lagSource, curIdx)
		}
		lagJoinQuery := fmt.Sprintf("LEFT OUTER JOIN (%s) t%d ON (t%d_entity = entity AND (t%d_ts + INTERVAL %f SECOND) <= label_ts)", lagWindowQuery, curIdx, curIdx, curIdx, timeDeltaSeconds)
		joinQueries = append(joinQueries, lagJoinQuery)
		feature_timestamps = append(feature_timestamps, fmt.Sprintf("t%d_ts", curIdx))
	}
	columnStr := strings.Join(columns, ", ")
	joinQueryString := strings.Join(joinQueries, " ")
	var labelWindowQuery string
	if labelSchema.TS == "" {
		labelWindowQuery = fmt.Sprintf("SELECT %s AS entity, %s AS value, CAST(0 AS TIMESTAMP) AS label_ts FROM source_0", labelSchema.Entity, labelSchema.Value)
	} else {
		labelWindowQuery = fmt.Sprintf("SELECT %s AS entity, %s AS value, %s AS label_ts FROM source_0", labelSchema.Entity, labelSchema.Value, labelSchema.TS)
	}
	labelPartitionQuery := fmt.Sprintf("(SELECT * FROM (SELECT entity, value, label_ts FROM (%s) t ) t0)", labelWindowQuery)
	labelJoinQuery := fmt.Sprintf("%s %s", labelPartitionQuery, joinQueryString)

	timeStamps := strings.Join(feature_timestamps, ", ")
	timeStampsDesc := strings.Join(feature_timestamps, " DESC,")
	fullQuery := fmt.Sprintf("SELECT %s, value AS %s, entity, label_ts, %s, ROW_NUMBER() over (PARTITION BY entity, value, label_ts ORDER BY label_ts DESC, %s DESC) as row_number FROM (%s) tt", columnStr, createQuotedIdentifier(def.Label), timeStamps, timeStampsDesc, labelJoinQuery)
	finalQuery := fmt.Sprintf("SELECT %s, %s FROM (SELECT * FROM (SELECT *, row_number FROM (%s) WHERE row_number=1 ))  ORDER BY label_ts", columnStr, createQuotedIdentifier(def.Label), fullQuery)
	return finalQuery
}

type SparkOfflineStore struct {
	Executor SparkExecutor
	Store    SparkFileStore
	Logger   *zap.SugaredLogger
	query    *defaultPythonOfflineQueries
	BaseProvider
}

func (store *SparkOfflineStore) AsOfflineStore() (OfflineStore, error) {
	return store, nil
}

func (store *SparkOfflineStore) GetBatchFeatures(ids []ResourceID) (BatchFeatureIterator, error) {
	if len(ids) == 0 {
		return &FileStoreBatchServing{store: store.Store, iter: nil}, fferr.NewInternalError(fmt.Errorf("no feature ids provided"))
	}
	// Convert all IDs to materialization IDs
	materializationIDs := make([]ResourceID, len(ids))
	batchDir := ""
	for i, id := range ids {
		materializationIDs[i] = ResourceID{Name: id.Name, Variant: id.Variant, Type: FeatureMaterialization}
		batchDir += fmt.Sprintf("%s-%s", id.Name, id.Variant)
		if i != len(ids)-1 {
			batchDir += "-"
		}
	}
	// Convert materialization ID to file paths
	materializationPaths, err := store.createFilePathsFromIDs(materializationIDs)
	if err != nil {
		return nil, err
	}

	// Create a query that selects all features from the table
	query := createJoinQuery(len(ids))

	// Create output file path
	batchDirUUID := uuid.NewSHA1(uuid.NameSpaceDNS, []byte(batchDir))
	outputPath, err := store.Store.CreateFilePath(fmt.Sprintf("featureform/BatchFeatures/%s", batchDirUUID), true)
	if err != nil {
		return nil, err
	}

	// Submit arguments for a spark job
	sparkArgs, err := store.Executor.SparkSubmitArgs(outputPath, query, materializationPaths, BatchFeatures, store.Store)
	if err != nil {
		store.Logger.Errorw("Problem creating spark submit arguments", "error", err, "args", sparkArgs)
		return nil, err
	}

	// Run the spark job
	if err := store.Executor.RunSparkJob(sparkArgs, store.Store); err != nil {
		store.Logger.Errorw("Error running Spark job", "error", err)
		return nil, err
	}
	// Create a batch iterator that iterates through the dir
	outputFiles, err := store.Store.List(outputPath, filestore.Parquet)
	if err != nil {
		return nil, err
	}
	groups, err := filestore.NewFilePathGroup(outputFiles, filestore.DateTimeDirectoryGrouping)
	if err != nil {
		return nil, err
	}
	newest, err := groups.GetFirst()
	if err != nil {
		return nil, err
	}
	iterator, err := store.Store.Serve(newest)
	if err != nil {
		return nil, err
	}
	store.Logger.Debug("Successfully created batch iterator")
	return &FileStoreBatchServing{store: store.Store, iter: iterator, numFeatures: len(ids)}, nil
}

func (store *SparkOfflineStore) createFilePathsFromIDs(materializationIDs []ResourceID) ([]string, error) {
	materializationPaths := make([]string, len(materializationIDs))
	for i, id := range materializationIDs {
		path, err := store.Store.CreateFilePath(id.ToFilestorePath(), true)
		if err != nil {
			return nil, err
		}
		sourceFiles, err := store.Store.List(path, filestore.Parquet)
		if err != nil {
			return nil, err
		}
		groups, err := filestore.NewFilePathGroup(sourceFiles, filestore.DateTimeDirectoryGrouping)
		if err != nil {
			return nil, err
		}
		newest, err := groups.GetFirst()
		if err != nil {
			return nil, err
		}
		matDir, err := store.Store.CreateFilePath(newest[0].KeyPrefix(), true)
		if err != nil {
			return nil, err
		}
		materializationPaths[i] = matDir.ToURI()
	}
	return materializationPaths, nil
}

func createJoinQuery(numFeatures int) string {
	query := ""
	asEntity := ""
	withFeatures := ""
	joinTables := ""
	featureColumns := ""

	for i := 0; i < numFeatures; i++ {
		if i > 0 {
			joinTables += "FULL OUTER JOIN "
		}
		withFeatures += fmt.Sprintf(", source_%d.value AS feature%d, source_%d.ts AS TS%d ", i, i+1, i, i+1)
		featureColumns += fmt.Sprintf(", feature%d", i+1)
		joinTables += fmt.Sprintf("source_%d ", i)
		if i == 1 {
			joinTables += fmt.Sprintf("ON %s = source_%d.entity ", asEntity, i)
			asEntity += ", "
		}
		if i > 1 {
			joinTables += fmt.Sprintf("ON COALESCE(%s) = source_%d.entity ", asEntity, i)
			asEntity += ", "
		}
		asEntity += fmt.Sprintf("source_%d.entity", i)
	}
	if numFeatures == 1 {
		query = fmt.Sprintf("SELECT %s AS entity %s FROM source_0", asEntity, withFeatures)
	} else {
		query = fmt.Sprintf("SELECT COALESCE(%s) AS entity %s FROM %s", asEntity, withFeatures, joinTables)
	}
	return query
}

func (store *SparkOfflineStore) Close() error {
	return nil
}

// For Spark, the CheckHealth method must confirm 3 things:
// 1. The Spark executor is able to run a Spark job
// 2. The Spark job is able to read/write to the configured blob store
// 3. Backend business logic is able to read/write to the configured blob store
// To achieve this check, we'll perform the following steps:
// 1. Write to <blob-store>/featureform/HealthCheck/health_check.csv
// 2. Run a Spark job that reads from <blob-store>/featureform/HealthCheck/health_check.csv and
// writes to <blob-store>/featureform/HealthCheck/health_check_out.csv
func (store *SparkOfflineStore) CheckHealth() (bool, error) {
	healthCheckPath, err := store.Store.CreateFilePath("featureform/HealthCheck/health_check.csv", false)
	if err != nil {
		wrapped := fferr.NewInternalError(err)
		wrapped.AddDetail("store_type", store.Type().String())
		wrapped.AddDetail("action", "file_path_creation")
		return false, wrapped
	}
	csvBytes, err := store.getHealthCheckCSVBytes()
	if err != nil {
		return false, fmt.Errorf("failed to create mock CSV data for health check file: %v", err)
	}
	if err := store.Store.Write(healthCheckPath, csvBytes); err != nil {
		wrapped := fferr.NewConnectionError(store.Type().String(), err)
		wrapped.AddDetail("action", "write")
		return false, wrapped
	}
	healthCheckOutPath, err := store.Store.CreateFilePath("featureform/HealthCheck/health_check_out", true)
	fmt.Println("HEALTH CHECK PATHS: ", healthCheckOutPath.ToURI(), healthCheckPath.ToURI())
	if err != nil {
		wrapped := fferr.NewInternalError(err)
		wrapped.AddDetail("store_type", store.Type().String())
		wrapped.AddDetail("action", "file_path_creation")
		return false, wrapped
	}
	args, err := store.Executor.SparkSubmitArgs(healthCheckOutPath, "SELECT * FROM source_0", []string{healthCheckPath.ToURI()}, Transform, store.Store)
	if err != nil {
		return false, fmt.Errorf("failed to build arguments for Spark submit due to: %v", err)
	}
	if err := store.Executor.RunSparkJob(args, store.Store); err != nil {
		wrapped := fferr.NewConnectionError(store.Type().String(), err)
		wrapped.AddDetail("action", "job_submission")
		return false, wrapped
	}
	return true, nil
}

func (store *SparkOfflineStore) getHealthCheckCSVBytes() ([]byte, error) {
	buf := new(bytes.Buffer)
	w := csv.NewWriter(buf)
	records := [][]string{
		{"entity", "value", "ts"},
		{"entity1", "value1", "2020-01-01T00:00:00Z"},
		{"entity2", "value3", "2020-01-02T00:00:00Z"},
		{"entity3", "value3", "2020-01-03T00:00:00Z"},
	}
	if err := w.WriteAll(records); err != nil {
		return nil, fferr.NewInternalError(err)
	}
	return buf.Bytes(), nil
}

func sparkOfflineStoreFactory(config pc.SerializedConfig) (Provider, error) {
	sc := pc.SparkConfig{}
	logger := logging.NewLogger("spark")
	if err := sc.Deserialize(config); err != nil {
		logger.Errorw("Invalid config to initialize spark offline store", "error", err)
		return nil, err
	}
	logger.Infow("Creating Spark executor:", "type", sc.ExecutorType)
	exec, err := NewSparkExecutor(sc.ExecutorType, sc.ExecutorConfig, logger.SugaredLogger)
	if err != nil {
		logger.Errorw("Failure initializing Spark executor", "type", sc.ExecutorType, "error", err)
		return nil, err
	}

	logger.Infow("Creating Spark store:", "type", sc.StoreType)
	serializedFilestoreConfig, err := sc.StoreConfig.Serialize()
	if err != nil {
		return nil, err
	}
	store, err := CreateSparkFileStore(sc.StoreType, Config(serializedFilestoreConfig))
	if err != nil {
		logger.Errorw("Failure initializing blob store", "type", sc.StoreType, "error", err)
		return nil, err
	}
	logger.Info("Uploading Spark script to store")

	logger.Debugf("Store type: %s", sc.StoreType)
	if err := exec.InitializeExecutor(store); err != nil {
		logger.Errorw("Failure initializing executor", "error", err)
		return nil, err
	}
	logger.Info("Created Spark Offline Store")
	queries := defaultPythonOfflineQueries{}
	sparkOfflineStore := SparkOfflineStore{
		Executor: exec,
		Store:    store,
		Logger:   logger.SugaredLogger,
		query:    &queries,
		BaseProvider: BaseProvider{
			ProviderType:   pt.SparkOffline,
			ProviderConfig: config,
		},
	}
	return &sparkOfflineStore, nil
}

type SparkExecutor interface {
	RunSparkJob(args []string, store SparkFileStore) error
	InitializeExecutor(store SparkFileStore) error
	PythonFileURI(store SparkFileStore) (filestore.Filepath, error)
	SparkSubmitArgs(destPath filestore.Filepath, cleanQuery string, sourceList []string, jobType JobType, store SparkFileStore) ([]string, error)
	GetDFArgs(outputURI filestore.Filepath, code string, sources []string, store SparkFileStore) ([]string, error)
}

type EMRExecutor struct {
	client       *emr.Client
	clusterName  string
	logger       *zap.SugaredLogger
	logFileStore *FileStore
}

func (e EMRExecutor) InitializeExecutor(store SparkFileStore) error {
	e.logger.Info("Uploading PySpark script to filestore")
	sparkLocalScriptPath := &filestore.LocalFilepath{}
	if err := sparkLocalScriptPath.SetKey(config.GetSparkLocalScriptPath()); err != nil {
		return err
	}
	sparkRemoteScriptPath, err := store.CreateFilePath(config.GetSparkRemoteScriptPath(), false)
	if err != nil {
		return err
	}

	err = readAndUploadFile(sparkLocalScriptPath, sparkRemoteScriptPath, store)
	if err != nil {
		return err
	}
	scriptExists, err := store.Exists(sparkRemoteScriptPath)
	if err != nil || !scriptExists {
		return fferr.NewInternalError(fmt.Errorf("could not upload spark script: Path: %s, Error: %v", sparkRemoteScriptPath.ToURI(), err))
	}
	return nil
}

type SparkGenericExecutor struct {
	master        string
	deployMode    string
	pythonVersion string
	coreSite      string
	yarnSite      string
	logger        *zap.SugaredLogger
}

func (s *SparkGenericExecutor) InitializeExecutor(store SparkFileStore) error {
	s.logger.Info("Uploading PySpark script to filestore")
	// We can't use CreateFilePath here because it calls Validate under the hood,
	// which will always fail given it's a local file without a valid scheme or bucket, for example.
	sparkLocalScriptPath := &filestore.LocalFilepath{}
	if err := sparkLocalScriptPath.SetKey(config.GetSparkLocalScriptPath()); err != nil {
		return err
	}

	sparkRemoteScriptPath, err := store.CreateFilePath(config.GetSparkRemoteScriptPath(), false)
	if err != nil {
		return err
	}

	err = readAndUploadFile(sparkLocalScriptPath, sparkRemoteScriptPath, store)
	if err != nil {
		return err
	}
	scriptExists, err := store.Exists(sparkRemoteScriptPath)
	if err != nil || !scriptExists {
		return fferr.NewInternalError(fmt.Errorf("could not upload spark script: Path: %s, Error: %v", sparkRemoteScriptPath.ToURI(), err))
	}
	return nil
}

func (s *SparkGenericExecutor) getYarnCommand(args string) (string, error) {
	configDir, err := os.MkdirTemp("", "hadoop-conf")
	if err != nil {
		return "", fferr.NewInternalError(fmt.Errorf("could not create temp dir: %v", err))
	}
	coreSitePath := filepath.Join(configDir, "core-site.xml")
	err = os.WriteFile(coreSitePath, []byte(s.coreSite), 0644)
	if err != nil {
		return "", fferr.NewInternalError(fmt.Errorf("could not write core-site.xml: %v", err))
	}
	yarnSitePath := filepath.Join(configDir, "yarn-site.xml")
	err = os.WriteFile(yarnSitePath, []byte(s.yarnSite), 0644)
	if err != nil {
		return "", fferr.NewInternalError(fmt.Errorf("could not write core-site.xml: %v", err))
	}
	return fmt.Sprintf(""+
		"pyenv global %s && "+
		"export HADOOP_CONF_DIR=%s &&  "+
		"pyenv exec %s; "+
		"rm -r %s", s.pythonVersion, configDir, args, configDir), nil
}

func (s *SparkGenericExecutor) getGenericCommand(args string) string {
	return fmt.Sprintf("pyenv global %s && pyenv exec %s", s.pythonVersion, args)
}

func (s *SparkGenericExecutor) RunSparkJob(args []string, store SparkFileStore) error {
	bashCommand := "bash"
	sparkArgsString := strings.Join(args, " ")
	var commandString string

	if s.master == "yarn" {
		s.logger.Info("Running spark job on yarn")
		var err error
		commandString, err = s.getYarnCommand(sparkArgsString)
		if err != nil {
			return err
		}
	} else {
		commandString = s.getGenericCommand(sparkArgsString)
	}

	bashCommandArgs := []string{"-c", commandString}

	s.logger.Info("Executing spark-submit")
	cmd := exec.Command(bashCommand, bashCommandArgs...)
	cmd.Env = append(os.Environ(), "FEATUREFORM_LOCAL_MODE=true")

	var outb, errb bytes.Buffer
	cmd.Stdout = &outb
	cmd.Stderr = &errb

	err := cmd.Start()
	if err != nil {
		wrapped := fferr.NewExecutionError(pt.SparkOffline.String(), fmt.Errorf("could not run spark job: %v", err))
		wrapped.AddDetail("executor_type", "Spark Generic")
		wrapped.AddDetail("store_type", store.Type())
		return wrapped
	}

	err = cmd.Wait()
	if err != nil {
		wrapped := fferr.NewExecutionError(pt.SparkOffline.String(), fmt.Errorf("spark job failed: %v", err))
		wrapped.AddDetail("executor_type", "Spark Generic")
		wrapped.AddDetail("store_type", store.Type())
		wrapped.AddDetail("stdout", outb.String())
		wrapped.AddDetail("stderr", errb.String())
		return wrapped
	}

	return nil
}

func (s *SparkGenericExecutor) PythonFileURI(store SparkFileStore) (filestore.Filepath, error) {
	// not used for Spark Generic Executor
	return nil, nil
}

func (s *SparkGenericExecutor) SparkSubmitArgs(destPath filestore.Filepath, cleanQuery string, sourceList []string, jobType JobType, store SparkFileStore) ([]string, error) {
	s.logger.Debugw("SparkSubmitArgs", "destPath", destPath.ToURI(), "cleanQuery", cleanQuery, "sourceList", sourceList, "jobType", jobType, "store", store)
	argList := []string{
		"spark-submit",
		"--deploy-mode",
		s.deployMode,
		"--master",
		s.master,
	}

	packageArgs := store.Packages()
	argList = append(argList, packageArgs...) // adding any packages needed for filestores

	sparkScriptPathEnv := config.GetSparkLocalScriptPath()
	scriptArgs := []string{
		sparkScriptPathEnv,
		"sql",
		"--output_uri",
		destPath.ToURI(),
		"--sql_query",
		fmt.Sprintf("'%s'", cleanQuery),
		"--job_type",
		fmt.Sprintf("'%s'", jobType),
		"--store_type",
		store.Type(),
	}
	argList = append(argList, scriptArgs...)

	sparkConfigs := store.SparkConfig()
	argList = append(argList, sparkConfigs...)

	credentialConfigs := store.CredentialsConfig()
	argList = append(argList, credentialConfigs...)

	argList = append(argList, "--source_list")
	argList = append(argList, sourceList...)
	return argList, nil
}

func (s *SparkGenericExecutor) GetDFArgs(outputURI filestore.Filepath, code string, sources []string, store SparkFileStore) ([]string, error) {
	argList := []string{
		"spark-submit",
		"--deploy-mode",
		s.deployMode,
		"--master",
		s.master,
	}

	packageArgs := store.Packages()
	argList = append(argList, packageArgs...) // adding any packages needed for filestores

	sparkScriptPathEnv := config.GetSparkLocalScriptPath()

	scriptArgs := []string{
		sparkScriptPathEnv,
		"df",
		"--output_uri",
		outputURI.ToURI(),
		"--code",
		code,
		"--store_type",
		store.Type(),
	}
	argList = append(argList, scriptArgs...)

	sparkConfigs := store.SparkConfig()
	argList = append(argList, sparkConfigs...)

	credentialConfigs := store.CredentialsConfig()
	argList = append(argList, credentialConfigs...)

	argList = append(argList, "--source")
	argList = append(argList, sources...)

	return argList, nil
}

func NewSparkGenericExecutor(sparkGenericConfig pc.SparkGenericConfig, logger *zap.SugaredLogger) (SparkExecutor, error) {
	sparkGenericExecutor := SparkGenericExecutor{
		master:        sparkGenericConfig.Master,
		deployMode:    sparkGenericConfig.DeployMode,
		pythonVersion: sparkGenericConfig.PythonVersion,
		coreSite:      sparkGenericConfig.CoreSite,
		yarnSite:      sparkGenericConfig.YarnSite,
		logger:        logger,
	}
	return &sparkGenericExecutor, nil
}

func NewSparkExecutor(execType pc.SparkExecutorType, config pc.SparkExecutorConfig, logger *zap.SugaredLogger) (SparkExecutor, error) {
	switch execType {
	case pc.EMR:
		emrConfig, ok := config.(*pc.EMRConfig)
		if !ok {
			return nil, fferr.NewInternalError(fmt.Errorf("cannot convert config into 'EMRConfig'"))
		}
		return NewEMRExecutor(*emrConfig, logger)
	case pc.Databricks:
		databricksConfig, ok := config.(*pc.DatabricksConfig)
		if !ok {
			return nil, fferr.NewInternalError(fmt.Errorf("cannot convert config into 'DatabricksConfig'"))
		}
		return NewDatabricksExecutor(*databricksConfig)
	case pc.SparkGeneric:
		sparkGenericConfig, ok := config.(*pc.SparkGenericConfig)
		if !ok {
			return nil, fferr.NewInternalError(fmt.Errorf("cannot convert config into 'SparkGenericConfig'"))
		}
		return NewSparkGenericExecutor(*sparkGenericConfig, logger)
	default:
		return nil, fferr.NewInvalidArgumentError(fmt.Errorf("the executor type ('%s') is not supported", execType))
	}
}

func NewEMRExecutor(emrConfig pc.EMRConfig, logger *zap.SugaredLogger) (SparkExecutor, error) {
	awsAccessKeyId := emrConfig.Credentials.AWSAccessKeyId
	awsSecretKey := emrConfig.Credentials.AWSSecretKey

	client := emr.New(emr.Options{
		Region:      emrConfig.ClusterRegion,
		Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(awsAccessKeyId, awsSecretKey, "")),
	})

	var logFileStore *FileStore
	describeEMR, err := client.DescribeCluster(context.TODO(), &emr.DescribeClusterInput{
		ClusterId: aws.String(emrConfig.ClusterName),
	})
	if err != nil {
		logger.Infof("could not pull information about the cluster '%s': %s", emrConfig.ClusterName, err)
	} else if describeEMR.Cluster.LogUri != nil {
		logLocation := *describeEMR.Cluster.LogUri
		logFileStore, err = createLogS3FileStore(emrConfig.ClusterRegion, logLocation, awsAccessKeyId, awsSecretKey)
		if err != nil {
			logger.Infof("could not create log file store at '%s': %s", logLocation, err)
		}
	}

	emrExecutor := EMRExecutor{
		client:       client,
		logger:       logger,
		clusterName:  emrConfig.ClusterName,
		logFileStore: logFileStore,
	}
	return &emrExecutor, nil
}

func (e *EMRExecutor) RunSparkJob(args []string, store SparkFileStore) error {
	params := &emr.AddJobFlowStepsInput{
		JobFlowId: aws.String(e.clusterName), //returned by listclusters
		Steps: []emrTypes.StepConfig{
			{
				Name: aws.String("Featureform execution step"),
				HadoopJarStep: &emrTypes.HadoopJarStepConfig{
					Jar:  aws.String("command-runner.jar"), //jar file for running pyspark scripts
					Args: args,
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
	var waitDuration time.Duration = time.Hour * 3
	e.logger.Debugw("Waiting for EMR job to complete")
	stepCompleteWaiter := emr.NewStepCompleteWaiter(e.client)
	_, err = stepCompleteWaiter.WaitForOutput(context.TODO(), &emr.DescribeStepInput{
		ClusterId: aws.String(e.clusterName),
		StepId:    aws.String(stepId),
	}, waitDuration)
	if err != nil {
		errorMessage, getErr := e.getStepErrorMessage(e.clusterName, stepId)
		if getErr != nil {
			e.logger.Infof("could not get error message for EMR step '%s': %s", stepId, getErr)
		}
		if errorMessage != "" {
			wrapped := fferr.NewExecutionError(pt.SparkOffline.String(), fmt.Errorf("step failed: %s", errorMessage))
			wrapped.AddDetail("executor_type", "EMR")
			wrapped.AddDetail("store_type", store.Type())
			return wrapped
		}

		e.logger.Errorf("Failure waiting for completion of EMR cluster: %s", err)
		wrapped := fferr.NewExecutionError(pt.SparkOffline.String(), fmt.Errorf("failure waiting for completion of cluster: %w", err))
		wrapped.AddDetail("executor_type", "EMR")
		wrapped.AddDetail("store_type", store.Type())
		return wrapped
	}
	return nil
}

func (e *EMRExecutor) getStepErrorMessage(clusterId string, stepId string) (string, error) {
	if e.logFileStore == nil {
		return "", fferr.NewInternalError(fmt.Errorf("cannot get error message for EMR step '%s' because the log file store is not set", stepId))
	}

	stepResults, err := e.client.DescribeStep(context.TODO(), &emr.DescribeStepInput{
		ClusterId: aws.String(clusterId),
		StepId:    aws.String(stepId),
	})
	if err != nil {
		wrapped := fferr.NewExecutionError(pt.SparkOffline.String(), fmt.Errorf("could not get information on step: %w", err))
		wrapped.AddDetail("executor_type", "EMR")
		wrapped.AddDetail("cluster_id", clusterId)
		wrapped.AddDetail("step_id", stepId)
		return "", wrapped
	}

	if stepResults.Step.Status.State == "FAILED" {
		var errorMsg string
		// check if there are any errors
		if stepResults.Step.Status.FailureDetails.Message != nil {
			// get the error message
			errorMsg = *stepResults.Step.Status.FailureDetails.Message
		}
		if errorMsg != "" {
			return errorMsg, nil
		}

		if stepResults.Step.Status.FailureDetails.LogFile != nil {
			logFile := *stepResults.Step.Status.FailureDetails.LogFile

			errorMessage, err := e.getLogFileMessage(logFile)
			if err != nil {
				wrapped := fferr.NewExecutionError(pt.SparkOffline.String(), fmt.Errorf("could not get error message from log file: %v", err))
				wrapped.AddDetail("executor_type", "EMR")
				wrapped.AddDetail("cluster_id", clusterId)
				wrapped.AddDetail("step_id", stepId)
				wrapped.AddDetail("log_file", logFile)
				return "", wrapped
			}

			return errorMessage, nil
		}
	}

	return "", nil
}

func (e *EMRExecutor) getLogFileMessage(logFile string) (string, error) {
	outputFilepath := &filestore.S3Filepath{}
	err := outputFilepath.ParseFilePath(fmt.Sprintf("%s/stdout.gz", logFile))
	if err != nil {
		return "", err
	}

	err = e.waitForLogFile(outputFilepath)
	if err != nil {
		return "", err
	}

	logs, err := (*e.logFileStore).Read(outputFilepath)
	if err != nil {
		return "", err
	}

	// the output file is compressed so we need uncompress it
	errorMessage, err := compression.GunZip(logs)
	if err != nil {
		return "", fferr.NewInternalError(fmt.Errorf("could not uncompress error message: %v", err))
	}
	return errorMessage, nil
}

func (e *EMRExecutor) waitForLogFile(logFile filestore.Filepath) error {
	// wait until log file exists
	for {
		fileExists, err := (*e.logFileStore).Exists(logFile)
		if err != nil {
			return err
		}

		if fileExists {
			return nil
		}

		time.Sleep(2 * time.Second)
	}
}

func (e *EMRExecutor) SparkSubmitArgs(destPath filestore.Filepath, cleanQuery string, sourceList []string, jobType JobType, store SparkFileStore) ([]string, error) {
	e.logger.Debugw("SparkSubmitArgs", "destPath", destPath, "cleanQuery", cleanQuery, "sourceList", sourceList, "jobType", jobType, "store", store)
	argList := []string{
		"spark-submit",
		"--deploy-mode",
		"client",
	}

	packageArgs := removeEspaceCharacters(store.Packages())
	argList = append(argList, packageArgs...) // adding any packages needed for filestores

	sparkScriptPathEnv := config.GetSparkRemoteScriptPath()
	sparkScriptPath, err := store.CreateFilePath(sparkScriptPathEnv, false)
	if err != nil {
		return nil, err
	}
	scriptArgs := []string{
		sparkScriptPath.ToURI(),
		"sql",
		"--output_uri",
		destPath.ToURI(),
		"--sql_query",
		cleanQuery,
		"--job_type",
		string(jobType),
		"--store_type",
		store.Type(),
	}
	argList = append(argList, scriptArgs...)

	sparkConfigs := removeEspaceCharacters(store.SparkConfig())
	argList = append(argList, sparkConfigs...)

	credentialConfigs := removeEspaceCharacters(store.CredentialsConfig())
	argList = append(argList, credentialConfigs...)

	argList = append(argList, "--source_list")
	argList = append(argList, sourceList...)
	return argList, nil
}

func createLogS3FileStore(emrRegion string, s3LogLocation string, awsAccessKeyId string, awsSecretKey string) (*FileStore, error) {
	if s3LogLocation == "" {
		return nil, fmt.Errorf("s3 log location is empty")
	}
	s3FilePath := &filestore.S3Filepath{}
	err := s3FilePath.ParseFilePath(s3LogLocation)
	if err != nil {
		return nil, err
	}

	bucketName := s3FilePath.Bucket()
	path := s3FilePath.Key()

	logS3Config := pc.S3FileStoreConfig{
		Credentials:  pc.AWSCredentials{AWSAccessKeyId: awsAccessKeyId, AWSSecretKey: awsSecretKey},
		BucketRegion: emrRegion,
		BucketPath:   bucketName,
		Path:         path,
	}

	config, err := logS3Config.Serialize()
	if err != nil {
		return nil, err
	}

	logFileStore, err := NewS3FileStore(config)
	if err != nil {
		return nil, err
	}
	return &logFileStore, nil
}

func removeEspaceCharacters(values []string) []string {
	for i, v := range values {
		v = strings.Replace(v, "\\", "", -1)
		v = strings.Replace(v, "\"", "", -1)
		values[i] = v
	}
	return values
}

func (d *DatabricksExecutor) SparkSubmitArgs(destPath filestore.Filepath, cleanQuery string, sourceList []string, jobType JobType, store SparkFileStore) ([]string, error) {
	argList := []string{
		"sql",
		"--output_uri",
		destPath.ToURI(),
		"--job_type",
		string(jobType),
		"--store_type",
		store.Type(),
	}
	sparkConfigs := store.SparkConfig()
	argList = append(argList, sparkConfigs...)

	credentialConfigs := store.CredentialsConfig()
	argList = append(argList, credentialConfigs...)

	// Databricks's API enforces a 10K-byte limit on job submit params, so to avoid a 400, we need to check to ensure
	// the args are below this limit. If they exceed this limit, it's most likely due to the query and/or the list of
	// sources, so we write these as a JSON file and read them from the PySpark runner script to side-step this constraint
	if d.exceedsSubmitParamsTotalByteLimit(argList, cleanQuery, sourceList) {
		if store.FilestoreType() != filestore.S3 {
			return argList, fmt.Errorf("%s is not a currently support file store for writing submit params; supported types: %s", store.FilestoreType(), filestore.S3)
		}

		paramsPath, err := d.writeSubmitParamsToFileStore(cleanQuery, sourceList, store)
		if err != nil {
			return nil, err
		}

		argList = append(argList, "--submit_params_uri", paramsPath.Key())
	} else {
		argList = append(argList, "--sql_query", cleanQuery)
		argList = append(argList, "--source_list")
		argList = append(argList, sourceList...)
	}

	return argList, nil
}

func (d *DatabricksExecutor) exceedsSubmitParamsTotalByteLimit(argsList []string, query string, sources []string) bool {
	totalBytes := 0
	for _, str := range argsList {
		totalBytes += len(str)
	}

	totalBytes += len(query)

	for _, source := range sources {
		totalBytes += len(source)
	}

	return totalBytes >= SPARK_SUBMIT_PARAMS_BYTE_LIMIT
}

func (d *DatabricksExecutor) writeSubmitParamsToFileStore(query string, sources []string, store SparkFileStore) (filestore.Filepath, error) {
	paramsFileId := uuid.New()
	paramsPath, err := store.CreateFilePath(fmt.Sprintf("featureform/spark-submit-params/%s.json", paramsFileId.String()), false)
	if err != nil {
		return nil, err
	}
	paramsMap := map[string]interface{}{}
	paramsMap["sql_query"] = query
	paramsMap["source_list"] = sources

	data, err := json.Marshal(paramsMap)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}

	if err := store.Write(paramsPath, data); err != nil {
		return nil, err
	}

	return paramsPath, nil
}

func (spark *SparkOfflineStore) RegisterPrimaryFromSourceTable(id ResourceID, sourcePath string) (PrimaryTable, error) {
	return blobRegisterPrimary(id, sourcePath, spark.Logger, spark.Store)
}

func (spark *SparkOfflineStore) pysparkArgs(destinationURI string, templatedQuery string, sourceList []string, jobType JobType) *[]string {
	args := []string{}
	return &args
}

func (spark *SparkOfflineStore) RegisterResourceFromSourceTable(id ResourceID, schema ResourceSchema) (OfflineTable, error) {
	return blobRegisterResourceFromSourceTable(id, schema, spark.Logger, spark.Store)
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
		return fferr.NewInvalidArgumentError(fmt.Errorf("the transformation type '%v' is not supported", config.Type))
	}
}

func (spark *SparkOfflineStore) sqlTransformation(config TransformationConfig, isUpdate bool) error {
	updatedQuery, sources, err := spark.updateQuery(config.Query, config.SourceMapping)
	if err != nil {
		spark.Logger.Errorw("Could not generate updated query for spark transformation", "error", err)
		return err
	}
	transformationDestination, err := spark.Store.CreateFilePath(config.TargetTableID.ToFilestorePath(), true)
	if err != nil {
		return err
	}
	transformationExists, err := spark.Store.Exists(transformationDestination)
	if err != nil {
		return err
	}
	if !isUpdate && transformationExists {
		spark.Logger.Errorw("Creation when transformation already exists", "target", config.TargetTableID, "path", transformationDestination)
		return fferr.NewDatasetAlreadyExistsError(config.TargetTableID.Name, config.TargetTableID.Variant, fmt.Errorf(transformationDestination.ToURI()))
	} else if isUpdate && !transformationExists {
		spark.Logger.Errorw("Update job attempted when transformation does not exist", "target", config.TargetTableID, "path", transformationDestination)
		return fferr.NewDatasetNotFoundError(config.TargetTableID.Name, config.TargetTableID.Variant, fmt.Errorf(transformationDestination.ToURI()))
	}

	spark.Logger.Debugw("Running SQL transformation")
	sparkArgs, err := spark.Executor.SparkSubmitArgs(transformationDestination, updatedQuery, sources, JobType(Transform), spark.Store)
	if err != nil {
		spark.Logger.Errorw("Problem creating spark submit arguments", "error", err, "args", sparkArgs)
		return err
	}
	if err := spark.Executor.RunSparkJob(sparkArgs, spark.Store); err != nil {
		spark.Logger.Errorw("spark submit job for transformation failed to run", "target", config.TargetTableID, "error", err)
		return err
	}
	spark.Logger.Debugw("Successfully ran SQL transformation")
	return nil
}

func (spark *SparkOfflineStore) dfTransformation(config TransformationConfig, isUpdate bool) error {
	logger := spark.Logger.With("type", config.Type, "name", config.TargetTableID.Name, "variant", config.TargetTableID.Variant)
	logger.Debugw("Creating DF transformation")

	pickledTransformationPath, err := spark.Store.CreateFilePath(ps.ResourceToPicklePath(config.TargetTableID.Name, config.TargetTableID.Variant), false)
	if err != nil {
		return err
	}

	pickleExists, err := spark.Store.Exists(pickledTransformationPath)
	if err != nil {
		return err
	}

	// If the transformation is not an update, the pickle file should not exist yet
	datasetAlreadyExists := pickleExists && !isUpdate
	// If the transformation is an update, as it will be for scheduled transformation, the pickle file must exist
	datasetNotFound := !pickleExists && isUpdate

	if datasetAlreadyExists {
		logger.Errorw("Transformation already exists", config.TargetTableID, pickledTransformationPath.ToURI())
		return fferr.NewDatasetAlreadyExistsError(config.TargetTableID.Name, config.TargetTableID.Variant, fmt.Errorf(pickledTransformationPath.ToURI()))
	}

	if datasetNotFound {
		logger.Errorw("Transformation doesn't exists at destination but is being updated", config.TargetTableID, pickledTransformationPath.ToURI())
		return fferr.NewDatasetNotFoundError(config.TargetTableID.Name, config.TargetTableID.Variant, fmt.Errorf(pickledTransformationPath.ToURI()))
	}

	// It's important to set the scheme to s3:// here because the runner script uses boto3 to read the file, and it expects an s3:// path
	if err := pickledTransformationPath.SetScheme(filestore.S3Prefix); err != nil {
		return err
	}

	if err := spark.Store.Write(pickledTransformationPath, config.Code); err != nil {
		return err
	}

	logger.Debugw("Successfully wrote transformation pickle file", "path", pickledTransformationPath.ToURI())

	sources, err := spark.getSources(config.SourceMapping)
	if err != nil {
		return err
	}

	transformationDestinationPath := ps.ResourceToDirectoryPath(config.TargetTableID.Type.String(), config.TargetTableID.Name, config.TargetTableID.Variant)
	transformationDestination, err := spark.Store.CreateFilePath(transformationDestinationPath, true)
	if err != nil {
		return err
	}
	logger.Debugw("Transformation destination path", "path", transformationDestination.ToURI())

	sparkArgs, err := spark.Executor.GetDFArgs(transformationDestination, pickledTransformationPath.Key(), sources, spark.Store)
	if err != nil {
		logger.Errorw("error getting spark dataframe arguments", err)
		return err
	}
	logger.Debugw("Running DF transformation")
	if err := spark.Executor.RunSparkJob(sparkArgs, spark.Store); err != nil {
		logger.Errorw("error running Spark dataframe job", "error", err)
		return err
	}
	logger.Debugw("Successfully ran transformation", "type", config.Type, "name", config.TargetTableID.Name, "variant", config.TargetTableID.Variant)
	return nil
}

// TODO: _possibly_ delete this method in favor of Filepath methods
func (spark *SparkOfflineStore) getSources(mapping []SourceMapping) ([]string, error) {
	sources := []string{}

	for _, m := range mapping {
		sourcePath, err := spark.getSourcePath(m.Source)
		if err != nil {
			spark.Logger.Errorw("Error getting source path for spark source", "source", m.Source, "error", err)
			return nil, err
		}

		sources = append(sources, sourcePath)
	}
	return sources, nil
}

// TODO: _possibly_ delete this method in favor of Filepath methods
func (spark *SparkOfflineStore) updateQuery(query string, mapping []SourceMapping) (string, []string, error) {
	sources := make([]string, len(mapping))
	replacements := make([]string, len(mapping)*2) // It's times 2 because each replacement will be a pair; (original, replacedValue)

	for i, m := range mapping {
		replacements = append(replacements, m.Template)
		replacements = append(replacements, fmt.Sprintf("source_%v", i))

		sourcePath, err := spark.getSourcePath(m.Source)
		if err != nil {
			spark.Logger.Errorw("Error getting source path of spark source", m.Source, err)
			return "", nil, err
		}

		sources[i] = sourcePath
	}

	replacer := strings.NewReplacer(replacements...)
	updatedQuery := replacer.Replace(query)

	if strings.Contains(updatedQuery, "{{") {
		spark.Logger.Errorw("Template replace failed", updatedQuery)
		err := fferr.NewInternalError(fmt.Errorf("template replacement error"))
		err.AddDetail("Query", updatedQuery)
		return "", nil, err
	}
	return updatedQuery, sources, nil
}

// TODO: delete this method in favor of Filepath methods
func (spark *SparkOfflineStore) getSourcePath(path string) (string, error) {
	fileType, fileName, fileVariant := spark.getResourceInformationFromFilePath(path)
	var filePath string
	if fileType == "primary" {
		fileResourceId := ResourceID{Name: fileName, Variant: fileVariant, Type: Primary}
		fileTable, err := spark.GetPrimaryTable(fileResourceId)
		if err != nil {
			spark.Logger.Errorw("Issue getting primary table", fileResourceId, err)
			return "", err
		}
		fsPrimary, ok := fileTable.(*FileStorePrimaryTable)
		if !ok {
			return "", fferr.NewInternalError(fmt.Errorf("expected primary table to be a FileStorePrimaryTable"))
		}
		filePath, err := fsPrimary.GetSource()
		if err != nil {
			return "", err
		}
		return filePath.ToURI(), nil
	} else if fileType == "transformation" {
		fileResourceId := ResourceID{Name: fileName, Variant: fileVariant, Type: Transformation}

		transformationDirPath, err := spark.Store.CreateFilePath(fileResourceId.ToFilestorePath(), true)
		if err != nil {
			return "", err
		}

		transformationPath, err := spark.Store.NewestFileOfType(transformationDirPath, filestore.Parquet)
		if err != nil {
			return "", err
		}
		exists, err := spark.Store.Exists(transformationPath)
		if err != nil {
			spark.Logger.Errorf("could not check if transformation file exists: %v", err)
			return "", err
		}
		if !exists {
			spark.Logger.Errorf("transformation file does not exist: %s", transformationPath.ToURI())
			return "", fmt.Errorf("transformation file does not exist: %s", transformationPath.ToURI())
		}
		transformationDirPathDateTime, err := spark.Store.CreateFilePath(transformationPath.KeyPrefix(), true)
		if err != nil {
			return "", err
		}
		return transformationDirPathDateTime.ToURI(), nil
	} else {
		return filePath, fferr.NewDatasetNotFoundError("", "", fmt.Errorf("could not find path for %s; fileType: %s, fileName: %s, fileVariant: %s", path, fileType, fileName, fileVariant))
	}
}

// TODO: delete this method in favor of Filepath methods
func (spark *SparkOfflineStore) getResourceInformationFromFilePath(path string) (string, string, string) {
	var fileType string
	var fileName string
	var fileVariant string
	containsSlashes := strings.Contains(path, "/")
	if strings.HasPrefix(path, filestore.AzureBlobPrefix) {
		id := ResourceID{}
		err := id.FromFilestorePath(path)
		if err != nil {
			spark.Logger.Errorf("could not construct ResourceID for Azure Blob Storage path %s due to %v", path, err)
			return "", "", ""
		}
		fileType, fileName, fileVariant = strings.ToLower(id.Type.String()), id.Name, id.Variant
	} else if strings.HasPrefix(path, filestore.S3Prefix) {
		id := ResourceID{}
		err := id.FromFilestorePath(path)
		if err != nil {
			spark.Logger.Errorf("could not construct ResourceID for S3 path %s due to %v", path, err)
			return "", "", ""
		}
		fileType, fileName, fileVariant = strings.ToLower(id.Type.String()), id.Name, id.Variant
	} else if strings.HasPrefix(path, filestore.S3APrefix) {
		id := ResourceID{}
		err := id.FromFilestorePath(path)
		if err != nil {
			spark.Logger.Errorf("could not construct ResourceID for S3A path %s due to %v", path, err)
			return "", "", ""
		}
		fileType, fileName, fileVariant = strings.ToLower(id.Type.String()), id.Name, id.Variant
	} else if strings.HasPrefix(path, filestore.HDFSPrefix) {
		filePaths := strings.Split(path[len(filestore.HDFSPrefix):], "/")
		if len(filePaths) <= 4 {
			return "", "", ""
		}
		fileType, fileName, fileVariant = strings.ToLower(filePaths[2]), filePaths[3], filePaths[4]
	} else if strings.HasPrefix(path, filestore.GSPrefix) {
		id := ResourceID{}
		err := id.FromFilestorePath(path)
		if err != nil {
			spark.Logger.Errorf("could not construct ResourceID for Google Cloud Storage path %s due to %v", path, err)
			return "", "", ""
		}
		fileType, fileName, fileVariant = strings.ToLower(id.Type.String()), id.Name, id.Variant
	} else if containsSlashes {
		filePaths := strings.Split(path[len("featureform/"):], "/")
		if len(filePaths) <= 2 {
			return "", "", ""
		}
		fileType, fileName, fileVariant = strings.ToLower(filePaths[0]), filePaths[1], filePaths[2]
	} else {
		filePaths := strings.Split(path[len("featureform_"):], "__")
		if len(filePaths) <= 2 {
			return "", "", ""
		}
		fileType, fileName, fileVariant = filePaths[0], filePaths[1], filePaths[2]
	}
	return fileType, fileName, fileVariant
}

func (spark *SparkOfflineStore) ResourceLocation(id ResourceID) (string, error) {
	path, err := spark.Store.CreateFilePath(id.ToFilestorePath(), true)
	if err != nil {
		return "", errors.Wrap(err, "could not create dir path")
	}

	newestFile, err := spark.Store.NewestFileOfType(path, filestore.Parquet)
	if err != nil {
		return "", errors.Wrap(err, "could not get newest file")
	}

	newestFileDirPathDateTime, err := spark.Store.CreateFilePath(newestFile.KeyPrefix(), true)
	if err != nil {
		return "", fmt.Errorf("could not create directory path for spark newestFile: %v", err)
	}
	return newestFileDirPathDateTime.ToURI(), nil
}

func (e *EMRExecutor) GetDFArgs(outputURI filestore.Filepath, code string, sources []string, store SparkFileStore) ([]string, error) {
	argList := []string{
		"spark-submit",
		"--deploy-mode",
		"client",
	}

	packageArgs := removeEspaceCharacters(store.Packages())
	argList = append(argList, packageArgs...) // adding any packages needed for filestores

	sparkScriptPathEnv := config.GetSparkRemoteScriptPath()
	sparkScriptPath, err := store.CreateFilePath(sparkScriptPathEnv, false)
	if err != nil {
		return nil, err
	}
	codePath := strings.Replace(code, filestore.S3APrefix, filestore.S3Prefix, -1)

	scriptArgs := []string{
		sparkScriptPath.ToURI(),
		"df",
		"--output_uri",
		outputURI.ToURI(),
		"--code",
		codePath,
		"--store_type",
		store.Type(),
	}
	argList = append(argList, scriptArgs...)

	sparkConfigs := removeEspaceCharacters(store.SparkConfig())
	argList = append(argList, sparkConfigs...)

	credentialConfigs := removeEspaceCharacters(store.CredentialsConfig())
	argList = append(argList, credentialConfigs...)

	argList = append(argList, "--source")
	argList = append(argList, sources...)

	return argList, nil
}

func (d *DatabricksExecutor) GetDFArgs(outputURI filestore.Filepath, code string, sources []string, store SparkFileStore) ([]string, error) {
	argList := []string{
		"df",
		"--output_uri",
		outputURI.ToURI(),
		"--code",
		code,
		"--store_type",
		store.Type(),
	}

	sparkConfigs := store.SparkConfig()
	argList = append(argList, sparkConfigs...)

	credentialConfigs := store.CredentialsConfig()
	argList = append(argList, credentialConfigs...)

	argList = append(argList, "--source")
	argList = append(argList, sources...)

	return argList, nil
}

func (spark *SparkOfflineStore) GetTransformationTable(id ResourceID) (TransformationTable, error) {
	transformationPath, err := spark.Store.CreateFilePath(id.ToFilestorePath(), true)
	if err != nil {
		return nil, err
	}
	spark.Logger.Debugw("Retrieved transformation source", "id", id, "filePath", transformationPath.ToURI())
	return &FileStorePrimaryTable{spark.Store, transformationPath, TableSchema{}, true, id}, nil
}

func (spark *SparkOfflineStore) UpdateTransformation(config TransformationConfig) error {
	return spark.transformation(config, true)
}

// TODO: add a comment akin to the one explaining the logic for CreateResourceTable
// **NOTE:** Unlike the pathway for registering a primary table from a data source that previously existed in the filestore, this
// method controls the location of the data source that will be written to once the primary table (i.e. a file that simply holds the
// fully qualified URL pointing to the source file), so it's important to consider what pattern we adopt here.
func (spark *SparkOfflineStore) CreatePrimaryTable(id ResourceID, schema TableSchema) (PrimaryTable, error) {
	if err := id.check(Primary); err != nil {
		return nil, err
	}
	primaryTableFilepath, err := spark.Store.CreateFilePath(id.ToFilestorePath(), false)
	if err != nil {
		return nil, err
	}
	if exists, err := spark.Store.Exists(primaryTableFilepath); err != nil {
		return nil, err
	} else if exists {
		return nil, fferr.NewDatasetAlreadyExistsError(id.Name, id.Variant, fmt.Errorf(primaryTableFilepath.ToURI()))
	}
	// Create a URL in the same directory as the primary table that follows the naming convention <VARIANT>_src.parquet
	schema.SourceTable = fmt.Sprintf("%s/%s/src.parquet", primaryTableFilepath.ToURI(), time.Now().Format("2006-01-02-15-04-05-999999"))
	data, err := schema.Serialize()
	if err != nil {
		return nil, err
	}
	err = spark.Store.Write(primaryTableFilepath, data)
	if err != nil {
		return nil, err
	}
	return &FileStorePrimaryTable{spark.Store, primaryTableFilepath, schema, false, id}, nil
}

func (spark *SparkOfflineStore) GetPrimaryTable(id ResourceID) (PrimaryTable, error) {
	return fileStoreGetPrimary(id, spark.Store, spark.Logger)
}

// Unlike a resource table created from a source table, which is effectively a pointer in the filestore to the source table
// with the names of the entity, value and timestamp columns, the resource table created by this method is the data itself.
// This requires a means of differentiating between the two types of resource tables such that we know when/how to read one
// versus the other.
//
// Currently, a resource table created from a source table is located at /featureform/Feature/<NAME DIR>/<VARIANT FILE>, where
// <VARIANT FILE>:
// * has no file extension
// * is the serialization JSON representation of the struct ResourceSchema (i.e. {"Entity":"entity","Value":"value","TS":"ts","SourceTable":"abfss://..."})
//
// One option is the keep with the above pattern by populating "SourceTable" with the path to a source table contained in a subdirectory of
// the resource directory in the pattern Spark uses (i.e. /featureform/Feature/<NAME DIR>/<VARIANT DIR>/<DATETIME DIR>/src.parquet).
func (spark *SparkOfflineStore) CreateResourceTable(id ResourceID, schema TableSchema) (OfflineTable, error) {
	if err := id.check(Feature, Label); err != nil {
		return nil, err
	}
	resourceTableFilepath, err := spark.Store.CreateFilePath(id.ToFilestorePath(), false)
	if err != nil {
		return nil, err
	}
	if exists, err := spark.Store.Exists(resourceTableFilepath); err != nil {
		return nil, err
	} else if exists {
		return nil, fferr.NewDatasetAlreadyExistsError(id.Name, id.Variant, fmt.Errorf(resourceTableFilepath.ToURI()))
	}
	table := BlobOfflineTable{
		store: spark.Store,
		schema: ResourceSchema{
			// Create a URI in the same directory as the resource table that follows the naming convention <VARIANT>_src.parquet
			SourceTable: fmt.Sprintf("%s/%s/src.parquet", resourceTableFilepath.ToURI(), time.Now().Format("2006-01-02-15-04-05-999999")),
		},
	}
	for _, col := range schema.Columns {
		switch col.Name {
		case string(Entity):
			table.schema.Entity = col.Name
		case string(Value):
			table.schema.Value = col.Name
		case string(TS):
			table.schema.TS = col.Name
		default:
			// TODO: verify the assumption that col.Name should be:
			// * Entity ("entity")
			// * Value ("value")
			// * TS ("ts")
			// makes sense in the context of the schema
			return nil, fmt.Errorf("unexpected column name: %s", col.Name)
		}
	}
	data, err := table.schema.Serialize()
	if err != nil {
		return nil, err
	}
	err = spark.Store.Write(resourceTableFilepath, data)
	if err != nil {
		return nil, err
	}
	return &table, nil
}

func (spark *SparkOfflineStore) GetResourceTable(id ResourceID) (OfflineTable, error) {
	return fileStoreGetResourceTable(id, spark.Store, spark.Logger)
}

func blobSparkMaterialization(id ResourceID, spark *SparkOfflineStore, isUpdate bool, outputFormat filestore.FileType, shouldIncludeHeaders bool) (Materialization, error) {
	if err := id.check(Feature); err != nil {
		spark.Logger.Errorw("Attempted to create a materialization of a non feature resource", "type", id.Type)
		return nil, err
	}
	resourceTable, err := spark.GetResourceTable(id)
	if err != nil {
		spark.Logger.Errorw("Attempted to fetch resource table of non registered resource", "error", err)
		return nil, err
	}
	sparkResourceTable, ok := resourceTable.(*BlobOfflineTable)
	if !ok {
		spark.Logger.Errorw("Could not convert resource table to blob offline table", "id", id)
		return nil, fferr.NewInternalError(fmt.Errorf("could not convert offline table with id %v to sparkResourceTable", id))
	}
	// get destination path for the materialization
	materializationID := ResourceID{Name: id.Name, Variant: id.Variant, Type: FeatureMaterialization}
	destinationPath, err := spark.Store.CreateFilePath(materializationID.ToFilestorePath(), true)
	if err != nil {
		return nil, err
	}
	materializationExists, err := spark.Store.Exists(destinationPath)
	if err != nil {
		return nil, err
	}
	if materializationExists && !isUpdate {
		spark.Logger.Errorw("Attempted to create a materialization that already exists", "id", id)
		return nil, fferr.NewDatasetAlreadyExistsError(id.Name, id.Variant, fmt.Errorf(destinationPath.ToURI()))
	} else if !materializationExists && isUpdate {
		spark.Logger.Errorw("Attempted to update a materialization that doesn't exists", "id", id)
		return nil, fferr.NewDatasetNotFoundError(id.Name, id.Variant, fmt.Errorf(destinationPath.ToURI()))
	}
	materializationQuery, err := spark.query.materializationCreate(sparkResourceTable.schema)
	if err != nil {
		return nil, err
	}
	sourcePath, err := filestore.NewEmptyFilepath(spark.Store.FilestoreType())
	if err != nil {
		return nil, err
	}

	var sourceURIs []string
	if sourcePath.IsDir() {
		err = sourcePath.ParseDirPath(sparkResourceTable.schema.SourceTable)
		if err != nil {
			return nil, err
		}
		spark.Logger.Debugw("Parsed source table path:", "sourceTablePath", sourcePath.ToURI(), "sourceTable", sparkResourceTable.schema.SourceTable)
		// TODO: Refactor this into a separate method
		sourceFiles, err := spark.Store.List(sourcePath, filestore.Parquet)
		if err != nil {
			return nil, err
		}
		groups, err := filestore.NewFilePathGroup(sourceFiles, filestore.DateTimeDirectoryGrouping)
		if err != nil {
			return nil, err
		}
		newest, err := groups.GetFirst()
		if err != nil {
			return nil, err
		}
		sourceUris := make([]string, len(newest))
		for i, sourceFile := range newest {
			sourceUris[i] = sourceFile.ToURI()
		}
		sourceURIs = sourceUris
	} else {
		err = sourcePath.ParseFilePath(sparkResourceTable.schema.SourceTable)
		if err != nil {
			return nil, err
		}
		sourceURIs = append(sourceURIs, sourcePath.ToURI())
	}
	spark.Logger.Debugw("Fetched source files of type", "latestSourcePath", sourcePath.ToURI(), "fileFound", len(sourceURIs), "fileType", filestore.Parquet)
	sparkArgs, err := spark.Executor.SparkSubmitArgs(destinationPath, materializationQuery, sourceURIs, Materialize, spark.Store)
	if err != nil {
		spark.Logger.Errorw("Problem creating spark submit arguments", "error", err, "args", sparkArgs)
		return nil, err
	}
	// The default value for output_format in offline_store_spark_runner.py is parquet,
	// so it's only necessary to append CSV in this case; if we support more output formats
	// (e.g. JSON), then we should refactor this to a method and append in all cases.
	if outputFormat == filestore.CSV {
		sparkArgs = append(sparkArgs, "--output_format", string(outputFormat))
	}
	// The default value for headers in offline_store_spark_runner.py is "include"
	if !shouldIncludeHeaders {
		sparkArgs = append(sparkArgs, "--headers", "exclude")
	}
	if isUpdate {
		spark.Logger.Debugw("Updating materialization", "id", id)
	} else {
		spark.Logger.Debugw("Creating materialization", "id", id)
	}

	if err := spark.Executor.RunSparkJob(sparkArgs, spark.Store); err != nil {
		spark.Logger.Errorw("Spark submit job failed to run", "error", err)
		return nil, err
	}
	exists, err := spark.Store.Exists(destinationPath)
	if err != nil {
		spark.Logger.Errorf("could not check if materialization file exists: %v", err)
		return nil, err
	}
	if !exists {
		spark.Logger.Errorf("materialization not found in directory: %s", destinationPath.ToURI())
		return nil, fferr.NewDatasetNotFoundError(materializationID.Name, materializationID.Variant, fmt.Errorf("materialization not found in directory: %s", destinationPath.ToURI()))
	}
	spark.Logger.Debugw("Successfully created materialization", "id", id)
	return &FileStoreMaterialization{materializationID, spark.Store}, nil
}

func (spark *SparkOfflineStore) CreateMaterialization(id ResourceID, options ...MaterializationOptions) (Materialization, error) {
	var option MaterializationOptions
	var outputFormat filestore.FileType
	shouldIncludeHeaders := true
	if len(options) > 0 {
		option = options[0]
		if option.StoreType() != spark.Type() {
			return nil, fferr.NewInternalError(fmt.Errorf("expected options for store type %s but received options for %s instead", spark.Type(), option.StoreType()))
		}
		outputFormat = option.Output()
		shouldIncludeHeaders = option.ShouldIncludeHeaders()
	} else {
		outputFormat = filestore.Parquet
	}
	return blobSparkMaterialization(id, spark, false, outputFormat, shouldIncludeHeaders)
}

func (spark *SparkOfflineStore) GetMaterialization(id MaterializationID) (Materialization, error) {
	return fileStoreGetMaterialization(id, spark.Store, spark.Logger)
}

func (spark *SparkOfflineStore) UpdateMaterialization(id ResourceID) (Materialization, error) {
	return blobSparkMaterialization(id, spark, true, filestore.Parquet, true)
}

func (spark *SparkOfflineStore) DeleteMaterialization(id MaterializationID) error {
	return fileStoreDeleteMaterialization(id, spark.Store, spark.Logger)
}

func (spark *SparkOfflineStore) getResourceSchema(id ResourceID) (ResourceSchema, error) {
	if err := id.check(Feature, Label); err != nil {
		return ResourceSchema{}, err
	}
	spark.Logger.Debugw("Getting resource schema", "id", id)
	table, err := spark.GetResourceTable(id)
	if err != nil {
		spark.Logger.Errorw("Resource not registered in spark store", "id", id, "error", err)
		return ResourceSchema{}, err
	}
	sparkResourceTable, ok := table.(*BlobOfflineTable)
	if !ok {
		spark.Logger.Errorw("could not convert offline table to sparkResourceTable", "id", id)
		return ResourceSchema{}, fferr.NewInternalError(fmt.Errorf("could not convert offline table with id %v to sparkResourceTable", id))
	}
	spark.Logger.Debugw("Successfully retrieved resource schema", "id", id)
	return sparkResourceTable.schema, nil
}

func sparkTrainingSet(def TrainingSetDef, spark *SparkOfflineStore, isUpdate bool) error {
	if err := def.check(); err != nil {
		spark.Logger.Errorw("Training set definition not valid", "definition", def, "error", err)
		return err
	}
	sourcePaths := make([]string, 0)
	featureSchemas := make([]ResourceSchema, 0)
	destinationPath, err := spark.Store.CreateFilePath(def.ID.ToFilestorePath(), true)
	if err != nil {
		return err
	}
	trainingSetExists, err := spark.Store.Exists(destinationPath)
	if err != nil {
		return err
	}
	if trainingSetExists && !isUpdate {
		spark.Logger.Errorw("Training set already exists", "id", def.ID)
		return fferr.NewDatasetAlreadyExistsError(def.ID.Name, def.ID.Variant, fmt.Errorf(destinationPath.ToURI()))
	} else if !trainingSetExists && isUpdate {
		spark.Logger.Errorw("Training set does not exist", "id", def.ID)
		return fferr.NewDatasetNotFoundError(def.ID.Name, def.ID.Variant, fmt.Errorf(destinationPath.ToURI()))
	}
	labelSchema, err := spark.getResourceSchema(def.Label)
	if err != nil {
		spark.Logger.Errorw("Could not get schema of label in spark store", "label", def.Label, "error", err)
		return err
	}
	// NOTE: labelSchema.SourceTable should be the absolute path to the label source table
	labelSourcePath := labelSchema.SourceTable
	filepath, err := filestore.NewEmptyFilepath(spark.Store.FilestoreType())
	if err != nil {
		return err
	}
	err = filepath.ParseFilePath(labelSourcePath)
	if err != nil {
		// Labels derived from transformations registered prior to PR #947 will not have a full path; given Spark requires an absolute path, we will
		// assume an error here means the value of SourceTable is just the relative path and attempt to construct the absolute path
		// prior to adding it to the list of source paths
		if filepath, err := spark.Store.CreateFilePath(labelSchema.SourceTable, false); err == nil {
			labelSourcePath = filepath.ToURI()
		} else {
			return err
		}
	}
	sourcePaths = append(sourcePaths, labelSourcePath)
	for _, feature := range def.Features {
		featureSchema, err := spark.getResourceSchema(feature)
		if err != nil {
			spark.Logger.Errorw("Could not get schema of feature in spark store", "feature", feature, "error", err)
			return err
		}
		featureSourcePath := featureSchema.SourceTable
		// NOTE: featureSchema.SourceTable should be the absolute path to the feature source table
		filepath, err := filestore.NewEmptyFilepath(spark.Store.FilestoreType())
		if err != nil {
			return err
		}
		// Features registered prior to PR #947 will not have a full path; given Spark requires an absolute path, we will
		// assume an error here means the value of SourceTable is just the relative path and attempt to construct the absolute path
		// prior to adding it to the list of source paths
		err = filepath.ParseFilePath(featureSourcePath)
		if err != nil {
			if filepath, err := spark.Store.CreateFilePath(featureSchema.SourceTable, false); err == nil {
				featureSourcePath = filepath.ToURI()
			} else {
				return err
			}
		}
		sourcePaths = append(sourcePaths, featureSourcePath)
		featureSchemas = append(featureSchemas, featureSchema)
	}
	trainingSetQuery := spark.query.trainingSetCreate(def, featureSchemas, labelSchema)

	sparkArgs, err := spark.Executor.SparkSubmitArgs(destinationPath, trainingSetQuery, sourcePaths, CreateTrainingSet, spark.Store)
	if err != nil {
		spark.Logger.Errorw("Problem creating spark submit arguments", "error", err, "args", sparkArgs)
		return err
	}

	spark.Logger.Debugw("Creating training set", "definition", def)
	if err := spark.Executor.RunSparkJob(sparkArgs, spark.Store); err != nil {
		spark.Logger.Errorw("Spark submit training set job failed to run", "definition", def.ID, "error", err)
		return err
	}
	trainingSetExists, err = spark.Store.Exists(destinationPath)
	if err != nil {
		return err
	}
	if !trainingSetExists {
		spark.Logger.Errorw("Could not get training set resource key in offline store")
		return fferr.NewDatasetNotFoundError(def.ID.Name, def.ID.Variant, fmt.Errorf(destinationPath.ToURI()))
	}
	spark.Logger.Debugw("Successfully created training set", "definition", def, "newestTrainingSet", destinationPath.ToURI())
	return nil
}

func (spark *SparkOfflineStore) CreateTrainingSet(def TrainingSetDef) error {
	return sparkTrainingSet(def, spark, false)

}

func (spark *SparkOfflineStore) UpdateTrainingSet(def TrainingSetDef) error {
	return sparkTrainingSet(def, spark, true)
}

func (spark *SparkOfflineStore) GetTrainingSet(id ResourceID) (TrainingSetIterator, error) {
	return fileStoreGetTrainingSet(id, spark.Store, spark.Logger)
}

func (spark *SparkOfflineStore) CreateTrainTestSplit(def TrainTestSplitDef) (func() error, error) {
	return nil, fmt.Errorf("not Implemented")
}

func (spark *SparkOfflineStore) GetTrainTestSplit(def TrainTestSplitDef) (TrainingSetIterator, TrainingSetIterator, error) {
	return nil, nil, fmt.Errorf("not Implemented")
}

func sanitizeSparkSQL(name string) string {
	return name
}
