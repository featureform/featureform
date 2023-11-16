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

	"github.com/featureform/filestore"
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
	"github.com/featureform/helpers/compression"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
)

type JobType string

const (
	Materialize       JobType = "Materialization"
	Transform         JobType = "Transformation"
	CreateTrainingSet JobType = "Training Set"
)

const MATERIALIZATION_ID_SEGMENTS = 3
const ENTITY_INDEX = 0
const VALUE_INDEX = 1
const TIMESTAMP_INDEX = 2

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
		return nil, fmt.Errorf("factory does not exist: %s", fsType)
	}
	FileStore, err := factory(config)
	if err != nil {
		return nil, err
	}
	return FileStore, nil
}

func NewSparkS3FileStore(config Config) (SparkFileStore, error) {
	fileStore, err := NewS3FileStore(config)
	if err != nil {
		return nil, err
	}
	s3, ok := fileStore.(*S3FileStore)
	if !ok {
		return nil, fmt.Errorf("could not cast file store to *S3FileStore")
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
		return nil, fmt.Errorf("could not cast file store to *AzureFileStore")
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
		return nil, fmt.Errorf("could not create gcs file store: %v", err)
	}
	gcs, ok := fileStore.(*GCSFileStore)
	if !ok {
		return nil, fmt.Errorf("could not cast file store to *GCSFileStore")
	}
	serializedCredentials, err := json.Marshal(gcs.Credentials.JSON)
	if err != nil {
		return nil, fmt.Errorf("could not serialize the credentials")
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
		return nil, fmt.Errorf("could not create hdfs file store: %v", err)
	}
	hdfs, ok := fileStore.(*HDFSFileStore)
	if !ok {
		return nil, fmt.Errorf("could not cast file store to *HDFSFileStore")
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
		return nil, fmt.Errorf("could not create local file store: %v", err)
	}
	local, ok := fileStore.(*LocalFileStore)
	if !ok {
		return nil, fmt.Errorf("could not cast file store to *LocalFileStore")
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
	filePath := config.GetSparkRemoteScriptPath()
	return store.CreateFilePath(filePath)
}

func readAndUploadFile(filePath filestore.Filepath, storePath filestore.Filepath, store SparkFileStore) error {
	fileExists, _ := store.Exists(storePath)
	if fileExists {
		return nil
	}

	f, err := os.Open(filePath.Key())
	if err != nil {
		return fmt.Errorf("could not open file: %v", err)
	}

	fileStats, err := f.Stat()
	if err != nil {
		return fmt.Errorf("could not get file stats: %v", err)
	}

	pythonScriptBytes := make([]byte, fileStats.Size())
	_, err = f.Read(pythonScriptBytes)
	if err != nil {
		return err
	}
	if err := store.Write(storePath, pythonScriptBytes); err != nil {
		return err
	}
	fmt.Printf("Uploaded %s to %s\n", filePath, storePath)
	return nil
}

func (db *DatabricksExecutor) InitializeExecutor(store SparkFileStore) error {
	// We can't use CreateFilePath here because it calls Validate under the hood,
	// which will always fail given it's a local file without a valid scheme or bucket, for example.
	sparkLocalScriptPath := &filestore.LocalFilepath{}
	if err := sparkLocalScriptPath.SetKey(config.GetSparkLocalScriptPath()); err != nil {
		return fmt.Errorf("could not create local script path: %v", err)
	}
	sparkRemoteScriptPath, err := store.CreateFilePath(config.GetSparkRemoteScriptPath())
	if err != nil {
		return fmt.Errorf("could not create remote script path: %v", err)
	}
	pythonLocalInitScriptPath := &filestore.LocalFilepath{}
	if err := pythonLocalInitScriptPath.SetKey(config.GetPythonLocalInitPath()); err != nil {
		return fmt.Errorf("could not create local init script path: %v", err)
	}
	if err != nil {
		return fmt.Errorf("could not create local init script path: %v", err)
	}
	pythonRemoteInitScriptPath := config.GetPythonRemoteInitPath()
	err = readAndUploadFile(sparkLocalScriptPath, sparkRemoteScriptPath, store)
	if err != nil {
		return fmt.Errorf("could not upload '%s' to '%s': %v", sparkLocalScriptPath.Key(), sparkRemoteScriptPath.ToURI(), err)
	}
	sparkExists, err := store.Exists(sparkRemoteScriptPath)
	if err != nil || !sparkExists {
		return fmt.Errorf("could not upload spark script: Path: %s, Error: %v", sparkRemoteScriptPath.ToURI(), err)
	}
	remoteInitScriptPathWithPrefix, err := store.CreateFilePath(pythonRemoteInitScriptPath)
	if err != nil {
		return fmt.Errorf("could not create remote init script path: %v", err)
	}
	err = readAndUploadFile(pythonLocalInitScriptPath, remoteInitScriptPathWithPrefix, store)
	if err != nil {
		return fmt.Errorf("could not upload '%s' to '%s': %v", pythonLocalInitScriptPath.Key(), remoteInitScriptPathWithPrefix, err)
	}
	initExists, err := store.Exists(remoteInitScriptPathWithPrefix)
	if err != nil || !initExists {
		return fmt.Errorf("could not upload python initialization script: Path: %s, Error: %v", remoteInitScriptPathWithPrefix, err)
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
					return fmt.Errorf("the hostname %s is invalid and resulted in a parsing error (%s); check that the hostname is correct before trying again", databricksConfig.Host, parsingError)
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
		return fmt.Errorf("could not get python file path: %v", err)
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
		return fmt.Errorf("error creating job: %v", err)
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

		return fmt.Errorf("the '%v' job failed: %v", jobToRun.JobId, errorMessage)
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
		return nil, fmt.Errorf("could not get run id: %v", err)
	}

	if len(runs) == 0 {
		return nil, fmt.Errorf("no runs found for job id: %v", jobId)
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
		return nil, fmt.Errorf("could not get run output: %v", err)
	}

	return fmt.Errorf("%s", runOutput.Error), nil
}

type PythonOfflineQueries interface {
	materializationCreate(schema ResourceSchema) string
	trainingSetCreate(def TrainingSetDef, featureSchemas []ResourceSchema, labelSchema ResourceSchema) string
}

type defaultPythonOfflineQueries struct{}

func (q defaultPythonOfflineQueries) materializationCreate(schema ResourceSchema) string {
	timestampColumn := schema.TS
	if schema.TS == "" {
		// If the schema lacks a timestamp, we assume each entity only has single entry. The
		// below query enforces this assumption by:
		// 1. Adding a row number to each row (this currently relies on the implicit ordering of the rows) - order_rows CTE
		// 2. Grouping by entity and selecting the max row number for each entity - max_row_per_entity CTE
		// 3. Joining the max row number back to the original table and selecting only the rows with the max row number - final select
		return fmt.Sprintf(`WITH ordered_rows AS (
				SELECT 
					%s AS entity,
					%s AS value,
					0 AS ts,
					ROW_NUMBER() over (PARTITION BY %s ORDER BY (SELECT NULL)) AS row_number
				FROM
					source_0
			),
			max_row_per_entity AS (
				SELECT 
					entity,
					MAX(row_number) AS max_row
				FROM
					ordered_rows
				GROUP BY
					entity
			)
			SELECT
				ord.entity 
				,ord.value 
				,ord.ts
			FROM
				max_row_per_entity maxr
			JOIN ordered_rows ord
				ON ord.entity = maxr.entity AND ord.row_number = maxr.max_row
			ORDER BY
				maxr.max_row DESC`, schema.Entity, schema.Value, schema.Entity)
	}
	return fmt.Sprintf(`SELECT
							entity
							,value
							,ts
						FROM (
								SELECT entity,
									value,
									ts,
									ROW_NUMBER() OVER (
										PARTITION BY entity
										ORDER BY ts DESC
									) AS rn2
								FROM (
										SELECT entity,
											value,
											ts,
											rn
										FROM (
												SELECT %s AS entity,
													%s AS value,
													%s AS ts,
													ROW_NUMBER() OVER (
														ORDER BY (
																SELECT NULL
															)
													) AS rn
												FROM %s
											) t
										ORDER BY rn DESC
									) t2
							) t3
						WHERE rn2 = 1`, schema.Entity, schema.Value, timestampColumn, "source_0")
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
	healthCheckPath, err := store.Store.CreateFilePath("featureform/HealthCheck/health_check.csv")
	if err != nil {
		return false, NewProviderError(Internal, store.Type(), FilePathCreation, fmt.Sprintf("failed to create file path for health check file due to: %v", err))
	}
	csvBytes, err := store.getHealthCheckCSVBytes()
	if err != nil {
		return false, fmt.Errorf("failed to create mock CSV data for health check file: %v", err)
	}
	if err := store.Store.Write(healthCheckPath, csvBytes); err != nil {
		return false, NewProviderError(Connection, store.Type(), Write, fmt.Sprintf("failed to write health check file due to: %v", err))
	}
	healthCheckOutPath, err := store.Store.CreateDirPath("featureform/HealthCheck/health_check_out")
	fmt.Println("HEALTH CHECK PATHS: ", healthCheckOutPath.ToURI(), healthCheckPath.ToURI())
	if err != nil {
		return false, NewProviderError(Internal, store.Type(), FilePathCreation, fmt.Sprintf("failed to create file path for health check output file due to: %v", err))
	}
	args, err := store.Executor.SparkSubmitArgs(healthCheckOutPath, "SELECT * FROM source_0", []string{healthCheckPath.ToURI()}, Transform, store.Store)
	if err != nil {
		return false, fmt.Errorf("failed to build arguments for Spark submit due to: %v", err)
	}
	if err := store.Executor.RunSparkJob(args, store.Store); err != nil {
		return false, NewProviderError(Connection, store.Type(), JobSubmission, fmt.Sprintf("failed to read health check file due to: %v", err))
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
		return nil, fmt.Errorf("could not write health check csv: %v", err)
	}
	return buf.Bytes(), nil
}

func sparkOfflineStoreFactory(config pc.SerializedConfig) (Provider, error) {
	sc := pc.SparkConfig{}
	logger := logging.NewLogger("spark")
	if err := sc.Deserialize(config); err != nil {
		logger.Errorw("Invalid config to initialize spark offline store", "error", err)
		return nil, NewProviderError(Runtime, "SPARK_OFFLINE", ConfigDeserialize, err.Error())
	}
	logger.Infow("Creating Spark executor:", "type", sc.ExecutorType)
	exec, err := NewSparkExecutor(sc.ExecutorType, sc.ExecutorConfig, logger)
	if err != nil {
		logger.Errorw("Failure initializing Spark executor", "type", sc.ExecutorType, "error", err)
		return nil, NewProviderError(Connection, pt.SparkOffline, ClientInitialization, err.Error())
	}

	logger.Infow("Creating Spark store:", "type", sc.StoreType)
	serializedFilestoreConfig, err := sc.StoreConfig.Serialize()
	if err != nil {
		return nil, NewProviderError(Runtime, pt.SparkOffline, ConfigSerialize, err.Error())
	}
	store, err := CreateSparkFileStore(sc.StoreType, Config(serializedFilestoreConfig))
	if err != nil {
		logger.Errorw("Failure initializing blob store", "type", sc.StoreType, "error", err)
		return nil, NewProviderError(Connection, pt.SparkOffline, ClientInitialization, fmt.Sprintf("failed to initialize blob store due to: %v", err))
	}
	logger.Info("Uploading Spark script to store")

	logger.Debugf("Store type: %s", sc.StoreType)
	if err := exec.InitializeExecutor(store); err != nil {
		logger.Errorw("Failure initializing executor", "error", err)
		return nil, NewProviderError(Connection, pt.SparkOffline, ClientInitialization, err.Error())
	}
	logger.Info("Created Spark Offline Store")
	queries := defaultPythonOfflineQueries{}
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
		return fmt.Errorf("could not create local script path: %v", err)
	}
	sparkRemoteScriptPath, err := store.CreateFilePath(config.GetSparkRemoteScriptPath())
	if err != nil {
		return fmt.Errorf("could not create file path: %v", err)
	}

	err = readAndUploadFile(sparkLocalScriptPath, sparkRemoteScriptPath, store)
	if err != nil {
		return fmt.Errorf("could not upload '%s' to '%s': %v", sparkLocalScriptPath.Key(), sparkRemoteScriptPath.ToURI(), err)
	}
	scriptExists, err := store.Exists(sparkRemoteScriptPath)
	if err != nil || !scriptExists {
		return fmt.Errorf("could not upload spark script: Path: %s, Error: %v", sparkRemoteScriptPath.ToURI(), err)
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
		return fmt.Errorf("could not create local script path: %v", err)
	}

	sparkRemoteScriptPath, err := store.CreateFilePath(config.GetSparkRemoteScriptPath())
	if err != nil {
		return fmt.Errorf("could not create file path: %v", err)
	}

	err = readAndUploadFile(sparkLocalScriptPath, sparkRemoteScriptPath, store)
	if err != nil {
		return fmt.Errorf("could not upload '%s' to '%s': %v", sparkLocalScriptPath.Key(), sparkRemoteScriptPath.ToURI(), err)
	}
	scriptExists, err := store.Exists(sparkRemoteScriptPath)
	if err != nil || !scriptExists {
		return fmt.Errorf("could not upload spark script: Path: %s, Error: %v", sparkRemoteScriptPath.ToURI(), err)
	}
	return nil
}

func (s *SparkGenericExecutor) getYarnCommand(args string) (string, error) {
	configDir, err := os.MkdirTemp("", "hadoop-conf")
	if err != nil {
		return "", fmt.Errorf("could not create temp dir: %v", err)
	}
	coreSitePath := filepath.Join(configDir, "core-site.xml")
	err = os.WriteFile(coreSitePath, []byte(s.coreSite), 0644)
	if err != nil {
		return "", fmt.Errorf("could not write core-site.xml: %v", err)
	}
	yarnSitePath := filepath.Join(configDir, "yarn-site.xml")
	err = os.WriteFile(yarnSitePath, []byte(s.yarnSite), 0644)
	if err != nil {
		return "", fmt.Errorf("could not write core-site.xml: %v", err)
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
			return fmt.Errorf("could not run yarn job: %v", err)
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
		return fmt.Errorf("could not run spark job: %v", err)
	}

	err = cmd.Wait()
	if err != nil {
		return fmt.Errorf("spark job failed: %v : stdout %s : stderr %s", err, outb.String(), errb.String())
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
			return nil, fmt.Errorf("cannot convert config into 'EMRConfig'")
		}
		return NewEMRExecutor(*emrConfig, logger)
	case pc.Databricks:
		databricksConfig, ok := config.(*pc.DatabricksConfig)
		if !ok {
			return nil, fmt.Errorf("cannot convert config into 'DatabricksConfig'")
		}
		return NewDatabricksExecutor(*databricksConfig)
	case pc.SparkGeneric:
		sparkGenericConfig, ok := config.(*pc.SparkGenericConfig)
		if !ok {
			return nil, fmt.Errorf("cannot convert config into 'SparkGenericConfig'")
		}
		return NewSparkGenericExecutor(*sparkGenericConfig, logger)
	default:
		return nil, fmt.Errorf("the executor type ('%s') is not supported", execType)
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
			return fmt.Errorf("the EMR step '%s' failed: %s", stepId, errorMessage)
		}

		e.logger.Errorf("Failure waiting for completion of EMR cluster: %s", err)
		return fmt.Errorf("failure waiting for completion of EMR cluster: %s", err)
	}
	return nil
}

func (e *EMRExecutor) getStepErrorMessage(clusterId string, stepId string) (string, error) {
	if e.logFileStore == nil {
		return "", fmt.Errorf("cannot get error message for EMR step '%s' because the log file store is not set", stepId)
	}

	stepResults, err := e.client.DescribeStep(context.TODO(), &emr.DescribeStepInput{
		ClusterId: aws.String(clusterId),
		StepId:    aws.String(stepId),
	})
	if err != nil {
		return "", fmt.Errorf("could not get information on EMR step '%s'", stepId)
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
				return "", fmt.Errorf("could not get error message from log file '%s': %v", logFile, err)
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
		return "", fmt.Errorf("could not parse log file path '%s': %v", logFile, err)
	}

	err = e.waitForLogFile(outputFilepath)
	if err != nil {
		return "", fmt.Errorf("could not wait for log file '%s' to be available: %v", outputFilepath.ToURI(), err)
	}

	logs, err := (*e.logFileStore).Read(outputFilepath)
	if err != nil {
		return "", fmt.Errorf("could not read log file in '%s' bucket at '%s' path: %v", outputFilepath.Bucket(), outputFilepath.ToURI(), err)
	}

	// the output file is compressed so we need uncompress it
	errorMessage, err := compression.GunZip(logs)
	if err != nil {
		return "", fmt.Errorf("could not uncompress error message: %v", err)
	}
	return errorMessage, nil
}

func (e *EMRExecutor) waitForLogFile(logFile filestore.Filepath) error {
	// wait until log file exists
	for {
		fileExists, err := (*e.logFileStore).Exists(logFile)
		if err != nil {
			return fmt.Errorf("could not determine if file '%s' exists: %v", logFile, err)
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
	sparkScriptPath, err := store.CreateFilePath(sparkScriptPathEnv)
	if err != nil {
		return nil, fmt.Errorf("could not create file path for '%s': %v", sparkScriptPathEnv, err)
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
		return nil, fmt.Errorf("could not parse file path '%s': %v", s3LogLocation, err)
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
		return nil, fmt.Errorf("could not serialize s3 file store config: %v", err)
	}

	logFileStore, err := NewS3FileStore(config)
	if err != nil {
		return nil, fmt.Errorf("could not create s3 file store (bucket: %s, path: %s) for emr logs: %v", err, bucketName, path)
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
		"--sql_query",
		cleanQuery,
		"--job_type",
		string(jobType),
		"--store_type",
		store.Type(),
	}
	sparkConfigs := store.SparkConfig()
	argList = append(argList, sparkConfigs...)

	credentialConfigs := store.CredentialsConfig()
	argList = append(argList, credentialConfigs...)

	argList = append(argList, "--source_list")
	argList = append(argList, sourceList...)
	return argList, nil
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
		return fmt.Errorf("the transformation type '%v' is not supported", config.Type)
	}
}

func (spark *SparkOfflineStore) sqlTransformation(config TransformationConfig, isUpdate bool) error {
	updatedQuery, sources, err := spark.updateQuery(config.Query, config.SourceMapping)
	if err != nil {
		spark.Logger.Errorw("Could not generate updated query for spark transformation", "error", err)
		return err
	}
	transformationDestination, err := spark.Store.CreateDirPath(config.TargetTableID.ToFilestorePath())
	if err != nil {
		return fmt.Errorf("could not create file path for spark transformation: %v", err)
	}
	transformationExists, err := spark.Store.Exists(transformationDestination)
	if err != nil {
		return fmt.Errorf("could not check if transformation exists: %v", err)
	}
	if !isUpdate && transformationExists {
		spark.Logger.Errorw("Creation when transformation already exists", "target", config.TargetTableID, "path", transformationDestination)
		return fmt.Errorf("transformation %v already exists at %s", config.TargetTableID, transformationDestination)
	} else if isUpdate && !transformationExists {
		spark.Logger.Errorw("Update job attempted when transformation does not exist", "target", config.TargetTableID, "path", transformationDestination)
		return fmt.Errorf("transformation %v doesn't exist at %s and you are trying to update", config.TargetTableID, transformationDestination)
	}

	spark.Logger.Debugw("Running SQL transformation")
	sparkArgs, err := spark.Executor.SparkSubmitArgs(transformationDestination, updatedQuery, sources, JobType(Transform), spark.Store)
	if err != nil {
		spark.Logger.Errorw("Problem creating spark submit arguments", "error", err)
		return fmt.Errorf("error with getting spark submit arguments %v", sparkArgs)
	}
	if err := spark.Executor.RunSparkJob(sparkArgs, spark.Store); err != nil {
		spark.Logger.Errorw("spark submit job for transformation failed to run", "target", config.TargetTableID, "error", err)
		return fmt.Errorf("spark submit job for transformation %v failed to run: %v", config.TargetTableID, err)
	}
	spark.Logger.Debugw("Successfully ran SQL transformation")
	return nil
}

// TODO: determine if we can delete this function
func GetTransformationFileLocation(id ResourceID) string {
	return fmt.Sprintf("featureform/DFTransformations/%s/%s", id.Name, id.Variant)
}

func (spark *SparkOfflineStore) dfTransformation(config TransformationConfig, isUpdate bool) error {
	transformationDestination, err := spark.Store.CreateFilePath(config.TargetTableID.ToFilestorePath())
	if err != nil {
		return fmt.Errorf("could not create file path for spark transformation: %v", err)
	}
	spark.Logger.Infow("Transformation Destination", "dest", transformationDestination)
	// TODO: understand why the trailing slash is needed
	// transformationDestinationWithSlash := strings.Join([]string{transformationDestination.ToURI(), ""}, "/")
	// spark.Logger.Infow("Transformation Destination With Slash", "dest", transformationDestinationWithSlash)

	transformationDirPath, err := spark.Store.CreateDirPath(GetTransformationFileLocation(config.TargetTableID))
	if err != nil {
		return fmt.Errorf("could not create directory path for spark transformation: %v", err)
	}

	transformationExists, err := spark.Store.Exists(transformationDirPath)
	if err != nil {
		return fmt.Errorf("error checking if transformation file exists")
	}
	spark.Logger.Infow("Transformation file", "dest", transformationDirPath.ToURI())
	if !isUpdate && transformationExists {
		spark.Logger.Errorw("Transformation already exists", config.TargetTableID, transformationDestination.ToURI())
		return fmt.Errorf("transformation %v already exists at %s", config.TargetTableID, transformationDestination.ToURI())
	} else if isUpdate && !transformationExists {
		spark.Logger.Errorw("Transformation doesn't exists at destination and you are trying to update", config.TargetTableID, transformationDestination.ToURI())
		return fmt.Errorf("transformation %v doesn't exist at %s and you are trying to update", config.TargetTableID, transformationDestination.ToURI())
	}

	pklFilepath, err := spark.Store.CreateFilePath(fmt.Sprintf("featureform/DFTransformations/%s/%s/transformation.pkl", config.TargetTableID.Name, config.TargetTableID.Variant))
	if err != nil {
		return fmt.Errorf("could not create file path for spark transformation: %v", err)
	}
	spark.Logger.Infow("Transformation file path", "dest", pklFilepath.ToURI())
	if err := spark.Store.Write(pklFilepath, config.Code); err != nil {
		return fmt.Errorf("could not upload file: %s", err)
	}

	sources, err := spark.getSources(config.SourceMapping)
	if err != nil {
		return fmt.Errorf("could not get sources for df transformation. Error: %v", err)
	}

	sparkArgs, err := spark.Executor.GetDFArgs(transformationDestination, pklFilepath.Key(), sources, spark.Store)
	if err != nil {
		spark.Logger.Errorw("Problem creating spark dataframe arguments", err)
		return fmt.Errorf("error with getting df arguments %v", sparkArgs)
	}
	spark.Logger.Debugw("Running DF transformation")
	if err := spark.Executor.RunSparkJob(sparkArgs, spark.Store); err != nil {
		spark.Logger.Errorw("Error running Spark dataframe job", "error", err)
		return fmt.Errorf("spark submit job for transformation failed to run: (name: %s variant:%s) %v", config.TargetTableID.Name, config.TargetTableID.Variant, err)
	}
	spark.Logger.Debugw("Successfully ran transformation", "type", config.Type, "name", config.TargetTableID.Name, "variant", config.TargetTableID.Variant)
	return nil
}

// TODO: _possibly_ delete this method in favor of Filepath methods
func (spark *SparkOfflineStore) getSources(mapping []SourceMapping) ([]string, error) {
	sources := []string{}

	for _, m := range mapping {
		sourcePath, err := spark.getSourcePath(m.Source)
		if err != nil {
			spark.Logger.Errorw("Error getting source path for spark source", "source", m.Source, "error", err)
			return nil, fmt.Errorf("issue with retrieving the source path for %s because %s", m.Source, err)
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

// TODO: delete this method in favor of Filepath methods
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
		fsPrimary, ok := fileTable.(*FileStorePrimaryTable)
		if !ok {
			return "", fmt.Errorf("expected primary table to be a FileStorePrimaryTable")
		}
		filePath, err := fsPrimary.GetSource()
		if err != nil {
			return "", fmt.Errorf("failed to get primary table source due to %v", err)
		}
		return filePath.ToURI(), nil
	} else if fileType == "transformation" {
		fileResourceId := ResourceID{Name: fileName, Variant: fileVariant, Type: Transformation}

		transformationDirPath, err := spark.Store.CreateDirPath(fileResourceId.ToFilestorePath())
		if err != nil {
			return "", fmt.Errorf("could not create directory path for spark transformation: %v", err)
		}

		transformationPath, err := spark.Store.NewestFileOfType(transformationDirPath, filestore.Parquet)
		if err != nil {
			return "", fmt.Errorf("could not get transformation file path: %v", err)
		}
		exists, err := spark.Store.Exists(transformationPath)
		if err != nil {
			spark.Logger.Errorf("could not check if transformation file exists: %v", err)
			return "", fmt.Errorf("could not check if transformation file exists: %v", err)
		}
		if !exists {
			spark.Logger.Errorf("transformation file does not exist: %s", transformationPath.ToURI())
			return "", fmt.Errorf("transformation file does not exist: %s", transformationPath.ToURI())
		}
		transformationDirPathDateTime, err := spark.Store.CreateDirPath(transformationPath.KeyPrefix())
		if err != nil {
			return "", fmt.Errorf("could not create directory path for spark transformation: %v", err)
		}
		return transformationDirPathDateTime.ToURI(), nil
	} else {
		return filePath, fmt.Errorf("could not find path for %s; fileType: %s, fileName: %s, fileVariant: %s", path, fileType, fileName, fileVariant)
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

func (e *EMRExecutor) GetDFArgs(outputURI filestore.Filepath, code string, sources []string, store SparkFileStore) ([]string, error) {
	argList := []string{
		"spark-submit",
		"--deploy-mode",
		"client",
	}

	packageArgs := removeEspaceCharacters(store.Packages())
	argList = append(argList, packageArgs...) // adding any packages needed for filestores

	sparkScriptPathEnv := config.GetSparkRemoteScriptPath()
	sparkScriptPath, err := store.CreateFilePath(sparkScriptPathEnv)
	if err != nil {
		return nil, fmt.Errorf("could not create spark script path: %v", err)
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
	transformationPath, err := spark.Store.CreateDirPath(id.ToFilestorePath())
	if err != nil {
		return nil, fmt.Errorf("could not create file path due to error %w (store type: %s; path: %s)", err, spark.Store.FilestoreType(), id.ToFilestorePath())
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
		return nil, fmt.Errorf("ID check failed: %v", err)
	}
	primaryTableFilepath, err := spark.Store.CreateFilePath(id.ToFilestorePath())
	if err != nil {
		return nil, fmt.Errorf("could not create file path due to error %w (store type: %s; path: %s)", err, spark.Store.FilestoreType(), id.ToFilestorePath())
	}
	if exists, err := spark.Store.Exists(primaryTableFilepath); err != nil {
		return nil, fmt.Errorf("could not check if table exists: %v", err)
	} else if exists {
		return nil, &TableAlreadyExists{id.Name, id.Variant}
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
		return nil, fmt.Errorf("ID check failed: %v", err)
	}
	resourceTableFilepath, err := spark.Store.CreateFilePath(id.ToFilestorePath())
	if err != nil {
		return nil, fmt.Errorf("could not create file path due to error %w (store type: %s; path: %s)", err, spark.Store.FilestoreType(), id.ToFilestorePath())
	}
	if exists, err := spark.Store.Exists(resourceTableFilepath); err != nil {
		return nil, fmt.Errorf("could not check if table exists: %v", err)
	} else if exists {
		return nil, &TableAlreadyExists{id.Name, id.Variant}
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
		return nil, fmt.Errorf("could not serialize schema: %v", err)
	}
	err = spark.Store.Write(resourceTableFilepath, data)
	if err != nil {
		return nil, fmt.Errorf("could not write schema to file: %v", err)
	}
	return &table, nil
}

func (spark *SparkOfflineStore) GetResourceTable(id ResourceID) (OfflineTable, error) {
	return fileStoreGetResourceTable(id, spark.Store, spark.Logger)
}

func blobSparkMaterialization(id ResourceID, spark *SparkOfflineStore, isUpdate bool, outputFormat filestore.FileType) (Materialization, error) {
	if err := id.check(Feature); err != nil {
		spark.Logger.Errorw("Attempted to create a materialization of a non feature resource", "type", id.Type)
		return nil, fmt.Errorf("only features can be materialized")
	}
	resourceTable, err := spark.GetResourceTable(id)
	if err != nil {
		spark.Logger.Errorw("Attempted to fetch resource table of non registered resource", "error", err)
		return nil, fmt.Errorf("resource not registered: %v", err)
	}
	sparkResourceTable, ok := resourceTable.(*BlobOfflineTable)
	if !ok {
		spark.Logger.Errorw("Could not convert resource table to blob offline table", "id", id)
		return nil, fmt.Errorf("could not convert offline table with id %v to sparkResourceTable", id)
	}
	// get destination path for the materialization
	materializationID := ResourceID{Name: id.Name, Variant: id.Variant, Type: FeatureMaterialization}
	destinationPath, err := spark.Store.CreateDirPath(materializationID.ToFilestorePath())
	if err != nil {
		return nil, fmt.Errorf("could not create file path due to error %w (store type: %s; path: %s)", err, spark.Store.FilestoreType(), materializationID.ToFilestorePath())
	}
	materializationExists, err := spark.Store.Exists(destinationPath)
	if err != nil {
		return nil, fmt.Errorf("could check if materialization exists: %v", err)
	}
	if materializationExists && !isUpdate {
		spark.Logger.Errorw("Attempted to materialize a materialization that already exists", "id", id)
		return nil, fmt.Errorf("materialization already exists")
	} else if !materializationExists && isUpdate {
		spark.Logger.Errorw("Attempted to materialize a materialization that already exists", "id", id)
		return nil, fmt.Errorf("materialization already exists")
	}
	materializationQuery := spark.query.materializationCreate(sparkResourceTable.schema)
	sourcePath, err := filestore.NewEmptyFilepath(spark.Store.FilestoreType())
	if err != nil {
		return nil, fmt.Errorf("could not create empty filepath due to error %w (store type: %s; path: %s)", err, spark.Store.FilestoreType(), sparkResourceTable.schema.SourceTable)
	}
	var sourceURIs []string
	if sourcePath.IsDir() {
		err = sourcePath.ParseDirPath(sparkResourceTable.schema.SourceTable)
		if err != nil {
			return nil, fmt.Errorf("could not parse full path due to error %w (store type: %s; path: %s)", err, spark.Store.FilestoreType(), sparkResourceTable.schema.SourceTable)
		}
		spark.Logger.Debugw("Parsed source table path:", "sourceTablePath", sourcePath.ToURI(), "sourceTable", sparkResourceTable.schema.SourceTable)
		// TODO: Refactor this into a separate method
		sourceFiles, err := spark.Store.List(sourcePath, filestore.Parquet)
		if err != nil {
			return nil, fmt.Errorf("could not get latest source file: %v", err)
		}
		groups, err := filestore.NewFilePathGroup(sourceFiles, filestore.DateTimeDirectoryGrouping)
		if err != nil {
			return nil, fmt.Errorf("could not get datetime directory grouping for source files: %v", err)
		}
		newest, err := groups.GetFirst()
		if err != nil {
			return nil, fmt.Errorf("could not get newest source file: %v", err)
		}
		sourceUris := make([]string, len(newest))
		for i, sourceFile := range newest {
			sourceUris[i] = sourceFile.ToURI()
		}
		sourceURIs = sourceUris
	} else {
		err = sourcePath.ParseFilePath(sparkResourceTable.schema.SourceTable)
		if err != nil {
			return nil, fmt.Errorf("could not parse full path due to error %w (store type: %s; path: %s)", err, spark.Store.FilestoreType(), sparkResourceTable.schema.SourceTable)
		}
		sourceURIs = append(sourceURIs, sourcePath.ToURI())
	}
	spark.Logger.Debugw("Fetched source files of type", "latestSourcePath", sourcePath.ToURI(), "fileFound", len(sourceURIs), "fileType", filestore.Parquet)
	sparkArgs, err := spark.Executor.SparkSubmitArgs(destinationPath, materializationQuery, sourceURIs, Materialize, spark.Store)
	if err != nil {
		spark.Logger.Errorw("Problem creating spark submit arguments", "error", err)
		return nil, fmt.Errorf("error with getting spark submit arguments %v", sparkArgs)
	}
	if outputFormat == filestore.CSV {
		sparkArgs = append(sparkArgs, "--output_format", string(outputFormat))
	}
	if isUpdate {
		spark.Logger.Debugw("Updating materialization", "id", id)
	} else {
		spark.Logger.Debugw("Creating materialization", "id", id)
	}
	if err := spark.Executor.RunSparkJob(sparkArgs, spark.Store); err != nil {
		spark.Logger.Errorw("Spark submit job failed to run", "error", err)
		return nil, fmt.Errorf("spark submit job for materialization %v failed to run: %v", materializationID, err)
	}
	exists, err := spark.Store.Exists(destinationPath)
	if err != nil {
		spark.Logger.Errorf("could not check if materialization file exists: %v", err)
		return nil, fmt.Errorf("could not check if materialization file exists: %v", err)
	}
	if !exists {
		spark.Logger.Errorf("materialization not found in directory: %s", destinationPath.ToURI())
		return nil, fmt.Errorf("materialization not found in directory: %s", destinationPath.ToURI())
	}
	spark.Logger.Debugw("Successfully created materialization", "id", id)
	return &FileStoreMaterialization{materializationID, spark.Store}, nil
}

func (spark *SparkOfflineStore) CreateMaterialization(id ResourceID, options ...MaterializationOptions) (Materialization, error) {
	var option MaterializationOptions
	var outputFormat filestore.FileType
	if len(options) > 0 {
		option = options[0]
		if option.StoreType() != spark.Type() {
			return nil, fmt.Errorf("expected options for store type %s but received options for %s instead", spark.Type(), option.StoreType())
		}
		outputFormat = option.Output()
	} else {
		outputFormat = filestore.Parquet
	}
	return blobSparkMaterialization(id, spark, false, outputFormat)
}

func (spark *SparkOfflineStore) GetMaterialization(id MaterializationID) (Materialization, error) {
	return fileStoreGetMaterialization(id, spark.Store, spark.Logger)
}

func (spark *SparkOfflineStore) UpdateMaterialization(id ResourceID) (Materialization, error) {
	return blobSparkMaterialization(id, spark, true, filestore.Parquet)
}

func (spark *SparkOfflineStore) DeleteMaterialization(id MaterializationID) error {
	return fileStoreDeleteMaterialization(id, spark.Store, spark.Logger)
}

func (spark *SparkOfflineStore) getResourceSchema(id ResourceID) (ResourceSchema, error) {
	if err := id.check(Feature, Label); err != nil {
		return ResourceSchema{}, fmt.Errorf("ID check failed: %v", err)
	}
	spark.Logger.Debugw("Getting resource schema", "id", id)
	table, err := spark.GetResourceTable(id)
	if err != nil {
		spark.Logger.Errorw("Resource not registered in spark store", "id", id, "error", err)
		return ResourceSchema{}, fmt.Errorf("resource not registered: %v", err)
	}
	sparkResourceTable, ok := table.(*BlobOfflineTable)
	if !ok {
		spark.Logger.Errorw("could not convert offline table to sparkResourceTable", "id", id)
		return ResourceSchema{}, fmt.Errorf("could not convert offline table with id %v to sparkResourceTable", id)
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
	destinationPath, err := spark.Store.CreateDirPath(def.ID.ToFilestorePath())
	if err != nil {
		return fmt.Errorf("could not create destination path: %v", err)
	}
	trainingSetExists, err := spark.Store.Exists(destinationPath)
	if err != nil {
		return fmt.Errorf("could not check if training set exists: %v", err)
	}
	if trainingSetExists && !isUpdate {
		spark.Logger.Errorw("Training set already exists", "id", def.ID)
		return fmt.Errorf("spark training set already exists: %v", def.ID)
	} else if !trainingSetExists && isUpdate {
		spark.Logger.Errorw("Training set does not exist", "id", def.ID)
		return fmt.Errorf("spark training set does not exist: %v", def.ID)
	}
	labelSchema, err := spark.getResourceSchema(def.Label)
	if err != nil {
		spark.Logger.Errorw("Could not get schema of label in spark store", "label", def.Label, "error", err)
		return fmt.Errorf("could not get schema of label %s: %v", def.Label, err)
	}
	// NOTE: labelSchema.SourceTable should be the absolute path to the label source table
	labelSourcePath := labelSchema.SourceTable
	filepath, err := filestore.NewEmptyFilepath(spark.Store.FilestoreType())
	if err != nil {
		return fmt.Errorf("could not create empty filepath due to error %w (store type: %s; path: %s)", err, spark.Store.FilestoreType(), labelSchema.SourceTable)
	}
	err = filepath.ParseFilePath(labelSourcePath)
	if err != nil {
		// Labels derived from transformations registered prior to PR #947 will not have a full path; given Spark requires an absolute path, we will
		// assume an error here means the value of SourceTable is just the relative path and attempt to construct the absolute path
		// prior to adding it to the list of source paths
		if filepath, err := spark.Store.CreateFilePath(labelSchema.SourceTable); err == nil {
			labelSourcePath = filepath.ToURI()
		} else {
			return fmt.Errorf("could not create file path due to error %w (store type: %s; path: %s)", err, spark.Store.FilestoreType(), labelSchema.SourceTable)
		}
	}
	sourcePaths = append(sourcePaths, labelSourcePath)
	for _, feature := range def.Features {
		featureSchema, err := spark.getResourceSchema(feature)
		if err != nil {
			spark.Logger.Errorw("Could not get schema of feature in spark store", "feature", feature, "error", err)
			return fmt.Errorf("could not get schema of feature %s: %v", feature, err)
		}
		featureSourcePath := featureSchema.SourceTable
		// NOTE: featureSchema.SourceTable should be the absolute path to the feature source table
		filepath, err := filestore.NewEmptyFilepath(spark.Store.FilestoreType())
		if err != nil {
			return fmt.Errorf("could not create empty filepath due to error %w (store type: %s; path: %s)", err, spark.Store.FilestoreType(), featureSchema.SourceTable)
		}
		// Features registered prior to PR #947 will not have a full path; given Spark requires an absolute path, we will
		// assume an error here means the value of SourceTable is just the relative path and attempt to construct the absolute path
		// prior to adding it to the list of source paths
		err = filepath.ParseFilePath(featureSourcePath)
		if err != nil {
			if filepath, err := spark.Store.CreateFilePath(featureSchema.SourceTable); err == nil {
				featureSourcePath = filepath.ToURI()
			} else {
				return fmt.Errorf("could not create file path due to error %w (store type: %s; path: %s)", err, spark.Store.FilestoreType(), featureSchema.SourceTable)
			}
		}
		sourcePaths = append(sourcePaths, featureSourcePath)
		featureSchemas = append(featureSchemas, featureSchema)
	}
	trainingSetQuery := spark.query.trainingSetCreate(def, featureSchemas, labelSchema)

	sparkArgs, err := spark.Executor.SparkSubmitArgs(destinationPath, trainingSetQuery, sourcePaths, CreateTrainingSet, spark.Store)
	if err != nil {
		spark.Logger.Errorw("Problem creating spark submit arguments", "error", err)
		return fmt.Errorf("error with getting spark submit arguments %v", sparkArgs)
	}

	spark.Logger.Debugw("Creating training set", "definition", def)
	if err := spark.Executor.RunSparkJob(sparkArgs, spark.Store); err != nil {
		spark.Logger.Errorw("Spark submit training set job failed to run", "definition", def.ID, "error", err)
		return fmt.Errorf("spark submit job for training set %v failed to run: %v", def.ID, err)
	}
	trainingSetExists, err = spark.Store.Exists(destinationPath)
	if err != nil {
		return fmt.Errorf("could not check that training set was created: %v", err)
	}
	if !trainingSetExists {
		spark.Logger.Errorw("Could not get training set resource key in offline store")
		return fmt.Errorf("training Set result does not exist in offline store")
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

func sanitizeSparkSQL(name string) string {
	return name
}
