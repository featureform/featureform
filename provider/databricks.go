// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	re "github.com/avast/retry-go/v4"
	"github.com/databricks/databricks-sdk-go"
	dbClient "github.com/databricks/databricks-sdk-go/client"
	dbConfig "github.com/databricks/databricks-sdk-go/config"
	"github.com/databricks/databricks-sdk-go/retries"
	"github.com/databricks/databricks-sdk-go/service/compute"
	"github.com/databricks/databricks-sdk-go/service/jobs"
	"github.com/featureform/fferr"
	"github.com/featureform/filestore"
	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type DatabricksResultState string

const (
	Success   DatabricksResultState = "SUCCESS"
	Failed    DatabricksResultState = "FAILED"
	Timeout   DatabricksResultState = "TIMEOUT"
	Cancelled DatabricksResultState = "CANCELLED"
)

func NewDatabricksExecutor(databricksConfig pc.DatabricksConfig, logger *zap.SugaredLogger) (SparkExecutor, error) {
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

	base, err := newBaseExecutor()
	if err != nil {
		return nil, err
	}

	return &DatabricksExecutor{
		client:             client,
		cluster:            databricksConfig.Cluster,
		config:             databricksConfig,
		errorMessageClient: errorMessageClient,
		logger:             logger,
		baseExecutor:       base,
	}, nil
}

type DatabricksExecutor struct {
	client             *databricks.WorkspaceClient
	cluster            string
	config             pc.DatabricksConfig
	errorMessageClient *dbClient.DatabricksClient
	logger             *zap.SugaredLogger
	baseExecutor
}

func (db *DatabricksExecutor) SupportsTransformationOption(opt TransformationOptionType) (bool, error) {
	return false, nil
}

func (db *DatabricksExecutor) RunSparkJob(args []string, store SparkFileStore, opts SparkJobOptions, tfopts TransformationOptions) error {
	logger := db.logger.With("args", args, "store", store.Type(), "job_name", opts.JobName, "cluster_id", db.cluster)
	pythonFilepath, err := db.PythonFileURI(store)
	if err != nil {
		logger.Errorw("could not get python file path", "error", err)
		return err
	}
	pythonTask := jobs.SparkPythonTask{
		PythonFile: pythonFilepath.ToURI(),
		Parameters: args,
	}
	ctx := context.Background()
	id := uuid.New().String()

	jobToRun, err := db.client.Jobs.Create(ctx, jobs.CreateJob{
		Name: fmt.Sprintf("%s-%s", opts.JobName, id),
		Tasks: []jobs.JobTaskSettings{
			{
				TaskKey:           fmt.Sprintf("featureform-task-%s", id),
				ExistingClusterId: db.cluster,
				SparkPythonTask:   &pythonTask,
			},
		},
	})
	if err != nil {
		logger.Errorw("could not create job", "error", err)
		wrapped := fferr.NewExecutionError(pt.SparkOffline.String(), err)
		wrapped.AddDetails("job_name", fmt.Sprintf("%s-%s", opts.JobName, id), "job_id", fmt.Sprint(jobToRun.JobId), "executor_type", "Databricks", "store_type", store.Type())
		wrapped.AddFixSuggestion("Check the cluster logs for more information")
		return wrapped
	}

	weekTimeout := retries.Timeout[jobs.Run](opts.MaxJobDuration)
	_, err = db.client.Jobs.RunNowAndWait(ctx, jobs.RunNow{
		JobId: jobToRun.JobId,
	}, weekTimeout)
	if err != nil {
		logger.Errorw("job failed", "error", err)
		errorMessage := err
		if db.errorMessageClient != nil {
			errorMessage, err = db.getErrorMessage(jobToRun.JobId)
			if err != nil {
				logger.Errorf("the '%v' job failed, could not get error message: %v\n", jobToRun.JobId, err)
			}
		}
		wrapped := fferr.NewExecutionError(pt.SparkOffline.String(), fmt.Errorf("job failed: %v", errorMessage))
		wrapped.AddDetails("job_name", fmt.Sprintf("%s-%s", opts.JobName, id), "job_id", fmt.Sprint(jobToRun.JobId), "executor_type", "Databricks", "store_type", store.Type())
		wrapped.AddFixSuggestion("Check the cluster logs for more information")
		return wrapped
	}

	return nil
}

func (db *DatabricksExecutor) InitializeExecutor(store SparkFileStore) error {
	logger := db.logger.With("store", store.Type(), "executor_type", "Databricks")
	// We can't use CreateFilePath here because it calls Validate under the hood,
	// which will always fail given it's a local file without a valid scheme or bucket, for example.
	sparkLocalScriptPath := &filestore.LocalFilepath{}
	if err := sparkLocalScriptPath.SetKey(db.files.LocalScriptPath); err != nil {
		logger.Errorw("could not set local script path", "error", err)
		return err
	}
	sparkRemoteScriptPath, err := store.CreateFilePath(db.files.RemoteScriptPath, false)
	if err != nil {
		logger.Errorw("could not create remote script path", "error", err)
		return err
	}
	pythonLocalInitScriptPath := &filestore.LocalFilepath{}
	if err := pythonLocalInitScriptPath.SetKey(db.files.PythonLocalInitPath); err != nil {
		logger.Errorw("could not set python local init script path", "error", err)
		return err
	}
	pythonRemoteInitScriptPath := db.files.PythonRemoteInitPath

	err = readAndUploadFile(sparkLocalScriptPath, sparkRemoteScriptPath, store)
	if err != nil {
		logger.Errorw("could not upload spark script", "error", err)
		return err
	}
	sparkExists, err := store.Exists(pl.NewFileLocation(sparkRemoteScriptPath))
	if err != nil || !sparkExists {
		logger.Errorw("spark script does not exist", "error", err)
		return err
	}
	remoteInitScriptPathWithPrefix, err := store.CreateFilePath(pythonRemoteInitScriptPath, false)
	if err != nil {
		logger.Errorw("could not create remote init script path", "error", err)
		return err
	}
	err = readAndUploadFile(pythonLocalInitScriptPath, remoteInitScriptPathWithPrefix, store)
	if err != nil {
		logger.Errorw("could not upload python init script", "error", err)
		return err
	}
	initExists, err := store.Exists(pl.NewFileLocation(remoteInitScriptPathWithPrefix))
	if err != nil || !initExists {
		logger.Errorw("python init script does not exist", "error", err)
		return err
	}
	return nil
}

// Need the bucket from here
func (db *DatabricksExecutor) PythonFileURI(store SparkFileStore) (filestore.Filepath, error) {
	relativePath := db.files.RemoteScriptPath
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

func (db *DatabricksExecutor) SparkSubmitArgs(outputLocation pl.Location, cleanQuery string, sourceList []string, jobType JobType, store SparkFileStore, mappings []SourceMapping) ([]string, error) {
	db.logger.Debugw("SparkSubmitArgs", "outputLocation", outputLocation.Location(), "cleanQuery", cleanQuery, "sourceList", sourceList, "jobType", jobType, "store", store)
	if _, isFilestoreLocation := outputLocation.(*pl.FileStoreLocation); !isFilestoreLocation {
		return nil, fmt.Errorf("output location must be a filestore location")
	}
	output, err := outputLocation.Serialize()
	if err != nil {
		return nil, err
	}
	argList := []string{
		"sql",
		"--output",
		output,
		"--job_type",
		string(jobType),
		"--store_type",
		store.Type(),
	}
	sparkConfigs := store.SparkConfig()
	argList = append(argList, sparkConfigs...)

	credentialConfigs := store.CredentialsConfig()
	argList = append(argList, credentialConfigs...)

	snowflakeConfig, err := getSnowflakeConfigFromSourceMapping(mappings)
	if err != nil {
		db.logger.Errorw("Could not get Snowflake config from source mapping", "error", err)
		return nil, err
	}

	if snowflakeConfig != nil {
		argList = append(argList, removeEscapeCharacters(snowflakeConnectorCredentials(snowflakeConfig))...)
	}

	// Databricks's API enforces a 10K-byte limit on job submit params, so to avoid a 400, we need to check to ensure
	// the args are below this limit. If they exceed this limit, it's most likely due to the query and/or the list of
	// sources, so we write these as a JSON file and read them from the PySpark runner script to side-step this constraint
	if exceedsSubmitParamsTotalByteLimit(argList, cleanQuery, sourceList) {
		db.logger.Debugw("Exceeded submit params byte limit; writing to file store", "store", store.FilestoreType())
		if store.FilestoreType() != filestore.S3 {
			return argList, fmt.Errorf("%s is not a currently support file store for writing submit params; supported types: %s", store.FilestoreType(), filestore.S3)
		}

		paramsPath, err := writeSubmitParamsToFileStore(cleanQuery, sourceList, store, db.logger)
		if err != nil {
			return nil, err
		}

		argList = append(argList, "--submit_params_uri", paramsPath.Key())
	} else {
		argList = append(argList, "--sql_query", cleanQuery)
		argList = append(argList, "--sources")
		argList = append(argList, sourceList...)
	}

	return argList, nil
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

func (d *DatabricksExecutor) GetDFArgs(outputLocation pl.Location, code string, sources []string, store SparkFileStore, mappings []SourceMapping) ([]string, error) {
	d.logger.Debugw("GetDFArgs", "outputLocation", outputLocation, "code", code, "sources", sources, "store", store)
	output, err := outputLocation.Serialize()
	if err != nil {
		return nil, err
	}
	argList := []string{
		"df",
		"--output",
		output,
		"--code",
		code,
		"--store_type",
		store.Type(),
	}

	sparkConfigs := store.SparkConfig()
	argList = append(argList, sparkConfigs...)

	credentialConfigs := store.CredentialsConfig()
	argList = append(argList, credentialConfigs...)

	snowflakeConfig, err := getSnowflakeConfigFromSourceMapping(mappings)
	if err != nil {
		return nil, err
	}

	if snowflakeConfig != nil {
		argList = append(argList, removeEscapeCharacters(snowflakeConnectorCredentials(snowflakeConfig))...)
	}

	argList = append(argList, "--sources")
	argList = append(argList, sources...)

	return argList, nil
}
