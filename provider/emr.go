// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsv2config "github.com/aws/aws-sdk-go-v2/config"
	awsv2Creds "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/emr"

	emrtypes "github.com/aws/aws-sdk-go-v2/service/emr/types"
	"github.com/featureform/config"
	"github.com/featureform/fferr"
	"github.com/featureform/filestore"
	"github.com/featureform/helpers"
	"github.com/featureform/helpers/compression"
	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/provider/types"
	"go.uber.org/zap"
)

// StepCompleteWaiter.WaitForOutput returns this formatted error message, which is our only means of determining
// that we've failed due to exceeding the max wait time set for the transformation. **NOTE**: Given this is a string
// comparison, it's important that this message is not changed; additionally, it's also possible that in future versions
// of the AWS SDK this message could change, so it's important to keep an eye on this.
const EMR_MAX_WAIT_DURATION_ERROR = "exceeded max wait time for StepComplete waiter"

func NewEMRExecutor(emrConfig pc.EMRConfig, logger *zap.SugaredLogger) (SparkExecutor, error) {
	var useServiceAccount bool
	var awsAccessKeyId, awsSecretKey string
	switch creds := emrConfig.Credentials.(type) {
	case pc.AWSStaticCredentials:
		awsAccessKeyId = creds.AccessKeyId
		awsSecretKey = creds.SecretKey
	case pc.AWSAssumeRoleCredentials:
		useServiceAccount = true
	default:
		return nil, fferr.NewInvalidArgumentErrorf("unsupported credentials type: %T", creds)
	}

	// If the user provides pc.AWSAssumeRoleCredentials, we will use the default credentials provider chain
	// to get the credentials stored on the pod. This is only possible if an IAM for Service Accounts has been
	// correctly configured on the EMR cluster and the K8s pod(s). See the following link for more information:
	// https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html
	opts := []func(*awsv2config.LoadOptions) error{awsv2config.WithRegion(emrConfig.ClusterRegion)}
	if !useServiceAccount {
		opts = append(opts, awsv2config.WithCredentialsProvider(awsv2Creds.NewStaticCredentialsProvider(awsAccessKeyId, awsSecretKey, "")))
	}
	cfg, err := awsv2config.LoadDefaultConfig(context.TODO(), opts...)
	if err != nil {
		return nil, fferr.NewConnectionError(pt.SparkOffline.String(), err)
	}
	client := emr.NewFromConfig(cfg)

	var logFileStore *FileStore
	describeEMR, err := client.DescribeCluster(context.TODO(), &emr.DescribeClusterInput{
		ClusterId: aws.String(emrConfig.ClusterName),
	})
	if err != nil {
		logger.Infof("could not pull information about the cluster '%s': %s", emrConfig.ClusterName, err)
	} else if describeEMR.Cluster.LogUri != nil {
		logLocation := *describeEMR.Cluster.LogUri
		logFileStore, err = createLogS3FileStore(emrConfig.ClusterRegion, logLocation, awsAccessKeyId, awsSecretKey, useServiceAccount)
		if err != nil {
			logger.Infof("could not create log file store at '%s': %s", logLocation, err)
		}
	}

	base, err := newBaseExecutor()
	if err != nil {
		return nil, err
	}

	emrExecutor := EMRExecutor{
		client:       client,
		logger:       logger,
		clusterName:  emrConfig.ClusterName,
		logFileStore: logFileStore,
		baseExecutor: base,
	}
	return &emrExecutor, nil
}

type EMRExecutor struct {
	client       *emr.Client
	clusterName  string
	logger       *zap.SugaredLogger
	logFileStore *FileStore
	baseExecutor
}

func (e *EMRExecutor) PythonFileURI(store SparkFileStore) (filestore.Filepath, error) {
	e.logger.Debug("Getting python file URI")
	sparkScriptPathEnv := e.files.RemoteScriptPath
	sparkScriptPath, err := store.CreateFilePath(sparkScriptPathEnv, false)
	if err != nil {
		e.logger.Errorw("Failed to parse remote script path", "path", sparkScriptPathEnv)
		return nil, err
	}
	// Need to replace s3a:// with s3:// for the script name to be correctly interpreted by the EMR cluster
	if sparkScriptPath.Scheme() == "s3a://" {
		if err := sparkScriptPath.SetScheme("s3://"); err != nil {
			e.logger.Errorw("Unable to change spark script scheme to s3 from s3a", "error", err)
			return nil, err
		}
	}
	return sparkScriptPath, nil
}

func (e EMRExecutor) Files() config.SparkFileConfigs {
	return e.files
}

func (e *EMRExecutor) SupportsTransformationOption(opt TransformationOptionType) (bool, error) {
	if opt == ResumableTransformation {
		return true, nil
	}
	return false, nil
}

func (e *EMRExecutor) RunSparkJob(args []string, store SparkFileStore, opts SparkJobOptions, tfOpts TransformationOptions) error {
	ctx := context.TODO()
	logger := e.logger.With("args", args, "opts", opts, "tfOpts", tfOpts)
	logger.Debugw("Running SparkJob")

	resumeOpt := e.getResumeOption(tfOpts, logger)
	hasResumeOpt := resumeOpt != nil
	jobName := opts.JobName
	clusterID := e.clusterName
	logger = logger.With("resume_opt_set", hasResumeOpt, "job_name", jobName, "cluster_id", clusterID)

	stepID, err := e.runOrResumeJob(ctx, args, clusterID, jobName, resumeOpt, logger)
	if err != nil {
		return err
	}

	if hasResumeOpt {
		return e.handleAsyncResumeOption(resumeOpt, clusterID, stepID, opts.MaxJobDuration, logger)
	} else {
		logger.Infow("Waiting for EMR job to complete", "wait_duration", opts.MaxJobDuration.String())
		return e.waitForStep(ctx, clusterID, stepID, opts.MaxJobDuration)
	}
}

func (e *EMRExecutor) getResumeOption(tfOpts TransformationOptions, logger *zap.SugaredLogger) *ResumeOption {
	tfOpt := tfOpts.GetByType(ResumableTransformation)
	if tfOpt == nil {
		logger.Debugw("ResumeOption not found")
		return nil
	}

	casted, ok := tfOpt.(*ResumeOption)
	if !ok {
		logger.DPanicw("Unknown transformation option with ResumableTransformation type", "option", tfOpt)
		return nil
	}

	logger.Debugw("Using ResumeOption")
	return casted
}

func (e *EMRExecutor) runOrResumeJob(ctx context.Context, args []string, clusterID, jobName string, resumeOpt *ResumeOption, logger *zap.SugaredLogger) (string, error) {
	if resumeOpt != nil && resumeOpt.IsResumeIDSet() {
		logger.Debugw("ResumeID is set")
		resumeID := resumeOpt.ResumeID()
		emrID, err := deserializeEMRResumeID(resumeID)
		if err != nil {
			logger.Errorw("Failed to deserialize resume ID", "error", err)
			return "", err
		}

		if clusterID != emrID.ClusterID {
			logger.Warnw("Resuming a step on a different cluster", "resuming_on_cluster_id", emrID.ClusterID)
		}

		stepID := emrID.StepID
		logger = logger.With("step_id", stepID)
		logger.Infow("Resuming Transformation on EMR")
		return stepID, nil
	}

	logger.Infow("Running Spark job on EMR")
	stepID, err := e.runSparkJob(ctx, args, clusterID, jobName)
	if err != nil {
		logger.Errorw("Failed to run Spark job on EMR", "error", err)
		return "", err
	}
	logger = logger.With("step_id", stepID)
	return stepID, nil
}

func (e *EMRExecutor) handleAsyncResumeOption(resumeOpt *ResumeOption, clusterID, stepID string, maxWait time.Duration, logger *zap.SugaredLogger) error {
	if !resumeOpt.IsResumeIDSet() {
		// Set the new ResumeID
		resumeID, err := (&emrResumeID{ClusterID: clusterID, StepID: stepID}).Marshal()
		if err != nil {
			return err
		}

		if err := resumeOpt.setResumeID(resumeID); err != nil {
			return err
		}
	}

	go func() {
		// Finish ResumeOption after step finishes.
		var stepErr error = fferr.NewInternalErrorf("Waiter panicked")
		defer func() {
			if err := resumeOpt.finishWithError(stepErr); err != nil {
				logger.Errorw("Unable to set error in resume option", "error", err)
			}
		}()
		logger.Infow("Waiting for EMR job to complete", "wait_duration", maxWait.String())
		stepErr = e.waitForStep(context.Background(), clusterID, stepID, maxWait)
		logger.Debugw("Resume option finished", "step_err", stepErr)
	}()

	return nil
}

func (e *EMRExecutor) runSparkJob(ctx context.Context, args []string, clusterID, jobName string) (string, error) {
	params := &emr.AddJobFlowStepsInput{
		JobFlowId: aws.String(clusterID),
		Steps: []emrtypes.StepConfig{
			{
				Name: aws.String(jobName),
				HadoopJarStep: &emrtypes.HadoopJarStepConfig{
					Jar:  aws.String("command-runner.jar"), //jar file for running pyspark scripts
					Args: args,
				},
				ActionOnFailure: emrtypes.ActionOnFailureContinue,
			},
		},
	}
	resp, err := e.client.AddJobFlowSteps(ctx, params)
	if err != nil {
		e.logger.Errorw("Could not add job flow steps to EMR cluster", "error", err)
		return "", err
	}
	stepId := resp.StepIds[0]
	return stepId, nil
}

func (e *EMRExecutor) waitForStep(ctx context.Context, clusterId, stepId string, maxWait time.Duration) error {
	stepCompleteWaiter := emr.NewStepCompleteWaiter(e.client)
	err := stepCompleteWaiter.Wait(ctx, &emr.DescribeStepInput{
		ClusterId: aws.String(clusterId),
		StepId:    aws.String(stepId),
	}, maxWait)
	if err != nil {
		if err.Error() == EMR_MAX_WAIT_DURATION_ERROR {
			return e.cancelStep(stepId, maxWait)
		}
		errorMessage, getErr := e.getStepErrorMessage(e.clusterName, stepId, maxWait)
		if getErr != nil {
			e.logger.Infof("could not get error message for EMR step '%s': %s", stepId, getErr)
		}
		if errorMessage != "" {
			wrapped := fferr.NewExecutionError(pt.SparkOffline.String(), fmt.Errorf("step failed: %s", errorMessage))
			wrapped.AddDetails("executor_type", "EMR", "cluster_id", clusterId, "step_id", stepId, "wait_duration", maxWait.String())
			wrapped.AddFixSuggestion("Check the cluster logs for more information")
			return wrapped
		}

		e.logger.Errorw("Failure waiting for completion of EMR cluster", "error", err, "cluster_id", clusterId, "step_id", stepId, "wait_duration", maxWait)
		wrapped := fferr.NewExecutionError(pt.SparkOffline.String(), fmt.Errorf("failure waiting for completion of cluster: %w", err))
		wrapped.AddDetails("executor_type", "EMR", "cluster_id", clusterId, "step_id", stepId, "wait_duration", maxWait.String())
		wrapped.AddFixSuggestion("Check the cluster logs for more information")
		return wrapped
	}
	return nil
}

func (e EMRExecutor) InitializeExecutor(store SparkFileStore) error {
	e.logger.Info("Uploading PySpark script to filestore")
	sparkLocalScriptPath := &filestore.LocalFilepath{}
	if err := sparkLocalScriptPath.SetKey(e.files.LocalScriptPath); err != nil {
		return err
	}
	sparkRemoteScriptPath, err := store.CreateFilePath(e.files.RemoteScriptPath, false)
	if err != nil {
		return err
	}

	err = readAndUploadFile(sparkLocalScriptPath, sparkRemoteScriptPath, store)
	if err != nil {
		return err
	}
	scriptExists, err := store.Exists(pl.NewFileLocation(sparkRemoteScriptPath))
	if err != nil || !scriptExists {
		return fferr.NewInternalError(fmt.Errorf("could not upload spark script: Path: %s, Error: %v", sparkRemoteScriptPath.ToURI(), err))
	}
	return nil
}

func (e *EMRExecutor) getStepErrorMessage(clusterId string, stepId string, maxWait time.Duration) (string, error) {
	logger := e.logger.With("cluster_id", clusterId, "step_id", stepId)
	if e.logFileStore == nil {
		errMsg := fmt.Sprintf("cannot get error message for EMR step '%s' because the log file store is not set", stepId)
		logger.Error(errMsg)
		return "", fferr.NewInternalErrorf(errMsg)
	}

	stepResults, err := e.client.DescribeStep(context.TODO(), &emr.DescribeStepInput{
		ClusterId: aws.String(clusterId),
		StepId:    aws.String(stepId),
	})
	if err != nil {
		logger.Error("DescribeStep failed", "err", err)
		wrapped := fferr.NewExecutionError(pt.SparkOffline.String(), fmt.Errorf("could not get information on step: %w", err))
		wrapped.AddDetail("executor_type", "EMR")
		wrapped.AddDetail("cluster_id", clusterId)
		wrapped.AddDetail("step_id", stepId)
		return "", wrapped
	}

	stepStatus := stepResults.Step.Status
	if stepStatus.State == emrtypes.StepStateFailed {
		logger.Info("EMR step failed")
		var errorMsg string
		// check if there are any errors
		failureDetails := stepStatus.FailureDetails
		if failureDetails != nil {
			if failureDetails.Message != nil {
				// get the error message
				errorMsg = *failureDetails.Message
			}
			if errorMsg != "" {
				logger.Infow("EMR step failed with error message", "error_message", errorMsg)
				return errorMsg, nil
			}
			if failureDetails.LogFile != nil {
				logFile := *failureDetails.LogFile
				logger = logger.With("emr_fail_log_file", logFile)
				logger.Infow("EMR step failed with log file")

				errorMessage, err := e.getLogFileMessage(logFile, logger, maxWait)
				if err != nil {
					logger.Errorw("Unable to get log file error message", "err", err)
					wrapped := fferr.NewExecutionError(pt.SparkOffline.String(), fmt.Errorf("could not get error message from log file: %v", err))
					wrapped.AddDetail("executor_type", "EMR")
					wrapped.AddDetail("cluster_id", clusterId)
					wrapped.AddDetail("step_id", stepId)
					wrapped.AddDetail("log_file", logFile)
					return "", wrapped
				}
				logger.Infow("Got error message from log file", "message", errorMessage)

				return errorMessage, nil
			}
		}
		logger.Info("EMR step failed but no error message was found")
	}

	return "", nil
}

func (e *EMRExecutor) getLogFileMessage(logFile string, logger *zap.SugaredLogger, maxWait time.Duration) (string, error) {
	logger.Debug("Getting log message")
	outputFilepath := &filestore.S3Filepath{}
	filePath := fmt.Sprintf("%s/stdout.gz", logFile)
	if err := outputFilepath.ParseFilePath(filePath); err != nil {
		logger.Errorw("Failed to parse file path", "error", err)
		return "", err
	}

	logger.Debug("Waiting for log file")
	if err := e.waitForLogFile(outputFilepath, logger, maxWait); err != nil {
		logger.Errorw("Failed while waiting for file", "error", err)
		return "", err
	}

	logger.Debug("Reading file")
	logs, err := (*e.logFileStore).Read(outputFilepath)
	if err != nil {
		logger.Errorw("Failed while waiting for file", "error", err)
		return "", err
	}

	logger.Debug("Unzipping file")
	// the output file is compressed so we need uncompress it
	errorMessage, err := compression.GunZip(logs)
	if err != nil {
		return "", fferr.NewInternalError(fmt.Errorf("could not uncompress error message: %v", err))
	}
	logger.Debugw("Received error message", "err_msg", errorMessage)
	return errorMessage, nil
}

func (e *EMRExecutor) waitForLogFile(logFile filestore.Filepath, logger *zap.SugaredLogger, maxWait time.Duration) error {
	// wait until log file exists
	elapsed := time.Duration(0)
	waitTime := 2 * time.Second
	for elapsed < maxWait {
		logger.Debug("Checking if log file exists")
		fileExists, err := (*e.logFileStore).Exists(pl.NewFileLocation(logFile))
		if err != nil {
			logger.Debugw("Failed to check if log file exists", "err", err)
			return err
		}

		if fileExists {
			logger.Debug("Log file is ready")
			return nil
		}
		logger.Debugw("File still doesn't exist. Waiting.", "wait_time", waitTime)
		time.Sleep(waitTime)
		elapsed += waitTime
	}
	errMsg := "Timed out waiting for log file"
	logger.Error(errMsg)
	return fferr.NewInternalErrorf(errMsg)
}

// In the event that a step exceeds the max wait duration, we cancel the step to avoid having a long running job that won't result in
// usable output to Featureform. This method cancels the step and returns an error; if there's an error cancelling the step, it will
// return an error with the details of why the step couldn't be cancelled.
func (e *EMRExecutor) cancelStep(stepId string, waitDuration time.Duration) error {
	cancelStepParams := &emr.CancelStepsInput{
		ClusterId: aws.String(e.clusterName),
		StepIds:   []string{stepId},
	}
	_, cancelErr := e.client.CancelSteps(context.TODO(), cancelStepParams)
	if cancelErr != nil {
		e.logger.Errorw("Could not cancel EMR step", "error", cancelErr, "cluster_id", e.clusterName, "step_id", stepId)
		wrapped := fferr.NewExecutionError(pt.SparkOffline.String(), fmt.Errorf("could not cancel EMR step that exceeded max wait duration: %w", cancelErr))
		wrapped.AddDetails("executor_type", "EMR", "cluster_id", e.clusterName, "step_id", stepId, "wait_duration", waitDuration.String())
		return wrapped
	}
	e.logger.Errorw("EMR step exceeded max wait duration and was cancelled", "cluster_id", e.clusterName, "step_id", stepId, "wait_duration", waitDuration)
	wrapped := fferr.NewExecutionError(pt.SparkOffline.String(), fmt.Errorf("EMR step exceeded max wait duration and was cancelled"))
	wrapped.AddDetails("executor_type", "EMR", "cluster_id", e.clusterName, "step_id", stepId, "wait_duration", waitDuration.String())
	return wrapped
}

func (e *EMRExecutor) SparkSubmitArgs(outputLocation pl.Location, cleanQuery string, sourceList []string, jobType JobType, store SparkFileStore, mappings []SourceMapping) ([]string, error) {
	e.logger.Debugw("SparkSubmitArgs", "outputLocation", outputLocation, "outputLocationType", fmt.Sprintf("%T", outputLocation), "cleanQuery", cleanQuery, "sourceList", sourceList, "jobType", jobType, "store", store)
	argList := []string{
		"spark-submit",
	}

	if helpers.GetEnvBool("USE_CLIENT_MODE", false) {
		argList = append(argList, "--deploy-mode", "client")
	} else {
		argList = append(argList, "--deploy-mode", "cluster")
	}

	snowflakeConfig, err := getSnowflakeConfigFromSourceMapping(mappings)
	if err != nil {
		e.logger.Errorw("Could not get Snowflake config from source mapping", "error", err)
		return nil, err
	}

	packageArgs := removeEscapeCharacters(store.Packages())

	if snowflakeConfig != nil {
		for idx, arg := range packageArgs {
			if arg == "--packages" && idx+1 < len(packageArgs) {
				packageArgs[idx+1] = fmt.Sprintf("%s,%s", packageArgs[idx+1], "net.snowflake:snowflake-jdbc:3.13.22,net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4")
				break
			}
		}
	}

	argList = append(argList, packageArgs...) // adding any packages needed for filestores

	sparkScriptPathEnv := e.files.RemoteScriptPath
	sparkScriptPath, err := store.CreateFilePath(sparkScriptPathEnv, false)
	if err != nil {
		return nil, err
	}
	// Need to replace s3a:// with s3:// for the script name to be correctly interpreted by the EMR cluster
	if sparkScriptPath.Scheme() == "s3a://" {
		if err := sparkScriptPath.SetScheme("s3://"); err != nil {
			e.logger.Errorw("Unable to change spark script scheme to s3 from s3a", "error", err)
			return nil, err
		}
	}
	sparkScriptRemotePath := sparkScriptPath.ToURI()

	output, err := outputLocation.Serialize()
	if err != nil {
		return nil, err
	}

	scriptArgs := []string{
		sparkScriptRemotePath,
		"sql",
		"--output",
		output,
		"--job_type",
		string(jobType),
		"--store_type",
		store.Type(),
	}
	argList = append(argList, scriptArgs...)

	sparkConfigs := removeEscapeCharacters(store.SparkConfig())
	argList = append(argList, sparkConfigs...)

	credentialConfigs := removeEscapeCharacters(store.CredentialsConfig())
	argList = append(argList, credentialConfigs...)

	if snowflakeConfig != nil {
		argList = append(argList, removeEscapeCharacters(snowflakeConnectorCredentials(snowflakeConfig))...)
	}

	// EMR's API enforces a 10K-character (i.e. bytes) limit on string values passed to HadoopJarStep, so to avoid a 400, we need
	// to check to ensure the args are below this limit. If they exceed this limit, it's most likely due to the query and/or the list
	// of sources, so we write these as a JSON file and read them from the PySpark runner script to side-step this constraint
	// Adding the additional conditional statement, || store.FilestoreType() == filestore.S3, so we always write the submit params
	// to a file when using S3 as the filestore type
	if exceedsSubmitParamsTotalByteLimit(argList, cleanQuery, sourceList) || store.FilestoreType() == filestore.S3 {
		e.logger.Debugw("Exceeded submit params byte limit; writing to file store", "store", store.FilestoreType())
		if store.FilestoreType() != filestore.S3 {
			return argList, fmt.Errorf("%s is not a currently support file store for writing submit params; supported types: %s", store.FilestoreType(), filestore.S3)
		}

		paramsPath, err := writeSubmitParamsToFileStore(cleanQuery, sourceList, store, e.logger)
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

func (e *EMRExecutor) GetDFArgs(outputLocation pl.Location, code string, sources []string, store SparkFileStore, mappings []SourceMapping) ([]string, error) {
	argList := []string{
		"spark-submit",
	}

	if helpers.GetEnvBool("USE_CLIENT_MODE", false) {
		argList = append(argList, "--deploy-mode", "client")
	} else {
		argList = append(argList, "--deploy-mode", "cluster")
	}

	snowflakeConfig, err := getSnowflakeConfigFromSourceMapping(mappings)
	if err != nil {
		return nil, err
	}

	packageArgs := removeEscapeCharacters(store.Packages())

	if snowflakeConfig != nil {
		for idx, arg := range packageArgs {
			if arg == "--packages" && idx+1 < len(packageArgs) {
				packageArgs[idx+1] = fmt.Sprintf("%s,%s", packageArgs[idx+1], "net.snowflake:snowflake-jdbc:3.13.22,net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4")
				break
			}
		}
	}
	argList = append(argList, packageArgs...) // adding any packages needed for filestores

	sparkScriptPathEnv := e.files.RemoteScriptPath
	sparkScriptPath, err := store.CreateFilePath(sparkScriptPathEnv, false)
	if err != nil {
		return nil, err
	}
	codePath := strings.Replace(code, filestore.S3APrefix, filestore.S3Prefix, -1)
	// Need to replace s3a:// with s3:// for the script name to be correctly interpreted by the EMR cluster
	sparkScriptRemotePath := strings.Replace(sparkScriptPath.ToURI(), filestore.S3APrefix, filestore.S3Prefix, -1)

	output, err := outputLocation.Serialize()
	if err != nil {
		return nil, err
	}

	scriptArgs := []string{
		sparkScriptRemotePath,
		"df",
		"--output",
		output,
		"--code",
		codePath,
		"--store_type",
		store.Type(),
	}
	argList = append(argList, scriptArgs...)

	sparkConfigs := removeEscapeCharacters(store.SparkConfig())
	argList = append(argList, sparkConfigs...)

	credentialConfigs := removeEscapeCharacters(store.CredentialsConfig())
	argList = append(argList, credentialConfigs...)

	if snowflakeConfig != nil {
		argList = append(argList, removeEscapeCharacters(snowflakeConnectorCredentials(snowflakeConfig))...)
	}

	argList = append(argList, "--sources")
	argList = append(argList, sources...)

	return argList, nil
}

func createLogS3FileStore(emrRegion string, s3LogLocation string, awsAccessKeyId string, awsSecretKey string, useServiceAccount bool) (*FileStore, error) {
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
		Credentials:  pc.AWSStaticCredentials{AccessKeyId: awsAccessKeyId, SecretKey: awsSecretKey},
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

// emrResourceID serialies into a ResumeID to be used via ResumeOption (a type of TransformationOption)
type emrResumeID struct {
	ClusterID string
	StepID    string
}

// emrResumeIDRecordV0 becomes that actual JSON format of the ResumeID in the database.
type emrResumeIDRecordV0 struct {
	// SchemaVersion will make it easier to retain backwards compatibility in the future and do schema
	// migration.
	SchemaVersion int
	ClusterID     string
	StepID        string
}

func (rec emrResumeIDRecordV0) ToEmrResumeID() *emrResumeID {
	return &emrResumeID{
		ClusterID: rec.ClusterID,
		StepID:    rec.StepID,
	}
}

func (resID *emrResumeID) Validate() error {
	if resID.ClusterID == "" {
		return fferr.NewInternalErrorf("EMR Resume ID must have ClusterID set: %v", resID)
	}
	if resID.StepID == "" {
		return fferr.NewInternalErrorf("EMR Resume ID must have StepID set: %v", resID)
	}
	return nil
}

func (resID *emrResumeID) Marshal() (types.ResumeID, error) {
	if err := resID.Validate(); err != nil {
		return types.NilResumeID, err
	}
	record := emrResumeIDRecordV0{
		// If you're changing the schema of the record, you should change the schema version and handle it in
		// the deserialize method.
		SchemaVersion: 0,
		ClusterID:     resID.ClusterID,
		StepID:        resID.StepID,
	}
	data, err := json.Marshal(record)
	if err != nil {
		return "", fferr.NewInternalErrorf("Unable to serialize EMR resume ID: %s", err)
	}
	return types.ResumeID(data), nil
}

func deserializeEMRResumeID(id types.ResumeID) (*emrResumeID, error) {
	var record emrResumeIDRecordV0
	if err := json.Unmarshal([]byte(id), &record); err != nil {
		return nil, err
	}
	return record.ToEmrResumeID(), nil
}
