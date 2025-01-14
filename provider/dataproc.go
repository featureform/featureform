package provider

import (
	"context"
	"fmt"
	"strings"

	"github.com/featureform/fferr"
	"github.com/featureform/logging"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/provider/spark"
	"github.com/featureform/provider/types"

	dataproclib "cloud.google.com/go/dataproc/v2/apiv1"
	"github.com/google/uuid"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type DataprocServerlessExecutor struct {
	logger    logging.Logger
	ctx       context.Context
	client    *dataproclib.BatchControllerClient
	projectID string
	region    string
	credsPath string
}

type DataprocServerlessExecutorConfig struct {
	ProjectID string
	Region    string
	CredsPath string
	Logger    logging.Logger
}

func NewDataprocServerlessExecutor(
	ctx context.Context, cfg DataprocServerlessExecutorConfig,
) (*DataprocServerlessExecutor, error) {
	logger := cfg.Logger.With(
		"executor-type", "dataproc-serverless",
		"project-id", cfg.ProjectID,
		"gcp-region", cfg.Region,
	)
	logger.Info("Creating new serverless dataproc executor")
	opts := []option.ClientOption{
		// This is an awful bug in gcloud. You have to set the region here.
		option.WithEndpoint(fmt.Sprintf("%s-dataproc.googleapis.com:443", cfg.Region)),
	}
	if cfg.CredsPath != "" {
		logger.Debug("Adding creds path option")
		opts = append(opts, option.WithCredentialsFile(cfg.CredsPath))
	}
	client, err := dataproclib.NewBatchControllerClient(ctx, opts...)
	if err != nil {
		msg := "Failed to create dataproc client"
		logger.Errorw(msg, "err", err)
		return nil, fferr.NewInternalErrorf("%s: %w\n", msg, err)
	}
	logger.Info("Created new serverless dataproc executor")
	return &DataprocServerlessExecutor{
		ctx:       ctx,
		client:    client,
		projectID: cfg.ProjectID,
		region:    cfg.Region,
		logger:    logger,
	}, nil
}

func (e *DataprocServerlessExecutor) InitializeExecutor(store SparkFileStoreV2) error {
	e.logger.Info("Initialized Dataproc Executor")
	return nil
}

func (e *DataprocServerlessExecutor) RunSparkJob(
	cmd *spark.Command,
	store SparkFileStoreV2,
	opts SparkJobOptions,
	tfOpts TransformationOptions,
) error {
	ctx := context.Background()
	redactedScript, redactedArgs := cmd.Redacted().CompileScriptOnly()
	logger := e.logger.With(
		"script", redactedScript,
		"args", redactedArgs,
		"store", store.Type(),
	)
	logger.Debug("Starting RunSparkJob")
	resumeOpt, hasResumeOpt := tfOpts.GetResumeOption(logger)
	var jobName string
	if hasResumeOpt && resumeOpt.IsResumeIDSet() {
		jobName = resumeOpt.ResumeID().String()
		logger = logger.With(
			"resuming-spark-job", "true",
			"spark-job-name", jobName,
		)
		logger.Info("Resuming spark job")
	} else {
		// GCloud wants job names to be lower cased
		id := uuid.New().String()
		jobName = strings.ToLower(fmt.Sprintf("featureform-%s-%s", opts.JobName, id))
		logger = logger.With(
			"spark-job-name", jobName,
		)
		if hasResumeOpt {
			logger.Debug("Setting resume id")
			resumeOpt.setResumeID(types.ResumeID(jobName))
		}
		logger.Info("Starting Spark Job")
	}
	req := cmd.CompileDataprocServerless(e.projectID, e.region)
	req.RequestId = jobName
	logger.Infow("request", "req", req)
	op, err := e.client.CreateBatch(ctx, req)
	if err != nil {
		wrapped := fferr.NewExecutionError(pt.SparkOffline.String(), err)
		st, ok := status.FromError(err)
		if ok && st.Code() == codes.InvalidArgument {
			logger = logger.With("err-details", st.Details())
			wrapped.AddDetails("err-details", st.Details())
		}
		logger.Errorw("Error submitting job", "error", err)
		wrapped.AddDetails(
			"jobName", jobName,
			"executor_type", "DataProc Serverless",
			"store_type", store.Type(),
		)
		wrapped.AddFixSuggestion("Check the cluster logs for more information")
		return err
	}
	waitFn := func() error {
		logger.Info("Waiting for job to finish")
		_, err = op.Wait(ctx)
		if err != nil {
			logger.Errorw("Job Failed", "error", err)
			wrapped := fferr.NewExecutionError(pt.SparkOffline.String(), err)
			wrapped.AddDetails(
				"jobName", jobName,
				"executor_type", "DataProc Serverless",
				"store_type", store.Type(),
			)
			wrapped.AddFixSuggestion("Check the cluster logs for more information")
			return err
		}
		logger.Info("Spark job completed successfully")
		return nil
	}
	if hasResumeOpt {
		go func() {
			var jobErr error = fferr.NewInternalErrorf("Waiter panicked")
			defer func() {
				logger.Debug("Setting error in resume opt")
				if err := resumeOpt.finishWithError(jobErr); err != nil {
					logger.Errorw("Unable to set error in resume opt", "err", err)
				}
				logger.Debug("Set error in resume opt")
			}()
			jobErr = waitFn()
		}()
		return nil
	} else {
		return waitFn()
	}
}

func (e *DataprocServerlessExecutor) SupportsTransformationOption(
	opt TransformationOptionType,
) (bool, error) {
	logger := e.logger.With("transform-option-type", opt)
	logger.Debugw("Checking if dataproc supports transform option")
	if opt == ResumableTransformation {
		logger.Debugw("Dataproc supports transformation option")
		return true, nil
	}
	logger.Debugw("Dataproc doesn't support transformation option")
	return false, nil
}
