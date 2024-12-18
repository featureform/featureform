// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/featureform/fferr"
	"github.com/featureform/filestore"
	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"go.uber.org/zap"
)

func NewSparkGenericExecutor(sparkGenericConfig pc.SparkGenericConfig, logger *zap.SugaredLogger) (SparkExecutor, error) {
	base, err := newBaseExecutor()
	if err != nil {
		return nil, err
	}
	sparkGenericExecutor := SparkGenericExecutor{
		master:        sparkGenericConfig.Master,
		deployMode:    sparkGenericConfig.DeployMode,
		pythonVersion: sparkGenericConfig.PythonVersion,
		coreSite:      sparkGenericConfig.CoreSite,
		yarnSite:      sparkGenericConfig.YarnSite,
		logger:        logger,
		baseExecutor:  base,
	}
	return &sparkGenericExecutor, nil
}

type SparkGenericExecutor struct {
	master        string
	deployMode    string
	pythonVersion string
	coreSite      string
	yarnSite      string
	logger        *zap.SugaredLogger
	baseExecutor
}

func (s *SparkGenericExecutor) InitializeExecutor(store SparkFileStoreV2) error {
	s.logger.Info("Uploading PySpark script to filestore")
	// We can't use CreateFilePath here because it calls Validate under the hood,
	// which will always fail given it's a local file without a valid scheme or bucket, for example.
	sparkLocalScriptPath := &filestore.LocalFilepath{}
	if err := sparkLocalScriptPath.SetKey(s.files.LocalScriptPath); err != nil {
		return err
	}

	sparkRemoteScriptPath, err := store.CreateFilePath(s.files.RemoteScriptPath, false)
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

func (s *SparkGenericExecutor) SupportsTransformationOption(opt TransformationOptionType) (bool, error) {
	return false, nil
}

func (s *SparkGenericExecutor) RunSparkJob(args []string, store SparkFileStoreV2, opts SparkJobOptions, tfOpts TransformationOptions) error {
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
		wrapped.AddDetails("executor_type", "Spark Generic", "store_type", store.Type())
		wrapped.AddFixSuggestion("Check the cluster logs for more information")
		return wrapped
	}

	err = cmd.Wait()
	if err != nil {
		wrapped := fferr.NewExecutionError(pt.SparkOffline.String(), fmt.Errorf("spark job failed: %v", err))
		wrapped.AddDetails("executor_type", "Spark Generic", "store_type", store.Type(), "stdout", outb, "stderr", errb)
		wrapped.AddFixSuggestion("Check the cluster logs for more information")
		return wrapped
	}

	return nil
}

func (s *SparkGenericExecutor) PythonFileURI(store SparkFileStoreV2) (filestore.Filepath, error) {
	// not used for Spark Generic Executor
	return nil, nil
}

func (s *SparkGenericExecutor) SparkSubmitArgs(
	outputLocation pl.Location,
	cleanQuery string,
	sourceList []string,
	jobType JobType,
	store SparkFileStoreV2,
	mappings []SourceMapping,
) ([]string, error) {

	s.logger.Debugw("SparkSubmitArgs", "outputLocation", outputLocation.Location(), "cleanQuery", cleanQuery, "sourceList", sourceList, "jobType", jobType, "store", store)
	if _, isFilestoreLocation := outputLocation.(*pl.FileStoreLocation); !isFilestoreLocation {
		return nil, fmt.Errorf("output location must be a filestore location")
	}
	output, err := outputLocation.Serialize()
	if err != nil {
		return nil, err
	}
	argList := []string{
		"spark-submit",
		"--deploy-mode",
		s.deployMode,
		"--master",
		s.master,
	}

	packageArgs := store.Packages()
	argList = append(argList, packageArgs...) // adding any packages needed for filestores

	sparkScriptPathEnv := s.files.LocalScriptPath
	scriptArgs := []string{
		sparkScriptPathEnv,
		"sql",
		"--output",
		output,
		"--sql_query",
		fmt.Sprintf("'%s'", cleanQuery),
		"--job_type",
		fmt.Sprintf("'%s'", jobType),
		"--store_type",
		store.Type().String(),
	}
	argList = append(argList, scriptArgs...)

	sparkConfigs := store.SparkConfig()
	argList = append(argList, sparkConfigs...)

	credentialConfigs := store.CredentialsConfig()
	argList = append(argList, credentialConfigs...)

	argList = append(argList, "--sources")
	argList = append(argList, sourceList...)

	return argList, nil
}

func (s *SparkGenericExecutor) GetDFArgs(
	outputLocation pl.Location,
	code string,
	sources []string,
	store SparkFileStoreV2,
	mappings []SourceMapping,
) ([]string, error) {

	argList := []string{
		"spark-submit",
		"--deploy-mode",
		s.deployMode,
		"--master",
		s.master,
	}

	packageArgs := store.Packages()
	argList = append(argList, packageArgs...) // adding any packages needed for filestores

	sparkScriptPathEnv := s.files.LocalScriptPath

	output, err := outputLocation.Serialize()
	if err != nil {
		return nil, err
	}

	scriptArgs := []string{
		sparkScriptPathEnv,
		"df",
		"--output",
		output,
		"--code",
		code,
		"--store_type",
		store.Type().String(),
	}
	argList = append(argList, scriptArgs...)

	sparkConfigs := store.SparkConfig()
	argList = append(argList, sparkConfigs...)

	credentialConfigs := store.CredentialsConfig()
	argList = append(argList, credentialConfigs...)

	argList = append(argList, "--sources")
	argList = append(argList, sources...)

	return argList, nil
}
