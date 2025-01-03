// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/google/uuid"

	"github.com/featureform/config"
	"github.com/featureform/fferr"
	"github.com/featureform/filestore"
	"github.com/featureform/helpers"
	"github.com/featureform/logging"
	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/provider/types"
)

func getSparkDeployModeFromEnv() types.SparkDeployMode{
	if helpers.GetEnvBool("USE_CLIENT_MODE", false) {
		return types.SparkClientDeployMode
	} else {
		return types.SparkClusterDeployMode
	}
}

func sparkPythonFileURI(store SparkFileStoreV2, logger logging.Logger) (filestore.Filepath, error) {
	logger.Debug("Getting python file URI")
	config, err := config.CreateSparkScriptConfig()
	if err != nil {
		logger.Errorw("Failed to get remote script path", "err", err)
		return nil, err
	}
	rawPath := config.RemoteScriptPath
	sparkScriptPath, err := store.CreateFilePath(rawPath, false)
	if err != nil {
		logger.Errorw("Failed to parse remote script path", "path", rawPath)
		return nil, err
	}
	// Need to replace s3a:// with s3:// for the script name
	// to be correctly interpreted by Spark
	if sparkScriptPath.Scheme() == filestore.S3APrefix {
		if err := sparkScriptPath.SetScheme(filestore.S3Prefix); err != nil {
			logger.Errorw("Unable to change spark script scheme to s3 from s3a", "error", err)
			return nil, err
		}
	}
	return sparkScriptPath, nil
}

func genericSparkSubmitArgs(execType pc.SparkExecutorType, deployMode types.SparkDeployMode, tfType TransformationType, outputLocation pl.Location, code string, sourceList []pysparkSourceInfo, jobType JobType, store SparkFileStoreV2, mappings []SourceMapping) (*sparkCommand, error) {
	logger := logger.With("deployMode", deployMode, "outputLocation", outputLocation, "outputLocationType", fmt.Sprintf("%T", outputLocation), "code", code, "sourceList", sourceList, "jobType", jobType, "store", store)
	logger.Debugw("SparkSubmitArgs")
	snowflakeConfig, err := getSnowflakeConfigFromSourceMapping(mappings)
	if err != nil {
		logger.Errorw("Could not get Snowflake config from source mapping", "error", err)
		return nil, err
	}
	sparkScriptRemotePath, err := sparkPythonFileURI(store, logger)
	if err != nil {
		logger.Errorw("Failed to get python file URI", "Err", err)
		return nil, err
	}
	var scriptArg string
	switch tfType {
	case SQLTransformation:
		scriptArg = "sql"
	case DFTransformation:
		scriptArg = "df"
	default:
		errMsg := fmt.Sprintf("Unknown transformation type: %s", tfType)
		logger.Error(errMsg)
		return nil, fferr.NewInternalErrorf(errMsg)
	}
	cmd := &sparkCommand{
		Script: sparkScriptRemotePath,
		ScriptArgs: []string{scriptArg},
		Configs: sparkCoreConfigs(
			sparkCoreConfigsArgs{
				JobType: jobType,
				ExecType: pc.EMR,
				Output: outputLocation,
				DeployMode: deployMode,
				SnowflakeConfig: snowflakeConfig,
				Store: store,
			},
		),
	}
	// In S3, we write the sql and sources to an extenral file to try to avoid going over the
	// maximum character limit
	if store.FilestoreType() == filestore.S3 && tfType == SQLTransformation {
		logger.Debugw("Writing submit params to file")
		paramsPath, err := writeSubmitParamsToFileStore(code, sourceList, store, logger)
		if err != nil {
			logger.Errorw("Failed to write submit params to file store", "err", err)
			return nil, err
		}
		logger.Debugw("submit params to file")
		cmd.AddConfigs(sparkSqlSubmitParamsURIFlag{
			URI: paramsPath,
		})
	} else if tfType == SQLTransformation {
		cmd.AddConfigs(sparkSqlQueryFlag{
			CleanQuery: code,
			Sources: sourceList,
		})
	} else if tfType == DFTransformation {
		cmd.AddConfigs(sparkDataframeQueryFlag{
			Code: code,
			Sources: sourceList,
		})
	}
	// EMR's API enforces a 10K-character (i.e. bytes) limit on string values passed to HadoopJarStep, so to avoid a 400, we need
	// to check to ensure the args are below this limit. If they exceed this limit, it's most likely due to the query and/or the list
	// of sources, so we write these as a JSON file and read them from the PySpark runner script to side-step this constraint
	if exceedsSubmitParamsTotalByteLimit(cmd) {
		logger.Errorw(
			"Command exceeded",
			"filestore", store.FilestoreType(),
			"command", cmd,
			"compiled command", cmd.Compile(),
		)
		return nil, fferr.NewInternalErrorf(
			"Featureform's spark submit is too long for Spark and file store type %s is not supported for remote args submit.",
			store.FilestoreType())
	}
	logger.Debugw("Compiled spark command", "command", cmd)
	return cmd, nil
}


type sparkCoreConfigsArgs struct {
	JobType JobType
	ExecType pc.SparkExecutorType
	Output pl.Location
	DeployMode types.SparkDeployMode
	SnowflakeConfig *pc.SnowflakeConfig
	Store SparkFileStoreV2
}

func sparkCoreConfigs(args sparkCoreConfigsArgs) sparkConfigs{
	configs := sparkConfigs{
		sparkSnowflakeFlags{
			Config: args.SnowflakeConfig,
			ExecutorType: args.ExecType,
		},
		sparkJobTypeFlag{
			Type: args.JobType,
		},
		sparkOutputFlag{
			Output: args.Output,
		},
		sparkDeployFlag{
			Mode: args.DeployMode,
		},
	}
	return append(configs, args.Store.SparkConfigs()...)
}

func readAndUploadFile(filePath filestore.Filepath, storePath filestore.Filepath, store SparkFileStoreV2) error {
	logger := logging.GlobalLogger.With(
		"fromPath", filePath.ToURI(),
		"toPath", filePath.ToURI(),
		"store", store.Type(),
	)
	fileExists, err := store.Exists(pl.NewFileLocation(storePath))
	if err != nil {
		logger.Errorw("Unable to check if file exists", "error", err)
		return err
	}
	if fileExists {
		logger.Infow("File already exists skipping copy")
		return nil
	}

	f, err := os.Open(filePath.Key())
	if err != nil {
		logger.Errorw("Unable open local file for copy", "error", err)
		return fferr.NewInternalError(err)
	}

	fileStats, err := f.Stat()
	if err != nil {
		logger.Errorw("Failed to get local file size", "error", err)
		return fferr.NewInternalError(err)
	}

	pythonScriptBytes := make([]byte, fileStats.Size())
	_, err = f.Read(pythonScriptBytes)
	if err != nil {
		logger.Errorw("Failed to read local file for copy", "error", err)
		return fferr.NewInternalError(err)
	}
	if err := store.Write(storePath, pythonScriptBytes); err != nil {
		logger.Errorw("Failed to write to remote path", "error", err)
		return err
	}
	logger.Infow("Copied local file to remote filestore")
	return nil
}

func removeEscapeCharacters(values []string) []string {
	for i, v := range values {
		v = strings.Replace(v, "\\", "", -1)
		v = strings.Replace(v, "\"", "", -1)
		values[i] = v
	}
	return values
}

func exceedsSubmitParamsTotalByteLimit(cmd *sparkCommand) bool {
	args := cmd.Compile()
	totalBytes := 0
	for _, str := range args {
		totalBytes += len(str)
	}
	spacesBetweenArgs := len(args) - 1
	totalBytes += spacesBetweenArgs
	return totalBytes >= SPARK_SUBMIT_PARAMS_BYTE_LIMIT
}

func writeSubmitParamsToFileStore(query string, sources []pysparkSourceInfo, store SparkFileStoreV2, logger logging.Logger) (filestore.Filepath, error) {
	paramsFileId := uuid.New()
	paramsPath, err := store.CreateFilePath(
		fmt.Sprintf(
			"featureform/spark-submit-params/%s.json",
			paramsFileId.String(),
		), false,
	)
	if err != nil {
		return nil, err
	}
	serializedSources := make([]string, len(sources))
	for i, source := range sources {
		serialized, err := source.Serialize()
		if err != nil {
			logger.Errorw("Failed to serialize source", "source", source, "err", err)
			return nil, err
		}
		serializedSources[i] = serialized
	}
	paramsMap := map[string]interface{}{}
	paramsMap["sql_query"] = query
	paramsMap["sources"] = serializedSources

	data, err := json.Marshal(paramsMap)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}

	logger.Debugw("Writing spark submit params to filestore", "path", paramsPath, "data", string(data))
	if err := store.Write(paramsPath, data); err != nil {
		return nil, err
	}

	return paramsPath, nil
}

func getSnowflakeConfigFromSourceMapping(mappings []SourceMapping) (*pc.SnowflakeConfig, error) {
	var snowflakeConfig *pc.SnowflakeConfig
	for _, mapping := range mappings {
		if mapping.ProviderType == pt.SnowflakeOffline {
			snowflakeConfig = &pc.SnowflakeConfig{}
			if err := snowflakeConfig.Deserialize(mapping.ProviderConfig); err != nil {
				return nil, err
			}
			break
		}
	}
	return snowflakeConfig, nil
}
