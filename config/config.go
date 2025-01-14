// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package config

import (
	"crypto/md5"
	"fmt"
	"os"
	"strings"

	"github.com/featureform/helpers"
)

// image paths
const (
	PandasBaseImage = "featureformenterprise/k8s_runner"
	WorkerImage     = "featureformenterprise/worker"
)

// script paths
const (
	SparkLocalScriptPath              = "/app/provider/scripts/spark/offline_store_spark_runner.py"
	PythonLocalInitPath               = "/app/provider/scripts/spark/python_packages.sh"
	PythonRemoteInitPath              = "featureform/scripts/spark/python_packages.sh"
	MaterializeNoTimestampQueryPath   = "/app/provider/queries/materialize_no_ts.sql"
	MaterializeWithTimestampQueryPath = "/app/provider/queries/materialize_ts.sql"
)

type SparkFileConfigs struct {
	LocalScriptPath      string
	RemoteScriptPath     string
	PythonLocalInitPath  string
	PythonRemoteInitPath string
}

func GetWorkerImage() string {
	return helpers.GetEnv("WORKER_IMAGE", WorkerImage)
}

func GetPandasRunnerImage() string {
	return helpers.GetEnv("PANDAS_RUNNER_IMAGE", PandasBaseImage)
}

func getSparkLocalScriptPath() string {
	return helpers.GetEnv("SPARK_LOCAL_SCRIPT_PATH", SparkLocalScriptPath)
}

func ShouldSkipSparkHealthCheck() bool {
	return helpers.GetEnvBool("SKIP_SPARK_HEALTH_CHECK", false)
}

func ShouldUseDebugLogging() bool {
	return helpers.GetEnvBool("FEATUREFORM_DEBUG_LOGGING", false)
}

func CreateSparkScriptConfig() (SparkFileConfigs, error) {
	remoteScriptPath, err := createSparkRemoteScriptPath()
	if err != nil {
		return SparkFileConfigs{}, err
	}
	return SparkFileConfigs{
		LocalScriptPath:      getSparkLocalScriptPath(),
		RemoteScriptPath:     remoteScriptPath,
		PythonRemoteInitPath: getPythonRemoteInitPath(),
		PythonLocalInitPath:  getPythonLocalInitPath(),
	}, nil
}

// In the event adding the MD5 hash as a suffix to the filename fails, the default ensures the program
// can continue to process transformations and materialization without exceptions
func createSparkRemoteScriptPath() (string, error) {
	// Don't change script if running in a test environment
	if strings.HasSuffix(os.Args[0], ".test") {
		return "featureform/scripts/spark/offline_store_spark_runner.py", nil
	}
	runnerMD5, err := os.ReadFile("/app/provider/scripts/spark/offline_store_spark_runner_md5.txt")
	if err != nil {
		fmt.Printf("failed to read MD5 hash file: %v\nAttempting to read the file from the local filesystem\n", err)
		// TODO remove this hardcoding
		if filename, err := createHashFromFile("./provider/scripts/spark/offline_store_spark_runner.py"); err != nil {
			fmt.Printf("failed to create MD5 hash from file: %v\n", err)
			return "", fmt.Errorf("Could not generate valid MD5 hash for the pyspark file. Exiting...")
		} else {
			return filename, nil
		}
	} else {
		return fmt.Sprintf("featureform/scripts/spark/offline_store_spark_runner_%s.py", string(runnerMD5)), nil
	}
}

func createHashFromFile(file string) (string, error) {
	if pysparkFile, err := os.ReadFile(file); err != nil {
		return "", err
	} else {
		sum := md5.Sum(pysparkFile)
		return fmt.Sprintf("featureform/scripts/spark/offline_store_spark_runner_%x.py", sum), nil
	}
}

func getPythonLocalInitPath() string {
	return helpers.GetEnv("PYTHON_LOCAL_INIT_PATH", PythonLocalInitPath)
}

func getPythonRemoteInitPath() string {
	return helpers.GetEnv("PYTHON_REMOTE_INIT_PATH", PythonRemoteInitPath)
}

func GetMaterializeNoTimestampQueryPath() string {
	return helpers.GetEnv("MATERIALIZE_NO_TIMESTAMP_QUERY_PATH", MaterializeNoTimestampQueryPath)
}

func GetMaterializeWithTimestampQueryPath() string {
	return helpers.GetEnv("MATERIALIZE_WITH_TIMESTAMP_QUERY_PATH", MaterializeWithTimestampQueryPath)
}

func GetSlackChannelId() string {
	return helpers.GetEnv("SLACK_CHANNEL_ID", "") //no meaningful fallback ID
}
