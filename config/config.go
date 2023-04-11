package config

import "github.com/featureform/helpers"

const PandasBaseImage = "featureformcom/k8s_runner"

const SparkLocalScriptPath = "/app/provider/scripts/spark/offline_store_spark_runner.py"
const SparkRemoteScriptPath = "featureform/scripts/spark/offline_store_spark_runner.py"
const PythonLocalInitPath = "/app/provider/scripts/spark/python_packages.sh"
const PythonRemoteInitPath = "featureform/scripts/spark/python_packages.sh"

func GetPandasRunnerImage() string {
	return helpers.GetEnv("PANDAS_RUNNER_IMAGE", PandasBaseImage)
}

func GetSparkLocalScriptPath() string {
	return helpers.GetEnv("SPARK_LOCAL_SCRIPT_PATH", SparkLocalScriptPath)
}

func GetSparkRemoteScriptPath() string {
	return helpers.GetEnv("SPARK_REMOTE_SCRIPT_PATH", SparkRemoteScriptPath)
}

func GetPythonLocalInitPath() string {
	return helpers.GetEnv("PYTHON_INIT_PATH", PythonLocalInitPath)
}

func GetPythonRemoteInitPath() string {
	return helpers.GetEnv("PYTHON_INIT_PATH", PythonRemoteInitPath)
}
