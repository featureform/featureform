package config

import "github.com/featureform/helpers"

func GetPandasRunnerImage() string {
	return helpers.GetEnv("PANDAS_RUNNER_IMAGE", "featureformcom/k8s_runner:latest")
}
