package config

import "github.com/featureform/helpers"

const PandasBaseImage

func GetPandasRunnerImage() string {
	return helpers.GetEnv("PANDAS_RUNNER_IMAGE", "featureformcom/k8s_runner:latest")
}
