package config

import "github.com/featureform/helpers"

const PandasBaseImage = "featureformcom/k8s_runner"

func GetPandasRunnerImage() string {
	return helpers.GetEnv("PANDAS_RUNNER_IMAGE", PandasBaseImage)
}
