package config

import "github.com/featureform/helpers"

const PandasBaseImage = "featureformcom/k8s_runner"

func GetPandasRunnerImage() string {
	return helpers.GetEnv("PANDAS_RUNNER_IMAGE", PandasBaseImage)
}

func GetFeatureformVersion() string {
	return helpers.GetEnv("FEATUREFORM_VERSION", "0.0.0")
}
