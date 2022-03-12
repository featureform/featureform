package main

import (
	"math/rand"
	"time"
	"github.com/featureform/serving/metrics"
)

var metricsHandler metrics.MetricsHandler

func testServing() {
	go func() {
		for {
			serveFeature("fixed_acidity", "feature_key")
			serveFeature("Non-free Sulfur Dioxide", "feature_key")
			serveFeature("Wine quality set", "Feature Set")
		}
	}()
}

func init() {
	metricsHandler = metrics.NewMetrics("test")
}

func serveFeature(feature string, key string) {
	featureObserver := metricsHandler.BeginObservingOnlineServe(feature, key)
	defer featureObserver.Finish()
	r := rand.Intn(10)
	time.Sleep(time.Duration(r) * time.Microsecond)
	if r <= 1 {
		featureObserver.SetError()
	}
}

func main() {

	testServing()
	metricsHandler.ExposePort(":2112")
}
