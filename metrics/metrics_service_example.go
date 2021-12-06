package main

import (
	"math/rand"
	"time"
)

var metrics MetricsHandler

func testServing() {
	go func() {
		for {
			serveFeature("Non-free Sulfur Dioxide", "feature_key")
		}
	}()
}

func init() {
	metrics = NewMetrics("test")
}

func serveFeature(feature string, key string) {
	featureObserver := metrics.BeginObservingFeatureServe(feature, key)
	defer featureObserver.Finish()
	r := rand.Intn(10)
	time.Sleep(time.Duration(r) * time.Microsecond)
	if r <= 1 {
		featureObserver.SetError()
	}
}

func main() {

	testServing()
	metrics.ExposePort(":2112")
}
