package main

import (
	"fmt"
	"testing"
	"time"

	metrics "github.com/featureform/serving/metrics"
	prometheus "github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
)

func GetCounterValue(metric *prometheus.CounterVec, labelValues ...string) (float64, error) {
	var m = &dto.Metric{}
	if err := metric.WithLabelValues(labelValues...).Write(m); err != nil {
		return 0.0, err
	}
	return m.Counter.GetValue(), nil
}

func GetHistogramValue(metric *prometheus.HistogramVec, labelValues ...string) (uint64, error) {
	var m = &dto.Metric{}
	if err := metric.WithLabelValues(labelValues...).(prometheus.Histogram).Write(m); err != nil {
		return 0.0, err
	}
	return m.GetHistogram().GetSampleCount(), nil
}

func training(obs metrics.TrainingDataObserver, promMetrics metrics.PromMetricsHandler, start time.Time, num int, errors int) {
	for i := 0; i < errors; i++ {
		obs.SetError()
	}

	for i := 0; i < num; i++ {
		obs.ServeRow()
	}

	obs.Finish()
}

func serving(obs metrics.PromFeatureObserver, promMetrics metrics.PromMetricsHandler, start time.Time, num int, errors int) {
	for i := 0; i < errors; i++ {
		obs.SetError()
	}
	for i := 0; i < num; i++ {
		obs.ServeRow()
	}
	obs.Finish()
}

func TestMetrics(t *testing.T) {
	start := time.Now()
	instanceName := "test"
	promMetrics := metrics.NewMetrics(instanceName)
	featureName := "example_feature"
	featureVariant := "example_variant"
	trainingDatasetName := "example_dataset"
	trainingDatasetVariant := "example_variant"

	servingNum := 5
	servingErrorNum := 5
	trainingNum := 5
	trainingErrorNum := 5
	latencyServingCount := servingNum + 1
	latencyTrainingCount := trainingNum + 1

	servingObserver := promMetrics.BeginObservingOnlineServe(featureName, featureVariant).(metrics.PromFeatureObserver)
	trainingObserver := promMetrics.BeginObservingTrainingServe(trainingDatasetName, trainingDatasetVariant).(metrics.TrainingDataObserver)
	serving(servingObserver, promMetrics, start, servingNum, servingErrorNum)
	training(trainingObserver, promMetrics, start, trainingNum, trainingErrorNum)

	servingCounterValue, err := GetCounterValue(servingObserver.Count, instanceName, featureName, featureVariant, "row serving")
	if err != nil {
		fmt.Println("error", err)
	}
	assert.Equal(t, int(servingCounterValue), servingNum, "5 feature rows should be served")
	servingCounterValueInt, err := servingObserver.GetObservedRowCount()
	if err != nil {
		fmt.Println("error", err)
	}
	assert.Equal(t, servingCounterValueInt, servingNum, "5 feature rows should be served")
	servingErrorCounterValue, err := GetCounterValue(servingObserver.Count, instanceName, featureName, featureVariant, "error")
	if err != nil {
		fmt.Println("error", err)
	}
	assert.Equal(t, int(servingErrorCounterValue), servingErrorNum, "5 feature error rows should be recorded")
	servingErrorCounterValueInt, err := servingObserver.GetObservedErrorCount()
	if err != nil {
		fmt.Println("error", err)
	}
	assert.Equal(t, servingErrorCounterValueInt, servingNum, "5 feature error rows should be recorded")
	trainingCounterValue, err := GetCounterValue(trainingObserver.Row_Count, instanceName, trainingDatasetName, trainingDatasetVariant, "row serve")
	if err != nil {
		fmt.Println("error", err)
	}
	assert.Equal(t, int(trainingCounterValue), trainingNum, "5 training data rows should be recorded")
	trainingCounterValueInt, err := trainingObserver.GetObservedRowCount()
	if err != nil {
		fmt.Println("error", err)
	}
	assert.Equal(t, trainingCounterValueInt, trainingNum, "5 training data rows should be recorded")
	trainingErrorCounterValue, err := GetCounterValue(trainingObserver.Row_Count, instanceName, trainingDatasetName, trainingDatasetVariant, "error")
	if err != nil {
		fmt.Println("error", err)
	}
	assert.Equal(t, int(trainingErrorCounterValue), trainingErrorNum, "5 training data errors should be recorded")
	trainingErrorCounterValueInt, err := trainingObserver.GetObservedErrorCount()
	if err != nil {
		fmt.Println("error", err)
	}
	assert.Equal(t, trainingErrorCounterValueInt, trainingNum, "5 training data errors should be recorded")
	latencyCounterValue, err := GetHistogramValue(promMetrics.Hist, instanceName, featureName, featureVariant, "")
	if err != nil {
		fmt.Println("error", err)
	}
	assert.Equal(t, int(latencyCounterValue), latencyServingCount, "Feature latency records 6 events")
	latencyTrainingCounterValue, err := GetHistogramValue(promMetrics.Hist, instanceName, trainingDatasetName, trainingDatasetVariant, "")
	if err != nil {
		fmt.Println("error", err)
	}
	assert.Equal(t, int(latencyTrainingCounterValue), latencyTrainingCount, "Training latency records 6 events")

}
