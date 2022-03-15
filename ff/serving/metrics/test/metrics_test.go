package main

import (
	"fmt"
	"time"
	"testing"

	"github.com/stretchr/testify/assert"
	dto "github.com/prometheus/client_model/go"
	prometheus "github.com/prometheus/client_golang/prometheus"
	metrics "github.com/featureform/serving/metrics"
)

func GetCounterValue(metric *prometheus.CounterVec, labelValues ...string) (float64, error){
    var m = &dto.Metric{}
    if err := metric.WithLabelValues(labelValues...).Write(m); err != nil {
        return 0.0, err
    }
    return m.Counter.GetValue(), nil
}

func GetHistogramValue(metric *prometheus.HistogramVec, labelValues ...string) (uint64, error){
    var m = &dto.Metric{}
    if err := metric.WithLabelValues(labelValues...).(prometheus.Histogram).Write(m); err != nil {
        return 0.0, err
    }
    return m.GetHistogram().GetSampleCount(), nil
}

func training(obs metrics.TrainingDataObserver, promMetrics metrics.PromMetricsHandler, start time.Time, num int, errors int) {
	for i := 0; i < errors; i++{
		obs.SetError()
	}
	
	for i := 0; i < num; i++{
		obs.ServeRow()
	}
		
	obs.Finish()
}

func serving(obs metrics.PromFeatureObserver, promMetrics metrics.PromMetricsHandler, start time.Time, num int, errors int) {
	for i := 0; i < errors; i++{
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
    promMetrics := metrics.NewMetrics("test")
	
	servingNum := 5
	servingErrorNum := 5
	trainingNum := 5
	trainingErrorNum := 5
	latencyServingCount := servingNum + 1
	latencyTrainingCount := trainingNum + 1

	servingObserver := promMetrics.BeginObservingOnlineServe("amt_spent", "30d").(metrics.PromFeatureObserver)
	trainingObserver := promMetrics.BeginObservingTrainingServe("is_fraud", "default").(metrics.TrainingDataObserver)
	serving(servingObserver, promMetrics, start, servingNum, servingErrorNum)
	training(trainingObserver, promMetrics, start, trainingNum, trainingErrorNum)

	servingCounterValue, err := GetCounterValue(servingObserver.Count, instanceName, "amt_spent", "30d", "row serving")
	if err != nil {
		fmt.Println("error")
	}
	assert.Equal(t, int(servingCounterValue),servingNum, "5 feature rows should be served")

	servingErrorCounterValue, err := GetCounterValue(servingObserver.Count, instanceName, "amt_spent", "30d", "error")
	if err != nil {
		fmt.Println("error")
	}
	assert.Equal(t, int(servingErrorCounterValue),servingErrorNum, "5 feature rows should be recorded")
	trainingCounterValue, err := GetCounterValue(trainingObserver.Row_Count, instanceName, "is_fraud", "default", "row serve")
	if err != nil {
		fmt.Println("error")
	}
	assert.Equal(t, int(trainingCounterValue),trainingNum, "5 training data rows should be recorded")
	trainingErrorCounterValue, err := GetCounterValue(trainingObserver.Row_Count, instanceName, "is_fraud", "default", "error")
	if err != nil {
		fmt.Println("error")
	}
	assert.Equal(t, int(trainingErrorCounterValue),trainingErrorNum, "5 training data errors should be recorded")
	latencyCounterValue, err := GetHistogramValue(promMetrics.Hist, instanceName, "amt_spent", "30d", "")
	if err != nil {
		fmt.Println("error")
	}
	assert.Equal(t, int(latencyCounterValue),latencyServingCount , "Feature latency records 6 events")
	latencyTrainingCounterValue, err := GetHistogramValue(promMetrics.Hist, instanceName, "is_fraud", "default", "")
	if err != nil {
		fmt.Println("error")
	}
	assert.Equal(t, int(latencyTrainingCounterValue),latencyTrainingCount, "Training latency records 6 events")

}