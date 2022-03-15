package main

import (
	"fmt"
	"math"
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

func oscillationFactor(start time.Time) float64 {
	oscillationPeriod := 10*time.Minute
	return 2 + math.Sin(math.Sin(2*math.Pi*float64(time.Since(start))/float64(oscillationPeriod)))
}

func training(obs metrics.TrainingDataObserver, promMetrics metrics.PromMetricsHandler, start time.Time) {
	obs.SetError()
	for i := 0; i < 8; i++{
				obs.ServeRow()
		}
		obs.Finish()
}

func serving(obs metrics.PromFeatureObserver, promMetrics metrics.PromMetricsHandler, start time.Time) {
	obs.SetError()
	for i := 0; i < 4; i++ {
		time.Sleep(time.Duration(3*oscillationFactor(start)) * time.Millisecond)
		obs.ServeRow()
		obs.Finish()
	
	}
}

func TestMetrics(t *testing.T) {
	start := time.Now()
	instanceName := "test"
    promMetrics := metrics.NewMetrics("test")
	
	servingNum := 4
	servingErrorNum := 1

	servingObserver := promMetrics.BeginObservingOnlineServe("amt_spent", "30d").(metrics.PromFeatureObserver)
	trainingObserver := promMetrics.BeginObservingTrainingServe("is_fraud", "default").(metrics.TrainingDataObserver)
	serving(servingObserver, promMetrics, start)
	training(trainingObserver, promMetrics, start)

	//fmt.Println(testutil.CollectAndCount(servingObserver.Count, "test"))
	servingCounterValue, err := GetCounterValue(servingObserver.Count, instanceName, "amt_spent", "30d", "row serving")
	if err != nil {
		fmt.Println("error")
	}
	assert.Equal(t, int(servingCounterValue),servingNum, "4 feature rows should be served")

	servingErrorCounterValue, err := GetCounterValue(servingObserver.Count, instanceName, "amt_spent", "30d", "error")
	if err != nil {
		fmt.Println("error")
	}
	assert.Equal(t, int(servingErrorCounterValue),servingErrorNum, "1 feature rows should be recorded")
	trainingCounterValue, err := GetCounterValue(trainingObserver.Row_Count, instanceName, "is_fraud", "default", "row serve")
	if err != nil {
		fmt.Println("error")
	}
	assert.Equal(t, int(trainingCounterValue),8, "8 training data rows should be recorded")
	trainingErrorCounterValue, err := GetCounterValue(trainingObserver.Row_Count, instanceName, "is_fraud", "default", "error")
	if err != nil {
		fmt.Println("error")
	}
	assert.Equal(t, int(trainingErrorCounterValue),1, "1 training data errors should be recorded")
	latencyCounterValue, err := GetHistogramValue(promMetrics.Hist, instanceName, "amt_spent", "30d", "")
	if err != nil {
		fmt.Println("error")
	}
	assert.Equal(t, int(latencyCounterValue),5, "Feature latency records 5 events")
	latencyTrainingCounterValue, err := GetHistogramValue(promMetrics.Hist, instanceName, "is_fraud", "default", "")
	if err != nil {
		fmt.Println("error")
	}
	assert.Equal(t, int(latencyTrainingCounterValue),2, "Training latency records 2 events")

}