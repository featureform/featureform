package main

import (
	//remote packages

	"fmt"
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

//generic interfaces exposed to the user
type MetricsHandler interface {
	BeginObservingFeatureServe(feature string, key string) FeatureObserver
	ExposePort(port string)
}

type FeatureObserver interface {
	SetError()
	Finish()
}

type PromMetricsHandler struct {
	Hist  *prometheus.HistogramVec
	Count *prometheus.CounterVec
	Name  string
}

type PromFeatureObserver struct {
	Timer   *prometheus.Timer
	Count   *prometheus.CounterVec
	Name    string
	Feature string
	Key     string
	Status  string
}

func NewMetrics(name string) PromMetricsHandler {
	var getFeatureCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: fmt.Sprintf("%s_counter", name), // metric name
			Help: "Counter for feature serve requests, labeled by feature name, key and type",
		},
		[]string{"instance", "feature", "key", "status"}, // labels
	)

	var getFeatureLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    fmt.Sprintf("%s_duration_seconds", name),
			Help:    "Latency for feature serve requests, labeled by feature name, key and type",
			Buckets: prometheus.LinearBuckets(0.01, 0.05, 10),
		},
		[]string{"instance", "feature", "key", "status"}, //labels
	)

	prometheus.MustRegister(getFeatureCounter)
	prometheus.MustRegister(getFeatureLatency)
	return PromMetricsHandler{
		Hist:  getFeatureLatency,
		Count: getFeatureCounter,
		Name:  name,
	}
}

func (p PromMetricsHandler) BeginObservingFeatureServe(feature string, key string) FeatureObserver {
	timer := prometheus.NewTimer(prometheus.ObserverFunc(func(v float64) {
		p.Hist.WithLabelValues(p.Name, feature, key, "error").Observe(v)
	}))
	return PromFeatureObserver{
		Timer:   timer,
		Count:   p.Count,
		Name:    p.Name,
		Feature: feature,
		Key:     key,
		Status:  "error",
	}
}

func (p PromMetricsHandler) ExposePort(port string) {
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(port, nil))

}

func (p PromFeatureObserver) SetError() {
	p.Timer.ObserveDuration()
	p.Count.WithLabelValues(p.Name, p.Feature, p.Key, p.Status).Inc()
}

func (p PromFeatureObserver) Finish() {
	p.Status = "success"
	p.Timer.ObserveDuration()
	p.Count.WithLabelValues(p.Name, p.Feature, p.Key, p.Status).Inc()
}
