package metrics

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

//generic interfaces exposed to the user
type MetricsHandler interface {
	BeginObservingOnlineServe(feature string, key string) FeatureObserver
	BeginObservingTrainingServe(name string, version string) FeatureObserver
	ExposePort(port string)
}

type FeatureObserver interface {
	SetError()
	ServeRow()
	Finish()
}

type PromMetricsHandler struct {
	Hist  *prometheus.HistogramVec
	Count *prometheus.CounterVec
	Name  string
}

type PromFeatureObserver struct {
	Timer     *prometheus.Timer
	Count     *prometheus.CounterVec
	Timestamp string
	Name      string
	Feature   string
	Key       string
	Status    string
}

type TrainingDataObserver struct {
	Timer     *prometheus.Timer
	Row_Count *prometheus.CounterVec
	Timestamp string
	Title     string
	Name      string
	Version   string
	Status    string
}

func NewMetrics(name string) PromMetricsHandler {
	var getFeatureCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: fmt.Sprintf("%s_counter", name), // metric name
			Help: "Counter for feature serve requests, labeled by feature name, key and type",
		},
		[]string{"instance", "feature", "key", "action", "status", "start"}, // labels
	)

	var getFeatureLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    fmt.Sprintf("%s_duration_seconds", name),
			Help:    "Latency for feature serve requests, labeled by feature name, key and type",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 10),
		},
		[]string{"instance", "feature", "key", "action", "status", "start"}, //labels
	)

	prometheus.MustRegister(getFeatureCounter)
	prometheus.MustRegister(getFeatureLatency)
	return PromMetricsHandler{
		Hist:  getFeatureLatency,
		Count: getFeatureCounter,
		Name:  name,
	}
}

func (p PromMetricsHandler) BeginObservingOnlineServe(feature string, key string) FeatureObserver {
	timer := prometheus.NewTimer(prometheus.ObserverFunc(func(v float64) {
		p.Hist.WithLabelValues(p.Name, feature, key, "success").Observe(v)
	}))
	return PromFeatureObserver{
		Timer:     timer,
		Count:     p.Count,
		Timestamp: time.Now().UTC().Format("20060102150405"),
		Name:      p.Name,
		Feature:   feature,
		Key:       key,
		Status:    "success",
	}
}
func (p PromMetricsHandler) BeginObservingTrainingServe(name string, version string) FeatureObserver {
	timer := prometheus.NewTimer(prometheus.ObserverFunc(func(v float64) {
		p.Hist.WithLabelValues(p.Name, name, version, "success").Observe(v)
	}))
	return TrainingDataObserver{
		Timer:     timer,
		Row_Count: p.Count,
		Timestamp: time.Now().UTC().Format("20060102150405"),
		Title:     p.Name,
		Name:      name,
		Version:   version,
		Status:    "success",
	}
}

func (p PromMetricsHandler) ExposePort(port string) {
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(port, nil))

}

func (p PromFeatureObserver) SetError() {
	p.Status = "error"
	p.Timer.ObserveDuration()
	p.Count.WithLabelValues(p.Name, p.Feature, p.Key, "feature serve", p.Status, p.Timestamp).Inc()
}

func (p PromFeatureObserver) ServeRow() {
	p.Count.WithLabelValues(p.Name, p.Feature, p.Key, "row serving", p.Status, p.Timestamp).Inc()
}

func (p PromFeatureObserver) Finish() {
	p.Timer.ObserveDuration()
	p.Count.WithLabelValues(p.Name, p.Feature, p.Key, "finish serve", p.Status, p.Timestamp).Inc()
}

func (p TrainingDataObserver) SetError() {
	p.Status = "error"
	p.Timer.ObserveDuration()
	p.Row_Count.WithLabelValues(p.Title, p.Name, p.Version, "row serving", p.Status, p.Timestamp).Inc()
}

func (p TrainingDataObserver) ServeRow() {
	p.Row_Count.WithLabelValues(p.Title, p.Name, p.Version, "row serving", p.Status, p.Timestamp).Inc()
}

func (p TrainingDataObserver) Finish() {
	p.Timer.ObserveDuration()
}
