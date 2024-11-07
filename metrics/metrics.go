// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package metrics

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
)

type Observation string

const (
	TRAINING_ROW_SERVE Observation = "training_row_serve"
	ONLINE_ROW_SERVE               = "online_row_serve"
	ERROR                          = "error"
	SUCCESS                        = "success"
)

// generic interfaces exposed to the user
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
	Timer   *prometheus.Timer
	Count   *prometheus.CounterVec
	Name    string
	Feature string
	Key     string
	Status  string
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
			Name: fmt.Sprintf("%srequests", name), // metric name
			Help: "Counter for feature and training-set serve requests, labeled by name, variant and status",
		},
		[]string{"instance", "name", "variant", "status"}, // labels
	)

	var getFeatureLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    fmt.Sprintf("%srequest_duration", name),
			Help:    "Latency for feature and training-set serve requests, labeled by name, variant and status",
			Buckets: prometheus.LinearBuckets(0.01, 0.05, 10),
		},
		[]string{"instance", "name", "variant", "status"}, //labels
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
		p.Hist.WithLabelValues(p.Name, feature, key, "").Observe(v)
	}))
	return PromFeatureObserver{
		Timer:   timer,
		Count:   p.Count,
		Name:    p.Name,
		Feature: feature,
		Key:     key,
		Status:  "running",
	}
}
func (p PromMetricsHandler) BeginObservingTrainingServe(name string, version string) FeatureObserver {
	timestamp := time.Now().UTC().Format("20060102150405")
	timer := prometheus.NewTimer(prometheus.ObserverFunc(func(v float64) {
		p.Hist.WithLabelValues(p.Name, name, version, "").Observe(v)
	}))
	return TrainingDataObserver{
		Timer:     timer,
		Row_Count: p.Count,
		Timestamp: timestamp,
		Title:     p.Name,
		Name:      name,
		Version:   version,
		Status:    "running",
	}
}

func (p PromMetricsHandler) ExposePort(port string) {
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(port, nil))

}

func (p PromFeatureObserver) SetError() {
	p.Status = string(ERROR)
	p.Timer.ObserveDuration()
	p.Count.WithLabelValues(p.Name, p.Feature, p.Key, p.Status).Inc()
}

func (p PromFeatureObserver) ServeRow() {
	p.Count.WithLabelValues(p.Name, p.Feature, p.Key, string(ONLINE_ROW_SERVE)).Inc()
}

func (p PromFeatureObserver) Finish() {
	p.Status = string(SUCCESS)
	p.Timer.ObserveDuration()
	p.Count.WithLabelValues(p.Name, p.Feature, p.Key, p.Status).Inc()
}

func (p PromFeatureObserver) GetObservedRowCount() (int, error) {
	var m = &dto.Metric{}
	if err := p.Count.WithLabelValues(p.Name, p.Feature, p.Key, string(ONLINE_ROW_SERVE)).Write(m); err != nil {
		return 0, err
	}
	return int(m.Counter.GetValue()), nil
}

func (p PromFeatureObserver) GetObservedErrorCount() (int, error) {
	var m = &dto.Metric{}
	if err := p.Count.WithLabelValues(p.Name, p.Feature, p.Key, string(ERROR)).Write(m); err != nil {
		return 0, err
	}
	return int(m.Counter.GetValue()), nil
}

func (p TrainingDataObserver) SetError() {
	p.Status = string(ERROR)
	p.Timer.ObserveDuration()
	p.Row_Count.WithLabelValues(p.Title, p.Name, p.Version, p.Status).Inc()
}

func (p TrainingDataObserver) ServeRow() {
	p.Row_Count.WithLabelValues(p.Title, p.Name, p.Version, string(TRAINING_ROW_SERVE)).Inc()
}

func (p TrainingDataObserver) GetObservedRowCount() (int, error) {
	var m = &dto.Metric{}
	if err := p.Row_Count.WithLabelValues(p.Title, p.Name, p.Version, string(TRAINING_ROW_SERVE)).Write(m); err != nil {
		return 0, err
	}
	return int(m.Counter.GetValue()), nil
}

func (p TrainingDataObserver) GetObservedErrorCount() (int, error) {
	var m = &dto.Metric{}
	if err := p.Row_Count.WithLabelValues(p.Title, p.Name, p.Version, string(ERROR)).Write(m); err != nil {
		return 0, err
	}
	return int(m.Counter.GetValue()), nil
}

func (p TrainingDataObserver) Finish() {
	p.Status = string(SUCCESS)
	p.Timer.ObserveDuration()
}
