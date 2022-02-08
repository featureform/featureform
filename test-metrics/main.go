package main

import (
	"flag"
	"fmt"
	"math"
	"time"

	metrics "github.com/featureform/embeddinghub/metrics"
)

func main() {

	promMetrics := metrics.NewMetrics("test")

	var (
		oscillationPeriod = flag.Duration("oscillation-period", 10*time.Minute, "The duration of the rate oscillation period.")
	)

	flag.Parse()

	start := time.Now()

	oscillationFactor := func() float64 {
		return 2 + math.Sin(math.Sin(2*math.Pi*float64(time.Since(start))/float64(*oscillationPeriod)))
	}

	go func() {
		for {

			obs := promMetrics.BeginObservingOnlineServe("Non-free Sulfur Dioxide", "first-variant")
			time.Sleep(time.Duration(75*oscillationFactor()) * time.Millisecond)
			obs.Finish()
		}
	}()

	go func() {
		for {
			featureObserver := promMetrics.BeginObservingTrainingServe("Wine Quality Dataset", "default-variant")

			time.Sleep(time.Duration(75*oscillationFactor()) * time.Millisecond)
			featureObserver.Finish()

		}
	}()

	metrics_port := ":2113"
	fmt.Println("Some metrics handling")
	promMetrics.ExposePort(metrics_port)

}
