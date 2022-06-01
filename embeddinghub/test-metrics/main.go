package main

import (
	"flag"
	"math"
	"math/rand"
	"time"

	metrics "github.com/featureform/embeddinghub/metrics"
)

func main() {

	promMetrics := metrics.NewMetrics("test")
	r := rand.New(rand.NewSource(99))

	var (
		oscillationPeriod        = flag.Duration("oscillation-period", 10*time.Minute, "The duration of the rate oscillation period.")
		trainingErrorStandardDev = 2.0
		onlineErrorStandardDev   = 2.0
	)

	flag.Parse()

	start := time.Now()

	oscillationFactor := func() float64 {
		return 2 + math.Sin(math.Sin(2*math.Pi*float64(time.Since(start))/float64(*oscillationPeriod)))
	}

	go func() {
		for {

			obs := promMetrics.BeginObservingOnlineServe("Non-free Sulfur Dioxide", "first-variant")
			time.Sleep(time.Duration(3*oscillationFactor()) * time.Millisecond)
			if r.NormFloat64() > onlineErrorStandardDev {
				obs.SetError()
			} else {
				obs.Finish()
			}

		}
	}()

	go func() {
		for {
			featureObserver := promMetrics.BeginObservingTrainingServe("Wine Quality Dataset", "default-variant")
			for i := 1; i < 100; i++ {
				if r.NormFloat64() > trainingErrorStandardDev {
					featureObserver.SetError()
				} else {
					featureObserver.ServeRow()
				}
				time.Sleep(time.Duration(oscillationFactor()) * time.Millisecond)
			}
			featureObserver.Finish()
			time.Sleep(10 * time.Second)

		}
	}()

	metrics_port := ":2113"
	promMetrics.ExposePort(metrics_port)

}
