// Copyright 2015 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// A simple example exposing fictional RPC latencies with different types of
// random distributions (uniform, normal, and exponential) as Prometheus
// metrics.
package main

import (
	"flag"
	"fmt"
	"math"
	//"math/rand"
	"time"

	metrics "github.com/featureform/embeddinghub/metrics"
)

func main() {

	promMetrics := metrics.NewMetrics("test")
	// Expose the registered metrics via HTTP.
	

	var (
		// addr              = flag.String("listen-address", ":8081", "The address to listen on for HTTP requests.")
		// uniformDomain     = flag.Float64("uniform.domain", 0.0002, "The domain for the uniform distribution.")
		//normDomain        = flag.Float64("normal.domain", 0.0002, "The domain for the normal distribution.")
		//normMean          = flag.Float64("normal.mean", 0.00001, "The mean for the normal distribution.")
		oscillationPeriod = flag.Duration("oscillation-period", 10*time.Minute, "The duration of the rate oscillation period.")
	)

	flag.Parse()


	start := time.Now()

	oscillationFactor := func() float64 {
		return 2 + math.Sin(math.Sin(2*math.Pi*float64(time.Since(start))/float64(*oscillationPeriod)))
	}

	// Periodically record some sample latencies for the three services.
	// go func() {
	// 	for {
	// 		v := rand.Float64() * *uniformDomain
	// 		rpcDurations.WithLabelValues("uniform").Observe(v)
	// 		time.Sleep(time.Duration(100*oscillationFactor()) * time.Millisecond)
	// 	}
	// }()

	go func() {
		for {
			// v := (rand.NormFloat64() * *normDomain) + *normMean
			//rpcDurations.WithLabelValues("normal").Observe(v)
			
			obs := promMetrics.BeginObservingOnlineServe("Non-free Sulfur Dioxide", "first-variant")
			// Demonstrate exemplar support with a dummy ID. This
			// would be something like a trace ID in a real
			// application.  Note the necessary type assertion. We
			// already know that rpcDurationsHistogram implements
			// the ExemplarObserver interface and thus don't need to
			// check the outcome of the type assertion.
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
	}

	
	metrics_port := ":2113"
	fmt.Println("Some metrics handling")
	promMetrics.ExposePort(metrics_port)
	
	// go func() {
	// 	for {
	// 		v := rand.ExpFloat64() / 1e6
	// 		rpcDurations.WithLabelValues("exponential").Observe(v)
	// 		time.Sleep(time.Duration(50*oscillationFactor()) * time.Millisecond)
	// 	}
	// }()

	
}