// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package metrics

type NoOpMetricsHandler struct{}

func (nop *NoOpMetricsHandler) BeginObservingOnlineServe(feature string, key string) FeatureObserver {
	return &NoOpFeatureObserver{}
}

func (nop *NoOpMetricsHandler) BeginObservingTrainingServe(name string, version string) FeatureObserver {
	return &NoOpFeatureObserver{}
}
func (nop *NoOpMetricsHandler) ExposePort(port string) {}

type NoOpFeatureObserver struct{}

func (nop *NoOpFeatureObserver) SetError() {}
func (nop *NoOpFeatureObserver) ServeRow() {}
func (nop *NoOpFeatureObserver) Finish()   {}
