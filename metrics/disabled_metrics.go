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
