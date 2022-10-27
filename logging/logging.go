package logging

import (
	"go.uber.org/zap"
)

func NewLogger(service string) *zap.SugaredLogger {
	baseLogger, err := zap.NewDevelopment(
		zap.AddStacktrace(zap.ErrorLevel),
	)
	if err != nil {
		panic(err)
	}
	logger := baseLogger.Sugar().Named(service)
	return logger
}
