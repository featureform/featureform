package logging

import (
	"encoding/json"
	"io"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Logger struct {
	SugaredLogger *zap.SugaredLogger
}

type RequestID string

func NewRequestID() string {
	return uuid.New().String()
}

// Can I change the function name to WithRequestID?
func (logger Logger) AddRequestID(id RequestID) Logger {
	return Logger{
		SugaredLogger: logger.SugaredLogger.With("request-id", id),
	}
}

func (logger Logger) AddResource(resourceType, name, variant, id string) Logger {
	return Logger{
		SugaredLogger: logger.SugaredLogger.With("request-id", id, "resource-type", resourceType, "name", name, "variant", variant),
	}
}

func NewLogger(service string) Logger {
	baseLogger, err := zap.NewDevelopment(
		zap.AddStacktrace(zap.ErrorLevel),
	)
	if err != nil {
		panic(err)
	}
	logger := baseLogger.Sugar().Named(service)
	return Logger{
		SugaredLogger: logger,
	}
}

func NewStackTraceLogger(service string) Logger {
	cfg := zap.Config{
		Encoding:         "json",
		Level:            zap.NewAtomicLevelAt(zap.DebugLevel),
		Development:      true,
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
		EncoderConfig: zapcore.EncoderConfig{
			NewReflectedEncoder: func(w io.Writer) zapcore.ReflectedEncoder {
				enc := json.NewEncoder(w)
				enc.SetEscapeHTML(false)
				enc.SetIndent("", "    ")
				return enc
			},
		},
	}
	logger, err := cfg.Build()
	if err != nil {
		panic(err)
	}
	return Logger{
		SugaredLogger: logger.Sugar().Named(service),
	}
}
