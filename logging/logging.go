package logging

import (
	"context"
	"encoding/json"
	"io"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Logger struct {
	*zap.SugaredLogger
}

type RequestID string

func NewRequestID() RequestID {
	return RequestID(uuid.New().String())
}

func (logger Logger) WithRequestID(id RequestID) Logger {
	return Logger{
		logger.With("request-id", id),
	}
}

func (logger Logger) WithResource(resourceType, name, variant, id string) Logger {
	return Logger{
		logger.With("request-id", id, "resource-type", resourceType, "name", name, "variant", variant),
	}
}

func (logger Logger) WithProvider(providerType, providerName string) Logger {
	return Logger{
		logger.With("provider-type", providerType, "provider-name", providerName),
	}
}

func (logger Logger) InitializeRequestID(ctx context.Context) (context.Context, Logger, string) {
	requestID, ok := ctx.Value("request-id").(RequestID)
	if !ok {
		requestID = NewRequestID()
		ctx = context.WithValue(ctx, "request-id", requestID)
	}
	logger, ok = ctx.Value("logger").(Logger)
	if !ok {
		logger = logger.WithRequestID(requestID)
		ctx = context.WithValue(ctx, "logger", logger)
	}
	return ctx, logger, string(requestID)
}

func GetRequestIDFromContext(ctx context.Context) string {
	requestID, ok := ctx.Value("request-id").(string)
	if !ok {
		NewLogger("logging").Warn("Request ID not found in context")
		return ""
	}
	return requestID
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
		logger,
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
		logger.Sugar().Named(service),
	}
}
