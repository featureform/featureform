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
	id RequestID
}

type RequestID string
type contextKey string

const (
	RequestIDKey = contextKey("request-id")
	LoggerKey    = contextKey("logger")
)

func NewRequestID() RequestID {
	return RequestID(uuid.New().String())
}

func (r RequestID) String() string {
	return string(r)
}

func (logger Logger) WithRequestID(id RequestID) Logger {
	if id == "" {
		logger.Warn("Request ID is empty")
		return logger
	}
	if logger.id != "" {
		logger.Warnw("Request ID already set in logger", "current request-id", logger.id, "new request-id", id)
		return logger
	}

	return Logger{SugaredLogger: logger.With("request-id", id),
		id: id}
}

func (logger Logger) WithResource(resourceType, name, variant string) Logger {
	return Logger{
		SugaredLogger: logger.With("resource-type", resourceType, "name", name, "variant", variant),
		id:            logger.id,
	}
}

func (logger Logger) WithProvider(providerType, providerName string) Logger {
	return Logger{
		SugaredLogger: logger.With("provider-type", providerType, "provider-name", providerName),
		id:            logger.id,
	}
}

func (logger Logger) InitializeRequestID(ctx context.Context) (string, context.Context, Logger) {
	requestID, ok := ctx.Value(RequestIDKey).(RequestID)
	if !ok {
		requestID = NewRequestID()
		ctx = context.WithValue(ctx, RequestIDKey, requestID)
	}
	ctxLogger, hasLogger := ctx.Value(LoggerKey).(Logger)
	if !hasLogger {
		ctxLogger = logger.WithRequestID(requestID)
		ctx = context.WithValue(ctx, LoggerKey, ctxLogger)
	}
	return requestID.String(), ctx, ctxLogger
}

func GetRequestIDFromContext(ctx context.Context) string {
	requestID, ok := ctx.Value(RequestIDKey).(string)
	if !ok {
		NewLogger("logging").Warn("Request ID not found in context")
		return ""
	}
	return requestID
}

func NewLogger(service string) Logger {
	baseLogger, err := zap.NewDevelopment(
		zap.AddStacktrace(zap.WarnLevel),
	)
	if err != nil {
		panic(err)
	}
	logger := baseLogger.Sugar().Named(service)
	return Logger{
		SugaredLogger: logger,
		id:            "",
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
		id:            "",
	}
}
