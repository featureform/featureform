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
	id     RequestID
	values map[string]interface{}
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
	if resourceType == "" {
		logger.Warn("Resource type is empty")
	}
	if name == "" {
		logger.Warn("Resource name is empty")
	}
	if variant == "" {
		logger.Warn("Resource variant is empty")
	}
	resourceValues := map[string]interface{}{"resource-type": resourceType, "resource-name": name, "resource-variant": variant}
	return Logger{
		SugaredLogger: logger.With("resource-type", resourceType, "name", name, "variant", variant),
		id:            logger.id,
		values:        resourceValues,
	}
}

func (logger Logger) WithProvider(providerType, providerName string) Logger {
	if providerType == "" {
		logger.Warn("Provider type is empty")
	}
	if providerName == "" {
		logger.Warn("Provider name is empty")
	}
	providerValues := map[string]interface{}{"provider-type": providerType, "provider-name": providerName}
	return Logger{
		SugaredLogger: logger.With("provider-type", providerType, "provider-name", providerName),
		id:            logger.id,
		values:        providerValues,
	}
}

func (logger Logger) WithValues(values map[string]interface{}) Logger {
	if values == nil {
		logger.Warn("Values are empty")
	}
	combinedValues := logger.values
	for k, v := range values {
		combinedValues[k] = v
		logger.SugaredLogger = logger.SugaredLogger.With(k, v)
	}
	return Logger{
		SugaredLogger: logger.SugaredLogger,
		id:            logger.id,
		values:        combinedValues,
	}
}

func (logger Logger) InitializeRequestID(ctx context.Context) (string, context.Context, Logger) {
	requestID := ctx.Value(RequestIDKey)
	if requestID == nil {
		requestID = NewRequestID()
		ctx = context.WithValue(ctx, RequestIDKey, requestID)
	}
	ctxLogger := ctx.Value(LoggerKey)
	if ctxLogger == nil {
		ctxLogger = logger.WithRequestID(requestID.(RequestID))
		ctx = context.WithValue(ctx, LoggerKey, ctxLogger)
	}
	return requestID.(RequestID).String(), ctx, ctxLogger.(Logger)
}

func GetRequestIDFromContext(ctx context.Context) string {
	requestID := ctx.Value(RequestIDKey)
	if requestID == nil {
		NewLogger("logging").Warn("Request ID not found in context")
		return ""
	}

	return requestID.(RequestID).String()
}

func GetLoggerFromContext(ctx context.Context) Logger {
	logger := ctx.Value(LoggerKey)
	if logger == nil {
		NewLogger("logging").Warn("Logger not found in context")
		return NewLogger("logger")
	}

	return logger.(Logger)
}

func (logger Logger) GetRequestID() RequestID {
	return logger.id
}

func UpdateContext(ctx context.Context, logger Logger, id string) context.Context {
	contextID := ctx.Value(RequestIDKey)
	if contextID == nil {
		if id == "" {
			id = NewRequestID().String()
		}
		ctx = context.WithValue(ctx, RequestIDKey, RequestID(id))
	}
	contextLogger := ctx.Value(LoggerKey)
	if contextLogger == nil {
		logger = logger.WithRequestID(RequestID(id))
		ctx = context.WithValue(ctx, LoggerKey, logger)
	}
	return ctx
}

func AddLoggerToContext(ctx context.Context, logger Logger) context.Context {
	contextLogger := ctx.Value(LoggerKey)
	if contextLogger == nil {
		ctx = context.WithValue(ctx, LoggerKey, logger)
	}
	return ctx
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
