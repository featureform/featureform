package logging

import (
	"context"
	"encoding/json"
	"io"
	"sync"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Logger struct {
	*zap.SugaredLogger
	id     RequestID
	values *sync.Map
}

type RequestID string
type contextKey string
type ResourceType string

// Constants when the provider type or variant is unknown
const (
	SkipProviderType string = ""
	NoVariant        string = ""
)

// Constants for the context keys
const (
	RequestIDKey = contextKey("request-id")
	LoggerKey    = contextKey("logger")
)

const (
	Provider           ResourceType = "provider"
	User               ResourceType = "user"
	Feature            ResourceType = "feature"
	FeatureVariant     ResourceType = "feature-variant"
	Source             ResourceType = "source"
	SourceVariant      ResourceType = "source-variant"
	TrainingSet        ResourceType = "training-set"
	TrainingSetVariant ResourceType = "training-set-variant"
	Entity             ResourceType = "entity"
	Model              ResourceType = "model"
	Label              ResourceType = "label"
	LabelVariant       ResourceType = "label-variant"
)

var GlobalLogger Logger

func init() {
	GlobalLogger = NewLogger("global-logger")
}

// NewRequestID generates a random request ID
func NewRequestID() RequestID {
	return RequestID(uuid.New().String())
}

// String returns the string representation of the RequestID
func (r RequestID) String() string {
	return string(r)
}

// WithRequestID adds the request id to the logger
func (logger Logger) withRequestID(id RequestID) Logger {
	if id == "" {
		logger.Warn("Request ID is empty")
		return logger
	}
	if logger.id != "" {
		logger.Warnw("Request ID already set in logger. Using existing Request ID", "current request-id", logger.id, "new request-id", id)
		return logger
	}
	valuesWithRequestID := logger.appendValueMap(map[string]interface{}{"request-id": id})
	return Logger{SugaredLogger: logger.With("request-id", id),
		id:     id,
		values: valuesWithRequestID}
}

// WithResource adds the resource type, resource name and variant to the logger and returns a new instance of the logger
func (logger Logger) WithResource(resourceType ResourceType, name, variant string) Logger {
	newValues := make(map[string]interface{})
	if resourceType != "" {
		newValues["resource-type"] = resourceType
		logger.SugaredLogger = logger.SugaredLogger.With("resource-type", resourceType)
	} else {
		logger.Warn("Resource type is an empty string")
	}

	if name != "" {
		newValues["resource-name"] = name
		logger.SugaredLogger = logger.SugaredLogger.With("resource-name", name)
	} else {
		logger.Warn("Resource name is empty")
	}

	if variant != "" {
		newValues["resource-variant"] = variant
		logger.SugaredLogger = logger.SugaredLogger.With("resource-variant", variant)
	} else {
		logger.Warn("Resource variant is empty")
	}

	combinedValues := logger.appendValueMap(newValues)

	return Logger{
		SugaredLogger: logger.SugaredLogger,
		id:            logger.id,
		values:        combinedValues,
	}
}

// WithProvider adds the provider type and provider name to the logger and returns a new instance of the logger
func (logger Logger) WithProvider(providerType, providerName string) Logger {
	newValues := make(map[string]interface{})
	if providerType != "" {
		newValues["provider-type"] = providerType
		logger.SugaredLogger = logger.SugaredLogger.With("provider-type", providerType)
	} else {
		logger.Warn("Provider type is empty")
	}

	if providerName != "" {
		newValues["provider-name"] = providerName
		logger.SugaredLogger = logger.SugaredLogger.With("provider-name", providerName)
	} else {
		logger.Warn("Provider name is empty")
	}

	combinedValues := logger.appendValueMap(newValues)

	return Logger{
		SugaredLogger: logger.SugaredLogger,
		id:            logger.id,
		values:        combinedValues,
	}
}

// Used for testing
func (logger Logger) WithValues(values map[string]interface{}) Logger {
	if values == nil {
		logger.Warn("Values are empty")
	}
	combinedValues := logger.appendValueMap(values)
	return Logger{
		SugaredLogger: logger.SugaredLogger,
		id:            logger.id,
		values:        combinedValues,
	}
}

// Used for testing
func (logger Logger) GetValue(key string) interface{} {
	value, ok := logger.values.Load(key)
	if !ok {
		logger.Warnw("Value not found", "key", key)
	}
	return value
}

func (logger Logger) appendValueMap(values map[string]interface{}) *sync.Map {

	combinedValues := &sync.Map{}
	for k, v := range values {
		combinedValues.Store(k, v)
	}
	logger.values.Range(func(key, value interface{}) bool {
		combinedValues.Store(key, value)
		return true
	})
	return combinedValues
}

// InitializeRequestID creates a new request ID and updates the logger and context
func (logger Logger) InitializeRequestID(ctx context.Context) (string, context.Context, Logger) {
	requestID := ctx.Value(RequestIDKey)
	if requestID == nil {
		logger.Debugw("Creating new Request ID", "request-id", requestID)
		requestID = NewRequestID()
		ctx = context.WithValue(ctx, RequestIDKey, requestID)
	}
	ctxLogger := ctx.Value(LoggerKey)
	if ctxLogger == nil {
		logger.Debugw("Adding logger to context")
		ctxLogger = logger.withRequestID(requestID.(RequestID))
		ctx = context.WithValue(ctx, LoggerKey, ctxLogger)
	}
	return requestID.(RequestID).String(), ctx, ctxLogger.(Logger)
}

// GetRequestIDFromContext returns the request ID from the context
func GetRequestIDFromContext(ctx context.Context) string {
	requestID := ctx.Value(RequestIDKey)
	if requestID == nil {
		GlobalLogger.Warn("Request ID not found in context")
		return ""
	}

	return requestID.(RequestID).String()
}

// GetLoggerFromContext returns the logger from the context
func GetLoggerFromContext(ctx context.Context) Logger {
	logger := ctx.Value(LoggerKey)
	if logger == nil {
		GlobalLogger.Warn("Logger not found in context")
		return NewLogger("logger")
	}
	return logger.(Logger)
}

// GetRequestID returns the request ID from the logger
func (logger Logger) GetRequestID() RequestID {
	return logger.id
}

// AttachRequestID updates the context and logger with an already existing request ID
func AttachRequestID(id string, ctx context.Context, logger Logger) context.Context {
	if ctx == nil {
		logger.Warn("Context is nil")
		return nil
	}
	contextID := ctx.Value(RequestIDKey)
	if contextID != nil {
		logger.Warnw("Request ID already set in context. Overwriting request ID", "old request-id", contextID, "new request-id", id)
	}
	ctx = context.WithValue(ctx, RequestIDKey, RequestID(id))
	logger = logger.withRequestID(RequestID(id))
	ctx = context.WithValue(ctx, LoggerKey, logger)
	return ctx
}

// AddLoggerToContext adds the logger to the context
func AddLoggerToContext(ctx context.Context, logger Logger) context.Context {
	contextLogger := ctx.Value(LoggerKey)
	if contextLogger == nil {
		ctx = context.WithValue(ctx, LoggerKey, logger)
	}
	return ctx
}

// NewLogger creates a new logger with the service name
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
		values:        &sync.Map{},
	}
}

// WrapZapLogger wraps an existing Sugared Logger with the logger interface
func WrapZapLogger(sugaredLogger *zap.SugaredLogger) Logger {
	return Logger{
		SugaredLogger: sugaredLogger,
		values:        &sync.Map{},
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
