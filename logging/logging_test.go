package logging

import (
	"context"
	"testing"
)

func Test_NewLogger(t *testing.T) {
	logger := NewLogger("test-logger")
	if logger.id != "" || logger.SugaredLogger == nil {
		t.Fatalf("Logger created incorrectly.")
	}
}

func Test_WithResource(t *testing.T) {
	logger := NewLogger("test-logger")
	logger = logger.WithResource("test-resource", "test-name", "test-variant")
	if logger.SugaredLogger == nil {
		t.Fatalf("SugaredLogger doesnt exist.")
	}
	if logger.values["resource-type"] != "test-resource" {
		t.Fatalf("Incorrect values for logger, expected %s, got %s", "test-resource", logger.values["resource-type"])
	}
	if logger.values["resource-name"] != "test-name" {
		t.Fatalf("Incorrect values for logger, expected %s, got %s", "test-name", logger.values["resource-name"])
	}
	if logger.values["resource-variant"] != "test-variant" {
		t.Fatalf("Incorrect values for logger, expected %s, got %s", "test-variant", logger.values["resource-variant"])
	}
}

func Test_WithProvider(t *testing.T) {
	logger := NewLogger("test-logger")
	logger = logger.WithProvider("test-provider", "test-name")
	if logger.SugaredLogger == nil {
		t.Fatalf("SugaredLogger doesnt exist.")
	}
	if logger.values["provider-type"] != "test-provider" {
		t.Fatalf("Incorrect values for logger, expected %s, got %s", "test-provider", logger.values["provider-type"])
	}
	if logger.values["provider-name"] != "test-name" {
		t.Fatalf("Incorrect values for logger, expected %s, got %s", "test-name", logger.values["provider-name"])
	}
}

func newContext() context.Context {
	return context.Background()
}

func Test_InitializeRequestID(t *testing.T) {
	logger := NewLogger("test-logger")
	ctx := newContext()
	requestID, updatedContext, newLogger := logger.InitializeRequestID(ctx)
	if newLogger.id.String() != requestID {
		t.Fatalf("Logger Request ID not set correctly. Expected %s, got %s", requestID, newLogger.id)
	}
	requestIDFromContext := GetRequestIDFromContext(updatedContext)
	if requestID != requestIDFromContext {
		t.Fatalf("Request ID not found in context. Expected %s, got %s", requestID, requestIDFromContext)
	}
	loggerFromContext := GetLoggerFromContext(updatedContext)
	if loggerFromContext.id != newLogger.id {
		t.Fatalf("Logger not found in context. Expected %s, got %s", newLogger.id, loggerFromContext.id)
	}
}

func Test_UpdateContext(t *testing.T) {
	logger := NewLogger("test-logger")
	ctx := newContext()
	requestID := NewRequestID()
	updatedCtx := UpdateContext(ctx, logger, requestID.String())

	requestIDFromContext := GetRequestIDFromContext(updatedCtx)
	if requestID.String() != requestIDFromContext {
		t.Fatalf("Request ID not found in context. Expected %s, got %s", requestID, requestIDFromContext)
	}
	loggerFromContext := GetLoggerFromContext(updatedCtx)
	if loggerFromContext.id != requestID {
		t.Fatalf("ID not found in logger. Expected %s, got %s", requestID, loggerFromContext.id)
	}
}
