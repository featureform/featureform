package logging

import (
	"context"
	"testing"
)

func TestNewLogger(t *testing.T) {
	logger := NewLogger("test-logger")
	if logger.id != "" || logger.SugaredLogger == nil {
		t.Fatalf("Logger created incorrectly.")
	}
}

func TestWithResource(t *testing.T) {
	logger := NewLogger("test-logger")
	logger = logger.WithResource("test-resource", "test-name", "test-variant")
	if logger.SugaredLogger == nil {
		t.Fatalf("SugaredLogger doesnt exist.")
	}
	if logger.Values["resource-type"] != "test-resource" {
		t.Fatalf("Incorrect values for logger, expected %s, got %s", "test-resource", logger.Values["resource-type"])
	}
	if logger.Values["resource-name"] != "test-name" {
		t.Fatalf("Incorrect values for logger, expected %s, got %s", "test-name", logger.Values["resource-name"])
	}
	if logger.Values["resource-variant"] != "test-variant" {
		t.Fatalf("Incorrect values for logger, expected %s, got %s", "test-variant", logger.Values["resource-variant"])
	}
}

func TestWithProvider(t *testing.T) {
	logger := NewLogger("test-logger")
	logger = logger.WithProvider("test-provider", "test-name")
	if logger.SugaredLogger == nil {
		t.Fatalf("SugaredLogger doesnt exist.")
	}
	if logger.Values["provider-type"] != "test-provider" {
		t.Fatalf("Incorrect values for logger, expected %s, got %s", "test-provider", logger.Values["provider-type"])
	}
	if logger.Values["provider-name"] != "test-name" {
		t.Fatalf("Incorrect values for logger, expected %s, got %s", "test-name", logger.Values["provider-name"])
	}
}

func newContext() context.Context {
	return context.Background()
}

func TestInitializeRequestID(t *testing.T) {
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

func TestUpdateContext(t *testing.T) {
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
