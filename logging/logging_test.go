// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package logging

import (
	"context"
	"fmt"
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
	logger = logger.WithResource(Entity, "test-name", "test-variant")
	if logger.SugaredLogger == nil {
		t.Fatalf("SugaredLogger doesnt exist.")
	}
	fmt.Println(logger.GetValue("resource-type"))
	if logger.GetValue("resource-type") != Entity {
		t.Fatalf("Incorrect values for logger, expected %s, got %s", Entity, logger.GetValue("resource-type"))
	}
	if logger.GetValue("resource-name") != "test-name" {
		t.Fatalf("Incorrect values for logger, expected %s, got %s", "test-name", logger.GetValue("resource-name"))
	}
	if logger.GetValue("resource-variant") != "test-variant" {
		t.Fatalf("Incorrect values for logger, expected %s, got %s", "test-variant", logger.GetValue("resource-variant"))
	}
}

func TestWithProvider(t *testing.T) {
	logger := NewLogger("test-logger")
	logger = logger.WithProvider("test-provider", "test-name")
	if logger.SugaredLogger == nil {
		t.Fatalf("SugaredLogger doesnt exist.")
	}
	if logger.GetValue("provider-type") != "test-provider" {
		t.Fatalf("Incorrect values for logger, expected %s, got %s", "test-provider", logger.GetValue("provider-type"))
	}
	if logger.GetValue("provider-name") != "test-name" {
		t.Fatalf("Incorrect values for logger, expected %s, got %s", "test-name", logger.GetValue("provider-name"))
	}
}

func newContext() context.Context {
	return context.Background()
}

func TestInitializeRequestID(t *testing.T) {
	logger := NewLogger("test-logger")
	ctx := newContext()
	requestID, updatedContext, newLogger := logger.InitializeRequestID(ctx)
	if newLogger.id != requestID {
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
	updatedCtx := AttachRequestID(requestID, ctx, logger)

	requestIDFromContext := GetRequestIDFromContext(updatedCtx)
	if requestID != requestIDFromContext {
		t.Fatalf("Request ID not found in context. Expected %s, got %s", requestID, requestIDFromContext)
	}
	loggerFromContext := GetLoggerFromContext(updatedCtx)
	if loggerFromContext.id != requestID {
		t.Fatalf("ID not found in logger. Expected %s, got %s", requestID, loggerFromContext.id)
	}
}
