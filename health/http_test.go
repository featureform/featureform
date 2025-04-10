// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package health

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestHandleStatus(t *testing.T) {
	err := os.Setenv("IMAGE_VERSION", "1.2.3")
	if err != nil {
		t.Fatalf("failed to set env variable: %v", err)
	}
	defer os.Unsetenv("IMAGE_VERSION")

	req, err := http.NewRequest("GET", "/status", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(handleStatus)

	handler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}

	expectedContentType := "application/json"
	if contentType := rr.Header().Get("Content-Type"); contentType != expectedContentType {
		t.Errorf("handler returned wrong content-type header: got %v want %v", contentType, expectedContentType)
	}

	expectedTime := time.Now().UTC()
	expectedResponse := map[string]string{
		"status":  "OK",
		"app":     "featureform",
		"version": "1.2.3",
		"time":    expectedTime.Format("1/2/06, 3:04:05 PM MST"),
	}

	var actualResponse map[string]string
	err = json.Unmarshal(rr.Body.Bytes(), &actualResponse)
	if err != nil {
		t.Fatalf("failed to unmarshal response body: %v", err)
	}

	if actualResponse["status"] != expectedResponse["status"] || actualResponse["app"] != expectedResponse["app"] || actualResponse["version"] != expectedResponse["version"] {
		t.Errorf("handler returned unexpected body: got %v want %v", actualResponse, expectedResponse)
	}

	actualTime, err := time.Parse("1/2/06, 3:04:05 PM MST", actualResponse["time"])
	if err != nil {
		t.Errorf("failed to parse time from response: %v", err)
	}
	if actualTime.Before(expectedTime.Add(-1*time.Minute)) || actualTime.After(expectedTime.Add(1*time.Minute)) {
		t.Errorf("handler returned unexpected time: got %v want something close to %v", actualResponse["time"], expectedTime.Format("1/2/06, 3:04:05 PM MST"))
	}
}
