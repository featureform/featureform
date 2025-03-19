// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package health

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	help "github.com/featureform/helpers"
	"github.com/featureform/logging"
)

func handleHealthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Strict-Transport-Security", "max-age=63072000; includeSubDomains")
	w.WriteHeader(http.StatusOK)

	_, err := io.WriteString(w, "OK")
	if err != nil {
		fmt.Printf("health check write response error: %+v", err)
	}

}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Strict-Transport-Security", "max-age=63072000; includeSubDomains")
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)

	_, err := io.WriteString(w, `<html><body>Welcome to featureform</body></html>`)
	if err != nil {
		fmt.Printf("index / write response error: %+v", err)
	}

}

func handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	imageVersion := help.GetEnv("IMAGE_VERSION", "0.0.0")
	currentTime := time.Now().UTC()
	status := map[string]string{
		"status":  "OK",
		"app":     "featureform",
		"version": imageVersion,
		"time":    currentTime.Format("1/2/06, 3:04:05 PM MST"),
	}

	jsonResponse, err := json.Marshal(status)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(jsonResponse)
}

func StartHttpServer(logger logging.Logger, port string) error {
	mux := &http.ServeMux{}

	mux.HandleFunc("/status", handleStatus)
	// handles possible trailing slash
	mux.HandleFunc("/status/", handleStatus)
	// Health check endpoint will handle all /_ah/* requests
	// e.g. /_ah/live, /_ah/ready and /_ah/lb
	// Create separate routes for specific health requests as needed.
	mux.HandleFunc("/_ah/", handleHealthCheck)
	mux.HandleFunc("/", handleIndex)
	// Add more routes as needed.

	addr := fmt.Sprintf(":%s", port)

	// Set timeouts so that a slow or malicious client doesn't hold resources forever.
	httpsSrv := &http.Server{
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		IdleTimeout:  60 * time.Second,
		Handler:      mux,
		Addr:         addr,
	}

	logger.Infof("starting health check HTTP server on port %s", port)

	go func() {
		err := httpsSrv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Errorw("error with health check HTTP server", "error", err)
		}
	}()

	return nil
}
