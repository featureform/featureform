// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func TestLoadData(t *testing.T) {
	os.Setenv("HOST", "localhost")
	os.Setenv("PORT", "5432")
	os.Setenv("TEST_SIZE", "short")
	maxRecord := 10
	columnCount := 8

	csvContent, err := os.ReadFile("./mock.csv")
	if err != nil {
		t.Fatalf("Could not read mock csv file: %v", err)
	}

	//mock handler function
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(csvContent))
	}))
	defer server.Close()

	// use the test server url
	mockGet := func(_ string) (resp *http.Response, err error) {
		return http.Get(server.URL)
	}

	mockDb, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock database: %s", err)
	}
	defer mockDb.Close()

	// expects
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS TRANSACTIONS").
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec("INSERT INTO TRANSACTIONS VALUES").
		WithArgs("T1", "C5841053", "10/1/94", "Panama City, Panama", "17819.05", "25.0", "2022-04-09 11:33:09", "False").
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec("INSERT INTO TRANSACTIONS VALUES").
		WithArgs("T2", "C2142763", "4/4/57", "Panama City, Florida", "2270.69", "27999.0", "2022-03-27 01:04:21", "True").
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec("INSERT INTO TRANSACTIONS VALUES").
		WithArgs("T3", "C5342380", "14/9/73", "Houston", "866503.21", "2060.0", "2022-04-14 07:56:59", "True").
		WillReturnResult(sqlmock.NewResult(1, 1))

	// start the data load
	err = loadData(mockDb, columnCount, maxRecord, mockGet)
	if err != nil {
		t.Fatalf("loadData returned an error: %v", err)
	}

	// assert expectations
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations were not met: %s", err)
	}
}

func TestLoadDataInvalidFileType(t *testing.T) {
	os.Setenv("HOST", "localhost")
	os.Setenv("PORT", "5432")
	os.Setenv("TEST_SIZE", "short")
	maxRecord := 10
	columnCount := 8

	//response returns an invalid content type
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`dummy content`))
	}))
	defer server.Close()

	// use the test server url
	mockGet := func(_ string) (resp *http.Response, err error) {
		return http.Get(server.URL)
	}

	mockDb, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock database: %s", err)
	}
	defer mockDb.Close()

	// start the data load with a faulty file type, should error
	loadErr := loadData(mockDb, columnCount, maxRecord, mockGet)
	assert.NotNil(t, loadErr)
	assert.Equal(t, "invalid content type: text/plain", loadErr.Error())
}
