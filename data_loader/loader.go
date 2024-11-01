// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"net/http"
	"os"

	_ "github.com/lib/pq"
)

func main() {
	host := os.Getenv("HOST")
	port := os.Getenv("PORT")
	size := os.Getenv("TEST_SIZE")
	maxRecords := 10_000
	columnCount := 8
	if size == "long" {
		maxRecords = 10_000_000
	}
	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, "postgres", "password", "postgres")
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		fmt.Println("Could not establish connection to postgres")
		panic(err)
	}
	defer db.Close()

	fmt.Println("Connected to postgres")

	loadErr := loadData(db, columnCount, maxRecords, http.Get)
	if loadErr != nil {
		fmt.Println("an error occurred while loading the data")
		panic(loadErr)
	}
}

func loadData(db *sql.DB, columnCount, maxRecords int, httpGet func(string) (*http.Response, error)) error {
	// pull the csv
	response, err := httpGet("https://featureform-demo-files.s3.amazonaws.com/transactions.csv")
	if err != nil {
		fmt.Println("could not fetch csv file")
		return err
	}
	defer response.Body.Close()

	if response.Header.Get("Content-Type") != "text/csv" {
		return fmt.Errorf("invalid content type: %s", response.Header.Get("Content-Type"))
	}

	fmt.Println("Got Demo File")

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS TRANSACTIONS  (" +
		"TRANSACTIONID VARCHAR, " +
		"CUSTOMERID VARCHAR," +
		"CUSTOMERDOB VARCHAR," +
		"CUSTLOCATION VARCHAR," +
		"CUSTACCOUNTBALANCE FLOAT," +
		"TRANSACTIONAMOUNT FLOAT," +
		"TIMESTAMP TIMESTAMPTZ," +
		"ISFRAUD BOOLEAN)")
	if err != nil {
		return err
	}
	fmt.Println("Created Table")

	csvReader := csv.NewReader(response.Body)
	csvReader.FieldsPerRecord = -1

	records, err := csvReader.ReadAll()
	if err != nil {
		return err
	}

	fmt.Println("Starting transactions.csv load...")
	for i, values := range records {
		if i == 0 {
			//skip the header line
			continue
		}
		if i > maxRecords {
			break
		}

		if len(values) > 1 {
			values = values[1:] //remove the initial increment value column
		}

		if len(values) < columnCount {
			fmt.Println("skipping record due to insufficient column values:", values[0])
			continue
		}

		_, err = db.Exec("INSERT INTO TRANSACTIONS VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
			values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7])
		if err != nil {
			fmt.Println(err)
			fmt.Println("faulty record:", values[0])
			continue
		}

	}
	fmt.Println("Load Complete")
	return nil
}
