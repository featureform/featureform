package main

import (
	"bufio"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"net/http"
	"os"
	"strings"
)

func main() {
	host := os.Getenv("HOST")
	port := os.Getenv("PORT")
	size := os.Getenv("TEST_SIZE")
	max_record := 10000
	if size == "long" {
		max_record = 10000000
	}
	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, "postgres", "password", "postgres")
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	fmt.Println("Connected to postgres")

	response, err := http.Get("https://featureform-demo-files.s3.amazonaws.com/transactions.csv") //use package "net/http"
	fmt.Println("Got Demo File")
	if err != nil {
		fmt.Println(err)
		return
	}

	defer response.Body.Close()

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
		panic(err)
	}
	fmt.Println("Created Table")

	var lines []string
	scanner := bufio.NewScanner(response.Body)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}
	fmt.Println("Starting Load")
	for i, line := range lines {
		if i == 0 {
			continue
		}
		if i > max_record {
			break
		}
		values := strings.Split(line, ",")
		_, err = db.Exec("INSERT INTO TRANSACTIONS VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
			values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7])
		if err != nil {
			fmt.Println(err)
			continue
		}

	}
	fmt.Println("Load Complete")

}
