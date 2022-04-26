package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	_ "github.com/lib/pq"
)

var db *sql.DB
var err error

// Different data metrics that can be dispalyed for feature data
type DataCategories int

const (
	NumUniqueCategories DataCategories = iota
	UniqueCategoriesList
	CategoryPercent
	QuantitativePercentile
	QuantitativeGraph
	Timestamp
)

// INCLUDED IN EVERY STRUCT
type Metadata struct {
	Metric string `json:"metric"`
}

// Assumption being made is that each table will consist of data values of 1 feature
// Assumption 2: The column names in every table will be the same

// For each row of table, meant for Bar Chart and Pie Chart
type Featurerow struct {
	Variable string  `json:"variable"`
	Value    float32 `json:"value"`
}

// Struct that holds all of the Bar Chart and Pie Chart data
type FeatureStruct struct {
	Metadata  Metadata     `json:"metadata"`
	ChartData []Featurerow `json:"chartdata"`
}

// For each row of table, meant for list of unique categories
type Categoryrow struct {
	Category string `json:"category"`
}

// Struct that holds the entire list of unique category names
type CategoryListStruct struct {
	Metadata   Metadata      `json:"metadata"`
	Categories []Categoryrow `json:"categories"`
}

// Struct that holds the number of unique categories
type CountStruct struct {
	Metadata Metadata `json:"metadata"`
	Count    int      `json:"categorycount"`
}

// Struct that holds percentile data
type PercentileStruct struct {
	Metadata      Metadata `json:"metadata"`
	Median        float32  `json:"median"`
	LowerQuartile float32  `json:"lowerquartile"`
	UpperQuartile float32  `json:"upperquartile"`
}

func init() {
	// Setup database
	psqlInfo := fmt.Sprintf("host=localhost port=5432 user=postgres " +
		"password=postgres dbname=postgres sslmode=disable")
	db, err = sql.Open("postgres", psqlInfo)

	if err != nil {
		panic(err)
	}

	if err = db.Ping(); err != nil {
		panic(err)
	}
	// this will be printed in the terminal, notifying that we successfully connected to our database
	fmt.Println("You are now connected to the database.")
}

func main() {
	http.HandleFunc("/search&retrievecolumn", searchRecord) // (2)
	http.ListenAndServe(":8080", nil)                       // (3)
}

// Gets the name of the table from the frontend, finds the table in the
// database and calls createMetric() which manages the data and sends it to the backend
// as a JSON
func searchRecord(w http.ResponseWriter, r *http.Request) {
	byts, err := io.ReadAll(r.Body)
	if err != nil {
		panic(err)
	}
	r.Body.Close()
	tableName := string(byts)

	// The second argument that goes into createMetric decides what kind of data is being sent
	createMetric(tableName, QuantitativeGraph, w)
}

// Method with switch statements, parses that data in the correct format based on METRICTYPE
func createMetric(tableName string, metricType DataCategories, w http.ResponseWriter) {
	switch metricType {

	case NumUniqueCategories:
		returnData := findNumUniqueCategories(tableName)
		w.Write(returnData)
		break

	case UniqueCategoriesList:
		returnData := findUniqueCategoriesList(tableName)
		w.Write(returnData)
		break

	case CategoryPercent:
		returnData := findPercentageOfEachCategory(tableName)
		w.Write(returnData)
		break

	case QuantitativePercentile:
		returnData := findPercentileForEachCategory(tableName)
		w.Write(returnData)
		break

	case QuantitativeGraph:
		returnData := findGraphData(tableName)
		w.Write(returnData)
		break

	case Timestamp:
		break
	}
}

// Takes in a table name containing 2 columns. It returns a JSON encapsulating data for the x and y axis of the desired graph
func findGraphData(tableName string) []byte {
	rows, err := db.Query("SELECT city, temp_lo FROM " + tableName)
	if err != nil {
		fmt.Println("Database querying error")
		panic("Database querying error")
	}
	defer rows.Close()

	featureTable := make([]Featurerow, 0)
	for rows.Next() {
		featurerow := Featurerow{}
		err := rows.Scan(&featurerow.Variable, &featurerow.Value)
		if err != nil {
			fmt.Println("Database querying error")
			panic("Database querying error")
		}
		featureTable = append(featureTable, featurerow)
	}

	meta := Metadata{Metric: "bar"}
	graphStruct := FeatureStruct{Metadata: meta, ChartData: featureTable}
	table_json, _ := json.Marshal(graphStruct)

	return table_json
}

// Takes a table name and returns a JSON encapsulating the number of unique categories in the table
func findNumUniqueCategories(tableName string) []byte {
	rows, err := db.Query("SELECT COUNT(DISTINCT city) FROM " + tableName)
	if err != nil {
		fmt.Println("Database querying error")
		panic("Database querying error")
	}
	meta := Metadata{Metric: "count"}
	for rows.Next() {
		countrow := CountStruct{}
		err := rows.Scan(&countrow.Count)
		if err != nil {
			log.Println(err)
			panic(err)
		}
		countrow.Metadata = meta
		countrowJson, _ := json.Marshal(countrow)
		return countrowJson
	}
	return nil
}

// Takes a table name and returns a JSON encapsulating a list of unique categories in the table
func findUniqueCategoriesList(tableName string) []byte {
	rows, err := db.Query("SELECT city FROM " + tableName + " GROUP BY city")
	if err != nil {
		fmt.Println("Database querying error")
		panic("Database querying error")
	}
	categoryList := make([]Categoryrow, 0)
	for rows.Next() {
		categoryRow := Categoryrow{}
		err := rows.Scan(&categoryRow.Category)
		if err != nil {
			log.Println(err)
			panic(err)
		}
		categoryList = append(categoryList, categoryRow)
	}
	meta := Metadata{Metric: "categoryList"}
	categoryListStruct := CategoryListStruct{Metadata: meta, Categories: categoryList}
	table_json, _ := json.Marshal(categoryListStruct)
	return table_json
}

// Takes a table name and returns a JSON encapsulating the percentage distribution of unique categories in the table
func findPercentageOfEachCategory(tableName string) []byte {
	rows, err := db.Query("SELECT city,(COUNT(city) * 100)/SUM(COUNT(city)) over () as percentage FROM " + tableName + " GROUP BY city")
	if err != nil {
		fmt.Println("Database querying error")
		panic("Database querying error")
	}
	percentTable := make([]Featurerow, 0)
	for rows.Next() {
		percentrow := Featurerow{}
		err := rows.Scan(&percentrow.Variable, &percentrow.Value)
		if err != nil {
			log.Println(err)
			panic(err)
		}
		percentTable = append(percentTable, percentrow)
	}
	meta := Metadata{Metric: "percent"}
	percentStruct := FeatureStruct{Metadata: meta, ChartData: percentTable}
	table_json, _ := json.Marshal(percentStruct)
	return table_json
}

// Takes a table name and returns a JSON encapsulating the median, lower and upper quartile of the data
func findPercentileForEachCategory(tableName string) []byte {
	rows, err := db.Query("SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY temp_hi) as median, percentile_cont(0.25) WITHIN GROUP (ORDER BY temp_hi) as twentyfive_percentile,percentile_cont(0.75) WITHIN GROUP (ORDER BY temp_hi) as seventyfive_percentile FROM " + tableName)
	if err != nil {
		fmt.Println("Database querying error")
		panic("Database querying error")
	}
	meta := Metadata{Metric: "percentile"}
	for rows.Next() {
		percentiledata := PercentileStruct{}
		err := rows.Scan(&percentiledata.Median, &percentiledata.LowerQuartile, &percentiledata.UpperQuartile)
		if err != nil {
			log.Println(err)
			panic(err)
		}
		percentiledata.Metadata = meta
		percentiledataJson, _ := json.Marshal(percentiledata)
		return percentiledataJson
	}
	return nil
}
