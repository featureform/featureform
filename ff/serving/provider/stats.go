package provider

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"log"
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

type StatSchema struct {
	Metadata      Metadata `json:"metadata"`
	Median        float32  `json:"median"`
	LowerQuartile float32  `json:"lowerquartile"`
	UpperQuartile float32  `json:"upperquartile"`
}

func (store *postgresOfflineStore) chooseMetric(dataTableName string) {
	query := createMetric(dataTableName, UniqueCategoriesList)
	if _, err := store.conn.Exec(context.Background(), query); err != nil {
		fmt.Println("IF", err)
		//return nil, err
	}
	//
	//return &postgresOfflineTable{
	//	conn: store.conn,
	//	name: "FigureOutCustomizedName",
	//}, nil
}

// Method with switch statements, parses that data in the correct format based on METRICTYPE
func createMetric(tableName string, metricType DataCategories) string {
	switch metricType {

	case NumUniqueCategories:
		//returnData := findNumUniqueCategories(tableName)
		//w.Write(returnData)
		break

	case UniqueCategoriesList:
		query := findUniqueCategoriesList(tableName)
		return query

	case CategoryPercent:
		//returnData := findPercentageOfEachCategory(tableName)
		//w.Write(returnData)
		break

	case QuantitativePercentile:
		//returnData := findPercentileForEachCategory(tableName)
		//w.Write(returnData)
		break

	case QuantitativeGraph:
		//returnData := findGraphData(tableName)
		//w.Write(returnData)
		break

		//case Timestamp:
		//	break
	}
	return ""
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
	fmt.Println("bar")
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
	fmt.Println("count")
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
func findUniqueCategoriesList(tableName string) string {
	query := fmt.Sprintf("CREATE TABLE %s AS SELECT city FROM %s GROUP BY Value", "Featureform_stats_"+tableName+"_unique_category_list", tableName)
	return query
	//rows, err := db.Query("SELECT city FROM " + tableName + " GROUP BY city")
	//if err != nil {
	//	fmt.Println("Database querying error")
	//	panic("Database querying error")
	//}
	//categoryList := make([]Categoryrow, 0)
	//for rows.Next() {
	//	categoryRow := Categoryrow{}
	//	err := rows.Scan(&categoryRow.Category)
	//	if err != nil {
	//		log.Println(err)
	//		panic(err)
	//	}
	//	categoryList = append(categoryList, categoryRow)
	//}
	//fmt.Println("categoryList")
	//meta := Metadata{Metric: "categoryList"}
	//categoryListStruct := CategoryListStruct{Metadata: meta, Categories: categoryList}
	//table_json, _ := json.Marshal(categoryListStruct)
	//return table_json
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
	fmt.Println("percent")
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
	fmt.Println("percentile")
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
