package main

import (
	"database/sql"
	"fmt"
	_ "github.com/snowflakedb/gosnowflake"
)

func main() {
	url := fmt.Sprintf("%s:%s@%s-%s/%s/PUBLIC", "sdreyer", "8HiB4dzznMJyL2d", "ZRJSMFL", "ED24939", "SANDBOX_SCRIBD")
	db, err := sql.Open("snowflake", url)
	if err != nil {
		panic(err)
	}
	_, err = db.Query("put file:///Users/sdreyer/Downloads/transactions.csv @my_stage; ")
	if err != nil {
		panic(err)
	}
	//file, err := os.Open("transactions.csv")
	//if err != nil {
	//	panic(err)
	//}
	//r := csv.NewReader(file)
	//i := 0
	//fmt.Println("Start")

	//for {
	//	record, err := r.Read()
	//	if i == 0 {
	//		i++
	//		continue
	//	}
	//	if err == io.EOF {
	//		break
	//	}
	//	if err != nil {
	//		fmt.Println(err)
	//	}
	//	_, err = db.Exec("INSERT INTO TRANSACTIONS VALUES (?, ?, ?, ?, ?, ?, ?, ?)", record[0], record[1], record[2], record[3], record[4], record[5], record[6], record[7])
	//	fmt.Printf("Records: %d/%d\r", i, 1048568)
	//	if err != nil {
	//		fmt.Println(err)
	//	}
	//	i++
	//}
}
