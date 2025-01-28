package main

import (
	"database/sql"
	"fmt"
	"os"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"  // driver "pgx"
	_ "github.com/microsoft/go-mssqldb" // driver "mssql"

	"github.com/joho/godotenv"
)

var db_conn string
var db_driver = ""
var table = ""
var format = "json"
var verbose = false

func main() {

	env_err := godotenv.Load()
	if env_err != nil {
		if verbose {
			fmt.Println("No .env file found.")
		}
	}
	db_conn = os.Getenv("DB_CONN")
	db_driver = os.Getenv("DB_DRIVER")

	// var dbTypeToNullType = map[string]string{
	// 	"VARCHAR":  "null.String",
	// 	"INT":      "null.Int",
	// 	"DECIMAL":  "null.Float",
	// 	"DATETIME": "null.Time",
	// 	"MONEY":    "null.Float",
	// 	"BIT":      "null.Bool",
	// 	"TEXT":     "null.String",
	// 	"CHAR":     "null.String",
	// }

	table := os.Args[1]

	for i, v := range os.Args[2:] {

		fmt.Println(v)
		switch v {
		// Format
		case "-v":
			// Verbose
			verbose = true
			continue
		case "-f":
			f := string(os.Args[i+3])
			f = strings.ToLower(f)
			f = strings.Trim(f, " \r\n")

			if f == "json" {
				format = "json"
			}
			if f == "yaml" {
				format = "yaml"
			}
			continue
		// DB Connection
		case "-c":
			db_conn = string(os.Args[i+3])
			if (len(db_conn) < 2) && (env_err != nil) {
				fmt.Println("No env or cli arg")
				os.Exit(1)
			}
			continue
		// DB Driver
		case "-d":
			db_driver = string(os.Args[i+3])
			continue
		}
	}

	if verbose {
		fmt.Println("Driver: " + db_driver)
		fmt.Println("Table: " + table)
		fmt.Println("Format: " + format)
	}

	if format == "json" {
		writeToJSON(table)
	}

	if format == "yaml" {
		writeToYAML(table)
	}
}

func writeToJSON(table string) {

	query_string := fmt.Sprintf("SELECT TOP 1 * FROM %s", table)
	rows, err := query(db_driver, db_conn, query_string)
	if err != nil {
		fmt.Println(err)
		return
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		fmt.Println(err)
		return
	}

	fields := []string{}
	for i, t := range columnTypes {
		// fmt.Printf("%s:  %s -> %s \n", t.Name(), t.DatabaseTypeName(), t.ScanType().String())
		// f := fmt.Sprintf(`{"name": "%s", "type": "%s"}`, t.Name(), dbTypeToNullType[t.DatabaseTypeName()])
		f := ""
		if i == 0 {
			f = fmt.Sprintf(`"%s"`, t.Name())
		} else {
			f = fmt.Sprintf(`%s  "%s"`, "\t\t", t.Name())
		}
		fields = append(fields, f)
	}

	new_request := `
  {	
		"name" : "%s", 
		"tableName":"%s",
		"fields": [
			%s	
		]
	}`

	filled_request := fmt.Sprintf(new_request, table, table, strings.Join(fields, ",\n"))

	byteData := []byte(filled_request)
	filename := fmt.Sprintf("./%s.json", table)
	writeToFile(filename, byteData)
}

func writeToYAML(table string) {
	query_string := fmt.Sprintf("SELECT TOP 1 * FROM %s", table)
	rows, err := query(db_driver, db_conn, query_string)
	if err != nil {
		fmt.Println(err)
		return
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		fmt.Println(err)
		return
	}

	fields := []string{}
	for i, t := range columnTypes {
		// fmt.Printf("%s:  %s -> %s \n", t.Name(), t.DatabaseTypeName(), t.ScanType().String())
		// f := fmt.Sprintf(`{"name": "%s", "type": "%s"}`, t.Name(), dbTypeToNullType[t.DatabaseTypeName()])
		f := ""
		if i == 0 {
			f = fmt.Sprintf(`- %s`, t.Name())
		} else {
			f = fmt.Sprintf(`%s - %s`, "\t\t", t.Name())
		}
		fields = append(fields, f)
	}

	new_request := `
%s:
	tableName:%s
	fields:
			%s	
	groups:
	`
	filled_request := fmt.Sprintf(new_request, table, table, strings.Join(fields, "\n"))

	byteData := []byte(filled_request)
	filename := fmt.Sprintf("./%s.yaml", table)
	writeToFile(filename, byteData)
}

func query(driver string, conn string, query string) (*sql.Rows, error) {
	db, err := sql.Open(driver, conn)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func writeToFile(filename string, data []byte) {
	err := os.WriteFile(filename, data, 0644)
	if err != nil {
		fmt.Println(err)
		return
	}
}
