package common

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"

	ap "github.com/raito-io/cli/base/access_provider"
	ds "github.com/raito-io/cli/base/data_source"
	logger "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

type QueryTestData struct {
	query                       string      `json:"query"`
	databaseName                string      `json:"databaseName"`
	schemaName                  string      `json:"schemaName"`
	expectedAccessedDataObjects []ap.Access `json:"expectedOutput"`
}

func testListOfQueries(t *testing.T, testQueries []QueryTestData) {
	logger.Info(fmt.Sprintf("%d queries to parse for test", len(testQueries)))
	for _, testData := range testQueries {
		actualADO, err := ExtractInfoFromQuery(testData.query, testData.databaseName, testData.schemaName)
		assert.Nil(t, err)
		if len(actualADO) == 0 {
			fmt.Println("Error")
		}
		logger.Info(fmt.Sprintf("Testing: %s, %d data objects retrieved", testData.query, len(actualADO)))

		fmt.Println(testData.query)

		assert.NotNil(t, actualADO)

		assert.ElementsMatch(t, testData.expectedAccessedDataObjects, actualADO)
	}
}

func TestQueryFromFile(t *testing.T) {

	var file io.ReadCloser
	file, err := os.Open("testdata/select_queries.ndjson")
	assert.Nil(t, err)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		item := QueryTestData{query: "<empty>", databaseName: "<empty>", schemaName: "<empty>"}
		// fmt.Println(string(scanner.Bytes()))
		if err := json.Unmarshal(scanner.Bytes(), &item); err != nil {
			logger.Error(fmt.Sprintf("%v", err))
			break
		}
		fmt.Printf("%v\n", item)
	}

}

func TestQueryParserBasic(t *testing.T) {

	// TODO: create and read test data from yaml file
	var testQueries []QueryTestData
	query := "SELECT user_id as userId, user_name, address FROM demo"
	expectedADO := []ap.Access{
		{DataObjectReference: &ds.DataObjectReference{FullName: "..DEMO.USER_ID", Type: "column"},
			Permissions: []string{"SELECT"}},
		{DataObjectReference: &ds.DataObjectReference{FullName: "..DEMO.USER_NAME", Type: "column"},
			Permissions: []string{"SELECT"}},
		{DataObjectReference: &ds.DataObjectReference{FullName: "..DEMO.ADDRESS", Type: "column"},
			Permissions: []string{"SELECT"}},
	}
	testQueries = append(testQueries, QueryTestData{query: query, databaseName: "", schemaName: "", expectedAccessedDataObjects: expectedADO})

	query = "SELECT user_id as userId, user_name, address FROM demo"
	expectedADO = []ap.Access{
		{DataObjectReference: &ds.DataObjectReference{FullName: ".SCHEMA1.DEMO.USER_ID", Type: "column"},
			Permissions: []string{"SELECT"}},
		{DataObjectReference: &ds.DataObjectReference{FullName: ".SCHEMA1.DEMO.USER_NAME", Type: "column"},
			Permissions: []string{"SELECT"}},
		{DataObjectReference: &ds.DataObjectReference{FullName: ".SCHEMA1.DEMO.ADDRESS", Type: "column"},
			Permissions: []string{"SELECT"}},
	}
	testQueries = append(testQueries, QueryTestData{query: query, databaseName: "", schemaName: "SCHEMA1", expectedAccessedDataObjects: expectedADO})

	query = "SELECT user_id as userId, user_name, address FROM demo"
	expectedADO = []ap.Access{
		{DataObjectReference: &ds.DataObjectReference{FullName: "DATABASE1.SCHEMA1.DEMO.USER_ID", Type: "column"},
			Permissions: []string{"SELECT"}},
		{DataObjectReference: &ds.DataObjectReference{FullName: "DATABASE1.SCHEMA1.DEMO.USER_NAME", Type: "column"},
			Permissions: []string{"SELECT"}},
		{DataObjectReference: &ds.DataObjectReference{FullName: "DATABASE1.SCHEMA1.DEMO.ADDRESS", Type: "column"},
			Permissions: []string{"SELECT"}},
	}
	testQueries = append(testQueries, QueryTestData{query: query, databaseName: "DATABASE1", schemaName: "SCHEMA1", expectedAccessedDataObjects: expectedADO})

	query = "SELECT demo.user_id as UserId, demo.user_name, address FROM schema1.demo as test"
	expectedADO = []ap.Access{
		{DataObjectReference: &ds.DataObjectReference{FullName: ".SCHEMA1.DEMO.USER_ID", Type: "column"},
			Permissions: []string{"SELECT"}},
		{DataObjectReference: &ds.DataObjectReference{FullName: ".SCHEMA1.DEMO.USER_NAME", Type: "column"},
			Permissions: []string{"SELECT"}},
		{DataObjectReference: &ds.DataObjectReference{FullName: ".SCHEMA1.DEMO.ADDRESS", Type: "column"},
			Permissions: []string{"SELECT"}},
	}
	testQueries = append(testQueries, QueryTestData{query: query, databaseName: "", schemaName: "", expectedAccessedDataObjects: expectedADO})

	query = "SELECT orders.product, SUM(orders.quantity) AS product_units, accounts.* " +
		"FROM orders LEFT JOIN accounts ON orders.account_id = accounts.id " +
		"WHERE orders.region IN (SELECT region FROM top_regions) " +
		"ORDER BY product_units LIMIT 100"
	expectedADO = []ap.Access{
		{DataObjectReference: &ds.DataObjectReference{FullName: "..ORDERS.PRODUCT", Type: "column"},
			Permissions: []string{"SELECT"}},
		{DataObjectReference: &ds.DataObjectReference{FullName: "..ORDERS.QUANTITY", Type: "column"},
			Permissions: []string{"SELECT"}},
		{DataObjectReference: &ds.DataObjectReference{FullName: "..ACCOUNTS", Type: "table"},
			Permissions: []string{"SELECT"}},
	}
	testQueries = append(testQueries, QueryTestData{query: query, databaseName: "", schemaName: "", expectedAccessedDataObjects: expectedADO})

	query = "select * from t GROUP BY ROLLUP(b,a)"
	expectedADO = []ap.Access{
		{DataObjectReference: &ds.DataObjectReference{FullName: "..T", Type: "table"},
			Permissions: []string{"SELECT"}},
	}
	testQueries = append(testQueries, QueryTestData{query: query, databaseName: "", schemaName: "", expectedAccessedDataObjects: expectedADO})

	query = "GRANT SELECT ON  demo_table TO  raito;"
	expectedADO = []ap.Access{
		{
			DataObjectReference: nil,
			Permissions:         []string{"GRANT"},
		},
	}
	testQueries = append(testQueries, QueryTestData{query: query, databaseName: "", schemaName: "", expectedAccessedDataObjects: expectedADO})

	query = "SHOW GRANTS TO ROLE MASKING_ADMIN"
	expectedADO = []ap.Access{
		{
			DataObjectReference: nil,
			Permissions:         []string{"SHOW"},
		},
	}
	testQueries = append(testQueries, QueryTestData{query: query, databaseName: "", schemaName: "", expectedAccessedDataObjects: expectedADO})

	query = "SELECT name, (SELECT max(pop) FROM cities\n WHERE cities.state = states.name)\n    FROM states;"
	expectedADO = []ap.Access{
		{DataObjectReference: &ds.DataObjectReference{FullName: "..STATES.NAME", Type: "column"},
			Permissions: []string{"SELECT"}},
		{DataObjectReference: &ds.DataObjectReference{FullName: "..CITIES.POP", Type: "column"},
			Permissions: []string{"SELECT"}},
	}
	testQueries = append(testQueries, QueryTestData{query: query, databaseName: "", schemaName: "", expectedAccessedDataObjects: expectedADO})

	testListOfQueries(t, testQueries)

}

func TestQueryParserCreate(t *testing.T) {
	var testQueries []QueryTestData
	query := `create table users (
		id integer,
		name varchar (100),  
		preferences varchar (50),
		created_at timestamp
	  );`
	expectedADO := []ap.Access{
		{DataObjectReference: &ds.DataObjectReference{FullName: "..USERS", Type: "table"},
			Permissions: []string{"CREATE"}},
	}
	testQueries = append(testQueries, QueryTestData{query: query, databaseName: "", schemaName: "", expectedAccessedDataObjects: expectedADO})

	// see query in wish list
	testQueries = []QueryTestData{}
	query = `create table sessions_dm_2 (id, start_date, end_date, category) as
		select * from sessions where id > 5;`
	expectedADO = []ap.Access{
		{DataObjectReference: &ds.DataObjectReference{FullName: "..SESSIONS_DM_2", Type: "table"},
			Permissions: []string{"CREATE"}},
	}
	testQueries = append(testQueries, QueryTestData{query: query, databaseName: "", schemaName: "", expectedAccessedDataObjects: expectedADO})

	testListOfQueries(t, testQueries)

}

func TestQueryParserInsert(t *testing.T) {
	var testQueries []QueryTestData
	query := `insert into sessions values (1, '2020-04-02 14:05:15.400', '2020-04-03 14:25:15.400', 1);`
	expectedADO := []ap.Access{
		{DataObjectReference: &ds.DataObjectReference{FullName: "..SESSIONS", Type: "table"},
			Permissions: []string{"INSERT"}},
	}
	testQueries = append(testQueries, QueryTestData{query: query, databaseName: "", schemaName: "", expectedAccessedDataObjects: expectedADO})

	query = `insert into sessions (id, start_date, end_date, category) values (12, '2020-04-02 14:05:15.400', '2020-04-04 16:57:53.653', 2);`
	expectedADO = []ap.Access{
		{DataObjectReference: &ds.DataObjectReference{FullName: "..SESSIONS", Type: "table"},
			Permissions: []string{"INSERT"}},
	}
	testQueries = append(testQueries, QueryTestData{query: query, databaseName: "", schemaName: "", expectedAccessedDataObjects: expectedADO})

	query = `insert into sessions (id, category, start_date, end_date) values (3, 2, '2020-04-02 14:05:15.400', '2020-04-04 16:57:53.653');`
	expectedADO = []ap.Access{
		{DataObjectReference: &ds.DataObjectReference{FullName: "..SESSIONS", Type: "table"},
			Permissions: []string{"INSERT"}},
	}
	testQueries = append(testQueries, QueryTestData{query: query, databaseName: "", schemaName: "", expectedAccessedDataObjects: expectedADO})

	query = `	insert into sessions (id, start_date, end_date, category)
			values
	  (5, '2020-04-02 15:05:15.400','2020-04-03 15:14:30.400', 3),
	  (6, '2020-04-02 17:07:16.300','2020-04-02 19:10:15.400', 4),
	  (7, '2020-04-03 15:05:45.127','2020-04-04 18:05:15.400', 2);`
	expectedADO = []ap.Access{
		{DataObjectReference: &ds.DataObjectReference{FullName: "..SESSIONS", Type: "table"},
			Permissions: []string{"INSERT"}},
	}
	testQueries = append(testQueries, QueryTestData{query: query, databaseName: "", schemaName: "", expectedAccessedDataObjects: expectedADO})

	testListOfQueries(t, testQueries)

}

func TestQueryParserUpdate(t *testing.T) {
	var testQueries []QueryTestData
	query := ""
	expectedADO := []ap.Access{}

	query = `update sessions set start_date = '2020-04-20 10:12:15.653', end_date = '2020-04-22 15:40:30.123' where id = 1;`
	expectedADO = []ap.Access{
		{DataObjectReference: &ds.DataObjectReference{FullName: "..SESSIONS", Type: "table"},
			Permissions: []string{"UPDATE"}},
	}
	testQueries = append(testQueries, QueryTestData{query: query, databaseName: "", schemaName: "", expectedAccessedDataObjects: expectedADO})

	testListOfQueries(t, testQueries)
}

func TestQueryParserDelete(t *testing.T) {
	var testQueries []QueryTestData
	query := ""
	expectedADO := []ap.Access{}

	query = `delete from sessions where id = 7;`
	expectedADO = []ap.Access{
		{DataObjectReference: &ds.DataObjectReference{FullName: "..SESSIONS", Type: "table"},
			Permissions: []string{"DELETE"}},
	}
	testQueries = append(testQueries, QueryTestData{query: query, databaseName: "", schemaName: "", expectedAccessedDataObjects: expectedADO})

	testListOfQueries(t, testQueries)
}

func WishList() {
	var testQueries []QueryTestData
	query := ""
	expectedADO := []ap.Access{}

	query = `create table sessions_dm_2 (id, start_date, end_date, category) as
		select * from sessions where id > 5;`
	expectedADO = []ap.Access{
		{DataObjectReference: &ds.DataObjectReference{FullName: "..SESSIONS", Type: "table"},
			Permissions: []string{"SELECT"}},
		{DataObjectReference: &ds.DataObjectReference{FullName: "..SESSIONS_DM_2", Type: "table"},
			Permissions: []string{"CREATE"}},
	}
	testQueries = append(testQueries, QueryTestData{query: query, databaseName: "", schemaName: "", expectedAccessedDataObjects: expectedADO})

	query = `	create table users (
		id integer default id_seq.nextval, -- auto incrementing IDs
		name varchar (100),  -- variable string column
		preferences string, -- column used to store JSON type of data
		created_at timestamp
	  );`
	expectedADO = []ap.Access{
		{DataObjectReference: &ds.DataObjectReference{FullName: "..USERS", Type: "table"},
			Permissions: []string{"CREATE"}},
	}
	testQueries = append(testQueries, QueryTestData{query: query, databaseName: "", schemaName: "", expectedAccessedDataObjects: expectedADO})

}

// SELECT - extracts data from a database
// UPDATE - updates data in a database
// DELETE - deletes data from a database
// INSERT INTO - inserts new data into a database
// CREATE DATABASE - creates a new database
// ALTER DATABASE - modifies a database
// CREATE TABLE - creates a new table
// ALTER TABLE - modifies a table
// DROP TABLE - deletes a table
// CREATE INDEX - creates an index (search key)
// DROP INDEX - deletes an index
