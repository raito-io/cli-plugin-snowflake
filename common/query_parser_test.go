package common

import (
	"fmt"
	"testing"

	ap "github.com/raito-io/cli/base/access_provider"
	ds "github.com/raito-io/cli/base/data_source"
	"github.com/stretchr/testify/assert"
)

type QueryTestData struct {
	query                       string
	databaseName                string
	schemaName                  string
	expectedAccessedDataObjects []ap.Access
}

func TestQueryParser(t *testing.T) {

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

	logger.Info(fmt.Sprintf("%d queries to parse for test", len(testQueries)))

	for _, testData := range testQueries {
		actualADO := ExtractInfoFromQuery(testData.query, testData.databaseName, testData.schemaName)
		if len(actualADO) == 0 {
			fmt.Println("Error")
		}
		logger.Info(fmt.Sprintf("Testing: %s, %d data objects retrieved", testData.query, len(actualADO)))

		fmt.Println(testData.query)

		assert.NotNil(t, actualADO)

		assert.Equal(t, testData.expectedAccessedDataObjects, actualADO)
	}

}
