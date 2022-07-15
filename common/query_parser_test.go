package common

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	ap "github.com/raito-io/cli/base/access_provider"
	logger "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

type QueryTestData struct {
	Query                       string      `json:"query"`
	DatabaseName                string      `json:"databaseName"`
	SchemaName                  string      `json:"schemaName"`
	ExpectedAccessedDataObjects []ap.Access `json:"expectedOutput"`
}

func testListOfQueries(t *testing.T, testQueries []QueryTestData) {
	logger.Info(fmt.Sprintf("%d queries to parse for test", len(testQueries)))
	for _, testData := range testQueries {
		actualADO, err := ExtractInfoFromQuery(testData.Query, testData.DatabaseName, testData.SchemaName)
		assert.Nil(t, err)
		if len(actualADO) == 0 {
			fmt.Println("Error")
		}
		logger.Info(fmt.Sprintf("Testing: %s, %d data objects retrieved", testData.Query, len(actualADO)))

		fmt.Println(testData.Query)

		assert.NotNil(t, actualADO)

		assert.ElementsMatch(t, testData.ExpectedAccessedDataObjects, actualADO)
	}
}

func getTestDataFromFile(t *testing.T, filename string) []QueryTestData {
	var file io.ReadCloser
	file, err := os.Open(filename)
	assert.Nil(t, err)
	if err != nil {
		logger.Error(fmt.Sprintf("error opening file %s: %s", filename, err.Error()))
	}
	defer file.Close()

	testData := []QueryTestData{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var item QueryTestData

		if strings.HasPrefix(scanner.Text(), "#") || len(scanner.Text()) <= 2 {
			continue
		}

		if err := json.Unmarshal(scanner.Bytes(), &item); err != nil {
			logger.Error(fmt.Sprintf("error unmarshalling json in %s, %v", filename, err))
			return nil
		}
		testData = append(testData, item)
	}

	assert.NotEmpty(t, testData)

	return testData
}

func printTestData(testData []QueryTestData) {
	for _, obj := range testData {
		byteData, _ := json.Marshal(obj)
		fmt.Println(string(byteData))
	}

}

func runTestFromFile(t *testing.T, filename string) {
	testData := getTestDataFromFile(t, filename)
	testListOfQueries(t, testData)

}

func TestQueryParserBasic(t *testing.T) {
	runTestFromFile(t, "./testdata/select_queries.ndjson")
}

func TestQueryParserCreate(t *testing.T) {
	runTestFromFile(t, "./testdata/create_queries.ndjson")
}

func TestQueryParserInsert(t *testing.T) {
	runTestFromFile(t, "./testdata/insert_queries.ndjson")
}

func TestQueryParserUpdate(t *testing.T) {
	runTestFromFile(t, "./testdata/update_queries.ndjson")
}

func TestQueryParserDelete(t *testing.T) {
	runTestFromFile(t, "./testdata/delete_queries.ndjson")
}
