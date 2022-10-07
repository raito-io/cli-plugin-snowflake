package snowflake

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/blockloop/scan"

	"github.com/raito-io/cli-plugin-snowflake/common"
)

// Implementation of Scanner interface for NullString
type NullString sql.NullString

func (nullString *NullString) Scan(value interface{}) error {
	var ns sql.NullString
	if err := ns.Scan(value); err != nil {
		return err
	}
	// if nil the make Valid false
	if reflect.TypeOf(value) == nil {
		*nullString = NullString{ns.String, false}
	} else {
		*nullString = NullString{ns.String, true}
	}

	return nil
}

type SnowflakeRepository struct {
	conn      *sql.DB
	queryTime time.Duration
}

func NewSnowflakeRepository(params map[string]interface{}, role string) (*SnowflakeRepository, error) {
	conn, err := ConnectToSnowflake(params, role)
	if err != nil {
		return nil, err
	}

	return &SnowflakeRepository{
		conn: conn,
	}, nil
}

func (repo *SnowflakeRepository) Close() error {
	return repo.conn.Close()
}

func (repo *SnowflakeRepository) TotalQueryTime() time.Duration {
	return repo.queryTime
}

func (repo *SnowflakeRepository) BatchingInformation(startDate *time.Time, historyTable string) (*string, *string, int, error) {
	filterClause := fmt.Sprintf("WHERE start_time > '%s'", startDate.Format(time.RFC3339))
	fetchBatchingInfoQuery := fmt.Sprintf("SELECT min(START_TIME) as minTime, max(START_TIME) as maxTime, COUNT(START_TIME) as numRows FROM %s %s", historyTable, filterClause)

	batchingInfoResult, _, err := repo.query(fetchBatchingInfoQuery)
	if err != nil {
		return nil, nil, 0, err
	}

	var minTime *string
	var maxTime *string
	numRows := 0

	for batchingInfoResult.Next() {
		err := batchingInfoResult.Scan(&minTime, &maxTime, &numRows)
		if err != nil {
			return nil, nil, 0, err
		}

		if numRows == 0 || minTime == nil || maxTime == nil {
			errorMessage := fmt.Sprintf("no usage information available with query: %s => result: numRows: %d, minTime: %v, maxtime: %v",
				fetchBatchingInfoQuery, numRows, minTime, maxTime)
			return nil, nil, 0, fmt.Errorf("%s", errorMessage)
		}
	}

	return minTime, maxTime, numRows, nil
}

func (repo *SnowflakeRepository) DataUsage(columns []string, limit int, offset int, historyTable string, minTime, maxTime *string, accessHistoryAvailable bool) ([]QueryDbEntities, error) {
	filterClause := fmt.Sprintf("WHERE START_TIME >= '%s' and START_TIME <= '%s'", *minTime, *maxTime)
	paginationClause := fmt.Sprintf("LIMIT %d OFFSET %d", limit, offset)

	var query string

	if accessHistoryAvailable {
		logger.Info("Using access history table in combination with history table")
		query = fmt.Sprintf(`SELECT %s, QID, DIRECT_OBJECTS_ACCESSED, BASE_OBJECTS_ACCESSED, OBJECTS_MODIFIED FROM (SELECT %s FROM %s %s) as QUERIES LEFT JOIN (SELECT QUERY_ID as QID, DIRECT_OBJECTS_ACCESSED, BASE_OBJECTS_ACCESSED, OBJECTS_MODIFIED FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY) as ACCESS on QUERIES.QUERY_ID = ACCESS.QID ORDER BY START_TIME, QUERIES.QUERY_ID DESC %s`,
			strings.Join(columns, ", "), strings.Join(columns, ", "), historyTable, filterClause, paginationClause)
	} else {
		query = fmt.Sprintf("SELECT %s FROM %s %s ORDER BY START_TIME, QUERY_ID DESC %s", strings.Join(columns, ", "), historyTable, filterClause, paginationClause)
	}

	logger.Debug(fmt.Sprintf("Retrieving paginated query log from Snowflake with query: %s", query))
	rows, sec, err := repo.query(query)

	if err != nil {
		return nil, err
	}

	var returnedRows []QueryDbEntities
	err = scan.Rows(&returnedRows, rows)

	if err != nil {
		return nil, err
	}

	logger.Info(fmt.Sprintf("Fetched %d rows from Snowflake in %s", len(returnedRows), sec))

	return returnedRows, nil
}

func (repo *SnowflakeRepository) CheckAccessHistoryAvailability(historyTable string) (bool, error) {
	checkAccessHistoryAvailabilityQuery := fmt.Sprintf("SELECT QUERY_ID, DIRECT_OBJECTS_ACCESSED, BASE_OBJECTS_ACCESSED, OBJECTS_MODIFIED FROM %s LIMIT 10", historyTable)

	result, _, err := repo.query(checkAccessHistoryAvailabilityQuery)
	if err != nil {
		return false, err
	}

	numRows := 0
	for result.Next() {
		numRows++
	}

	if numRows > 0 {
		logger.Debug(fmt.Sprintf("Access history query returned %d rows", numRows))
		return true, nil
	}

	return false, nil
}

func (repo *SnowflakeRepository) GetUsers() ([]userEntity, error) {
	q := "SHOW USERS"

	rows, _, err := repo.query(q)
	if err != nil {
		return nil, err
	}

	var userRows []userEntity

	err = scan.Rows(&userRows, rows)
	if err != nil {
		return nil, err
	}

	if err = CheckSFLimitExceeded(q, len(userRows)); err != nil {
		return nil, fmt.Errorf("error while fetching users: %s", err.Error())
	}

	return userRows, nil
}

func (repo *SnowflakeRepository) GetSnowFlakeAccountName() (string, error) {
	rows, _, err := repo.query("select current_account()")
	if err != nil {
		return "", err
	}

	var r []string
	err = scan.Rows(&r, rows)

	if err != nil {
		return "", fmt.Errorf("error while querying Snowflake: %s", err.Error())
	}

	if len(r) != 1 {
		return "", fmt.Errorf("error retrieving account information from snowflake")
	}

	return r[0], nil
}

func (repo *SnowflakeRepository) GetWarehouses() ([]dbEntity, error) {
	q := "SHOW WAREHOUSES"
	return repo.getDbEntities(q)
}

func (repo *SnowflakeRepository) GetShares() ([]dbEntity, error) {
	q := "SHOW SHARES"
	_, err := repo.getDbEntities(q)

	if err != nil {
		return nil, err
	}

	q = "select \"database_name\" as \"name\" from table(result_scan(LAST_QUERY_ID())) WHERE \"kind\" = 'INBOUND'"

	return repo.getDbEntities(q)
}

func (repo *SnowflakeRepository) GetDataBases() ([]dbEntity, error) {
	q := "SHOW DATABASES IN ACCOUNT"
	return repo.getDbEntities(q)
}

func (repo *SnowflakeRepository) GetSchemaInDatabase(databaseName string) ([]dbEntity, error) {
	q := getSchemasInDatabaseQuery(databaseName)
	return repo.getDbEntities(q)
}

func (repo *SnowflakeRepository) GetTablesInSchema(sfObject *common.SnowflakeObject) ([]dbEntity, error) {
	q := getTablesInSchemaQuery(sfObject, "TABLES")
	return repo.getDbEntities(q)
}

func (repo *SnowflakeRepository) GetViewsInSchema(sfObject *common.SnowflakeObject) ([]dbEntity, error) {
	q := getTablesInSchemaQuery(sfObject, "VIEWS")
	return repo.getDbEntities(q)
}

func (repo *SnowflakeRepository) GetColumnsInTable(sfObject *common.SnowflakeObject) ([]dbEntity, error) {
	q := getColumnsInTableQuery(sfObject)
	_, err := repo.getDbEntities(q)

	if err != nil {
		return nil, err
	}

	q = "select \"column_name\" as \"name\" from table(result_scan(LAST_QUERY_ID()))"

	return repo.getDbEntities(q)
}

func (repo *SnowflakeRepository) getDbEntities(query string) ([]dbEntity, error) {
	rows, _, err := repo.query(query)
	if err != nil {
		return nil, err
	}

	var dbs []dbEntity
	err = scan.Rows(&dbs, rows)

	if err != nil {
		return nil, err
	}

	err = CheckSFLimitExceeded(query, len(dbs))
	if err != nil {
		return nil, fmt.Errorf("error while fetching db entitities: %s", err.Error())
	}

	return dbs, nil
}

func (repo *SnowflakeRepository) query(query string) (*sql.Rows, time.Duration, error) {
	logger.Debug(fmt.Sprintf("Sending query: %s", query))
	startQuery := time.Now()
	result, err := QuerySnowflake(repo.conn, query)
	sec := time.Since(startQuery).Round(time.Millisecond)
	repo.queryTime += sec

	return result, sec, err
}

func getSchemasInDatabaseQuery(dbName string) string {
	//nolint // %q does not yield expected results
	return fmt.Sprintf(`SHOW SCHEMAS IN DATABASE "%s"`, dbName)
}

func getTablesInSchemaQuery(sfObject *common.SnowflakeObject, tableLevelObject string) string {
	return fmt.Sprintf(`SHOW %s IN SCHEMA %s`, tableLevelObject, sfObject.GetFullName(true))
}

func getColumnsInTableQuery(sfObject *common.SnowflakeObject) string {
	return fmt.Sprintf(`SHOW COLUMNS IN TABLE %s`, sfObject.GetFullName(true))
}
