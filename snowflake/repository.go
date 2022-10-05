package snowflake

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/blockloop/scan"
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

func (repo *SnowflakeRepository) checkAccessHistoryAvailability(historyTable string) (bool, error) {
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

func (repo *SnowflakeRepository) query(query string) (*sql.Rows, time.Duration, error) {
	logger.Debug(fmt.Sprintf("Sending query: %s", query))
	startQuery := time.Now()
	result, err := QuerySnowflake(repo.conn, query)
	sec := time.Since(startQuery).Round(time.Millisecond)
	repo.queryTime += sec

	return result, sec, err
}