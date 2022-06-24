package main

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/blockloop/scan"
	"github.com/raito-io/cli-plugin-snowflake/common"
	dub "github.com/raito-io/cli/base/data_usage"
	"github.com/raito-io/cli/common/api"
	"github.com/raito-io/cli/common/api/data_usage"
)

type DataUsageSyncer struct {
}

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

func (s *DataUsageSyncer) SyncDataUsage(config *data_usage.DataUsageSyncConfig) data_usage.DataUsageSyncResult {
	logger.Info("Creating file for storing data")
	fileCreator, err := dub.NewDataUsageFileCreator(config)

	if err != nil {
		return data_usage.DataUsageSyncResult{Error: api.ToErrorResult(err)}
	}
	defer fileCreator.Close()

	conn, err := ConnectToSnowflake(config.Parameters, "")
	if err != nil {
		return data_usage.DataUsageSyncResult{Error: api.ToErrorResult(err)}
	}
	defer conn.Close()

	const numRowsPerBatch = 10000
	historyTable := "SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY"

	// TODO: should be configurable
	numberOfDays := 7
	startDate := time.Now().AddDate(0, 0, -numberOfDays)
	dateFormat := "2006-01-02"
	filterClause := fmt.Sprintf("WHERE start_time >= '%s'", startDate.Format(dateFormat))

	fetchBatchingInfoQuery := fmt.Sprintf("SELECT min(START_TIME) as minTime, max(START_TIME) as maxTime, COUNT(START_TIME) as numRows FROM %s %s", historyTable, filterClause)
	start := time.Now()
	startQuery := time.Now()
	snowflakeQueryTime := time.Duration(0)
	batchingInfoResult, err := QuerySnowflake(conn, fetchBatchingInfoQuery)

	if err != nil {
		return data_usage.DataUsageSyncResult{Error: api.ToErrorResult(err)}
	}
	snowflakeQueryTime += time.Since(startQuery).Round(time.Millisecond)

	minTime := ""
	maxTime := ""
	numRows := 0

	for batchingInfoResult.Next() {
		err := batchingInfoResult.Scan(&minTime, &maxTime, &numRows)
		if err != nil {
			return data_usage.DataUsageSyncResult{Error: api.ToErrorResult(err)}
		}

		logger.Info(fmt.Sprintf("Batch information result; min time: %s, max time: %s, num rows: %d", minTime, maxTime, numRows))
	}

	columns := []string{
		"QUERY_ID", "EXECUTION_STATUS", "QUERY_TEXT", "DATABASE_NAME", "SCHEMA_NAME", "USER_NAME",
		"START_TIME", "END_TIME", "EXECUTION_TIME", "OUTBOUND_DATA_TRANSFER_BYTES", "EXTERNAL_FUNCTION_TOTAL_SENT_ROWS"}

	filterClause = fmt.Sprintf("WHERE START_TIME >= '%s' and START_TIME <= '%s'", minTime, maxTime)

	currentBatch := 0
	for currentBatch*numRowsPerBatch < numRows {
		paginationClause := fmt.Sprintf("LIMIT %d OFFSET %d", numRowsPerBatch, currentBatch*numRowsPerBatch)
		if numRows < numRowsPerBatch {
			paginationClause = ""
		}
		dataUsageQuery := fmt.Sprintf("SELECT %s FROM %s %s ORDER BY START_TIME, QUERY_ID DESC %s", strings.Join(columns, ", "), historyTable, filterClause, paginationClause)

		logger.Info(fmt.Sprintf("Retrieving paginated query log from Snowflake with query: %s, batch %d", dataUsageQuery, currentBatch))
		startQuery = time.Now()
		rows, err := QuerySnowflake(conn, dataUsageQuery)
		snowflakeQueryTime += time.Since(startQuery).Round(time.Millisecond)

		if err != nil {
			return data_usage.DataUsageSyncResult{Error: api.ToErrorResult(err)}
		}

		logger.Info("Scanning query results for batch %d", currentBatch)
		var returnedRows []queryDbEntities
		err = scan.Rows(&returnedRows, rows)

		if err != nil {
			logger.Error(fmt.Sprintf("Error scanning results into objects during batch %d", currentBatch))
			return data_usage.DataUsageSyncResult{Error: api.ToErrorResult(err)}
		}

		sec := time.Since(startQuery).Round(time.Millisecond)
		logger.Info(fmt.Sprintf("Fetched %d rows from Snowflake in %s for batch %d", len(returnedRows), sec, currentBatch))

		timeFormat := "2006-01-02T15:04:05.999999-07:00"
		executedStatements := make([]dub.Statement, 0, 20)

		for ind := range returnedRows {
			returnedRow := returnedRows[ind]
			startTime, e := time.Parse(timeFormat, returnedRow.StartTime)

			if e != nil {
				logger.Error(fmt.Sprintf("Error parsing start time of '%s', expected format is: '%s'", returnedRow.StartTime, timeFormat))
			}
			endTime, e := time.Parse(timeFormat, returnedRow.EndTime)

			if e != nil {
				logger.Error(fmt.Sprintf("Error parsing end time of '%s', expected format is: '%s'", returnedRow.StartTime, timeFormat))
			}

			databaseName := ""
			if returnedRow.DatabaseName.Valid {
				databaseName = returnedRow.DatabaseName.String
			}
			schemaName := ""

			if returnedRow.SchemaName.Valid {
				schemaName = returnedRow.SchemaName.String
			}
			accessedDataObjects := common.ExtractInfoFromQuery(returnedRow.Query, databaseName, schemaName)

			if len(accessedDataObjects) == 0 {
				logger.Debug(fmt.Sprintf("No data objects returned for query: %s, batch %d", returnedRow.Query, currentBatch))
			}

			du := dub.Statement{
				ExternalId:          returnedRow.ExternalId,
				AccessedDataObjects: accessedDataObjects,
				Status:              returnedRow.Status == "SUCCESS",
				User:                returnedRow.User,
				StartTime:           startTime.UnixMilli(),
				EndTime:             endTime.UnixMilli(),
				TotalTime:           returnedRow.TotalTime,
				BytesTransferred:    returnedRow.BytesTranferred,
				RowsReturned:        returnedRow.RowsReturned,
			}
			executedStatements = append(executedStatements, du)
		}

		logger.Info(fmt.Sprintf("Writing statement log to file for batch %d", currentBatch))
		err = fileCreator.AddStatements(executedStatements)

		if err != nil {
			return data_usage.DataUsageSyncResult{Error: api.ToErrorResult(err)}
		}

		logger.Info(fmt.Sprintf("Written %d queries to file for batch %d", len(returnedRows), currentBatch))
		currentBatch++
	}

	sec := time.Since(start).Round(time.Millisecond)
	logger.Info(fmt.Sprintf("Retrieved %d rows from Snowflake (in %s) and written them to file in %d batch(es), for a total time of %s",
		fileCreator.GetStatementCount(), snowflakeQueryTime, currentBatch, sec))

	return data_usage.DataUsageSyncResult{}
}

type queryDbEntities struct {
	ExternalId      string     `db:"QUERY_ID"`
	Status          string     `db:"EXECUTION_STATUS"`
	Query           string     `db:"QUERY_TEXT"`
	DatabaseName    NullString `db:"DATABASE_NAME"`
	SchemaName      NullString `db:"SCHEMA_NAME"`
	User            string     `db:"USER_NAME"`
	StartTime       string     `db:"START_TIME"`
	EndTime         string     `db:"END_TIME"`
	TotalTime       float32    `db:"EXECUTION_TIME"`
	BytesTranferred int        `db:"OUTBOUND_DATA_TRANSFER_BYTES"`
	RowsReturned    int        `db:"EXTERNAL_FUNCTION_TOTAL_SENT_ROWS"`
}

func (entity queryDbEntities) String() string {
	return fmt.Sprintf("ID: %s, Status: %s, SQL Query: %s, DatabaseName: %s, SchemaName: %s, UserName: %s, StartTime: %s, EndTime: %s, TotalTime: %f",
		entity.ExternalId, entity.Status, entity.Query, entity.DatabaseName.String, entity.SchemaName.String, entity.User, entity.StartTime, entity.EndTime, entity.TotalTime)
}
