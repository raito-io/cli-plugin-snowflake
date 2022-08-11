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
	logger.Info("Starting data usage synchronisation")
	logger.Debug("Creating file for storing data usage")
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
	queryHistoryTable := "SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY"

	// TODO: should be configurable
	numberOfDays := 14
	startDate := time.Now().AddDate(0, 0, -numberOfDays)
	if config != nil && config.ConfigMap.Parameters["lastUsed"] != nil {
		startDateRaw, err := time.Parse(time.RFC3339, config.ConfigMap.Parameters["lastUsed"].(string))
		if err == nil && startDateRaw.After(startDate) {
			startDate = startDateRaw
		}
	}
	logger.Info(fmt.Sprintf("using start date %s", startDate.Format(time.RFC3339)))
	filterClause := fmt.Sprintf("WHERE start_time >= '%s'", startDate.Format(time.RFC3339))

	fetchBatchingInfoQuery := fmt.Sprintf("SELECT min(START_TIME) as minTime, max(START_TIME) as maxTime, COUNT(START_TIME) as numRows FROM %s %s", queryHistoryTable, filterClause)
	start := time.Now()
	startQuery := time.Now()
	snowflakeQueryTime := time.Duration(0)

	logger.Info(fmt.Sprintf("Send query: %s", fetchBatchingInfoQuery))

	batchingInfoResult, err := QuerySnowflake(conn, fetchBatchingInfoQuery)

	if err != nil {
		return data_usage.DataUsageSyncResult{Error: api.ToErrorResult(err)}
	}
	snowflakeQueryTime += time.Since(startQuery).Round(time.Millisecond)

	var minTime *string
	var maxTime *string
	numRows := 0

	for batchingInfoResult.Next() {
		err := batchingInfoResult.Scan(&minTime, &maxTime, &numRows)
		if err != nil {
			logger.Info(fmt.Sprintf("%v", batchingInfoResult))
			return data_usage.DataUsageSyncResult{Error: api.ToErrorResult(err)}
		}
		if numRows == 0 || minTime == nil || maxTime == nil {
			errorMessage := fmt.Sprintf("no usage information available with query: %s => result: numRows: %d, minTime: %v, maxtime: %v",
				fetchBatchingInfoQuery, numRows, minTime, maxTime)
			logger.Info(errorMessage)
			return data_usage.DataUsageSyncResult{Error: api.ToErrorResult(fmt.Errorf(errorMessage))}
		}

		logger.Info(fmt.Sprintf("Batch information result; min time: %s, max time: %s, num rows: %d", *minTime, *maxTime, numRows))
	}

	columns := s.getColumnNames("db")

	filterClause = fmt.Sprintf("WHERE START_TIME >= '%s' and START_TIME <= '%s'", *minTime, *maxTime)

	currentBatch := 0
	accessHistoryAvailable := s.checkAccessHistoryAvailability(conn)

	for currentBatch*numRowsPerBatch < numRows {
		paginationClause := fmt.Sprintf("LIMIT %d OFFSET %d", numRowsPerBatch, currentBatch*numRowsPerBatch)
		if numRows < numRowsPerBatch {
			paginationClause = ""
		}
		dataUsageQuery := fmt.Sprintf("SELECT %s FROM %s %s ORDER BY START_TIME, QUERY_ID DESC %s", strings.Join(columns, ", "), queryHistoryTable, filterClause, paginationClause)

		if accessHistoryAvailable {
			logger.Info("Using access history table in combination with history table")
			dataUsageQuery = fmt.Sprintf(`SELECT %s, QID, DIRECT_OBJECTS_ACCESSED, BASE_OBJECTS_ACCESSED, OBJECTS_MODIFIED FROM (SELECT %s FROM %s %s) as QUERIES LEFT JOIN (SELECT QUERY_ID as QID, DIRECT_OBJECTS_ACCESSED, BASE_OBJECTS_ACCESSED, OBJECTS_MODIFIED FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY) as ACCESS on QUERIES.QUERY_ID = ACCESS.QID ORDER BY START_TIME, QUERIES.QUERY_ID DESC %s`,
				strings.Join(columns, ", "), strings.Join(columns, ", "), queryHistoryTable, filterClause, paginationClause)
		}

		logger.Debug(fmt.Sprintf("Retrieving paginated query log from Snowflake with query: %s, batch %d", dataUsageQuery, currentBatch))
		startQuery = time.Now()
		rows, err := QuerySnowflake(conn, dataUsageQuery)
		snowflakeQueryTime += time.Since(startQuery).Round(time.Millisecond)

		if err != nil {
			return data_usage.DataUsageSyncResult{Error: api.ToErrorResult(err)}
		}

		logger.Info(fmt.Sprintf("Scanning query results for batch %d", currentBatch))
		var returnedRows []QueryDbEntities
		err = scan.Rows(&returnedRows, rows)

		if err != nil {
			logger.Error(fmt.Sprintf("Error scanning results into objects during batch %d, %s", currentBatch, err.Error()))
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

			accessedDataObjects, localErr := common.ParseSnowflakeInformation(returnedRow.Query, databaseName, schemaName,
				returnedRow.BaseObjectsAccessed, returnedRow.DirectObjectsAccessed, returnedRow.ObjectsModified)

			if localErr != nil {
				// TODO: add logic to include query
			} else if len(accessedDataObjects) == 0 || accessedDataObjects[0].DataObjectReference == nil {
				// nolint
				// logger.Debug(fmt.Sprintf("No data objects returned for query: %s, batch %d", returnedRow.Query, currentBatch))
				continue
			}

			du := dub.Statement{
				ExternalId:          returnedRow.ExternalId,
				AccessedDataObjects: accessedDataObjects,
				Success:             returnedRow.Status == "SUCCESS",
				Status:              returnedRow.Status,
				User:                returnedRow.User,
				Role:                returnedRow.Role,
				StartTime:           startTime.Unix(),
				EndTime:             endTime.Unix(),
				Bytes:               returnedRow.BytesTranferred,
				Rows:                returnedRow.RowsReturned,
				Credits:             returnedRow.CloudCreditsUsed,
			}
			executedStatements = append(executedStatements, du)
		}

		currentStatements := fileCreator.GetStatementCount()

		logger.Debug(fmt.Sprintf("Writing data usage export log to file for batch %d", currentBatch))

		err = fileCreator.AddStatements(executedStatements)

		if err != nil {
			return data_usage.DataUsageSyncResult{Error: api.ToErrorResult(err)}
		}

		logger.Info(fmt.Sprintf("Written %d statements to file for batch %d", fileCreator.GetStatementCount()-currentStatements, currentBatch))
		currentBatch++
	}

	sec := time.Since(start).Round(time.Millisecond)
	logger.Info(fmt.Sprintf("Retrieved %d rows from Snowflake (in %s) and written them to file in %d batch(es), for a total time of %s",
		fileCreator.GetStatementCount(), snowflakeQueryTime, currentBatch, sec))

	return data_usage.DataUsageSyncResult{}
}

func (s *DataUsageSyncer) checkAccessHistoryAvailability(conn *sql.DB) bool {
	accessHistoryTable := "SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY"
	checkAccessHistoryAvailabilityQuery := fmt.Sprintf("SELECT QUERY_ID, DIRECT_OBJECTS_ACCESSED, BASE_OBJECTS_ACCESSED, OBJECTS_MODIFIED FROM %s LIMIT 10", accessHistoryTable)
	logger.Debug(fmt.Sprintf("Sending query: %s", checkAccessHistoryAvailabilityQuery))
	accessHistoryInfoResult, err := QuerySnowflake(conn, checkAccessHistoryAvailabilityQuery)

	if err != nil {
		logger.Debug(fmt.Sprintf("Error accessing access history table: %s", err.Error()))
		return false
	}

	numRows := 0
	for accessHistoryInfoResult.Next() {
		numRows++
	}

	if numRows > 0 {
		logger.Debug(fmt.Sprintf("Access history query returned %d rows", numRows))
		return true
	}

	return false
}

func (s *DataUsageSyncer) getColumnNames(tag string) []string {
	columNames := []string{}
	val := reflect.ValueOf(QueryDbEntities{})
	for i := 0; i < val.Type().NumField(); i++ {
		tagValue := val.Type().Field(i).Tag.Get(tag)
		if tagValue != "" {
			columNames = append(columNames, val.Type().Field(i).Tag.Get(tag))
		}
	}
	return columNames
}

type QueryDbEntities struct {
	ExternalId            string     `db:"QUERY_ID"`
	Status                string     `db:"EXECUTION_STATUS"`
	Query                 string     `db:"QUERY_TEXT"`
	ErrorMessage          NullString `db:"ERROR_MESSAGE"`
	DatabaseName          NullString `db:"DATABASE_NAME"`
	SchemaName            NullString `db:"SCHEMA_NAME"`
	User                  string     `db:"USER_NAME"`
	Role                  string     `db:"ROLE_NAME"`
	StartTime             string     `db:"START_TIME"`
	EndTime               string     `db:"END_TIME"`
	BytesTranferred       int        `db:"OUTBOUND_DATA_TRANSFER_BYTES"`
	RowsReturned          int        `db:"EXTERNAL_FUNCTION_TOTAL_SENT_ROWS"`
	CloudCreditsUsed      float32    `db:"CREDITS_USED_CLOUD_SERVICES"`
	AccessId              NullString `db-table2:"QID"`
	DirectObjectsAccessed *string    `db-table2:"DIRECT_OBJECTS_ACCESSED"`
	BaseObjectsAccessed   *string    `db-table2:"BASE_OBJECTS_ACCESSED"`
	ObjectsModified       *string    `db-table2:"OBJECTS_MODIFIED"`
}

func (entity QueryDbEntities) String() string {
	return fmt.Sprintf("ID: %s, Status: %s, SQL Query: %s, DatabaseName: %s, SchemaName: %s, UserName: %s, StartTime: %s, EndTime: %s",
		entity.ExternalId, entity.Status, entity.Query, entity.DatabaseName.String, entity.SchemaName.String, entity.User, entity.StartTime, entity.EndTime)
}
