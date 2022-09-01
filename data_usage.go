package main

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/blockloop/scan"
	"github.com/raito-io/cli-plugin-snowflake/common"
	du "github.com/raito-io/cli/base/data_usage"
	e "github.com/raito-io/cli/base/util/error"
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

func (s *DataUsageSyncer) SyncDataUsage(config *du.DataUsageSyncConfig) du.DataUsageSyncResult {
	logger.Info("Starting data usage synchronisation")
	logger.Debug("Creating file for storing data usage")
	fileCreator, err := du.NewDataUsageFileCreator(config)

	if err != nil {
		logger.Info(err.Error())
		return du.DataUsageSyncResult{Error: e.ToErrorResult(err)}
	}
	defer fileCreator.Close()

	conn, err := ConnectToSnowflake(config.Parameters, "")
	if err != nil {
		logger.Info(err.Error())
		return du.DataUsageSyncResult{Error: e.ToErrorResult(err)}
	}
	defer conn.Close()

	const numRowsPerBatch = 10000
	queryHistoryTable := "SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY"

	// TODO: should be configurable
	numberOfDays := 14
	startDate := time.Now().Truncate(24*time.Hour).AddDate(0, 0, -numberOfDays)

	if config != nil && config.ConfigMap.Parameters["lastUsed"] != nil {
		startDateRaw, errLocal := time.Parse(time.RFC3339, config.ConfigMap.Parameters["lastUsed"].(string))
		if errLocal == nil && startDateRaw.After(startDate) {
			startDate = startDateRaw
		}
	}

	logger.Info(fmt.Sprintf("using start date %s", startDate.Format(time.RFC3339)))
	filterClause := fmt.Sprintf("WHERE start_time > '%s'", startDate.Format(time.RFC3339))

	fetchBatchingInfoQuery := fmt.Sprintf("SELECT min(START_TIME) as minTime, max(START_TIME) as maxTime, COUNT(START_TIME) as numRows FROM %s %s", queryHistoryTable, filterClause)
	start := time.Now()
	startQuery := time.Now()
	snowflakeQueryTime := time.Duration(0)

	logger.Info(fmt.Sprintf("Send query: %s", fetchBatchingInfoQuery))

	batchingInfoResult, err := QuerySnowflake(conn, fetchBatchingInfoQuery)

	if err != nil {
		return du.DataUsageSyncResult{Error: e.ToErrorResult(err)}
	}
	snowflakeQueryTime += time.Since(startQuery).Round(time.Millisecond)

	var minTime *string
	var maxTime *string
	numRows := 0

	for batchingInfoResult.Next() {
		err := batchingInfoResult.Scan(&minTime, &maxTime, &numRows)
		if err != nil {
			logger.Info(fmt.Sprintf("%v", batchingInfoResult))
			return du.DataUsageSyncResult{Error: e.ToErrorResult(err)}
		}

		if numRows == 0 || minTime == nil || maxTime == nil {
			errorMessage := fmt.Sprintf("no usage information available with query: %s => result: numRows: %d, minTime: %v, maxtime: %v",
				fetchBatchingInfoQuery, numRows, minTime, maxTime)
			logger.Info(errorMessage)

			return du.DataUsageSyncResult{Error: e.ToErrorResult(fmt.Errorf("%s", errorMessage))}
		}

		logger.Info(fmt.Sprintf("Batch information result; min time: %s, max time: %s, num rows: %d", *minTime, *maxTime, numRows))
	}

	columns := s.getColumnNames("db", "useColumnName")

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
			return du.DataUsageSyncResult{Error: e.ToErrorResult(err)}
		}

		logger.Info(fmt.Sprintf("Scanning query results for batch %d", currentBatch))
		var returnedRows []QueryDbEntities
		err = scan.Rows(&returnedRows, rows)

		if err != nil {
			logger.Error(fmt.Sprintf("Error scanning results into objects during batch %d, %s", currentBatch, err.Error()))
			return du.DataUsageSyncResult{Error: e.ToErrorResult(err)}
		}

		sec := time.Since(startQuery).Round(time.Millisecond)
		logger.Info(fmt.Sprintf("Fetched %d rows from Snowflake in %s for batch %d", len(returnedRows), sec, currentBatch))

		timeFormat := "2006-01-02T15:04:05.999999-07:00"
		executedStatements := make([]du.Statement, 0, 20)

		unparsableQueries := map[string]interface{}{}

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

			emptyDu := du.Statement{
				StartTime: 0,
				Query:     returnedRow.Query,
			}
			if localErr != nil || len(accessedDataObjects) == 0 || accessedDataObjects[0].DataObject == nil {
				if _, found := unparsableQueries[emptyDu.Query]; !found {
					executedStatements = append(executedStatements, emptyDu)
					unparsableQueries[emptyDu.Query] = true
				}
			} else {
				du := du.Statement{
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
		}

		currentStatements := fileCreator.GetStatementCount()

		logger.Debug(fmt.Sprintf("Writing data usage export log to file for batch %d", currentBatch))

		err = fileCreator.AddStatements(executedStatements)

		if err != nil {
			return du.DataUsageSyncResult{Error: e.ToErrorResult(err)}
		}

		logger.Info(fmt.Sprintf("Written %d statements to file for batch %d", fileCreator.GetStatementCount()-currentStatements, currentBatch))
		currentBatch++
	}

	sec := time.Since(start).Round(time.Millisecond)
	logger.Info(fmt.Sprintf("Retrieved %d rows from Snowflake (in %s) and written them to file in %d batch(es), for a total time of %s",
		fileCreator.GetStatementCount(), snowflakeQueryTime, currentBatch, sec))

	return du.DataUsageSyncResult{}
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

func (s *DataUsageSyncer) getColumnNames(tag string, includeTag string) []string {
	columNames := []string{}
	val := reflect.ValueOf(QueryDbEntities{})

	for i := 0; i < val.Type().NumField(); i++ {
		tagValue := val.Type().Field(i).Tag.Get(tag)
		includeTagValue := val.Type().Field(i).Tag.Get(includeTag)

		if tagValue != "" && strings.EqualFold(includeTagValue, "true") {
			columNames = append(columNames, val.Type().Field(i).Tag.Get(tag))
		}
	}

	return columNames
}

type QueryDbEntities struct {
	ExternalId            string     `db:"QUERY_ID" useColumnName:"true"`
	Status                string     `db:"EXECUTION_STATUS" useColumnName:"true"`
	Query                 string     `db:"QUERY_TEXT" useColumnName:"true"`
	ErrorMessage          NullString `db:"ERROR_MESSAGE" useColumnName:"true"`
	DatabaseName          NullString `db:"DATABASE_NAME" useColumnName:"true"`
	SchemaName            NullString `db:"SCHEMA_NAME" useColumnName:"true"`
	User                  string     `db:"USER_NAME" useColumnName:"true"`
	Role                  string     `db:"ROLE_NAME" useColumnName:"true"`
	StartTime             string     `db:"START_TIME" useColumnName:"true"`
	EndTime               string     `db:"END_TIME" useColumnName:"true"`
	BytesTranferred       int        `db:"OUTBOUND_DATA_TRANSFER_BYTES" useColumnName:"true"`
	RowsReturned          int        `db:"EXTERNAL_FUNCTION_TOTAL_SENT_ROWS" useColumnName:"true"`
	CloudCreditsUsed      float32    `db:"CREDITS_USED_CLOUD_SERVICES" useColumnName:"true"`
	AccessId              NullString `db:"QID"`
	DirectObjectsAccessed *string    `db:"DIRECT_OBJECTS_ACCESSED"`
	BaseObjectsAccessed   *string    `db:"BASE_OBJECTS_ACCESSED"`
	ObjectsModified       *string    `db:"OBJECTS_MODIFIED"`
}

func (entity QueryDbEntities) String() string {
	return fmt.Sprintf("ID: %s, Status: %s, SQL Query: %s, DatabaseName: %s, SchemaName: %s, UserName: %s, RoleName: %s, StartTime: %s, EndTime: %s, DirectObjectsAccessed: %v, BaseObjectsAccess: %v",
		entity.ExternalId, entity.Status, entity.Query, entity.DatabaseName.String, entity.SchemaName.String, entity.User, entity.Role, entity.StartTime, entity.EndTime, entity.DirectObjectsAccessed, entity.BaseObjectsAccessed)
}
