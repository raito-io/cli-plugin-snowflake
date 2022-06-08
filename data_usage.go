package main

import (
	"database/sql"
	"fmt"
	"reflect"
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

	start := time.Now()

	conn, err := ConnectToSnowflake(config.Parameters, "")
	if err != nil {
		return data_usage.DataUsageSyncResult{Error: api.ToErrorResult(err)}
	}
	defer conn.Close()

	// TODO: should be configurable
	numberOfDays := 7
	startDate := time.Now().AddDate(0, 0, -numberOfDays)
	dateFormat := "2006-01-02"

	historyQuery := fmt.Sprintf("SELECT QUERY_ID, EXECUTION_STATUS, QUERY_TEXT, DATABASE_NAME, SCHEMA_NAME, USER_NAME, START_TIME, END_TIME, EXECUTION_TIME, OUTBOUND_DATA_TRANSFER_BYTES, EXTERNAL_FUNCTION_TOTAL_SENT_ROWS FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY WHERE start_time >= '%s'", startDate.Format(dateFormat))
	dataUsageQuery := historyQuery

	logger.Info(fmt.Sprintf("Retrieving query log from Snowflake with query: %s", dataUsageQuery))
	rows, err := QuerySnowflake(conn, dataUsageQuery)
	if err != nil {
		return data_usage.DataUsageSyncResult{Error: api.ToErrorResult(err)}
	}

	var returnedRows []queryDbEntities

	logger.Info("Scanning query results")
	err = scan.Rows(&returnedRows, rows)
	if err != nil {
		logger.Error("Error scanning results into objects")
		return data_usage.DataUsageSyncResult{Error: api.ToErrorResult(err)}
	}

	sec := time.Since(start).Round(time.Millisecond)
	logger.Info(fmt.Sprintf("Fetched %d rows from Snowflake in %s", len(returnedRows), sec))

	logger.Info("Preparing import file")
	timeFormat := "2006-01-02T15:04:05.999999-07:00"
	executedStatements := make([]dub.Statement, 0, 20)
	for _, returnedRow := range returnedRows {
		startTime, err := time.Parse(timeFormat, returnedRow.StartTime)
		if err != nil {
			logger.Error(fmt.Sprintf("Error parsing start time of '%s', expected format is: '%s'", returnedRow.StartTime, timeFormat))
		}
		endTime, err := time.Parse(timeFormat, returnedRow.EndTime)
		if err != nil {
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
		accessDataObjects := common.ExtractInfoFromQuery(returnedRow.Query, databaseName, schemaName)

		if len(accessDataObjects) == 0 {
			logger.Info(fmt.Sprintf("No data objects returned for query: %s", returnedRow.Query))
		}

		du := dub.Statement{
			ExternalId:          returnedRow.ExternalId,
			AccessedDataObjects: accessDataObjects,
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

	logger.Info("Writing statement log to file")
	err = fileCreator.AddStatements(executedStatements)
	if err != nil {
		return data_usage.DataUsageSyncResult{Error: api.ToErrorResult(err)}
	}

	logger.Info(fmt.Sprintf("Written %d queries to file", len(returnedRows)))

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
