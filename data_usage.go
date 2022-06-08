package main

import (
	"fmt"
	"time"

	"github.com/blockloop/scan"
	"github.com/raito-io/cli-plugin-snowflake/common"
	dub "github.com/raito-io/cli/base/data_usage"
	"github.com/raito-io/cli/common/api"
	"github.com/raito-io/cli/common/api/data_usage"
)

type DataUsageSyncer struct {
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

	// historyQuery := fmt.Sprintf("SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY WHERE start_time >= '%s' ORDER BY START_TIME DESC LIMIT 15;", startDate.Format(dateFormat))
	historyQuery := fmt.Sprintf("SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY WHERE start_time >= '%s'", startDate.Format(dateFormat))
	dataUsageQuery := historyQuery

	logger.Info(fmt.Sprintf("Retrieving query log from Snowflake with query: %s", dataUsageQuery))
	rows, err := QuerySnowflake(conn, dataUsageQuery)
	if err != nil {
		return data_usage.DataUsageSyncResult{Error: api.ToErrorResult(err)}
	}

	var queries []queryDbEntities

	logger.Info("Scanning query results")
	err = scan.Rows(&queries, rows)
	if err != nil {
		return data_usage.DataUsageSyncResult{Error: api.ToErrorResult(err)}
	}
	logger.Info(fmt.Sprintf("%d results returned from Snowflake", len(queries)))

	sec := time.Since(start).Round(time.Millisecond)
	logger.Info(fmt.Sprintf("Fetched %d queries from Snowflake in %s", len(queries), sec))

	logger.Info("Preparing import file")
	timeFormat := "2006-01-02T15:04:05.999999-07:00"
	executedQueries := make([]dub.Statement, 0, 20)
	for _, query := range queries {
		startTime, err := time.Parse(timeFormat, query.StartTime)
		if err != nil {
			logger.Error(fmt.Sprintf("Error parsing query start time of '%s', expected format is: '%s'", query.StartTime, timeFormat))
		}
		endTime, err := time.Parse(timeFormat, query.EndTime)
		if err != nil {
			logger.Error(fmt.Sprintf("Error parsing query end time of '%s', expected format is: '%s'", query.StartTime, timeFormat))
		}

		accessDataObjects := common.ExtractInfoFromQuery(query.Query)

		if len(accessDataObjects) == 0 {
			logger.Info(fmt.Sprintf("No data objects returned for query: %s", query.Query))
		}

		du := dub.Statement{
			ExternalId:          query.ExternalId,
			AccessedDataObjects: accessDataObjects,
			Status:              query.Status == "SUCCESS",
			User:                query.User,
			StartTime:           startTime.UnixMilli(),
			EndTime:             endTime.UnixMilli(),
			TotalTime:           query.TotalTime,
			BytesTransferred:    query.BytesTranferred,
			RowsReturned:        query.RowsReturned,
		}
		executedQueries = append(executedQueries, du)
	}

	logger.Info("Writing statement log to file")
	err = fileCreator.AddStatements(executedQueries)
	if err != nil {
		return data_usage.DataUsageSyncResult{Error: api.ToErrorResult(err)}
	}

	logger.Info(fmt.Sprintf("Written %d queries to file", len(queries)))

	return data_usage.DataUsageSyncResult{}
}

type queryDbEntities struct {
	ExternalId      string  `db:"QUERY_ID"`
	Status          string  `db:"EXECUTION_STATUS"`
	Query           string  `db:"QUERY_TEXT"`
	User            string  `db:"USER_NAME"`
	StartTime       string  `db:"START_TIME"`
	EndTime         string  `db:"END_TIME"`
	TotalTime       float32 `db:"EXECUTION_TIME"`
	BytesTranferred int     `db:"OUTBOUND_DATA_TRANSFER_BYTES"`
	RowsReturned    int     `db:"EXTERNAL_FUNCTION_TOTAL_SENT_ROWS"`
}

func (entity queryDbEntities) String() string {
	return fmt.Sprintf("ID: %s, Status: %s, SQL Query: %s, UserName: %s, StartTime: %s, EndTime: %s, TotalTime: %f",
		entity.ExternalId, entity.Status, entity.Query, entity.User, entity.StartTime, entity.EndTime, entity.TotalTime)
}
