package snowflake

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	du "github.com/raito-io/cli/base/data_usage"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers"

	"github.com/raito-io/cli-plugin-snowflake/common"
)

//go:generate go run github.com/vektra/mockery/v2 --name=dataUsageRepository --with-expecter --inpackage
type dataUsageRepository interface {
	Close() error
	TotalQueryTime() time.Duration
	BatchingInformation(startDate *time.Time, historyTable string) (*string, *string, int, error)
	DataUsage(columns []string, limit int, offset int, historyTable string, minTime, maxTime *string, accessHistoryAvailable bool) ([]QueryDbEntities, error)
	CheckAccessHistoryAvailability(historyTable string) (bool, error)
}

type DataUsageSyncer struct {
	repoProvider func(params map[string]string, role string) (dataUsageRepository, error)
}

func NewDataUsageSyncer() *DataUsageSyncer {
	return &DataUsageSyncer{repoProvider: newDataUsageSnowflakeRepo}
}

func newDataUsageSnowflakeRepo(params map[string]string, role string) (dataUsageRepository, error) {
	return NewSnowflakeRepository(params, role)
}

func (s *DataUsageSyncer) SyncDataUsage(ctx context.Context, fileCreator wrappers.DataUsageStatementHandler, configParams *config.ConfigMap) error {
	repo, err := s.repoProvider(configParams.Parameters, "")
	if err != nil {
		return err
	}

	defer func() {
		logger.Info(fmt.Sprintf("Total snowflake query time:  %s", repo.TotalQueryTime()))
		repo.Close()
	}()

	const numRowsPerBatch = 10000
	queryHistoryTable := "SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY"

	numberOfDays := configParams.GetIntWithDefault(SfDataUsageWindow, 90)
	if numberOfDays > 90 {
		logger.Info(fmt.Sprintf("Capping data usage window to 90 days (from %d days)", numberOfDays))
		numberOfDays = 90
	}

	if numberOfDays <= 0 {
		logger.Info(fmt.Sprintf("Invalid input for data usage window (%d), setting to default 14 days", numberOfDays))
		numberOfDays = 14
	}

	startDate := time.Now().Truncate(24*time.Hour).AddDate(0, 0, -numberOfDays)

	if v, found := configParams.Parameters["lastUsed"]; found && v != "" {
		logger.Info(fmt.Sprintf("last used from config: %s", configParams.Parameters["lastUsed"]))
		startDateRaw, errLocal := time.Parse(time.RFC3339, configParams.Parameters["lastUsed"])

		if errLocal == nil && startDateRaw.After(startDate) {
			startDate = startDateRaw
		}
	}

	if v, found := configParams.Parameters["firstUsed"]; found && v != "" {
		logger.Info(fmt.Sprintf("first used from config: %s", configParams.Parameters["firstUsed"]))
	}

	logger.Info(fmt.Sprintf("using start date %s", startDate.Format(time.RFC3339)))

	minTime, maxTime, numRows, err := repo.BatchingInformation(&startDate, queryHistoryTable)
	if err != nil {
		return err
	}

	logger.Info(fmt.Sprintf("Batch information result; min time: %s, max time: %s, num rows: %d", *minTime, *maxTime, numRows))

	columns := GetQueryDbEntitiesColumnNames("db", "useColumnName")

	currentBatch := 0

	accessHistoryAvailable, err := repo.CheckAccessHistoryAvailability("SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY")
	if err != nil {
		logger.Warn(fmt.Sprintf("Error accessing access history table: %s", err.Error()))
	}

	for currentBatch*numRowsPerBatch < numRows {
		returnedRows, err := repo.DataUsage(columns, numRowsPerBatch, currentBatch*numRowsPerBatch, queryHistoryTable, minTime, maxTime, accessHistoryAvailable)
		if err != nil {
			return err
		}

		timeFormat := "2006-01-02T15:04:05.999999-07:00"
		executedStatements := make([]du.Statement, 0, 20)

		unparsableQueries := map[string]interface{}{}

		for ind := range returnedRows {
			returnedRow := returnedRows[ind]
			startTime, e := time.Parse(timeFormat, returnedRow.StartTime)

			if e != nil {
				logger.Warn(fmt.Sprintf("Error parsing start time of '%s', expected format is: '%s'", returnedRow.StartTime, timeFormat))
			}
			endTime, e := time.Parse(timeFormat, returnedRow.EndTime)

			if e != nil {
				logger.Warn(fmt.Sprintf("Error parsing end time of '%s', expected format is: '%s'", returnedRow.StartTime, timeFormat))
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
				roleName := ""
				if returnedRow.Role.Valid {
					roleName = returnedRow.Role.String
				}
				du := du.Statement{
					ExternalId:          returnedRow.ExternalId,
					AccessedDataObjects: accessedDataObjects,
					Success:             returnedRow.Status == "SUCCESS",
					Status:              returnedRow.Status,
					User:                returnedRow.User,
					Role:                roleName,
					StartTime:           startTime.Unix(),
					EndTime:             endTime.Unix(),
					Bytes:               returnedRow.BytesTranferred,
					Rows:                returnedRow.RowsReturned,
					Credits:             returnedRow.CloudCreditsUsed,
				}
				executedStatements = append(executedStatements, du)
			}
		}

		logger.Debug(fmt.Sprintf("Writing data usage export log to file for batch %d", currentBatch))

		err = fileCreator.AddStatements(executedStatements)

		if err != nil {
			return err
		}

		logger.Info(fmt.Sprintf("Written %d statements to file for batch %d", len(executedStatements), currentBatch))
		currentBatch++
	}

	return nil
}

func GetQueryDbEntitiesColumnNames(tag string, includeTag string) []string {
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
