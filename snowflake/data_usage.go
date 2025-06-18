package snowflake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"time"

	du "github.com/raito-io/cli/base/data_usage"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers"
	"github.com/raito-io/golang-set/set"

	"github.com/raito-io/cli-plugin-snowflake/common"
	"github.com/raito-io/cli-plugin-snowflake/common/stream"
)

//go:generate go run github.com/vektra/mockery/v2 --name=dataUsageRepository --with-expecter --inpackage
type dataUsageRepository interface {
	Close() error
	TotalQueryTime() time.Duration
	GetDataUsage(ctx context.Context, minTime time.Time, maxTime *time.Time, excludedUsers set.Set[string]) <-chan stream.MaybeError[UsageQueryResult]
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
	if configParams.GetBoolWithDefault(SfStandardEdition, false) {
		return errors.New("data usage is not supported in standard edition. Please upgrade to enterprise edition or skip usage sync")
	}

	repo, err := s.repoProvider(configParams.Parameters, "")
	if err != nil {
		return err
	}

	defer func() {
		Logger.Info(fmt.Sprintf("Total snowflake query time:  %s", repo.TotalQueryTime()))
		repo.Close()
	}()

	numberOfDays := configParams.GetIntWithDefault(SfDataUsageWindow, 90)
	if numberOfDays > 90 {
		Logger.Info(fmt.Sprintf("Capping data usage window to 90 days (from %d days)", numberOfDays))
		numberOfDays = 90
	}

	if numberOfDays <= 0 {
		Logger.Info(fmt.Sprintf("Invalid input for data usage window (%d), setting to default 14 days", numberOfDays))
		numberOfDays = 14
	}

	startDate := time.Now().Truncate(24*time.Hour).AddDate(0, 0, -numberOfDays)

	if v, found := configParams.Parameters["lastUsed"]; found && v != "" {
		Logger.Info(fmt.Sprintf("last used from config: %s", configParams.Parameters["lastUsed"]))
		startDateRaw, errLocal := time.Parse(time.RFC3339, configParams.Parameters["lastUsed"])

		if errLocal == nil && startDateRaw.After(startDate) {
			startDate = startDateRaw
		}
	}

	if v, found := configParams.Parameters["firstUsed"]; found && v != "" {
		Logger.Info(fmt.Sprintf("first used from config: %s", configParams.Parameters["firstUsed"]))
	}

	Logger.Info(fmt.Sprintf("using start date %s", startDate.Format(time.RFC3339)))

	queryCtx, cancelCtx := context.WithCancel(ctx)
	defer cancelCtx()

	excludedUsers := parseCommaSeparatedList(configParams.GetString(SfUsageUserExcludes))

	if len(excludedUsers) > 0 {
		Logger.Info("Excluding %d users from data usage sync", len(excludedUsers))
	} else {
		Logger.Info("No users excluded from data usage sync")
	}

	usageStatementSqlRows := repo.GetDataUsage(queryCtx, startDate, nil, excludedUsers)

	i := 0

	defer func() {
		Logger.Info(fmt.Sprintf("Processed %d statements", i))
	}()

	for usageStatement := range usageStatementSqlRows {
		if usageStatement.HasError() {
			return fmt.Errorf("get usage information: %w", usageStatement.Error())
		}

		statement := usageQueryResultToStatement(usageStatement.ValueIfNoError())

		err = fileCreator.AddStatements([]du.Statement{statement})
		if err != nil {
			return fmt.Errorf("add statement to file: %w", err)
		}

		i += 1

		if i%10000 == 0 {
			logUsageBatch(i)
		}
	}

	logUsageBatch(i)

	return nil
}

func logUsageBatch(count int) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	Logger.Info(fmt.Sprintf("Processed %d statements (Heap: %v MiB; System memory: %v MiB)", count, bToMb(m.Alloc), bToMb(m.Sys)))
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func stringWithDefault(value NullString) string {
	if value.Valid {
		return value.String
	}

	return ""
}

func usageQueryResultToStatement(input *UsageQueryResult) (statement du.Statement) {
	var objects []du.UsageDataObjectItem

	statement.ExternalId = input.ExternalId
	statement.User = input.User.String
	statement.Role = stringWithDefault(input.Role)
	statement.Success = input.Status.String == "SUCCESS"
	statement.Status = input.Status.String
	statement.Query = input.Query.String
	statement.Bytes = int(input.BytesWrittenToResult)
	statement.Credits = float32(input.CloudCreditsUsed)

	if input.RowsProduced.Valid {
		statement.Rows = int(input.RowsProduced.Int64)
	}

	statement.StartTime = input.StartTime.Time.Unix()
	statement.EndTime = input.EndTime.Time.Unix()

	objects, err := parseAccessedObjects(&input.DirectObjectsAccessed, objects, du.Read)
	if err != nil {
		statement.Error = fmt.Sprintf("parse direct objects accessed: %s", err.Error())

		return statement
	}

	// We maybe should handle this differently in a later stage
	objects, err = parseAccessedObjects(&input.BaseObjectsAccessed, objects, du.Read)
	if err != nil {
		statement.Error = fmt.Sprintf("parse base objects accessed: %s", err.Error())

		return statement
	}

	objects, err = parseAccessedObjects(&input.ObjectsModified, objects, du.Write)
	if err != nil {
		statement.Error = fmt.Sprintf("parse objects modified: %s", err.Error())

		return statement
	}

	objects, err = parseDdlModifiedObject(&input.ObjectsModifiedByDdl, objects)
	if err != nil {
		statement.Error = fmt.Sprintf("parse ddl modified object: %s", err.Error())

		return statement
	}

	statement.AccessedDataObjects = objects

	return statement
}

var typeParentMap = map[string]string{
	"table":             "schema",
	"external table":    "schema",
	"schema":            "database",
	"database":          "account",
	"view":              "schema",
	"materialized view": "schema",
}

func parseDdlModifiedObject(objectString *NullString, objects []du.UsageDataObjectItem) ([]du.UsageDataObjectItem, error) {
	if !objectString.Valid {
		return objects, nil
	}

	var modifiedObject common.SnowflakeModifiedObjectsByDdl

	err := json.Unmarshal([]byte(objectString.String), &modifiedObject)
	if err != nil {
		return objects, fmt.Errorf("unmarshal: %w", err)
	}

	fullName := modifiedObject.ObjectName
	objectType := strings.ToLower(modifiedObject.ObjectDomain)

	if modifiedObject.OperationType == common.ModifiedObjectByDdlOperationTypeCreate || modifiedObject.OperationType == common.ModifiedObjectByDdlOperationTypeDrop {
		newObjectType, found := typeParentMap[objectType]
		if !found {
			Logger.Warn(fmt.Sprintf("Unknown object type '%s' for object '%s'", objectType, fullName))
		} else {
			objectType = newObjectType

			nameParts := strings.Split(fullName, ".")
			fullName = strings.Join(nameParts[:len(nameParts)-1], ".")
		}
	}

	objects = append(objects, du.UsageDataObjectItem{
		DataObject: du.UsageDataObjectReference{
			FullName: fullName,
			Type:     objectType,
		},
		GlobalPermission: du.Admin,
	})

	return objects, nil
}

var versionPostFix = regexp.MustCompile(`\$V\d+$`) // Fullname version postfix (e.g. SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY$V1)

func parseAccessedObjects(objectString *NullString, objects []du.UsageDataObjectItem, permission du.ActionType) ([]du.UsageDataObjectItem, error) {
	if !objectString.Valid {
		return objects, nil
	}

	var snowflakeObjects []common.SnowflakeAccessedObjects

	err := json.Unmarshal([]byte(objectString.String), &snowflakeObjects)
	if err != nil {
		return objects, err
	}

	for _, object := range snowflakeObjects {
		fullName := object.Name

		fullName = versionPostFix.ReplaceAllString(fullName, "")

		objects = append(objects, du.UsageDataObjectItem{
			DataObject: du.UsageDataObjectReference{
				FullName: fullName,
				Type:     strings.ToLower(object.Domain),
			},
			GlobalPermission: permission,
		})
	}

	return objects, nil
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
