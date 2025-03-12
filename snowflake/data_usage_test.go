package snowflake

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/raito-io/cli/base/data_usage"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/raito-io/cli-plugin-snowflake/common/stream"
)

func TestDataUsageSyncer_SyncDataUsage(t *testing.T) {
	//Given
	configParams := config.ConfigMap{
		Parameters: map[string]string{"key": "value"},
	}

	repoMock := newMockDataUsageRepository(t)
	fileCreator := mocks.NewSimpleDataUsageStatementHandler(t)

	startTime := time.Now().Add(-1 * time.Hour)
	endTime := time.Now()

	repoMock.EXPECT().Close().Return(nil)
	repoMock.EXPECT().TotalQueryTime().Return(time.Minute)
	repoMock.EXPECT().GetDataUsage(mock.Anything, mock.AnythingOfType("time.Time"), mock.AnythingOfType("*time.Time"), mock.Anything).Return(stream.ArrayToChannel(context.Background(), []stream.MaybeError[UsageQueryResult]{
		stream.NewMaybeErrorValue(UsageQueryResult{
			ExternalId:           "queryId1",
			Query:                NullString{String: "UPDATE table1 SET column1 = 1 WHERE column2 IN (SELECT column3 FROM view1)", Valid: true},
			DatabaseName:         NullString{String: "DBName1", Valid: true},
			SchemaName:           NullString{String: "SchemaName1", Valid: true},
			QueryType:            NullString{String: "UPDATE", Valid: true},
			SessionID:            NullString{Valid: false},
			User:                 NullString{String: "user1", Valid: true},
			Role:                 NullString{String: "role1", Valid: true},
			Status:               NullString{String: "SUCCESS", Valid: true},
			StartTime:            sql.NullTime{Time: startTime, Valid: true},
			EndTime:              sql.NullTime{Time: endTime, Valid: true},
			TotalElapsedTime:     int64(time.Hour.Seconds()),
			BytesScanned:         20,
			BytesWritten:         30,
			BytesWrittenToResult: 40,
			RowsProduced: sql.NullInt64{
				Int64: 50,
				Valid: true,
			},
			RowsInserted:     10,
			RowsUpdated:      20,
			RowsDeleted:      30,
			RowsUnloaded:     5,
			CloudCreditsUsed: 10.5,
			DirectObjectsAccessed: NullString{
				String: `[{"objectDomain": "Table", "objectName": "DBNAME1.SCHEMANAME1.TABLE1"}, {"objectDomain": "View", "objectName": "DBNAME1.SCHEMANAME1.VIEW1"}]`,
				Valid:  true,
			},
			BaseObjectsAccessed: NullString{
				String: `[{"objectDomain": "Table", "objectName": "DBNAME1.SCHEMANAME1.TABLE2"}]`,
				Valid:  true,
			},
			ObjectsModified: NullString{
				String: `[{"objectDomain": "Table", "objectName": "DBNAME1.SCHEMANAME1.TABLE1"}]`,
				Valid:  true,
			},
			ObjectsModifiedByDdl: NullString{},
			ParentQueryID:        NullString{},
			RootQueryID:          NullString{},
		}),
		stream.NewMaybeErrorValue(UsageQueryResult{
			ExternalId:            "queryId2",
			Query:                 NullString{String: "CREATE TABLE `table2` (amount NUMBER)", Valid: true},
			DatabaseName:          NullString{String: "DBName1", Valid: true},
			SchemaName:            NullString{String: "SchemaName2", Valid: true},
			QueryType:             NullString{String: "CREATE_TABLE", Valid: true},
			SessionID:             NullString{Valid: false},
			User:                  NullString{String: "user2", Valid: true},
			Role:                  NullString{String: "role2", Valid: true},
			Status:                NullString{String: "status1", Valid: true},
			StartTime:             sql.NullTime{Time: startTime, Valid: true},
			EndTime:               sql.NullTime{Time: endTime, Valid: true},
			TotalElapsedTime:      int64(time.Hour.Seconds()),
			BytesScanned:          0,
			BytesWritten:          10,
			BytesWrittenToResult:  0,
			RowsProduced:          sql.NullInt64{},
			RowsInserted:          0,
			RowsUpdated:           0,
			RowsDeleted:           0,
			RowsUnloaded:          0,
			CloudCreditsUsed:      9.5,
			DirectObjectsAccessed: NullString{},
			BaseObjectsAccessed:   NullString{},
			ObjectsModified:       NullString{},
			ObjectsModifiedByDdl: NullString{
				String: `{"objectDomain": "Table", "objectId": 65542, "objectName": "DBNAME1.SCHEMANAME2.TABLE2", "operationType": "CREATE"}`,
				Valid:  true,
			},
			ParentQueryID: NullString{},
			RootQueryID:   NullString{},
		}),
	}))

	syncer := &DataUsageSyncer{
		repoProvider: func(params map[string]string, role string) (dataUsageRepository, error) {
			return repoMock, nil
		},
	}

	//When
	err := syncer.SyncDataUsage(context.Background(), fileCreator, &configParams)

	//Then
	assert.NoError(t, err)
	assert.Len(t, fileCreator.Statements, 2)

	assert.ElementsMatch(t, fileCreator.Statements, []data_usage.Statement{
		{
			ExternalId: "queryId1",
			AccessedDataObjects: []data_usage.UsageDataObjectItem{
				{
					GlobalPermission: data_usage.Read,
					DataObject: data_usage.UsageDataObjectReference{
						FullName: "DBNAME1.SCHEMANAME1.TABLE1",
						Type:     "table",
					},
				},
				{
					GlobalPermission: data_usage.Read,
					DataObject: data_usage.UsageDataObjectReference{
						FullName: "DBNAME1.SCHEMANAME1.VIEW1",
						Type:     "view",
					},
				},
				{
					GlobalPermission: data_usage.Read,
					DataObject: data_usage.UsageDataObjectReference{
						FullName: "DBNAME1.SCHEMANAME1.TABLE2",
						Type:     "table",
					},
				},
				{
					GlobalPermission: data_usage.Write,
					DataObject: data_usage.UsageDataObjectReference{
						FullName: "DBNAME1.SCHEMANAME1.TABLE1",
						Type:     "table",
					},
				},
			},
			User:      "user1",
			Role:      "role1",
			Success:   true,
			Status:    "SUCCESS",
			Query:     "UPDATE table1 SET column1 = 1 WHERE column2 IN (SELECT column3 FROM view1)",
			StartTime: startTime.Unix(),
			EndTime:   endTime.Unix(),
			Bytes:     40,
			Rows:      50,
			Credits:   10.5,
			Error:     "",
		},
		{
			ExternalId: "queryId2",
			AccessedDataObjects: []data_usage.UsageDataObjectItem{
				{
					GlobalPermission: data_usage.Admin,
					DataObject: data_usage.UsageDataObjectReference{
						FullName: "DBNAME1.SCHEMANAME2",
						Type:     "schema",
					},
				},
			},
			User:      "user2",
			Role:      "role2",
			Success:   false,
			Status:    "status1",
			Query:     "CREATE TABLE `table2` (amount NUMBER)",
			StartTime: startTime.Unix(),
			EndTime:   endTime.Unix(),
			Bytes:     0,
			Rows:      0,
			Credits:   9.5,
			Error:     "",
		},
	})
}
