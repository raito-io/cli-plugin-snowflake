package snowflake

import (
	"context"
	"testing"
	"time"

	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestDataUsageSyncer_SyncDataUsage(t *testing.T) {
	//Given
	configParams := config.ConfigMap{
		Parameters: map[string]interface{}{"key": "value"},
	}

	repoMock := newMockDataUsageRepository(t)
	fileCreator := mocks.NewSimpleDataUsageStatementHandler(t)

	minTime := time.Now().AddDate(0, 0, -14).Format(time.RFC3339)
	maxtime := time.Now().Format(time.RFC3339)
	numRows := 15000

	repoMock.EXPECT().Close().Return(nil)
	repoMock.EXPECT().TotalQueryTime().Return(time.Minute)
	repoMock.EXPECT().BatchingInformation(mock.AnythingOfType("*time.Time"), mock.AnythingOfType("string")).Return(&minTime, &maxtime, numRows, nil)
	repoMock.EXPECT().CheckAccessHistoryAvailability(mock.AnythingOfType("string")).Return(false, nil)
	repoMock.EXPECT().DataUsage(mock.AnythingOfType("[]string"), mock.AnythingOfType("int"), 0, mock.AnythingOfType("string"),
		mock.AnythingOfType("*string"), mock.AnythingOfType("*string"), false).Return([]QueryDbEntities{
		{
			Query:        "query1",
			User:         "user1",
			Status:       "status1",
			DatabaseName: NullString{String: "DBName1"},
			SchemaName:   NullString{String: "SchemaName1"},
		},
		{
			Query:        "query2",
			User:         "user2",
			Status:       "status2",
			DatabaseName: NullString{String: "DBName2"},
			SchemaName:   NullString{String: "SchemaName1"},
		},
	}, nil)
	repoMock.EXPECT().DataUsage(mock.AnythingOfType("[]string"), mock.AnythingOfType("int"), 10000, mock.AnythingOfType("string"),
		mock.AnythingOfType("*string"), mock.AnythingOfType("*string"), false).Return([]QueryDbEntities{
		{
			Query:  "query3",
			User:   "user3",
			Status: "status3",
		},
	}, nil)

	syncer := &DataUsageSyncer{
		repoProvider: func(params map[string]interface{}, role string) (dataUsageRepository, error) {
			return repoMock, nil
		},
	}

	//When
	err := syncer.SyncDataUsage(context.Background(), fileCreator, &configParams)

	//Then
	assert.NoError(t, err)
	assert.Len(t, fileCreator.Statements, 3)
}
