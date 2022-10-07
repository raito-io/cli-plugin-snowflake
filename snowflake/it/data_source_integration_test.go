//go:build integration

package it

import (
	"context"
	"testing"

	"github.com/raito-io/cli/base/wrappers/mocks"
	"github.com/stretchr/testify/suite"

	"github.com/raito-io/cli-plugin-snowflake/snowflake"
)

type DataSourceTestSuite struct {
	SnowflakeTestSuite
}

func TestDataSourceTestSuite(t *testing.T) {
	ts := DataSourceTestSuite{}
	suite.Run(t, &ts)
}

func (s *DataSourceTestSuite) TestDataSourceSync_SyncDataSource() {
	//Given
	dataSourceObjectHandler := mocks.NewSimpleDataSourceObjectHandler(s.T(), 1)
	dataSourceSyncer := snowflake.NewDataSourceSyncer()

	config := s.getConfig()
	config.Parameters[snowflake.SfExcludedDatabases] = "UTIL_DB,SNOWFLAKE_SAMPLE_DATA"

	//When
	err := dataSourceSyncer.SyncDataSource(context.Background(), dataSourceObjectHandler, config)

	//Then
	s.NoError(err)
	s.True(len(dataSourceObjectHandler.DataObjects) > 0)
	s.Equal("KQ54735", dataSourceObjectHandler.DataSourceName)
	s.Equal("KQ54735", dataSourceObjectHandler.DataSourceFullName)
}
