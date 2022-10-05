//go:build integration

package it

import (
	"context"
	"testing"

	"github.com/raito-io/cli/base/wrappers/mocks"
	"github.com/stretchr/testify/suite"

	"github.com/raito-io/cli-plugin-snowflake/snowflake"
)

type DataUsageTestSuite struct {
	SnowflakeTestSuite
}

func TestDataUsageTestSuite(t *testing.T) {
	ts := DataUsageTestSuite{}
	suite.Run(t, &ts)
}

func (s *DataUsageTestSuite) TestDataUsage() {
	//Given
	fileCreator := mocks.NewSimpleDataUsageStatementHandler(s.T())
	dataUsage := snowflake.NewDataUsageSyncer()

	//When
	err := dataUsage.SyncDataUsage(context.Background(), fileCreator, s.getConfig())

	//Then
	s.NoError(err)
	s.True(len(fileCreator.Statements) > 0)
}
