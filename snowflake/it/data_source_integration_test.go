//go:build integration

package it

import (
	"context"
	"strings"
	"testing"

	"github.com/raito-io/cli/base/data_source"
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
	config.Parameters[snowflake.SfExcludedDatabases] = "SNOWFLAKE,SNOWFLAKE_SAMPLE_DATA"
	config.Parameters[snowflake.SfRole] = "ACCOUNTADMIN"

	//When
	err := dataSourceSyncer.SyncDataSource(context.Background(), dataSourceObjectHandler, &data_source.DataSourceSyncConfig{ConfigMap: config})

	//Then
	sourceName := strings.ToUpper(strings.Split(sfAccount, ".")[0])

	s.NoError(err)
	s.Len(dataSourceObjectHandler.DataObjects, 16)

	warehouses := getByType(dataSourceObjectHandler.DataObjects, "warehouse")
	s.Len(warehouses, 1)
	s.Contains(warehouses, data_source.DataObject{
		ExternalId:       snowflakeWarehouse,
		Name:             snowflakeWarehouse,
		FullName:         snowflakeWarehouse,
		Type:             "warehouse",
		Description:      "",
		ParentExternalId: "",
		Tags:             nil,
	})

	sharedDatabases := getByType(dataSourceObjectHandler.DataObjects, "shared-database")
	s.Empty(sharedDatabases)

	databases := getByType(dataSourceObjectHandler.DataObjects, "database")
	s.Len(databases, 1)
	s.Contains(databases, data_source.DataObject{
		ExternalId:       "SNOWFLAKE_INTEGRATION_TEST",
		Name:             "SNOWFLAKE_INTEGRATION_TEST",
		FullName:         "SNOWFLAKE_INTEGRATION_TEST",
		Type:             "database",
		Description:      "Database created for integration testing",
		ParentExternalId: "",
		Tags:             nil,
	})

	schemas := getByType(dataSourceObjectHandler.DataObjects, "schema")
	s.Len(schemas, 2)
	s.Contains(schemas, data_source.DataObject{
		ExternalId:       "SNOWFLAKE_INTEGRATION_TEST.ORDERING",
		Name:             "ORDERING",
		FullName:         "SNOWFLAKE_INTEGRATION_TEST.ORDERING",
		Type:             "schema",
		Description:      "",
		ParentExternalId: "SNOWFLAKE_INTEGRATION_TEST",
		Tags:             nil,
	})

	Views := getByType(dataSourceObjectHandler.DataObjects, "view")
	s.Len(Views, 1)
	s.Contains(Views, data_source.DataObject{
		ExternalId:       "SNOWFLAKE_INTEGRATION_TEST.ORDERING.ORDER_VIEW",
		Name:             "ORDER_VIEW",
		FullName:         "SNOWFLAKE_INTEGRATION_TEST.ORDERING.ORDER_VIEW",
		Type:             "view",
		Description:      "",
		ParentExternalId: "SNOWFLAKE_INTEGRATION_TEST.ORDERING",
	})

	tables := getByType(dataSourceObjectHandler.DataObjects, "table")
	s.Len(tables, 1)
	s.Contains(tables, data_source.DataObject{
		ExternalId:       "SNOWFLAKE_INTEGRATION_TEST.ORDERING.ORDERS",
		Name:             "ORDERS",
		FullName:         "SNOWFLAKE_INTEGRATION_TEST.ORDERING.ORDERS",
		Type:             "table",
		Description:      "",
		ParentExternalId: "SNOWFLAKE_INTEGRATION_TEST.ORDERING",
		Tags:             nil,
	})

	column := getByType(dataSourceObjectHandler.DataObjects, "column")
	s.Len(column, 10)
	s.Contains(column, data_source.DataObject{
		ExternalId:       "SNOWFLAKE_INTEGRATION_TEST.ORDERING.ORDERS.ORDERKEY",
		Name:             "ORDERKEY",
		FullName:         "SNOWFLAKE_INTEGRATION_TEST.ORDERING.ORDERS.ORDERKEY",
		Type:             "column",
		Description:      "",
		ParentExternalId: "SNOWFLAKE_INTEGRATION_TEST.ORDERING.ORDERS",
		Tags:             nil,
	})

	s.True(len(dataSourceObjectHandler.DataObjects) > 0)
	s.Equal(sourceName, dataSourceObjectHandler.DataSourceName)
	s.Equal(sourceName, dataSourceObjectHandler.DataSourceFullName)
}

func getByType(dataObjects []data_source.DataObject, dataObjectType string) []data_source.DataObject {
	result := make([]data_source.DataObject, 0)

	for _, do := range dataObjects {
		if do.Type == dataObjectType {
			result = append(result, do)
		}
	}

	return result
}
