package snowflake

import (
	"context"
	"fmt"
	"github.com/raito-io/cli/base/tag"
	"testing"
	"time"

	"github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestDataSourceSyncer_GetMetaData(t *testing.T) {
	//Given
	syncer := DataSourceSyncer{repoProvider: func(params map[string]string, role string) (dataSourceRepository, error) {
		return nil, nil
	}}

	//When
	result, err := syncer.GetDataSourceMetaData(context.Background())

	//Then
	assert.NoError(t, err)
	assert.Equal(t, "snowflake", result.Type)
	assert.NotEmpty(t, result.DataObjectTypes)
}

func TestDataSourceSyncer_SyncDataSource(t *testing.T) {
	//Given
	configParams := config.ConfigMap{
		Parameters: map[string]string{"key": "value"},
	}

	repoMock := newMockDataSourceRepository(t)
	dataSourceObjectHandlerMock := mocks.NewSimpleDataSourceObjectHandler(t, 1)

	repoMock.EXPECT().Close().Return(nil).Once()
	repoMock.EXPECT().TotalQueryTime().Return(time.Minute).Once()
	repoMock.EXPECT().GetSnowFlakeAccountName().Return("SnowflakeAccountName", nil).Once()
	repoMock.EXPECT().GetWarehouses().Return([]DbEntity{
		{Name: "Warehouse1"},
		{Name: "Warehouse2"},
	}, nil).Once()
	repoMock.EXPECT().GetShares().Return([]DbEntity{
		{Name: "Share1"},
	}, nil).Once()
	repoMock.EXPECT().GetDataBases().Return([]DbEntity{
		{Name: "Database1"}, {Name: "Database2"},
	}, nil).Once()

	repoMock.EXPECT().GetTags(mock.Anything).Return(map[string][]*tag.Tag{}, nil)

	repoMock.EXPECT().GetSchemasInDatabase("Database1", mock.Anything).RunAndReturn(func(s string, handler EntityHandler) error {
		handler(&SchemaEntity{Database: s, Name: "schema1"})
		return nil
	}).Once()

	repoMock.EXPECT().GetSchemasInDatabase("Database2", mock.Anything).RunAndReturn(func(s string, handler EntityHandler) error {
		handler(&SchemaEntity{Database: s, Name: "schema2"})
		return nil
	}).Once()

	repoMock.EXPECT().GetSchemasInDatabase("Share1", mock.Anything).RunAndReturn(func(s string, handler EntityHandler) error {
		handler(&SchemaEntity{Database: s, Name: "schema3"})
		return nil
	}).Once()

	repoMock.EXPECT().GetTablesInDatabase("Database1", "", mock.Anything).RunAndReturn(func(s string, s2 string, handler EntityHandler) error {
		handler(&TableEntity{Database: s, Schema: s2, Name: "Table1"})
		handler(&TableEntity{Database: s, Schema: s2, Name: "Table2"})
		return nil
	}).Once()

	repoMock.EXPECT().GetTablesInDatabase("Database2", "", mock.Anything).RunAndReturn(func(s string, s2 string, handler EntityHandler) error {
		handler(&TableEntity{Database: s, Schema: s2, Name: "Table3"})
		return nil
	}).Once()

	repoMock.EXPECT().GetTablesInDatabase("Share1", "", mock.Anything).RunAndReturn(func(s string, s2 string, handler EntityHandler) error {
		handler(&TableEntity{Database: s, Schema: s2, Name: "Table4"})
		return nil
	}).Once()

	repoMock.EXPECT().GetColumnsInDatabase("Database1", mock.Anything).RunAndReturn(func(s string, handler EntityHandler) error {
		handler(&ColumnEntity{Database: s, Schema: "schema1", Table: "Table1", Name: "IDColumn"})
		return nil
	}).Once()

	repoMock.EXPECT().GetColumnsInDatabase("Database2", mock.Anything).RunAndReturn(func(s string, handler EntityHandler) error {
		return nil
	}).Once()

	repoMock.EXPECT().GetColumnsInDatabase("Share1", mock.Anything).RunAndReturn(func(s string, handler EntityHandler) error {
		return nil
	}).Once()

	repoMock.EXPECT().GetViewsInDatabase("Database1", "", mock.Anything).RunAndReturn(func(s string, s2 string, handler EntityHandler) error {
		handler(&TableEntity{Database: s, Schema: s2, Name: "View1"})
		return nil
	}).Once()

	repoMock.EXPECT().GetViewsInDatabase("Database2", "", mock.Anything).RunAndReturn(func(s string, s2 string, handler EntityHandler) error {
		return nil
	}).Once()

	repoMock.EXPECT().GetViewsInDatabase("Share1", "", mock.Anything).RunAndReturn(func(s string, s2 string, handler EntityHandler) error {
		return nil
	}).Once()

	syncer := createSyncer(repoMock)

	//When
	err := syncer.SyncDataSource(context.Background(), dataSourceObjectHandlerMock, &configParams)

	//Then
	assert.NoError(t, err)
	assert.Len(t, dataSourceObjectHandlerMock.DataObjects, 14)
	assert.Equal(t, "SnowflakeAccountName", dataSourceObjectHandlerMock.DataSourceName)
	assert.Equal(t, "SnowflakeAccountName", dataSourceObjectHandlerMock.DataSourceFullName)
}

func TestDataSourceSyncer_SyncDataSource_ErrorOnSnowflakeAccount(t *testing.T) {
	//Given
	configParams := config.ConfigMap{
		Parameters: map[string]string{"key": "value"},
	}

	repoMock := newMockDataSourceRepository(t)
	dataSourceObjectHandlerMock := mocks.NewSimpleDataSourceObjectHandler(t, 1)

	repoMock.EXPECT().Close().Return(nil).Once()
	repoMock.EXPECT().TotalQueryTime().Return(time.Minute).Once()
	repoMock.EXPECT().GetSnowFlakeAccountName().Return("", fmt.Errorf("boom")).Once()

	syncer := createSyncer(repoMock)

	//When
	err := syncer.SyncDataSource(context.Background(), dataSourceObjectHandlerMock, &configParams)

	//Then
	assert.Error(t, err)
	assert.Len(t, dataSourceObjectHandlerMock.DataObjects, 0)
	assert.Equal(t, "", dataSourceObjectHandlerMock.DataSourceName)
	assert.Equal(t, "", dataSourceObjectHandlerMock.DataSourceFullName)
}

func TestDataSourceSyncer_SyncDataSource_addDbEntitiesToImporter(t *testing.T) {
	//Given
	dataSourceObjectHandlerMock := mocks.NewSimpleDataSourceObjectHandler(t, 1)

	comment := "Comment"

	entities := []DbEntity{{Name: "Object1", Comment: &comment}, {Name: "Object2"}, {Name: "ObjectToFilter"}, {Name: "FilterByFullName"}}
	doType := "doType"
	parent := "DB1.Schema1"
	filter := func(name string, fullName string) bool {
		if name == "ObjectToFilter" || fullName == "external-FilterByFullName" {
			return false
		} else {
			return true
		}
	}

	externalIdGenerator := func(name string) string {
		return "external-" + name
	}

	syncer := createSyncer(nil)

	//When
	returnedEntities, err := syncer.addDbEntitiesToImporter(dataSourceObjectHandlerMock, entities, doType, parent, externalIdGenerator, filter)

	//Then
	assert.NoError(t, err)
	assert.Len(t, returnedEntities, 2)

	assert.Contains(t, dataSourceObjectHandlerMock.DataObjects, data_source.DataObject{
		Name:             "Object1",
		Type:             "doType",
		FullName:         "external-Object1",
		ParentExternalId: parent,
		ExternalId:       "external-Object1",
		Description:      comment,
	})
	assert.Contains(t, dataSourceObjectHandlerMock.DataObjects, data_source.DataObject{
		Name:             "Object2",
		Type:             "doType",
		FullName:         "external-Object2",
		ParentExternalId: parent,
		ExternalId:       "external-Object2",
	})

	assert.Equal(t, []DbEntity{{Name: "Object1", Comment: &comment}, {Name: "Object2"}}, returnedEntities)
}

func TestDataSourceSyncer_SyncDataSource_addDbEntitiesToImporter_ErrorOnAddDataObjects(t *testing.T) {
	//Given
	dataSourceObjectHandlerMock := mocks.NewDataSourceObjectHandler(t)
	dataSourceObjectHandlerMock.EXPECT().AddDataObjects(mock.Anything).Return(fmt.Errorf("boom"))

	entities := []DbEntity{{Name: "Object1"}, {Name: "Object2"}, {Name: "ObjectToFilter"}, {Name: "FilterByFullName"}}
	doType := "doType"
	parent := "DB1.Schema1"
	filter := func(name string, fullName string) bool {
		if name == "ObjectToFilter" || fullName == "external-FilterByFullName" {
			return false
		} else {
			return true
		}
	}

	externalIdGenerator := func(name string) string {
		return "external-" + name
	}

	syncer := createSyncer(nil)

	//When
	returnedEntities, err := syncer.addDbEntitiesToImporter(dataSourceObjectHandlerMock, entities, doType, parent, externalIdGenerator, filter)

	//Then
	assert.Error(t, err)
	assert.Nil(t, returnedEntities)
}

func TestDataSourceSyncer_SyncDataSource_readWarehouses(t *testing.T) {
	//Given
	repoMock := newMockDataSourceRepository(t)
	dataSourceObjectHandlerMock := mocks.NewSimpleDataSourceObjectHandler(t, 1)

	repoMock.EXPECT().GetWarehouses().Return([]DbEntity{
		{Name: "Warehouse1"},
		{Name: "Warehouse2"},
	}, nil).Once()

	syncer := createSyncer(nil)

	//When
	err := syncer.readWarehouses(repoMock, dataSourceObjectHandlerMock)

	//Then
	assert.NoError(t, err)
	assert.Len(t, dataSourceObjectHandlerMock.DataObjects, 2)
	assert.Contains(t, dataSourceObjectHandlerMock.DataObjects, data_source.DataObject{
		Name:       "Warehouse1",
		Type:       "warehouse",
		FullName:   "Warehouse1",
		ExternalId: "Warehouse1",
	})
	assert.Contains(t, dataSourceObjectHandlerMock.DataObjects, data_source.DataObject{
		Name:       "Warehouse2",
		Type:       "warehouse",
		FullName:   "Warehouse2",
		ExternalId: "Warehouse2",
	})
}

func TestDataSourceSyncer_SyncDataSource_readShares(t *testing.T) {
	//Given
	repoMock := newMockDataSourceRepository(t)
	dataSourceObjectHandlerMock := mocks.NewSimpleDataSourceObjectHandler(t, 1)

	excludedDatabases := "ExcludeShare1,ExcludeShare2"

	repoMock.EXPECT().GetShares().Return([]DbEntity{
		{Name: "Share1"}, {Name: "ExcludeShare1"}, {Name: "Share2"}, {Name: "ExcludeShare2"},
	}, nil).Once()

	syncer := createSyncer(nil)

	//When
	shares, shareMap, err := syncer.readShares(repoMock, excludedDatabases, dataSourceObjectHandlerMock)

	//Then
	assert.NoError(t, err)
	assert.Len(t, dataSourceObjectHandlerMock.DataObjects, 2)
	assert.Contains(t, dataSourceObjectHandlerMock.DataObjects, data_source.DataObject{
		Name:       "Share1",
		Type:       "shared-database",
		FullName:   "Share1",
		ExternalId: "Share1",
	})
	assert.Contains(t, dataSourceObjectHandlerMock.DataObjects, data_source.DataObject{
		Name:       "Share2",
		Type:       "shared-database",
		FullName:   "Share2",
		ExternalId: "Share2",
	})

	assert.Equal(t, []DbEntity{{Name: "Share1"}, {Name: "Share2"}}, shares)
	assert.Equal(t, map[string]struct{}{"Share1": {}, "Share2": {}}, shareMap)
}

func TestDataSourceSyncer_SyncDataSource_readDatabases(t *testing.T) {
	//Given
	repoMock := newMockDataSourceRepository(t)
	dataSourceObjectHandlerMock := mocks.NewSimpleDataSourceObjectHandler(t, 1)

	excludedDatabases := "ExcludeDatabase1,ExcludeDatabase2"

	repoMock.EXPECT().GetDataBases().Return([]DbEntity{
		{Name: "DB1"}, {Name: "ExcludeDatabase1"}, {Name: "DB2"}, {Name: "ExcludeDatabase2"},
	}, nil).Once()

	syncer := createSyncer(nil)

	//When
	entities, err := syncer.readDatabases(repoMock, excludedDatabases, dataSourceObjectHandlerMock, map[string]struct{}{})

	//Then
	assert.NoError(t, err)

	assert.Len(t, dataSourceObjectHandlerMock.DataObjects, 2)
	assert.Contains(t, dataSourceObjectHandlerMock.DataObjects, data_source.DataObject{
		Name:       "DB1",
		Type:       "database",
		FullName:   "DB1",
		ExternalId: "DB1",
	})
	assert.Contains(t, dataSourceObjectHandlerMock.DataObjects, data_source.DataObject{
		Name:       "DB2",
		Type:       "database",
		FullName:   "DB2",
		ExternalId: "DB2",
	})

	assert.Equal(t, []DbEntity{{Name: "DB1"}, {Name: "DB2"}}, entities)
}

func TestDataSourceSyncer_SyncDataSource_readSchemaInDatabase(t *testing.T) {
	//Given
	repoMock := newMockDataSourceRepository(t)
	dataSourceObjectHandlerMock := mocks.NewSimpleDataSourceObjectHandler(t, 1)

	databaseName := "DB1"
	excludeSchemas := map[string]struct{}{
		"ExcludeSchema1":     {},
		"DB1.ExcludeSchema2": {},
	}

	repoMock.EXPECT().GetSchemasInDatabase(databaseName, mock.Anything).RunAndReturn(func(s string, handler EntityHandler) error {
		handler(&SchemaEntity{Database: s, Name: "Schema1"})
		handler(&SchemaEntity{Database: s, Name: "ExcludeSchema1"})
		handler(&SchemaEntity{Database: s, Name: "ExcludeSchema2"})
		handler(&SchemaEntity{Database: s, Name: "Schema2"})
		return nil
	}).Once()

	syncer := createSyncer(nil)

	//When
	err := syncer.readSchemasInDatabase(repoMock, databaseName, excludeSchemas, dataSourceObjectHandlerMock, "prefix-", map[string][]*tag.Tag{})

	//Then
	assert.NoError(t, err)
	assert.Len(t, dataSourceObjectHandlerMock.DataObjects, 2)
	assert.Contains(t, dataSourceObjectHandlerMock.DataObjects, data_source.DataObject{
		Name:             "Schema1",
		Type:             "prefix-schema",
		FullName:         "DB1.Schema1",
		ExternalId:       "DB1.Schema1",
		ParentExternalId: "DB1",
	})
	assert.Contains(t, dataSourceObjectHandlerMock.DataObjects, data_source.DataObject{
		Name:             "Schema2",
		Type:             "prefix-schema",
		FullName:         "DB1.Schema2",
		ExternalId:       "DB1.Schema2",
		ParentExternalId: "DB1",
	})
}

func createSyncer(repo dataSourceRepository) *DataSourceSyncer {
	return &DataSourceSyncer{
		repoProvider: func(params map[string]string, role string) (dataSourceRepository, error) {
			return repo, nil
		},
	}
}
