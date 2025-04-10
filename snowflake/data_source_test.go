package snowflake

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/tag"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers/mocks"
	"github.com/raito-io/golang-set/set"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestDataSourceSyncer_GetMetaData(t *testing.T) {
	//Given
	repo := newMockDataSourceRepository(t)
	repo.EXPECT().GetSnowFlakeAccountName(mock.Anything).Return("SnowflakeAccountName", nil).Once()

	syncer := DataSourceSyncer{repoProvider: func(params map[string]string, role string) (dataSourceRepository, error) {
		return repo, nil
	}}

	syncer.repo = repo

	//When
	result, err := syncer.GetDataSourceMetaData(context.Background(), &config.ConfigMap{})

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
	repoMock.EXPECT().GetInboundShares().Return([]DbEntity{
		{Name: "Share1"},
	}, nil).Once()
	repoMock.EXPECT().GetDatabases().Return([]DbEntity{
		{Name: "Database1"}, {Name: "Database2"},
	}, nil).Once()

	repoMock.EXPECT().GetTagsByDomain("WAREHOUSE").Return(map[string][]*tag.Tag{}, nil).Once()
	repoMock.EXPECT().GetTagsByDomain("INTEGRATION").Return(map[string][]*tag.Tag{}, nil).Once()
	repoMock.EXPECT().GetTagsLinkedToDatabaseName(mock.Anything).Return(map[string][]*tag.Tag{}, nil).Times(3)

	repoMock.EXPECT().GetIntegrations().Return([]DbEntity{}, nil).Once()

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
		handler(&TableEntity{Database: s, Schema: s2, Name: "Table1", TableType: "BASE TABLE"})
		handler(&TableEntity{Database: s, Schema: s2, Name: "Table2", TableType: "BASE TABLE"})
		handler(&TableEntity{Database: s, Schema: s2, Name: "View1", TableType: "VIEW"})
		return nil
	}).Once()

	repoMock.EXPECT().GetFunctionsInDatabase("Database1", mock.Anything).RunAndReturn(func(s string, handler EntityHandler) error {
		handler(&FunctionEntity{Database: s, Schema: "schema1", Name: "Decrypt", ArgumentSignature: "(VAL VARCHAR)"})
		return nil
	}).Once()

	repoMock.EXPECT().GetFunctionsInDatabase("Database2", mock.Anything).RunAndReturn(func(s string, handler EntityHandler) error {
		return nil
	}).Once()

	repoMock.EXPECT().GetProceduresInDatabase("Database1", mock.Anything).RunAndReturn(func(s string, handler EntityHandler) error {
		return nil
	}).Once()

	repoMock.EXPECT().GetProceduresInDatabase("Database2", mock.Anything).RunAndReturn(func(s string, handler EntityHandler) error {
		handler(&ProcedureEntity{Database: s, Schema: "schema2", Name: "myProc", ArgumentSignature: "(VAL VARCHAR)"})
		return nil
	}).Once()

	repoMock.EXPECT().GetTablesInDatabase("Database2", "", mock.Anything).RunAndReturn(func(s string, s2 string, handler EntityHandler) error {
		handler(&TableEntity{Database: s, Schema: s2, Name: "Table3", TableType: "BASE TABLE"})
		return nil
	}).Once()

	repoMock.EXPECT().GetTablesInDatabase("Share1", "", mock.Anything).RunAndReturn(func(s string, s2 string, handler EntityHandler) error {
		handler(&TableEntity{Database: s, Schema: s2, Name: "Table4", TableType: "BASE TABLE"})
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

	syncer := createSyncer(repoMock)

	//When
	err := syncer.SyncDataSource(context.Background(), dataSourceObjectHandlerMock, &data_source.DataSourceSyncConfig{
		ConfigMap: &configParams,
	})

	//Then
	assert.NoError(t, err)
	assert.Len(t, dataSourceObjectHandlerMock.DataObjects, 16)
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
	err := syncer.SyncDataSource(context.Background(), dataSourceObjectHandlerMock, &data_source.DataSourceSyncConfig{
		ConfigMap: &configParams,
	})

	//Then
	assert.Error(t, err)
	assert.Len(t, dataSourceObjectHandlerMock.DataObjects, 0)
	assert.Equal(t, "", dataSourceObjectHandlerMock.DataSourceName)
	assert.Equal(t, "", dataSourceObjectHandlerMock.DataSourceFullName)
}

func TestDataSourceSyncer_SyncDataSource_addDbEntitiesToImporter(t *testing.T) {
	//Given
	dataSourceObjectHandlerMock := mocks.NewSimpleDataSourceObjectHandler(t, 1)
	repoMock := newMockDataSourceRepository(t)

	comment := "Comment"

	entities := []DbEntity{{Name: "Object1", Comment: &comment}, {Name: "Object2"}, {Name: "ObjectToFilter"}, {Name: "FilterByFullName"}}
	doType := "doType"
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

	repoMock.EXPECT().GetTagsLinkedToDatabaseName("Object1").Once().Return(nil, nil)
	repoMock.EXPECT().GetTagsLinkedToDatabaseName("Object2").Once().Return(nil, nil)

	syncer := createSyncer(nil)
	syncer.dataSourceHandler = dataSourceObjectHandlerMock

	//When
	returnedEntities, err := syncer.addTopLevelEntitiesToImporter(entities, doType, true, repoMock.GetTagsLinkedToDatabaseName, externalIdGenerator, filter)

	//Then
	assert.NoError(t, err)
	assert.Len(t, returnedEntities, 2)

	assert.Contains(t, dataSourceObjectHandlerMock.DataObjects, data_source.DataObject{
		Name:        "Object1",
		Type:        "doType",
		FullName:    "external-Object1",
		ExternalId:  "external-Object1",
		Description: comment,
	})
	assert.Contains(t, dataSourceObjectHandlerMock.DataObjects, data_source.DataObject{
		Name:       "Object2",
		Type:       "doType",
		FullName:   "external-Object2",
		ExternalId: "external-Object2",
	})

	assert.Equal(t, []ExtendedDbEntity{{Entity: DbEntity{Name: "Object1", Comment: &comment}}, {Entity: DbEntity{Name: "Object2"}}}, returnedEntities)
}

func TestDataSourceSyncer_SyncDataSource_addDbEntitiesToImporter_ErrorOnAddDataObjects(t *testing.T) {
	//Given
	repoMock := newMockDataSourceRepository(t)
	dataSourceObjectHandlerMock := mocks.NewDataSourceObjectHandler(t)
	dataSourceObjectHandlerMock.EXPECT().AddDataObjects(mock.Anything).Return(fmt.Errorf("boom"))

	entities := []DbEntity{{Name: "Object1"}, {Name: "Object2"}, {Name: "ObjectToFilter"}, {Name: "FilterByFullName"}}
	doType := "doType"
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
	syncer.dataSourceHandler = dataSourceObjectHandlerMock

	//When
	returnedEntities, err := syncer.addTopLevelEntitiesToImporter(entities, doType, false, repoMock.GetTagsLinkedToDatabaseName, externalIdGenerator, filter)

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

	repoMock.EXPECT().GetTagsByDomain("WAREHOUSE").Return(map[string][]*tag.Tag{
		"Warehouse1": {
			{Key: "tag1", Value: "value1"},
		},
	}, nil).Once()

	syncer := createSyncer(nil)
	syncer.repo = repoMock
	syncer.dataSourceHandler = dataSourceObjectHandlerMock

	//When
	err := syncer.readWarehouses(true)

	//Then
	assert.NoError(t, err)
	assert.Len(t, dataSourceObjectHandlerMock.DataObjects, 2)
	assert.Contains(t, dataSourceObjectHandlerMock.DataObjects, data_source.DataObject{
		Name:       "Warehouse1",
		Type:       "warehouse",
		FullName:   "Warehouse1",
		ExternalId: "Warehouse1",
		Tags: []*tag.Tag{
			{Key: "tag1", Value: "value1"},
		},
	})
	assert.Contains(t, dataSourceObjectHandlerMock.DataObjects, data_source.DataObject{
		Name:       "Warehouse2",
		Type:       "warehouse",
		FullName:   "Warehouse2",
		ExternalId: "Warehouse2",
	})
}

func TestDataSourceSyncer_SyncDataSource_readIntegrations(t *testing.T) {
	//Given
	repoMock := newMockDataSourceRepository(t)
	dataSourceObjectHandlerMock := mocks.NewSimpleDataSourceObjectHandler(t, 1)

	repoMock.EXPECT().GetIntegrations().Return([]DbEntity{
		{Name: "Integration1"},
	}, nil).Once()

	repoMock.EXPECT().GetTagsByDomain("INTEGRATION").Return(map[string][]*tag.Tag{
		"Integration1": {
			{Key: "tag1", Value: "value1"},
		},
	}, nil).Once()

	syncer := createSyncer(nil)
	syncer.repo = repoMock
	syncer.dataSourceHandler = dataSourceObjectHandlerMock

	//When
	err := syncer.readIntegrations(true)

	//Then
	assert.NoError(t, err)
	assert.Len(t, dataSourceObjectHandlerMock.DataObjects, 1)
	assert.Contains(t, dataSourceObjectHandlerMock.DataObjects, data_source.DataObject{
		Name:       "Integration1",
		Type:       "integration",
		FullName:   "Integration1",
		ExternalId: "Integration1",
		Tags: []*tag.Tag{
			{Key: "tag1", Value: "value1"},
		},
	})
}

func TestDataSourceSyncer_SyncDataSource_readShares(t *testing.T) {
	//Given
	repoMock := newMockDataSourceRepository(t)
	dataSourceObjectHandlerMock := mocks.NewSimpleDataSourceObjectHandler(t, 1)

	excludedDatabases := set.NewSet[string]("ExcludeShare1", "ExcludeShare2")

	repoMock.EXPECT().GetInboundShares().Return([]DbEntity{
		{Name: "Share1"}, {Name: "ExcludeShare1"}, {Name: "Share2"}, {Name: "ExcludeShare2"},
	}, nil).Once()

	syncer := createSyncer(nil)
	syncer.repo = repoMock
	syncer.dataSourceHandler = dataSourceObjectHandlerMock

	//When
	shares, shareMap, err := syncer.readShares(excludedDatabases, false)

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

	assert.Equal(t, []ExtendedDbEntity{{Entity: DbEntity{Name: "Share1"}}, {Entity: DbEntity{Name: "Share2"}}}, shares)
	assert.ElementsMatch(t, []string{"Share1", "Share2"}, shareMap.Slice())
}

func TestDataSourceSyncer_SyncDataSource_readDatabases(t *testing.T) {
	//Given
	repoMock := newMockDataSourceRepository(t)
	dataSourceObjectHandlerMock := mocks.NewSimpleDataSourceObjectHandler(t, 1)

	excludedDatabases := set.NewSet[string]("ExcludeDatabase1", "ExcludeDatabase2")

	repoMock.EXPECT().GetDatabases().Return([]DbEntity{
		{Name: "DB1"}, {Name: "ExcludeDatabase1"}, {Name: "DB2"}, {Name: "ExcludeDatabase2"},
	}, nil).Once()
	repoMock.EXPECT().GetTagsLinkedToDatabaseName("DB1").Return(map[string][]*tag.Tag{
		"DB1": {
			{Key: "key1", Value: "val1"},
		}, "DB1.Other": {
			{Key: "key2", Value: "val2"},
		},
	}, nil).Once()
	repoMock.EXPECT().GetTagsLinkedToDatabaseName("DB2").Return(map[string][]*tag.Tag{
		"DB2.Another": {
			{Key: "key2", Value: "val2"},
		},
	}, nil).Once()

	syncer := createSyncer(nil)
	syncer.repo = repoMock
	syncer.dataSourceHandler = dataSourceObjectHandlerMock

	//When
	entities, err := syncer.readDatabases(excludedDatabases, map[string]struct{}{}, true)

	//Then
	assert.NoError(t, err)

	assert.Len(t, dataSourceObjectHandlerMock.DataObjects, 2)
	assert.Contains(t, dataSourceObjectHandlerMock.DataObjects, data_source.DataObject{
		Name:       "DB1",
		Type:       "database",
		FullName:   "DB1",
		ExternalId: "DB1",
		Tags: []*tag.Tag{
			{Key: "key1", Value: "val1"},
		},
	})
	assert.Contains(t, dataSourceObjectHandlerMock.DataObjects, data_source.DataObject{
		Name:       "DB2",
		Type:       "database",
		FullName:   "DB2",
		ExternalId: "DB2",
	})

	assert.Equal(t, []ExtendedDbEntity{
		{
			Entity: DbEntity{Name: "DB1"},
			LinkedTags: map[string][]*tag.Tag{
				"DB1": {
					{Key: "key1", Value: "val1"},
				}, "DB1.Other": {
					{Key: "key2", Value: "val2"},
				},
			}},
		{
			Entity: DbEntity{Name: "DB2"},
			LinkedTags: map[string][]*tag.Tag{
				"DB2.Another": {
					{Key: "key2", Value: "val2"},
				},
			}}}, entities)
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
	syncer.repo = repoMock
	syncer.dataSourceHandler = dataSourceObjectHandlerMock
	syncer.schemaExcludes = excludeSchemas

	//When
	err := syncer.readSchemasInDatabase(databaseName, "prefix-", map[string][]*tag.Tag{})

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

func TestDataSourceSyncer_SyncDataSource_partial(t *testing.T) {
	//Given
	repoMock := newMockDataSourceRepository(t)
	dataSourceObjectHandlerMock := mocks.NewSimpleDataSourceObjectHandler(t, 1)

	repoMock.EXPECT().Close().Return(nil).Once()
	repoMock.EXPECT().TotalQueryTime().Return(time.Minute).Once()
	repoMock.EXPECT().GetSnowFlakeAccountName().Return("SnowflakeAccountName", nil).Once()
	repoMock.EXPECT().GetWarehouses().Return([]DbEntity{
		{Name: "Warehouse1"},
		{Name: "Warehouse2"},
	}, nil).Once()
	repoMock.EXPECT().GetIntegrations().Return([]DbEntity{}, nil).Once()
	repoMock.EXPECT().GetInboundShares().Return([]DbEntity{
		{Name: "Share1"},
	}, nil).Once()
	repoMock.EXPECT().GetDatabases().Return([]DbEntity{
		{Name: "Database1"}, {Name: "Database2"},
	}, nil).Once()

	repoMock.EXPECT().GetTagsByDomain("WAREHOUSE").Return(nil, nil).Once()
	repoMock.EXPECT().GetTagsByDomain("INTEGRATION").Return(nil, nil).Once()
	repoMock.EXPECT().GetTagsLinkedToDatabaseName(mock.Anything).Return(map[string][]*tag.Tag{}, nil).Once()

	repoMock.EXPECT().GetSchemasInDatabase("Database1", mock.Anything).RunAndReturn(func(s string, handler EntityHandler) error {
		handler(&SchemaEntity{Database: s, Name: "schema1"})
		handler(&SchemaEntity{Database: s, Name: "schema2"})
		return nil
	}).Once()

	repoMock.EXPECT().GetTablesInDatabase("Database1", "", mock.Anything).RunAndReturn(func(s string, s2 string, handler EntityHandler) error {
		handler(&TableEntity{Database: s, Schema: "schema1", Name: "Table1", TableType: "BASE TABLE"})
		handler(&TableEntity{Database: s, Schema: "schema1", Name: "Table2", TableType: "BASE TABLE"})
		handler(&TableEntity{Database: s, Schema: "schema1", Name: "View1", TableType: "VIEW"})
		return nil
	}).Once()

	repoMock.EXPECT().GetFunctionsInDatabase("Database1", mock.Anything).RunAndReturn(func(s string, handler EntityHandler) error {
		handler(&FunctionEntity{Database: s, Schema: "schema1", Name: "Decrypt", ArgumentSignature: "(VAL VARCHAR)"})
		return nil
	}).Once()

	repoMock.EXPECT().GetProceduresInDatabase("Database1", mock.Anything).RunAndReturn(func(s string, handler EntityHandler) error {
		return nil
	}).Once()

	repoMock.EXPECT().GetColumnsInDatabase("Database1", mock.Anything).RunAndReturn(func(s string, handler EntityHandler) error {
		handler(&ColumnEntity{Database: s, Schema: "schema1", Table: "Table1", Name: "IDColumn"})
		handler(&ColumnEntity{Database: s, Schema: "schema1", Table: "Table2", Name: "AnotherColumn"})
		handler(&ColumnEntity{Database: s, Schema: "schema2", Table: "View1", Name: "ViewColumn"})
		return nil
	}).Once()

	syncer := createSyncer(repoMock)

	//When
	err := syncer.SyncDataSource(context.Background(), dataSourceObjectHandlerMock, &data_source.DataSourceSyncConfig{
		ConfigMap:          &config.ConfigMap{Parameters: map[string]string{"key": "value"}},
		DataObjectParent:   "Database1.schema1",
		DataObjectExcludes: []string{"Table2", "View1"},
	})

	//Then
	assert.NoError(t, err)
	assert.Len(t, dataSourceObjectHandlerMock.DataObjects, 3)
	assert.Equal(t, `Database1.schema1."Decrypt"(VARCHAR)`, dataSourceObjectHandlerMock.DataObjects[0].FullName)
	assert.Equal(t, "Database1.schema1.Table1", dataSourceObjectHandlerMock.DataObjects[1].FullName)
	assert.Equal(t, "Database1.schema1.Table1.IDColumn", dataSourceObjectHandlerMock.DataObjects[2].FullName)
}

func createSyncer(repo dataSourceRepository) *DataSourceSyncer {
	return &DataSourceSyncer{
		repoProvider: func(params map[string]string, role string) (dataSourceRepository, error) {
			return repo, nil
		},
	}
}
