package main

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/blockloop/scan"
	dsb "github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/common/api"
	ds "github.com/raito-io/cli/common/api/data_source"
)

type DataSourceSyncer struct {
}

func (s *DataSourceSyncer) SyncDataSource(config *ds.DataSourceSyncConfig) ds.DataSourceSyncResult {
	logger.Debug("Start syncing data source meta data for snowflake")
	fileCreator, err := dsb.NewDataSourceFileCreator(config)

	if err != nil {
		return ds.DataSourceSyncResult{Error: api.ToErrorResult(err)}
	}
	defer fileCreator.Close()

	start := time.Now()

	conn, err := ConnectToSnowflake(config.Parameters, "")
	if err != nil {
		return ds.DataSourceSyncResult{Error: api.ToErrorResult(err)}
	}
	defer conn.Close()

	excludedDatabases := ""
	if v, ok := config.Parameters[SfExcludedDatabases]; ok && v != nil {
		excludedDatabases = v.(string)
	}
	excludedSchemas := ""

	if v, ok := config.Parameters[SfExcludedSchemas]; ok && v != nil {
		excludedSchemas = v.(string)
	}

	databases, err := readDatabases(fileCreator, conn, excludedDatabases)
	if err != nil {
		return ds.DataSourceSyncResult{Error: api.ToErrorResult(err)}
	}

	for _, database := range databases {
		schemas, err := readSchemas(fileCreator, conn, database.Name, excludedSchemas)
		if err != nil {
			return ds.DataSourceSyncResult{Error: api.ToErrorResult(fmt.Errorf("error while syncing schemas for database %q between Snowflake and Raito: %s", database.Name, err.Error()))}
		}

		for _, schema := range schemas {
			tables, err := readTables(fileCreator, conn, database.Name+"."+schema.Name)
			if err != nil {
				return ds.DataSourceSyncResult{Error: api.ToErrorResult(fmt.Errorf("error while syncing tables for schema %q between Snowflake and Raito: %s", schema.Name, err.Error()))}
			}

			for _, table := range tables {
				_, err := readColumns(fileCreator, conn, database.Name+"."+schema.Name+"."+table.Name)

				if err != nil {
					return ds.DataSourceSyncResult{Error: api.ToErrorResult(fmt.Errorf("error while syncing columns for table %q between Snowflake and Raito: %s", table.Name, err.Error()))}
				}
			}
		}
	}

	sec := time.Since(start).Round(time.Millisecond)

	logger.Info(fmt.Sprintf("Fetched %d data objects from Snowflake in %s", fileCreator.GetDataObjectCount(), sec))

	return ds.DataSourceSyncResult{}
}

func readDbEntities(conn *sql.DB, query string) ([]dbEntity, error) {
	rows, err := conn.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error while querying Snowflake: %s", err.Error())
	}
	var dbs []dbEntity
	err = scan.Rows(&dbs, rows)

	if err != nil {
		return nil, fmt.Errorf("error while querying Snowflake: %s", err.Error())
	}
	err = CheckSFLimitExceeded(query, len(dbs))

	if err != nil {
		return nil, fmt.Errorf("error while querying Snowflake: %s", err.Error())
	}

	return dbs, nil
}

func addDbEntitiesToImporter(fileCreator dsb.DataSourceFileCreator, conn *sql.DB, doType string, parent string, query string, externalIdGenerator func(name string) string, filter func(name, fullName string) bool) ([]dbEntity, error) {
	dbs, err := readDbEntities(conn, query)

	if err != nil {
		return nil, err
	}

	dataObjects := make([]dsb.DataObject, 0, 20)

	for _, db := range dbs {
		logger.Debug(fmt.Sprintf("Handling data object (type %s) %q", doType, db.Name))

		fullName := externalIdGenerator(db.Name)
		if filter(db.Name, fullName) {
			do := dsb.DataObject{
				ExternalId:       fullName,
				Name:             db.Name,
				FullName:         fullName,
				Type:             doType,
				Description:      "",
				ParentExternalId: parent,
			}
			dataObjects = append(dataObjects, do)
		}
	}

	err = fileCreator.AddDataObjects(dataObjects)
	if err != nil {
		return nil, err
	}

	return dbs, nil
}

func readDatabases(fileCreator dsb.DataSourceFileCreator, conn *sql.DB, excludedDatabases string) ([]dbEntity, error) {
	excludes := make(map[string]struct{})

	if excludedDatabases != "" {
		for _, e := range strings.Split(excludedDatabases, ",") {
			excludes[e] = struct{}{}
		}
	}

	return addDbEntitiesToImporter(fileCreator, conn, "database", "", "SHOW DATABASES IN ACCOUNT",
		func(name string) string { return name },
		func(name, fullName string) bool {
			_, f := excludes[fullName]
			return !f
		})
}

func readSchemas(fileCreator dsb.DataSourceFileCreator, conn *sql.DB, dbName string, excludedSchemas string) ([]dbEntity, error) {
	excludes := make(map[string]struct{})

	if excludedSchemas != "" {
		for _, e := range strings.Split(excludedSchemas, ",") {
			excludes[e] = struct{}{}
		}
	}

	return addDbEntitiesToImporter(fileCreator, conn, "schema", dbName, "SHOW SCHEMAS IN DATABASE "+dbName,
		func(name string) string { return dbName + "." + name },
		func(name, fullName string) bool {
			_, f := excludes[fullName]
			if f {
				return !f
			}
			_, f = excludes[name]
			return !f
		})
}

func readTables(fileCreator dsb.DataSourceFileCreator, conn *sql.DB, schemaFullName string) ([]dbEntity, error) {
	return addDbEntitiesToImporter(fileCreator, conn, "table", schemaFullName, "SHOW TABLES IN SCHEMA "+schemaFullName,
		func(name string) string { return schemaFullName + "." + name },
		func(name, fullName string) bool { return true })
}

func readColumns(fileCreator dsb.DataSourceFileCreator, conn *sql.DB, tableFullName string) ([]dbEntity, error) {
	_, err := readDbEntities(conn, "SHOW COLUMNS IN TABLE "+tableFullName)
	if err != nil {
		return nil, err
	}

	return addDbEntitiesToImporter(fileCreator, conn, "column", tableFullName, "select \"column_name\" as \"name\" from table(result_scan(LAST_QUERY_ID()))",
		func(name string) string { return tableFullName + "." + name },
		func(name, fullName string) bool { return true })
}

type dbEntity struct {
	Name string `db:"name"`
}

func (s *DataSourceSyncer) GetMetaData() ds.MetaData {
	logger.Debug("Returning meta data for Snowflake")
	return ds.MetaData{
		SupportedFeatures: []string{ds.RowFiltering, ds.ColumnMasking},
		DataObjectTypes: []ds.DataObjectType{
			// TODO add data source
			{
				Name: ds.Datasource,
				Permissions: []ds.DataObjectTypePermission{
					{
						Permission: "APPLY MASKING POLICY",
					},
					{
						Permission: "APPLY ROW ACCESS POLICY",
					},
					{
						Permission: "APPLY SESSION POLICY",
					},
					{
						Permission: "APPLY TAG",
					},
					{
						Permission: "ATTACH POLICY",
					},
					{
						Permission: "CREATE ACCOUNT",
					},
					{
						Permission: "CREATE ROLE",
					},
					{
						Permission: "CREATE USER",
					},
					{
						Permission: "MANAGE GRANTS",
					},
					{
						Permission: "CREATE DATA EXCHANGE LISTING",
					},
					{
						Permission: "CREATE INTEGRATION",
					},
					{
						Permission: "CREATE NETWORK POLICY",
					},
					{
						Permission: "CREATE SHARE",
					},
					{
						Permission: "CREATE WAREHOUSE",
					},
					{
						Permission: "EXECUTE MANAGED TASK",
					},
					{
						Permission: "EXECUTE TASK",
					},
					{
						Permission: "IMPORT SHARE",
					},
					{
						Permission: "MONITOR EXECUTION",
					},
					{
						Permission: "MONITOR USAGE",
					},
					{
						Permission: "OVERRIDE SHARE RESTRICTIONS",
					},
				},
				Children: []string{ds.Database, "warehouse"},
			},
			{
				Name: "warehouse",
				Permissions: []ds.DataObjectTypePermission{
					{
						Permission: "MODIFY",
					},
					{
						Permission: "MONITOR",
					},
					{
						Permission: "OPERATE",
					},
					{
						Permission: "USAGE",
					},
					{
						Permission: "OWNERSHIP",
					},
				},
				Children: []string{ds.Database},
			},
			{
				Name: ds.Database,
				Permissions: []ds.DataObjectTypePermission{
					{
						Permission: "CREATE SCHEMA",
					},
					{
						Permission: "USAGE",
					},
					{
						Permission: "MODIFY",
					},
					{
						Permission: "MONITOR",
					},
					{
						Permission: "IMPORTED PRIVILEGES",
					},
					{
						Permission: "OWNERSHIP",
					},
				},
				Children: []string{ds.Schema},
			},
			{
				Name: ds.Schema,
				Permissions: []ds.DataObjectTypePermission{
					{
						Permission: "MODIFY",
					},
					{
						Permission: "MONITOR",
					},
					{
						Permission: "USAGE",
					},
					{
						Permission: "CREATE TABLE",
					},
					{
						Permission: "CREATE EXTERNAL TABLE",
					},
					{
						Permission: "CREATE VIEW",
					},
					{
						Permission: "CREATE MATERIALIZED VIEW",
					},
					{
						Permission: "CREATE MASKING POLICY",
					},
					{
						Permission: "CREATE ROW ACCESS POLICY",
					},
					{
						Permission: "CREATE SESSION POLICY",
					},
					{
						Permission: "CREATE STAGE",
					},
					{
						Permission: "CREATE FILE FORMAT",
					},
					{
						Permission: "CREATE SEQUENCE",
					},
					{
						Permission: "CREATE FUNCTION",
					},
					{
						Permission: "CREATE PIPE",
					},
					{
						Permission: "CREATE STREAM",
					},
					{
						Permission: "CREATE TAG",
					},
					{
						Permission: "CREATE TASK",
					},
					{
						Permission: "CREATE PROCEDURE",
					},
					{
						Permission: "ADD SEARCH OPTIMIZATION",
					},
					{
						Permission: "OWNERSHIP",
					},
				},
				Children: []string{ds.Table, ds.View},
			},
			{
				Name: ds.Table,
				Permissions: []ds.DataObjectTypePermission{
					{
						Permission: "SELECT",
					},
					{
						Permission: "INSERT",
					},
					{
						Permission: "UPDATE",
					},
					{
						Permission: "TRUNCATE",
					},
					{
						Permission: "DELETE",
					},
					{
						Permission: "REFERENCES",
					},
					{
						Permission: "OWNERSHIP",
					},
				},
				Children: []string{ds.Column},
			},
			{
				Name: ds.View,
				Permissions: []ds.DataObjectTypePermission{
					{
						Permission: "SELECT",
					},
					{
						Permission: "REFERENCES",
					},
					{
						Permission: "OWNERSHIP",
					},
				},
				Children: []string{ds.Column},
			},
			{
				Name: ds.Column,
			},
		},
	}
}
