package main

import (
	"database/sql"
	"fmt"
	"github.com/blockloop/scan"
	dsb "github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/common/api"
	"github.com/raito-io/cli/common/api/data_source"
	"strings"
	"time"
)

type DataSourceSyncer struct {
}

func (s *DataSourceSyncer) SyncDataSource(config *data_source.DataSourceSyncConfig) data_source.DataSourceSyncResult {
	logger.Debug("Start syncing data source meta data for snowflake")
	fileCreator, err := dsb.NewDataSourceFileCreator(config)
	if err != nil {
		return data_source.DataSourceSyncResult{ Error: api.ToErrorResult(err) }
	}
	defer fileCreator.Close()

	start := time.Now()

	conn, err := ConnectToSnowflake(config.Parameters, "")
	if err != nil {
		return data_source.DataSourceSyncResult{ Error: api.ToErrorResult(err) }
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

	databases, err := readDatabases(&fileCreator, conn, excludedDatabases)
	if err != nil {
		return data_source.DataSourceSyncResult{ Error: api.ToErrorResult(err) }
	}
	for _, database := range databases {
		schemas, err := readSchemas(&fileCreator, conn, database.Name, excludedSchemas)
		if err != nil {
			return data_source.DataSourceSyncResult{ Error: api.ToErrorResult(fmt.Errorf("Error while syncing schemas for database %q between Snowflake and Raito: %s", database.Name, err.Error())) }
		}

		for _, schema := range schemas {
			_, err := readTables(&fileCreator, conn, database.Name + "." + schema.Name)
			// TODO go to column level
			if err != nil {
				return data_source.DataSourceSyncResult{ Error: api.ToErrorResult(fmt.Errorf("Error while syncing tables for schema %q between Snowflake and Raito: %s", schema.Name, err.Error())) }
			}
		}
	}

	sec := time.Since(start).Round(time.Millisecond)

	logger.Info(fmt.Sprintf("Fetched %d data objects from Snowflake in %s", fileCreator.GetDataObjectCount(), sec))

	return data_source.DataSourceSyncResult{}
}

func readDbEntities(conn *sql.DB, query string) ([]dbEntity, error) {
	rows, err := conn.Query(query)
	if err != nil {
		return nil, fmt.Errorf("Error while querying Snowflake: %s", err.Error())
	}
	var dbs []dbEntity
	err = scan.Rows(&dbs, rows)
	if err != nil {
		return nil, fmt.Errorf("Error while querying Snowflake: %s", err.Error())
	}
	err = CheckSFLimitExceeded(query, len(dbs))
	if err != nil {
		return nil, fmt.Errorf("Error while querying Snowflake: %s", err.Error())
	}
	return dbs, nil
}

func addDbEntitiesToImporter(fileCreator *dsb.DataSourceFileCreator, conn *sql.DB, doType string, parent string, query string, externalIdGenerator func(name string) string, filter func(name, fullName string) bool) ([]dbEntity, error) {
	dbs, err := readDbEntities(conn, query)
	if err != nil {
		return nil, err
	}

	dataObjects := make([]dsb.DataObject, 0, 20)
	for _, db := range dbs {
		logger.Debug(fmt.Sprintf("Handling database %q", db.Name))
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
	err = (*fileCreator).AddDataObjects(dataObjects)
	if err != nil {
		return nil, err
	}
	return dbs, nil
}

func readDatabases(fileCreator *dsb.DataSourceFileCreator, conn *sql.DB, excludedDatabases string) ([]dbEntity, error) {
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

func readSchemas(fileCreator *dsb.DataSourceFileCreator, conn *sql.DB, dbName string, excludedSchemas string) ([]dbEntity, error) {
	excludes := make(map[string]struct{})
	if excludedSchemas != "" {
		for _, e := range strings.Split(excludedSchemas, ",") {
			excludes[e] = struct{}{}
		}
	}
	return addDbEntitiesToImporter(fileCreator, conn, "schema", dbName, "SHOW SCHEMAS IN DATABASE "+dbName,
		func(name string) string { return dbName +"." + name },
		func(name, fullName string) bool {
			_, f := excludes[fullName]
			if f {
				return !f
			}
			_, f = excludes[name]
			return !f
		})
}

func readTables(fileCreator *dsb.DataSourceFileCreator, conn *sql.DB, schemaFullName string) ([]dbEntity, error) {
	return addDbEntitiesToImporter(fileCreator, conn, "table", schemaFullName, "SHOW TABLES IN SCHEMA "+schemaFullName,
		func(name string) string { return schemaFullName +"." + name },
		func(name, fullName string) bool { return true})
}

type dbEntity struct {
	Name string `db:"name"`
}