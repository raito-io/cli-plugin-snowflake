package main

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/blockloop/scan"
	ds "github.com/raito-io/cli/base/data_source"
	e "github.com/raito-io/cli/base/util/error"
)

type DataSourceSyncer struct {
}

func (s *DataSourceSyncer) SyncDataSource(config *ds.DataSourceSyncConfig) ds.DataSourceSyncResult {
	logger.Debug("Start syncing data source meta data for snowflake")
	fileCreator, err := ds.NewDataSourceFileCreator(config)

	if err != nil {
		return ds.DataSourceSyncResult{Error: e.ToErrorResult(err)}
	}
	defer fileCreator.Close()

	start := time.Now()

	conn, err := ConnectToSnowflake(config.Parameters, "")
	if err != nil {
		return ds.DataSourceSyncResult{Error: e.ToErrorResult(err)}
	}
	defer conn.Close()

	// for data source level access import & export convenience we retrieve the snowflake account and use it as datasource name
	sfAccount, err := getSnowflakeAccountName(conn)

	if err != nil {
		return ds.DataSourceSyncResult{Error: e.ToErrorResult(err)}
	}

	fileCreator.GetDataSourceDetails().SetName(sfAccount)
	fileCreator.GetDataSourceDetails().SetFullname(sfAccount)

	excludedDatabases := ""
	if v, ok := config.Parameters[SfExcludedDatabases]; ok && v != nil {
		excludedDatabases = v.(string)
	}

	excludedSchemas := "INFORMATION_SCHEMA"
	if v, ok := config.Parameters[SfExcludedSchemas]; ok && v != nil {
		excludedSchemas += "," + v.(string)
	}

	_, err = readWarehouses(fileCreator, conn)
	if err != nil {
		return ds.DataSourceSyncResult{Error: e.ToErrorResult(err)}
	}

	// main reason is that for export they can only have "IMPORTED PRIVILEGES" granted on the shared db level and nothing else.
	// for now we can just exclude them but they need to be treated later on
	shares, err := readShares(fileCreator, conn, excludedDatabases)
	if err != nil {
		return ds.DataSourceSyncResult{Error: e.ToErrorResult(err)}
	}

	sharesMap := make(map[string]struct{}, 0)

	// exclude shares from database import as we treat them separately
	for _, share := range shares {
		if excludedDatabases != "" {
			excludedDatabases += ","
		}
		excludedDatabases += share.Name
		sharesMap[share.Name] = struct{}{}
	}

	databases, err := readDatabases(fileCreator, conn, excludedDatabases)
	if err != nil {
		return ds.DataSourceSyncResult{Error: e.ToErrorResult(err)}
	}

	// add shares to the list again to fetch their descendants
	databases = append(databases, shares...)

	for _, database := range databases {
		doTypePrefix := ""
		if _, f := sharesMap[database.Name]; f {
			doTypePrefix = "shared-"
		}

		schemas, err := readSchemas(fileCreator, conn, doTypePrefix, database.Name, excludedSchemas)
		if err != nil {
			logger.Error(fmt.Sprintf("error while syncing schemas for database %q between Snowflake and Raito: %s", database.Name, err.Error()))
			return ds.DataSourceSyncResult{Error: e.ToErrorResult(fmt.Errorf("error while syncing schemas for database %q between Snowflake and Raito: %s", database.Name, err.Error()))}
		}

		for _, schema := range schemas {
			tables, err := readTables(fileCreator, conn, doTypePrefix, database.Name+"."+schema.Name)
			if err != nil {
				logger.Error(fmt.Sprintf("error while syncing tables for schema %q between Snowflake and Raito: %s", schema.Name, err.Error()))
				return ds.DataSourceSyncResult{Error: e.ToErrorResult(fmt.Errorf("error while syncing tables for schema %q between Snowflake and Raito: %s", schema.Name, err.Error()))}
			}

			for _, table := range tables {
				err = readColumns(fileCreator, conn, doTypePrefix, database.Name+"."+schema.Name+"."+table.Name)

				if err != nil {
					logger.Error(fmt.Sprintf("error while syncing columns for table %q between Snowflake and Raito: %s", table.Name, err.Error()))
					return ds.DataSourceSyncResult{Error: e.ToErrorResult(fmt.Errorf("error while syncing columns for table %q between Snowflake and Raito: %s", table.Name, err.Error()))}
				}
			}

			views, err := readViews(fileCreator, conn, doTypePrefix, database.Name+"."+schema.Name)
			if err != nil {
				logger.Error(fmt.Sprintf("error while syncing tables for schema %q between Snowflake and Raito: %s", schema.Name, err.Error()))
				return ds.DataSourceSyncResult{Error: e.ToErrorResult(fmt.Errorf("error while syncing tables for schema %q between Snowflake and Raito: %s", schema.Name, err.Error()))}
			}

			for _, view := range views {
				err := readColumns(fileCreator, conn, doTypePrefix, database.Name+"."+schema.Name+"."+view.Name)

				if err != nil {
					if strings.Contains(err.Error(), "Insufficient privileges to operate on table") {
						logger.Error(fmt.Sprintf("error while syncing columns for view %q between Snowflake and Raito. The snowflake user should either have OWNERSHIP or SELECT permissions on the underlying table", view.Name))
					} else {
						logger.Error(fmt.Sprintf("error while syncing columns for view %q between Snowflake and Raito: %s", view.Name, err.Error()))
						return ds.DataSourceSyncResult{Error: e.ToErrorResult(fmt.Errorf("error while syncing columns for view %q between Snowflake and Raito: %s", view.Name, err.Error()))}
					}
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

func getSnowflakeAccountName(conn *sql.DB) (string, error) {
	rows, err := conn.Query("select current_account()")
	if err != nil {
		return "", fmt.Errorf("error while querying Snowflake: %s", err.Error())
	}
	var r []string
	err = scan.Rows(&r, rows)

	if err != nil {
		return "", fmt.Errorf("error while querying Snowflake: %s", err.Error())
	}

	if len(r) != 1 {
		return "", fmt.Errorf("error retrieving account information from snowflake")
	}

	return r[0], nil
}

func addDbEntitiesToImporter(fileCreator ds.DataSourceFileCreator, conn *sql.DB, doType string, parent string, query string, externalIdGenerator func(name string) string, filter func(name, fullName string) bool) ([]dbEntity, error) {
	dbs, err := readDbEntities(conn, query)

	if err != nil {
		return nil, err
	}

	dataObjects := make([]ds.DataObject, 0, 20)
	dbEntities := make([]dbEntity, 0, 20)

	for _, db := range dbs {
		logger.Debug(fmt.Sprintf("Handling data object (type %s) %q", doType, db.Name))

		fullName := externalIdGenerator(db.Name)
		if filter(db.Name, fullName) {
			do := ds.DataObject{
				ExternalId:       fullName,
				Name:             db.Name,
				FullName:         fullName,
				Type:             doType,
				Description:      "",
				ParentExternalId: parent,
			}
			dataObjects = append(dataObjects, do)
			dbEntities = append(dbEntities, db)
		}
	}

	err = fileCreator.AddDataObjects(dataObjects)
	if err != nil {
		return nil, err
	}

	return dbEntities, nil
}

func readShares(fileCreator ds.DataSourceFileCreator, conn *sql.DB, excludedDatabases string) ([]dbEntity, error) {
	_, err := readDbEntities(conn, "SHOW SHARES")
	if err != nil {
		return nil, err
	}

	excludes := make(map[string]struct{})

	if excludedDatabases != "" {
		for _, e := range strings.Split(excludedDatabases, ",") {
			excludes[e] = struct{}{}
		}
	}

	return addDbEntitiesToImporter(fileCreator, conn, "shared-database", "", "select \"database_name\" as \"name\" from table(result_scan(LAST_QUERY_ID())) WHERE \"kind\" = 'INBOUND'",
		func(name string) string { return name },
		func(name, fullName string) bool {
			_, f := excludes[fullName]
			return !f
		})
}

func readWarehouses(fileCreator ds.DataSourceFileCreator, conn *sql.DB) ([]dbEntity, error) {
	return addDbEntitiesToImporter(fileCreator, conn, "warehouse", "", "SHOW WAREHOUSES",
		func(name string) string { return name },
		func(name, fullName string) bool { return true })
}

func readDatabases(fileCreator ds.DataSourceFileCreator, conn *sql.DB, excludedDatabases string) ([]dbEntity, error) {
	excludes := make(map[string]struct{})

	if excludedDatabases != "" {
		for _, e := range strings.Split(excludedDatabases, ",") {
			excludes[e] = struct{}{}
		}
	}

	return addDbEntitiesToImporter(fileCreator, conn, ds.Database, "", "SHOW DATABASES IN ACCOUNT",
		func(name string) string { return name },
		func(name, fullName string) bool {
			_, f := excludes[fullName]
			return !f
		})
}
func readSchemas(fileCreator ds.DataSourceFileCreator, conn *sql.DB, doTypePrefix string, dbName string, excludedSchemas string) ([]dbEntity, error) {
	excludes := make(map[string]struct{})

	if excludedSchemas != "" {
		for _, e := range strings.Split(excludedSchemas, ",") {
			excludes[e] = struct{}{}
		}
	}

	return addDbEntitiesToImporter(fileCreator, conn, doTypePrefix+ds.Schema, dbName, "SHOW SCHEMAS IN DATABASE "+dbName,
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

func readTables(fileCreator ds.DataSourceFileCreator, conn *sql.DB, doTypePrefix string, schemaFullName string) ([]dbEntity, error) {
	return addDbEntitiesToImporter(fileCreator, conn, doTypePrefix+ds.Table, schemaFullName, "SHOW TABLES IN SCHEMA "+schemaFullName,
		func(name string) string { return schemaFullName + "." + name },
		func(name, fullName string) bool { return true })
}

func readViews(fileCreator ds.DataSourceFileCreator, conn *sql.DB, doTypePrefix string, schemaFullName string) ([]dbEntity, error) {
	return addDbEntitiesToImporter(fileCreator, conn, doTypePrefix+ds.View, schemaFullName, "SHOW VIEWS IN SCHEMA "+schemaFullName,
		func(name string) string { return schemaFullName + "." + name },
		func(name, fullName string) bool { return true })
}

func readColumns(fileCreator ds.DataSourceFileCreator, conn *sql.DB, doTypePrefix string, tableFullName string) error {
	_, err := readDbEntities(conn, "SHOW COLUMNS IN TABLE "+tableFullName)
	if err != nil {
		return err
	}

	_, err = addDbEntitiesToImporter(fileCreator, conn, doTypePrefix+ds.Column, tableFullName, "select \"column_name\" as \"name\" from table(result_scan(LAST_QUERY_ID()))",
		func(name string) string { return tableFullName + "." + name },
		func(name, fullName string) bool { return true })

	return err
}

type dbEntity struct {
	Name string `db:"name"`
}

func (s *DataSourceSyncer) GetMetaData() ds.MetaData {
	logger.Debug("Returning meta data for Snowflake")

	return ds.MetaData{
		Type:              "snowflake",
		SupportedFeatures: []string{ds.RowFiltering, ds.ColumnMasking},
		DataObjectTypes: []ds.DataObjectType{
			{
				Name: ds.Datasource,
				Type: ds.Datasource,
				Permissions: []ds.DataObjectTypePermission{
					{
						Permission:  "APPLY MASKING POLICY",
						Description: "Grants ability to set a Column-level Security masking policy on a table or view column and to set a masking policy on a tag. This global privilege also allows executing the DESCRIBE operation on tables and views.",
					},
					{
						Permission:  "APPLY ROW ACCESS POLICY",
						Description: "Grants the ability to add and drop a row access policy on a table or view. This global privilege also allows executing the DESCRIBE operation on tables and views.",
					},
					{
						Permission:  "APPLY SESSION POLICY",
						Description: "Grants the ability to set or unset a session policy on an account or user.",
					},
					{
						Permission:  "APPLY TAG",
						Description: "Grants the ability to add or drop a tag on a Snowflake object.",
					},
					{
						Permission:  "ATTACH POLICY",
						Description: "Grants ability to activate a network policy by associating it with your account.",
					},
					{
						Permission:  "CREATE ACCOUNT",
						Description: "Enables a data provider to create a new managed account (i.e. reader account).",
					},
					{
						Permission:  "CREATE ROLE",
						Description: "Enables creating a new role.",
					},
					{
						Permission:  "CREATE USER",
						Description: "Enables creating a new user.",
					},
					{
						Permission:  "MANAGE GRANTS",
						Description: "Enables granting or revoking privileges on objects for which the role is not the owner.",
					},
					{
						Permission:  "CREATE DATA EXCHANGE LISTING",
						Description: "Enables creating a new Data Exchange listing.",
					},
					{
						Permission:  "CREATE INTEGRATION",
						Description: "Enables creating a new notification, security, or storage integration.",
					},
					{
						Permission:  "CREATE NETWORK POLICY",
						Description: "Enables creating a new network policy.",
					},
					{
						Permission:  "CREATE SHARE",
						Description: "Enables a data provider to create a new share.",
					},
					{
						Permission:  "CREATE WAREHOUSE",
						Description: "Enables creating a new virtual warehouse.",
					},
					{
						Permission:  "EXECUTE MANAGED TASK",
						Description: "Grants ability to create tasks that rely on Snowflake-managed compute resources (serverless compute model). Only required for serverless tasks. The role that has the OWNERSHIP privilege on a task must have both the EXECUTE MANAGED TASK and the EXECUTE TASK privilege for the task to run.",
					},
					{
						Permission:  "EXECUTE TASK",
						Description: "Grants ability to run tasks owned by the role. For serverless tasks to run, the role that has the OWNERSHIP privilege on the task must also have the global EXECUTE MANAGED TASK privilege.",
					},
					{
						Permission:  "IMPORT SHARE",
						Description: "Enables a data consumer to view shares shared with their account. Also grants the ability to create databases from shares; requires the global CREATE DATABASE privilege.",
					},
					{
						Permission:  "MONITOR EXECUTION",
						Description: "Grants ability to monitor any pipes or tasks in the account. The USAGE privilege is also required on each database and schema that stores these objects.",
					},
					{
						Permission:  "MONITOR USAGE",
						Description: "Grants ability to monitor account-level usage and historical information for databases and warehouses. Additionally grants ability to view managed accounts using SHOW MANAGED ACCOUNTS.",
					},
					{
						Permission:  "OVERRIDE SHARE RESTRICTIONS",
						Description: "Grants ability to set value for the SHARE_RESTRICTIONS parameter which enables a Business Critical provider account to add a consumer account (with Non-Business Critical edition) to a share.",
					},
				},
				Children: []string{ds.Database, "shared-" + ds.Database, "warehouse"},
			},
			{
				Name: "warehouse",
				Type: "warehouse",
				Permissions: []ds.DataObjectTypePermission{
					{
						Permission:  "MODIFY",
						Description: "Enables altering any properties of a warehouse, including changing its size. ",
					},
					{
						Permission:  "MONITOR",
						Description: "Enables viewing current and past queries executed on a warehouse as well as usage statistics on that warehouse.",
					},
					{
						Permission:  "OPERATE",
						Description: "Enables changing the state of a warehouse (stop, start, suspend, resume). In addition, enables viewing current and past queries executed on a warehouse and aborting any executing queries.",
					},
					{
						Permission:  "USAGE",
						Description: "Enables using a virtual warehouse and, as a result, executing queries on the warehouse. If the warehouse is configured to auto-resume when a SQL statement (e.g. query) is submitted to it, the warehouse resumes automatically and executes the statement.",
					},
				},
				Children: []string{},
			},
			{
				Name: ds.Database,
				Type: ds.Database,
				Permissions: []ds.DataObjectTypePermission{
					{
						Permission:  "CREATE SCHEMA",
						Description: "Enables creating a new schema in a database, including cloning a schema.",
					},
					{
						Permission:  "USAGE",
						Description: "Enables using a database, including returning the database details in the SHOW DATABASES command output. Additional privileges are required to view or take actions on objects in a database.",
					},
					{
						Permission:  "MODIFY",
						Description: "Enables altering any settings of a database.",
					},
					{
						Permission:  "MONITOR",
						Description: "Enables performing the DESCRIBE command on the database.",
					},
				},
				Children: []string{ds.Schema},
			},
			{
				Name: ds.Schema,
				Type: ds.Schema,
				Permissions: []ds.DataObjectTypePermission{
					{
						Permission:  "MODIFY",
						Description: "Enables altering any settings of a schema.",
					},
					{
						Permission:  "MONITOR",
						Description: "Enables performing the DESCRIBE command on the schema.",
					},
					{
						Permission:  "USAGE",
						Description: "Enables using a schema, including returning the schema details in the SHOW SCHEMAS command output. To execute SHOW <objects> commands for objects (tables, views, stages, file formats, sequences, pipes, or functions) in the schema, a role must have at least one privilege granted on the object.",
					},
					{
						Permission:  "CREATE TABLE",
						Description: "Enables creating a new table in a schema, including cloning a table. Note that this privilege is not required to create temporary tables, which are scoped to the current user session and are automatically deleted when the session ends.",
					},
					{
						Permission:  "CREATE EXTERNAL TABLE",
						Description: "Enables creating a new external table in a schema.",
					},
					{
						Permission:  "CREATE VIEW",
						Description: "Enables creating a new view in a schema.",
					},
					{
						Permission:  "CREATE MATERIALIZED VIEW",
						Description: "Enables creating a new materialized view in a schema.",
					},
					{
						Permission:  "CREATE MASKING POLICY",
						Description: "Enables creating a new Column-level Security masking policy in a schema.",
					},
					{
						Permission:  "CREATE ROW ACCESS POLICY",
						Description: "Enables creating a new row access policy in a schema.",
					},
					{
						Permission:  "CREATE SESSION POLICY",
						Description: "Enables creating a new session policy in a schema.",
					},
					{
						Permission:  "CREATE STAGE",
						Description: "Enables creating a new stage in a schema, including cloning a stage.",
					},
					{
						Permission:  "CREATE FILE FORMAT",
						Description: "Enables creating a new file format in a schema, including cloning a file format.",
					},
					{
						Permission:  "CREATE SEQUENCE",
						Description: "Enables creating a new sequence in a schema, including cloning a sequence.",
					},
					{
						Permission:  "CREATE FUNCTION",
						Description: "Enables creating a new UDF or external function in a schema.",
					},
					{
						Permission:  "CREATE PIPE",
						Description: "Enables creating a new pipe in a schema.",
					},
					{
						Permission:  "CREATE STREAM",
						Description: "Enables creating a new stream in a schema, including cloning a stream.",
					},
					{
						Permission:  "CREATE TAG",
						Description: "Enables creating a new tag key in a schema.",
					},
					{
						Permission:  "CREATE TASK",
						Description: "Enables creating a new task in a schema, including cloning a task.",
					},
					{
						Permission:  "CREATE PROCEDURE",
						Description: "Enables creating a new stored procedure in a schema.",
					},
					{
						Permission:  "ADD SEARCH OPTIMIZATION",
						Description: "Enables adding search optimization to a table in a schema.",
					},
				},
				Children: []string{ds.Table, ds.View},
			},
			{
				Name: ds.Table,
				Type: ds.Table,
				Permissions: []ds.DataObjectTypePermission{
					{
						Permission:  "SELECT",
						Description: "Enables executing a SELECT statement on a table.",
					},
					{
						Permission:  "INSERT",
						Description: "Enables executing an INSERT command on a table. Also enables using the ALTER TABLE command with a RECLUSTER clause to manually recluster a table with a clustering key.",
					},
					{
						Permission:  "UPDATE",
						Description: "Enables executing an UPDATE command on a table.",
					},
					{
						Permission:  "TRUNCATE",
						Description: "Enables executing a TRUNCATE TABLE command on a table.",
					},
					{
						Permission:  "DELETE",
						Description: "Enables executing a DELETE command on a table.",
					},
					{
						Permission:  "REFERENCES",
						Description: "Enables referencing a table as the unique/primary key table for a foreign key constraint. Also enables viewing the structure of a table (but not the data) via the DESCRIBE or SHOW command or by querying the Information Schema.",
					},
				},
				Children: []string{ds.Column},
			},
			{
				Name: ds.View,
				Type: ds.View,
				Permissions: []ds.DataObjectTypePermission{
					{
						Permission:  "SELECT",
						Description: "Enables executing a SELECT statement on a view.",
					},
					{
						Permission:  "REFERENCES",
						Description: "Enables viewing the structure of a view (but not the data) via the DESCRIBE or SHOW command or by querying the Information Schema.",
					},
				},
				Children: []string{ds.Column},
			},
			{
				Name: ds.Column,
				Type: ds.Column,
			},
			{
				Name: "shared-" + ds.Database,
				Type: ds.Database,
				Permissions: []ds.DataObjectTypePermission{
					{
						Permission:  "IMPORTED PRIVILEGES",
						Description: "Enables roles other than the owning role to access a shared database; applies only to shared databases.",
					},
				},
				Children: []string{"shared-" + ds.Schema},
			},
			{
				Name:     "shared-" + ds.Schema,
				Type:     ds.Schema,
				Children: []string{"shared-" + ds.Table, "shared-" + ds.View},
			},
			{
				Name:     "shared-" + ds.Table,
				Type:     ds.Table,
				Children: []string{"shared-" + ds.Column},
			},
			{
				Name:        "shared-" + ds.View,
				Type:        ds.View,
				Permissions: []ds.DataObjectTypePermission{},
				Children:    []string{"shared-" + ds.Column},
			},
			{
				Name: "shared-" + ds.Column,
				Type: ds.Column,
			},
		},
	}
}
