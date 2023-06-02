package snowflake

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/raito-io/cli-plugin-snowflake/common"
	"github.com/raito-io/cli/base/tag"

	ds "github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers"
)

const AccountAdmin = "ACCOUNTADMIN"

//go:generate go run github.com/vektra/mockery/v2 --name=dataSourceRepository --with-expecter --inpackage
type dataSourceRepository interface {
	Close() error
	TotalQueryTime() time.Duration
	GetSnowFlakeAccountName() (string, error)
	GetWarehouses() ([]DbEntity, error)
	GetShares() ([]DbEntity, error)
	GetDataBases() ([]DbEntity, error)
	GetSchemasInDatabase(databaseName string, handleEntity EntityHandler) error
	GetTablesInDatabase(databaseName string, schemaName string, handleEntity EntityHandler) error
	GetViewsInDatabase(databaseName string, schemaName string, handleEntity EntityHandler) error
	GetColumnsInDatabase(databaseName string, handleEntity EntityHandler) error
	GetTags(databaseName string) (map[string][]*tag.Tag, error)
	ExecuteGrant(perm, on, role string) error
	ExecuteRevoke(perm, on, role string) error
}

type DataSourceSyncer struct {
	repoProvider func(params map[string]string, role string) (dataSourceRepository, error)
	SfSyncRole   string
}

func NewDataSourceSyncer() *DataSourceSyncer {
	return &DataSourceSyncer{
		repoProvider: newDataSourceSnowflakeRepo,
	}
}

func newDataSourceSnowflakeRepo(params map[string]string, role string) (dataSourceRepository, error) {
	return NewSnowflakeRepository(params, role)
}

func (s *DataSourceSyncer) SyncDataSource(ctx context.Context, dataSourceHandler wrappers.DataSourceObjectHandler, configParams *config.ConfigMap) error {
	repo, err := s.repoProvider(configParams.Parameters, "")
	if err != nil {
		return err
	}

	defer func() {
		logger.Info(fmt.Sprintf("Total snowflake query time:  %s", repo.TotalQueryTime()))
		repo.Close()
	}()

	// for data source level access import & export convenience we retrieve the snowflake account and use it as datasource name
	sfAccount, err := repo.GetSnowFlakeAccountName()
	if err != nil {
		return err
	}

	s.SfSyncRole = configParams.GetStringWithDefault(SfRole, AccountAdmin)

	dataSourceHandler.SetDataSourceName(sfAccount)
	dataSourceHandler.SetDataSourceFullname(sfAccount)

	standard := configParams.GetBoolWithDefault(SfStandardEdition, false)
	skipTags := configParams.GetBoolWithDefault(SfSkipTags, false)
	skipColumns := configParams.GetBoolWithDefault(SfSkipColumns, false)

	excludedDatabases := ""
	if v, ok := configParams.Parameters[SfExcludedDatabases]; ok {
		excludedDatabases = v
	}

	excludedSchemaList := "INFORMATION_SCHEMA"
	if v, ok := configParams.Parameters[SfExcludedSchemas]; ok {
		excludedSchemaList += "," + v
	}

	excludedSchemas := make(map[string]struct{})

	if excludedSchemaList != "" {
		for _, e := range strings.Split(excludedSchemaList, ",") {
			excludedSchemas[e] = struct{}{}
		}
	}

	err = s.readWarehouses(repo, dataSourceHandler)
	if err != nil {
		return err
	}

	shares, sharesMap, err := s.readShares(repo, excludedDatabases, dataSourceHandler)
	if err != nil {
		return err
	}

	databases, err := s.readDatabases(repo, excludedDatabases, dataSourceHandler, sharesMap)
	if err != nil {
		return err
	}

	// add shares to the list again to fetch their descendants
	databases = append(databases, shares...)

	for _, database := range databases {
		err := s.setupDatabasePermissions(repo, database)

		if err != nil {
			return err
		}

		doTypePrefix := ""
		if _, f := sharesMap[database.Name]; f {
			doTypePrefix = "shared-"
		}

		tagMap := make(map[string][]*tag.Tag)
		if !standard && !skipTags {
			tagMap, err = repo.GetTags(database.Name)

			if err != nil {
				return err
			}
		}

		err = s.readSchemasInDatabase(repo, database.Name, excludedSchemas, dataSourceHandler, doTypePrefix, tagMap)
		if err != nil {
			return err
		}

		err = s.readTablesInDatabase(database.Name, excludedSchemas, dataSourceHandler, doTypePrefix+ds.Table, repo.GetTablesInDatabase, tagMap)
		if err != nil {
			return err
		}

		err = s.readTablesInDatabase(database.Name, excludedSchemas, dataSourceHandler, doTypePrefix+ds.View, repo.GetViewsInDatabase, tagMap)
		if err != nil {
			return err
		}

		if !skipColumns {
			err = s.readColumnsInDatabase(repo, database.Name, excludedSchemas, dataSourceHandler, doTypePrefix, tagMap)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *DataSourceSyncer) readColumnsInDatabase(repo dataSourceRepository, dbName string, excludedSchemas map[string]struct{}, dataSourceHandler wrappers.DataSourceObjectHandler, doTypePrefix string, tagMap map[string][]*tag.Tag) error {
	typeName := doTypePrefix + ds.Column

	return repo.GetColumnsInDatabase(dbName, func(entity interface{}) error {
		column := entity.(*ColumnEntity)
		schemaName := column.Schema
		schemaFullName := column.Database + "." + schemaName
		_, ff := excludedSchemas[schemaFullName]
		_, fs := excludedSchemas[schemaName]

		fullName := schemaFullName + "." + column.Table + "." + column.Name

		if ff || fs {
			logger.Debug(fmt.Sprintf("Skipping data object (type %s) '%s'", typeName, fullName))
			return nil
		}

		logger.Debug(fmt.Sprintf("Handling data object (type %s) '%s'", typeName, fullName))

		comment := ""
		if column.Comment != nil {
			comment = *column.Comment
		}
		do := ds.DataObject{
			ExternalId:       fullName,
			Name:             column.Name,
			FullName:         fullName,
			Type:             typeName,
			Description:      comment,
			ParentExternalId: schemaFullName + "." + column.Table,
			Tags:             tagMap[fullName],
		}

		return dataSourceHandler.AddDataObjects(&do)
	})
}

func (s *DataSourceSyncer) readSchemasInDatabase(repo dataSourceRepository, databaseName string, excludedSchemas map[string]struct{}, dataSourceHandler wrappers.DataSourceObjectHandler, doTypePrefix string, tagMap map[string][]*tag.Tag) error {
	typeName := doTypePrefix + ds.Schema

	return repo.GetSchemasInDatabase(databaseName, func(entity interface{}) error {
		schema := entity.(*SchemaEntity)

		fullName := schema.Database + "." + schema.Name

		_, ff := excludedSchemas[fullName]
		_, fs := excludedSchemas[schema.Name]

		if ff || fs {
			logger.Debug(fmt.Sprintf("Skipping data object (type %s) '%s'", typeName, fullName))
			return nil
		}

		logger.Debug(fmt.Sprintf("Handling data object (type %s) '%s'", typeName, fullName))

		comment := ""
		if schema.Comment != nil {
			comment = *schema.Comment
		}
		do := ds.DataObject{
			ExternalId:       fullName,
			Name:             schema.Name,
			FullName:         fullName,
			Type:             typeName,
			Description:      comment,
			ParentExternalId: schema.Database,
			Tags:             tagMap[fullName],
		}

		return dataSourceHandler.AddDataObjects(&do)
	})
}

func (s *DataSourceSyncer) readTablesInDatabase(databaseName string, excludedSchemas map[string]struct{}, dataSourceHandler wrappers.DataSourceObjectHandler, typeName string, fetcher func(dbName string, schemaName string, entityHandler EntityHandler) error, tagMap map[string][]*tag.Tag) error {
	return fetcher(databaseName, "", func(entity interface{}) error {
		table := entity.(*TableEntity)

		schemaName := table.Schema
		schemaFullName := table.Database + "." + schemaName
		_, ff := excludedSchemas[schemaFullName]
		_, fs := excludedSchemas[schemaName]

		fullName := schemaFullName + "." + table.Name

		if ff || fs {
			logger.Debug(fmt.Sprintf("Skipping data object (type %s) '%s'", typeName, fullName))
			return nil
		}

		logger.Debug(fmt.Sprintf("Handling data object (type %s) '%s'", typeName, fullName))

		comment := ""
		if table.Comment != nil {
			comment = *table.Comment
		}
		do := ds.DataObject{
			ExternalId:       fullName,
			Name:             table.Name,
			FullName:         fullName,
			Type:             typeName,
			Description:      comment,
			ParentExternalId: table.Database + "." + table.Schema,
			Tags:             tagMap[fullName],
		}

		return dataSourceHandler.AddDataObjects(&do)
	})
}

func (s *DataSourceSyncer) setupDatabasePermissions(repo dataSourceRepository, db DbEntity) error {
	// grant the SYNC role USAGE/IMPORTED PRIVILEGES on each database so it can query the INFORMATION_SCHEMA
	if s.SfSyncRole != AccountAdmin {
		err := repo.ExecuteGrant("USAGE", fmt.Sprintf("DATABASE %s", common.FormatQuery("%s", db.Name)), s.SfSyncRole)

		if err != nil && strings.Contains(err.Error(), "IMPORTED PRIVILEGES") {
			err2 := repo.ExecuteGrant("IMPORTED PRIVILEGES", fmt.Sprintf("DATABASE %s", common.FormatQuery("%s", db.Name)), s.SfSyncRole)

			if err2 != nil {
				return err2
			}
		} else if err != nil {
			return err
		} else {
			err2 := repo.ExecuteGrant("USAGE", fmt.Sprintf("ALL SCHEMAS IN DATABASE %s", common.FormatQuery("%s", db.Name)), s.SfSyncRole)

			if err2 != nil {
				return err2
			}

			err2 = repo.ExecuteGrant("SELECT", fmt.Sprintf("ALL TABLES IN DATABASE %s", common.FormatQuery("%s", db.Name)), s.SfSyncRole)

			if err2 != nil {
				return err2
			}

			err2 = repo.ExecuteGrant("SELECT", fmt.Sprintf("ALL EXTERNAL TABLES IN DATABASE %s", common.FormatQuery("%s", db.Name)), s.SfSyncRole)

			if err2 != nil {
				return err2
			}

			err2 = repo.ExecuteGrant("SELECT", fmt.Sprintf("ALL EXTERNAL TABLES IN DATABASE %s", common.FormatQuery("%s", db.Name)), s.SfSyncRole)

			if err2 != nil {
				return err2
			}

			err2 = repo.ExecuteGrant("SELECT", fmt.Sprintf("ALL VIEWS IN DATABASE %s", common.FormatQuery("%s", db.Name)), s.SfSyncRole)

			if err2 != nil {
				return err2
			}

			err2 = repo.ExecuteGrant("SELECT", fmt.Sprintf("ALL MATERIALIZED VIEWS IN DATABASE %s", common.FormatQuery("%s", db.Name)), s.SfSyncRole)

			if err2 != nil {
				return err2
			}
		}
	}

	return nil
}

func (s *DataSourceSyncer) readDatabases(repo dataSourceRepository, excludedDatabases string, dataSourceHandler wrappers.DataSourceObjectHandler, shares map[string]struct{}) ([]DbEntity, error) {
	databases, err := repo.GetDataBases()
	if err != nil {
		return nil, err
	}

	excludes := make(map[string]struct{})

	if excludedDatabases != "" {
		for _, e := range strings.Split(excludedDatabases, ",") {
			excludes[e] = struct{}{}
		}
	}

	databases, err = s.addDbEntitiesToImporter(dataSourceHandler, databases, ds.Database, "",
		func(name string) string { return name },
		func(name, fullName string) bool {
			_, shared := shares[fullName]
			_, f := excludes[fullName]
			return !f && !shared
		})
	if err != nil {
		return nil, err
	}

	return databases, nil
}

func (s *DataSourceSyncer) readShares(repo dataSourceRepository, excludedDatabases string, dataSourceHandler wrappers.DataSourceObjectHandler) ([]DbEntity, map[string]struct{}, error) {
	// main reason is that for export they can only have "IMPORTED PRIVILEGES" granted on the shared db level and nothing else.
	// for now we can just exclude them but they need to be treated later on
	shares, err := repo.GetShares()
	if err != nil {
		return nil, nil, err
	}

	excludes := make(map[string]struct{})

	if excludedDatabases != "" {
		for _, e := range strings.Split(excludedDatabases, ",") {
			excludes[e] = struct{}{}
		}
	}

	shares, err = s.addDbEntitiesToImporter(dataSourceHandler, shares, "shared-database", "",
		func(name string) string { return name },
		func(name, fullName string) bool {
			_, f := excludes[fullName]
			return !f
		})
	if err != nil {
		return nil, nil, err
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

	return shares, sharesMap, nil
}

func (s *DataSourceSyncer) readWarehouses(repo dataSourceRepository, dataSourceHandler wrappers.DataSourceObjectHandler) error {
	dbWarehouses, err := repo.GetWarehouses()
	if err != nil {
		return err
	}

	_, err = s.addDbEntitiesToImporter(dataSourceHandler, dbWarehouses, "warehouse", "",
		func(name string) string { return name },
		func(name, fullName string) bool { return true })
	if err != nil {
		return err
	}

	return nil
}

func (s *DataSourceSyncer) addDbEntitiesToImporter(dataObjectHandler wrappers.DataSourceObjectHandler, entities []DbEntity, doType string, parent string, externalIdGenerator func(name string) string, filter func(name, fullName string) bool) ([]DbEntity, error) {
	dbEntities := make([]DbEntity, 0, 20)

	for _, db := range entities {
		logger.Debug(fmt.Sprintf("Handling data object (type %s) '%s'", doType, db.Name))

		fullName := externalIdGenerator(db.Name)
		if filter(db.Name, fullName) {
			comment := ""
			if db.Comment != nil {
				comment = *db.Comment
			}
			do := ds.DataObject{
				ExternalId:       fullName,
				Name:             db.Name,
				FullName:         fullName,
				Type:             doType,
				Description:      comment,
				ParentExternalId: parent,
			}

			err := dataObjectHandler.AddDataObjects(&do)
			if err != nil {
				return nil, err
			}

			dbEntities = append(dbEntities, db)
		}
	}

	return dbEntities, nil
}

/*func (s *DataSourceSyncer) HandleTags(ctx context.Context, configMap *config.ConfigMap, fullName string, resourceType string) ([]*tag.Tag, error) {
	standard := configMap.GetBoolWithDefault(SfStandardEdition, false)
	skipTags := configMap.GetBoolWithDefault(SfSkipTags, false)

	if standard || skipTags {
		return nil, nil
	}

	`SELECT *
		FROM TABLE(
		  MASTER_DATA.INFORMATION_SCHEMA.TAG_REFERENCES(
			'MASTER_DATA.SALES.CUSTOMER',
			'table'
		  )
		)`
}*/

func (s *DataSourceSyncer) GetDataSourceMetaData(ctx context.Context) (*ds.MetaData, error) {
	logger.Debug("Returning meta data for Snowflake data source")

	return &ds.MetaData{
		Type:              "snowflake",
		SupportedFeatures: []string{ds.RowFiltering, ds.ColumnMasking},
		DataObjectTypes: []*ds.DataObjectType{
			{
				Name: ds.Datasource,
				Type: ds.Datasource,
				Permissions: []*ds.DataObjectTypePermission{
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
						Permission:        "IMPORT SHARE",
						Description:       "Enables a data consumer to view shares shared with their account. Also grants the ability to create databases from shares; requires the global CREATE DATABASE privilege.",
						GlobalPermissions: ds.ReadGlobalPermission().StringValues(),
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
				Permissions: []*ds.DataObjectTypePermission{
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
					{
						Permission:             "OWNERSHIP",
						Description:            "Grants full control over a warehouse. Only a single role can hold this privilege on a specific object at a time.",
						UsageGlobalPermissions: []string{ds.Read, ds.Write, ds.Admin},
						CannotBeGranted:        true,
					},
				},
				Children: []string{},
			},
			{
				Name: ds.Database,
				Type: ds.Database,
				Permissions: []*ds.DataObjectTypePermission{
					{
						Permission:  "CREATE SCHEMA",
						Description: "Enables creating a new schema in a database, including cloning a schema.",
					},
					{
						Permission:      "USAGE",
						Description:     "Enables using a database, including returning the database details in the SHOW DATABASES command output. Additional privileges are required to view or take actions on objects in a database.",
						CannotBeGranted: true,
					},
					{
						Permission:  "MODIFY",
						Description: "Enables altering any settings of a database.",
					},
					{
						Permission:  "MONITOR",
						Description: "Enables performing the DESCRIBE command on the database.",
					},
					{
						Permission:             "OWNERSHIP",
						Description:            "Grants full control over the database. Only a single role can hold this privilege on a specific object at a time.",
						UsageGlobalPermissions: []string{ds.Read, ds.Write, ds.Admin},
						CannotBeGranted:        true,
					},
				},
				Children: []string{ds.Schema},
			},
			{
				Name: ds.Schema,
				Type: ds.Schema,
				Permissions: []*ds.DataObjectTypePermission{
					{
						Permission:  "MODIFY",
						Description: "Enables altering any settings of a schema.",
					},
					{
						Permission:  "MONITOR",
						Description: "Enables performing the DESCRIBE command on the schema.",
					},
					{
						Permission:      "USAGE",
						Description:     "Enables using a schema, including returning the schema details in the SHOW SCHEMAS command output. To execute SHOW <objects> commands for objects (tables, views, stages, file formats, sequences, pipes, or functions) in the schema, a role must have at least one privilege granted on the object.",
						CannotBeGranted: true,
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
					{
						Permission:             "OWNERSHIP",
						Description:            "Grants full control over the schema. Only a single role can hold this privilege on a specific object at a time.",
						UsageGlobalPermissions: []string{ds.Read, ds.Write, ds.Admin},
						CannotBeGranted:        true,
					},
				},
				Children: []string{ds.Table, ds.View},
			},
			{
				Name: ds.Table,
				Type: ds.Table,
				Permissions: []*ds.DataObjectTypePermission{
					{
						Permission:             "SELECT",
						Description:            "Enables executing a SELECT statement on a table.",
						UsageGlobalPermissions: []string{ds.Read},
						GlobalPermissions:      ds.ReadGlobalPermission().StringValues(),
					},
					{
						Permission:             "INSERT",
						Description:            "Enables executing an INSERT command on a table. Also enables using the ALTER TABLE command with a RECLUSTER clause to manually recluster a table with a clustering key.",
						UsageGlobalPermissions: []string{ds.Write},
						GlobalPermissions:      ds.WriteGlobalPermission().StringValues(),
					},
					{
						Permission:             "UPDATE",
						Description:            "Enables executing an UPDATE command on a table.",
						UsageGlobalPermissions: []string{ds.Write},
						GlobalPermissions:      ds.WriteGlobalPermission().StringValues(),
					},
					{
						Permission:             "TRUNCATE",
						Description:            "Enables executing a TRUNCATE TABLE command on a table.",
						UsageGlobalPermissions: []string{ds.Write},
						GlobalPermissions:      ds.WriteGlobalPermission().StringValues(),
					},
					{
						Permission:             "DELETE",
						Description:            "Enables executing a DELETE command on a table.",
						UsageGlobalPermissions: []string{ds.Write},
						GlobalPermissions:      ds.WriteGlobalPermission().StringValues(),
					},
					{
						Permission:             "REFERENCES",
						UsageGlobalPermissions: []string{ds.Admin},
						Description:            "Enables referencing a table as the unique/primary key table for a foreign key constraint. Also enables viewing the structure of a table (but not the data) via the DESCRIBE or SHOW command or by querying the Information Schema.",
					},
					{
						Permission:             "OWNERSHIP",
						Description:            "Grants full control over the table. Required to alter most properties of a table, with the exception of reclustering. Only a single role can hold this privilege on a specific object at a time. Note that in a managed access schema, only the schema owner (i.e. the role with the OWNERSHIP privilege on the schema) or a role with the MANAGE GRANTS privilege can grant or revoke privileges on objects in the schema, including future grants.",
						UsageGlobalPermissions: []string{ds.Read, ds.Write, ds.Admin},
						CannotBeGranted:        true,
					},
				},
				Actions: []*ds.DataObjectTypeAction{
					{
						Action:        "SELECT",
						GlobalActions: []string{ds.Read},
					},
					{
						Action:        "INSERT",
						GlobalActions: []string{ds.Write},
					},
					{
						Action:        "UPDATE",
						GlobalActions: []string{ds.Write},
					},
					{
						Action:        "DELETE",
						GlobalActions: []string{ds.Write},
					},
					{
						Action:        "TRUNCATE",
						GlobalActions: []string{ds.Write},
					},
				},
				Children: []string{ds.Column},
			},
			{
				Name: ds.View,
				Type: ds.View,
				Permissions: []*ds.DataObjectTypePermission{
					{
						Permission:             "SELECT",
						Description:            "Enables executing a SELECT statement on a view.",
						UsageGlobalPermissions: []string{ds.Read},
						GlobalPermissions:      ds.ReadGlobalPermission().StringValues(),
					},
					{
						Permission:  "REFERENCES",
						Description: "Enables viewing the structure of a view (but not the data) via the DESCRIBE or SHOW command or by querying the Information Schema.",
					},
					{
						Permission:             "OWNERSHIP",
						Description:            "Grants full control over the view. Required to alter a view. Only a single role can hold this privilege on a specific object at a time. Note that in a managed access schema, only the schema owner (i.e. the role with the OWNERSHIP privilege on the schema) or a role with the MANAGE GRANTS privilege can grant or revoke privileges on objects in the schema, including future grants.",
						UsageGlobalPermissions: []string{ds.Read, ds.Write, ds.Admin},
					},
				},
				Actions: []*ds.DataObjectTypeAction{
					{
						Action:        "SELECT",
						GlobalActions: []string{ds.Read},
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
				Permissions: []*ds.DataObjectTypePermission{
					{
						Permission:        "IMPORTED PRIVILEGES",
						Description:       "Enables roles other than the owning role to access a shared database; applies only to shared databases.",
						GlobalPermissions: ds.ReadGlobalPermission().StringValues(),
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
				Permissions: []*ds.DataObjectTypePermission{},
				Children:    []string{"shared-" + ds.Column},
			},
			{
				Name: "shared-" + ds.Column,
				Type: ds.Column,
			},
		},
		UsageMetaInfo: &ds.UsageMetaInput{
			DefaultLevel: "table",
			Levels: []*ds.UsageMetaInputDetail{
				{
					Name:            "table",
					DataObjectTypes: []string{"table", "view"},
				},
				{
					Name:            "schema",
					DataObjectTypes: []string{"schema"},
				},
			},
		},
	}, nil
}
