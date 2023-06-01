package snowflake

import (
	"context"

	ds "github.com/raito-io/cli/base/data_source"
)

const ExternalTable = "external-" + ds.Table
const MaterializedView = "materialized-" + ds.View

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
				Children: []string{ds.Table, ds.View, ExternalTable, MaterializedView},
			},
			{
				Name: ds.Table,
				Type: ds.Table,
				Permissions: []*ds.DataObjectTypePermission{
					{
						Permission:        "SELECT",
						Description:       "Enables executing a SELECT statement on a table.",
						GlobalPermissions: ds.ReadGlobalPermission().StringValues(),
					},
					{
						Permission:        "INSERT",
						Description:       "Enables executing an INSERT command on a table. Also enables using the ALTER TABLE command with a RECLUSTER clause to manually recluster a table with a clustering key.",
						GlobalPermissions: ds.InsertGlobalPermission().StringValues(),
					},
					{
						Permission:        "UPDATE",
						Description:       "Enables executing an UPDATE command on a table.",
						GlobalPermissions: ds.UpdateGlobalPermission().StringValues(),
					},
					{
						Permission:        "TRUNCATE",
						Description:       "Enables executing a TRUNCATE TABLE command on a table.",
						GlobalPermissions: ds.DeleteGlobalPermission().StringValues(),
					},
					{
						Permission:        "DELETE",
						Description:       "Enables executing a DELETE command on a table.",
						GlobalPermissions: ds.DeleteGlobalPermission().StringValues(),
					},
					{
						Permission:  "REFERENCES",
						Description: "Enables referencing a table as the unique/primary key table for a foreign key constraint. Also enables viewing the structure of a table (but not the data) via the DESCRIBE or SHOW command or by querying the Information Schema.",
					},
				},
				Children: []string{ds.Column},
			},
			{
				Name:  ExternalTable,
				Label: "External Table",
				Type:  ds.Table,
				Permissions: []*ds.DataObjectTypePermission{
					{
						Permission:        "SELECT",
						Description:       "Enables executing a SELECT statement on a table.",
						GlobalPermissions: ds.ReadGlobalPermission().StringValues(),
					},
				},
			},
			{
				Name: ds.View,
				Type: ds.View,
				Permissions: []*ds.DataObjectTypePermission{
					{
						Permission:        "SELECT",
						Description:       "Enables executing a SELECT statement on a view.",
						GlobalPermissions: ds.ReadGlobalPermission().StringValues(),
					},
					{
						Permission:  "REFERENCES",
						Description: "Enables viewing the structure of a view (but not the data) via the DESCRIBE or SHOW command or by querying the Information Schema.",
					},
				},
				Children: []string{ds.Column},
			},
			{
				Name:  MaterializedView,
				Label: "Materialized View",
				Type:  ds.View,
				Permissions: []*ds.DataObjectTypePermission{
					{
						Permission:        "SELECT",
						Description:       "Enables executing a SELECT statement on a view.",
						GlobalPermissions: ds.ReadGlobalPermission().StringValues(),
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
	}, nil
}
