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
						Permission:             "APPLY MASKING POLICY",
						Description:            "Grants ability to set a Column-level Security masking policy on a table or view column and to set a masking policy on a tag. This global privilege also allows executing the DESCRIBE operation on tables and views.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "APPLY ROW ACCESS POLICY",
						Description:            "Grants the ability to add and drop a row access policy on a table or view. This global privilege also allows executing the DESCRIBE operation on tables and views.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "APPLY SESSION POLICY",
						Description:            "Grants the ability to set or unset a session policy on an account or user.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "APPLY TAG",
						Description:            "Grants the ability to add or drop a tag on a Snowflake object.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "ATTACH POLICY",
						Description:            "Grants ability to activate a network policy by associating it with your account.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "CREATE ACCOUNT",
						Description:            "Enables a data provider to create a new managed account (i.e. reader account).",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "CREATE ROLE",
						Description:            "Enables creating a new role.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "CREATE USER",
						Description:            "Enables creating a new user.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "MANAGE GRANTS",
						Description:            "Enables granting or revoking privileges on objects for which the role is not the owner.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "CREATE DATA EXCHANGE LISTING",
						Description:            "Enables creating a new Data Exchange listing.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "CREATE INTEGRATION",
						Description:            "Enables creating a new notification, security, or storage integration.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "CREATE NETWORK POLICY",
						Description:            "Enables creating a new network policy.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "CREATE SHARE",
						Description:            "Enables a data provider to create a new share.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "CREATE WAREHOUSE",
						Description:            "Enables creating a new virtual warehouse.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
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
						Permission:             "MONITOR EXECUTION",
						Description:            "Grants ability to monitor any pipes or tasks in the account. The USAGE privilege is also required on each database and schema that stores these objects.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "MONITOR USAGE",
						Description:            "Grants ability to monitor account-level usage and historical information for databases and warehouses. Additionally grants ability to view managed accounts using SHOW MANAGED ACCOUNTS.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
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
						Permission:             "MODIFY",
						Description:            "Enables altering any properties of a warehouse, including changing its size. ",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "MONITOR",
						Description:            "Enables viewing current and past queries executed on a warehouse as well as usage statistics on that warehouse.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "OPERATE",
						Description:            "Enables changing the state of a warehouse (stop, start, suspend, resume). In addition, enables viewing current and past queries executed on a warehouse and aborting any executing queries.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:        "USAGE",
						Description:       "Enables using a virtual warehouse and, as a result, executing queries on the warehouse. If the warehouse is configured to auto-resume when a SQL statement (e.g. query) is submitted to it, the warehouse resumes automatically and executes the statement.",
						GlobalPermissions: ds.ReadGlobalPermission().StringValues(),
					},
				},
				Children: []string{},
			},
			{
				Name: ds.Database,
				Type: ds.Database,
				Permissions: []*ds.DataObjectTypePermission{
					{
						Permission:             "CREATE SCHEMA",
						Description:            "Enables creating a new schema in a database, including cloning a schema.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:        "USAGE",
						Description:       "Enables using a database, including returning the database details in the SHOW DATABASES command output. Additional privileges are required to view or take actions on objects in a database.",
						GlobalPermissions: ds.ReadGlobalPermission().StringValues(),
						CannotBeGranted:   true,
					},
					{
						Permission:             "MODIFY",
						Description:            "Enables altering any settings of a database.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "MONITOR",
						Description:            "Enables performing the DESCRIBE command on the database.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
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
						Permission:             "MODIFY",
						Description:            "Enables altering any settings of a schema.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "MONITOR",
						Description:            "Enables performing the DESCRIBE command on the schema.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:        "USAGE",
						Description:       "Enables using a schema, including returning the schema details in the SHOW SCHEMAS command output. To execute SHOW <objects> commands for objects (tables, views, stages, file formats, sequences, pipes, or functions) in the schema, a role must have at least one privilege granted on the object.",
						GlobalPermissions: []string{ds.Admin},
						CannotBeGranted:   true,
					},
					{
						Permission:             "CREATE TABLE",
						Description:            "Enables creating a new table in a schema, including cloning a table. Note that this privilege is not required to create temporary tables, which are scoped to the current user session and are automatically deleted when the session ends.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "CREATE EXTERNAL TABLE",
						Description:            "Enables creating a new external table in a schema.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "CREATE VIEW",
						Description:            "Enables creating a new view in a schema.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "CREATE MATERIALIZED VIEW",
						Description:            "Enables creating a new materialized view in a schema.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "CREATE MASKING POLICY",
						Description:            "Enables creating a new Column-level Security masking policy in a schema.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "CREATE ROW ACCESS POLICY",
						Description:            "Enables creating a new row access policy in a schema.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "CREATE SESSION POLICY",
						Description:            "Enables creating a new session policy in a schema.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "CREATE STAGE",
						Description:            "Enables creating a new stage in a schema, including cloning a stage.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "CREATE FILE FORMAT",
						Description:            "Enables creating a new file format in a schema, including cloning a file format.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "CREATE SEQUENCE",
						Description:            "Enables creating a new sequence in a schema, including cloning a sequence.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "CREATE FUNCTION",
						Description:            "Enables creating a new UDF or external function in a schema.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "CREATE PIPE",
						Description:            "Enables creating a new pipe in a schema.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "CREATE STREAM",
						Description:            "Enables creating a new stream in a schema, including cloning a stream.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "CREATE TAG",
						Description:            "Enables creating a new tag key in a schema.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "CREATE TASK",
						Description:            "Enables creating a new task in a schema, including cloning a task.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "CREATE PROCEDURE",
						Description:            "Enables creating a new stored procedure in a schema.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "ADD SEARCH OPTIMIZATION",
						Description:            "Enables adding search optimization to a table in a schema.",
						GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "OWNERSHIP",
						Description:            "Grants full control over the schema. Only a single role can hold this privilege on a specific object at a time.",
						UsageGlobalPermissions: []string{ds.Read, ds.Write, ds.Admin},
						CannotBeGranted:        true,
					},
				},
				Children: []string{ds.Table, ds.View, ExternalTable, MaterializedView},
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
						Description:            "Enables referencing a table as the unique/primary key table for a foreign key constraint. Also enables viewing the structure of a table (but not the data) via the DESCRIBE or SHOW command or by querying the Information Schema.",
						UsageGlobalPermissions: []string{ds.Admin},
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
				Name:  ExternalTable,
				Label: "External Table",
				Type:  ds.Table,
				Permissions: []*ds.DataObjectTypePermission{
					{
						Permission:             "SELECT",
						Description:            "Enables executing a SELECT statement on a table.",
						GlobalPermissions:      ds.ReadGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Read},
					},
					{
						Permission:             "OWNERSHIP",
						Description:            "Grants full control over the external table. Only a single role can hold this privilege on a specific object at a time.",
						UsageGlobalPermissions: []string{ds.Read, ds.Write, ds.Admin},
						CannotBeGranted:        true,
					},
				},
				Actions: []*ds.DataObjectTypeAction{
					{
						Action:        "SELECT",
						GlobalActions: []string{ds.Read},
					},
				},
			},
			{
				Name: ds.View,
				Type: ds.View,
				Permissions: []*ds.DataObjectTypePermission{
					{
						Permission:             "SELECT",
						Description:            "Enables executing a SELECT statement on a view.",
						GlobalPermissions:      ds.ReadGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Read},
					},
					{
						Permission:             "REFERENCES",
						Description:            "Enables viewing the structure of a view (but not the data) via the DESCRIBE or SHOW command or by querying the Information Schema.",
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "OWNERSHIP",
						Description:            "Grants full control over the view. Only a single role can hold this privilege on a specific object at a time.",
						UsageGlobalPermissions: []string{ds.Read, ds.Write, ds.Admin},
						CannotBeGranted:        true,
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
				Name:  MaterializedView,
				Label: "Materialized View",
				Type:  ds.View,
				Permissions: []*ds.DataObjectTypePermission{
					{
						Permission:             "SELECT",
						Description:            "Enables executing a SELECT statement on a view.",
						GlobalPermissions:      ds.ReadGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Read},
					},
					{
						Permission:             "REFERENCES",
						Description:            "Enables viewing the structure of a view (but not the data) via the DESCRIBE or SHOW command or by querying the Information Schema.",
						UsageGlobalPermissions: []string{ds.Admin},
					},
					{
						Permission:             "OWNERSHIP",
						Description:            "Grants full control over the materialized view. Only a single role can hold this privilege on a specific object at a time.",
						UsageGlobalPermissions: []string{ds.Read, ds.Write, ds.Admin},
						CannotBeGranted:        true,
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
				Name: "shared-" + ds.Table,
				Type: ds.Table,
				Permissions: []*ds.DataObjectTypePermission{
					{
						Permission:             "SELECT",
						Description:            "Enables executing a SELECT statement on a table.",
						GlobalPermissions:      ds.ReadGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Read},
					},
				},
				Actions: []*ds.DataObjectTypeAction{
					{
						Action:        "SELECT",
						GlobalActions: []string{ds.Read},
					},
				},
				Children: []string{"shared-" + ds.Column},
			},
			{
				Name: "shared-" + ds.View,
				Type: ds.View,
				Permissions: []*ds.DataObjectTypePermission{
					{
						Permission:             "SELECT",
						Description:            "Enables executing a SELECT statement on a view.",
						GlobalPermissions:      ds.ReadGlobalPermission().StringValues(),
						UsageGlobalPermissions: []string{ds.Read},
					},
				},
				Actions: []*ds.DataObjectTypeAction{
					{
						Action:        "SELECT",
						GlobalActions: []string{ds.Read},
					},
				},
				Children: []string{"shared-" + ds.Column},
			},
			{
				Name: "shared-" + ds.Column,
				Type: ds.Column,
			},
		},
		UsageMetaInfo: &ds.UsageMetaInput{
			DefaultLevel: ds.Table,
			Levels: []*ds.UsageMetaInputDetail{
				{
					Name:            ds.Table,
					DataObjectTypes: []string{ds.Table, ds.View, ExternalTable, MaterializedView, "shared-" + ds.Table, "shared-" + ds.View},
				},
			},
		},
	}, nil
}
