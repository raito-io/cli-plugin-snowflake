package snowflake

import (
	"strings"

	ds "github.com/raito-io/cli/base/data_source"
)

// raitoTypeToSnowflakeGrantType maps the Raito data objects types for tabular data onto the Snowflake names
var raitoTypeToSnowflakeGrantType = map[string]string{
	ds.Table:          "TABLE",
	ds.View:           "VIEW",
	MaterializedView:  "VIEW",
	ExternalTable:     "EXTERNAL TABLE",
	"shared-database": "DATABASE",
	"shared-table":    "TABLE",
	"shared-view":     "VIEW",
	"shared-schema":   "SCHEMA",
}

func isTableType(t string) bool {
	return t == ds.Table || t == ds.View || t == MaterializedView || t == ExternalTable || t == IcebergTable
}

// convertSnowflakeTableTypeToRaito maps the Snowflake types coming from the INFORMATION_SCHEMA views to the corresponding Raito type
// If unknown, it returns a lower case version of the input
func convertSnowflakeTableTypeToRaito(entity *TableEntity) string {
	if raitoType, f2 := snowflakeTableTypeToRaito[entity.TableType]; f2 {
		if raitoType == ds.Table && entity.IsIceberg() {
			return IcebergTable
		}

		return raitoType
	}

	return strings.ToLower(entity.TableType)
}

var snowflakeTableTypeToRaito = map[string]string{
	"BASE TABLE":        ds.Table,
	"VIEW":              ds.View,
	"MATERIALIZED VIEW": MaterializedView,
	"EXTERNAL TABLE":    ExternalTable,
}

// convertSnowflakeGrantTypeToRaito maps the Snowflake types coming from the SHOW GRANTS queries to the corresponding Raito type
// If unknown, it returns a lower case version of the input
func convertSnowflakeGrantTypeToRaito(sfGrant string) string {
	if raitoType, f2 := snowflakeGrantTypeToRaito[sfGrant]; f2 {
		return raitoType
	}

	return strings.ToLower(sfGrant)
}

var snowflakeGrantTypeToRaito = map[string]string{
	"TABLE":             ds.Table,
	"VIEW":              ds.View,
	"DATABASE":          ds.Database,
	"SCHEMA":            ds.Schema,
	"WAREHOUSE":         "warehouse",
	"MATERIALIZED_VIEW": MaterializedView,
	"EXTERNAL_TABLE":    ExternalTable,
}
