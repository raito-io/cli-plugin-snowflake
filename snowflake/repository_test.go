package snowflake

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/raito-io/cli-plugin-snowflake/common"
)

func TestSchemaQuery(t *testing.T) {
	databaseName := "db🫘"
	assert.Equal(t, `SHOW SCHEMAS IN DATABASE "db🫘"`, getSchemasInDatabaseQuery(databaseName))
}

func TestTableQuery(t *testing.T) {
	databaseName := "db🫘"
	schemaName := "🛟schema"
	assert.Equal(t, `SHOW TABLES IN SCHEMA "db🫘"."🛟schema"`, getTablesInSchemaQuery(&common.SnowflakeObject{Database: &databaseName, Schema: &schemaName}, "TABLES"))
}

func TestViewsQuery(t *testing.T) {
	databaseName := "db🫘"
	schemaName := "🛟schema"
	assert.Equal(t, `SHOW VIEWS IN SCHEMA "db🫘"."🛟schema"`, getTablesInSchemaQuery(&common.SnowflakeObject{Database: &databaseName, Schema: &schemaName}, "VIEWS"))
}

func TestColumnsQuery(t *testing.T) {
	databaseName := "db🫘"
	schemaName := "🛟schema"
	tableName := "ta🥹ble"
	assert.Equal(t, `SHOW COLUMNS IN TABLE "db🫘"."🛟schema"."ta🥹ble"`, getColumnsInTableQuery(&common.SnowflakeObject{Database: &databaseName, Schema: &schemaName, Table: &tableName}))
}
