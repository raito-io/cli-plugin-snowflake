package snowflake

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSchemaQuery(t *testing.T) {
	databaseName := "db🫘"
	assert.Equal(t, `SELECT * FROM "db🫘".INFORMATION_SCHEMA.SCHEMATA`, getSchemasInDatabaseQuery(databaseName))
}

func TestTableQuery(t *testing.T) {
	databaseName := "db🫘"
	schemaName := "🛟schema"
	assert.Equal(t, `SELECT * FROM "db🫘".INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE != 'VIEW' AND TABLE_SCHEMA = '🛟schema'`, getTablesInDatabaseQuery(databaseName, schemaName))
}

func TestViewsQuery(t *testing.T) {
	databaseName := "db🫘"
	schemaName := "🛟schema"
	assert.Equal(t, `SELECT * FROM "db🫘".INFORMATION_SCHEMA.VIEWS  WHERE TABLE_SCHEMA = '🛟schema'`, getViewsInDatabaseQuery(databaseName, schemaName))
}

func TestColumnsQuery(t *testing.T) {
	databaseName := "db🫘"
	assert.Equal(t, `SELECT * FROM "db🫘".INFORMATION_SCHEMA.COLUMNS`, getColumnsInDatabaseQuery(databaseName))
}
