package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFullNameWithoutSpecialChars(t *testing.T) {

	databaseName := "db"
	schemaName := "schema"
	tableName := "table"
	columnName := "column"

	object := SnowflakeObject{&databaseName, nil, nil, nil}
	assert.Equal(t, "db", object.getFullName(false))
	assert.Equal(t, `"db"`, object.getFullName(true))

	object.Schema = &schemaName
	assert.Equal(t, "db.schema", object.getFullName(false))
	assert.Equal(t, `"db"."schema"`, object.getFullName(true))

	object.Table = &tableName
	assert.Equal(t, "db.schema.table", object.getFullName(false))
	assert.Equal(t, `"db"."schema"."table"`, object.getFullName(true))

	object.Column = &columnName
	assert.Equal(t, "db.schema.table.column", object.getFullName(false))
	assert.Equal(t, `"db"."schema"."table"."column"`, object.getFullName(true))
}

func TestFullNameWithSpecialChars(t *testing.T) {

	databaseName := "db🫘"
	schemaName := "🛟schema"
	tableName := "ta🥹ble"
	columnName := "c🫶olumn"

	object := SnowflakeObject{&databaseName, nil, nil, nil}
	assert.Equal(t, "db🫘", object.getFullName(false))
	assert.Equal(t, `"db🫘"`, object.getFullName(true))

	object.Schema = &schemaName
	assert.Equal(t, "db🫘.🛟schema", object.getFullName(false))
	assert.Equal(t, `"db🫘"."🛟schema"`, object.getFullName(true))

	object.Table = &tableName
	assert.Equal(t, "db🫘.🛟schema.ta🥹ble", object.getFullName(false))
	assert.Equal(t, `"db🫘"."🛟schema"."ta🥹ble"`, object.getFullName(true))

	object.Column = &columnName
	assert.Equal(t, "db🫘.🛟schema.ta🥹ble.c🫶olumn", object.getFullName(false))
	assert.Equal(t, `"db🫘"."🛟schema"."ta🥹ble"."c🫶olumn"`, object.getFullName(true))
}

func TestSchemaQuery(t *testing.T) {
	databaseName := "db🫘"
	assert.Equal(t, `SHOW SCHEMAS IN DATABASE "db🫘"`, getSchemasInDatabaseQuery(databaseName))
}

func TestTableQuery(t *testing.T) {
	databaseName := "db🫘"
	schemaName := "🛟schema"
	assert.Equal(t, `SHOW TABLES IN SCHEMA "db🫘"."🛟schema"`, getTablesInSchemaQuery(SnowflakeObject{&databaseName, &schemaName, nil, nil}, "TABLES"))
}

func TestViewsQuery(t *testing.T) {
	databaseName := "db🫘"
	schemaName := "🛟schema"
	assert.Equal(t, `SHOW VIEWS IN SCHEMA "db🫘"."🛟schema"`, getTablesInSchemaQuery(SnowflakeObject{&databaseName, &schemaName, nil, nil}, "VIEWS"))
}

func TestColumnsQuery(t *testing.T) {
	databaseName := "db🫘"
	schemaName := "🛟schema"
	tableName := "ta🥹ble"
	assert.Equal(t, `SHOW COLUMNS IN TABLE "db🫘"."🛟schema"."ta🥹ble"`, getColumnsInTableQuery(SnowflakeObject{&databaseName, &schemaName, &tableName, nil}))
}

func TestFullNameParser(t *testing.T) {

	var databaseName, schemaName, tableName, columnName string

	databaseName = "db🫘"
	schemaName = "🛟schema"
	tableName = "ta🥹ble"
	columnName = "c🫶olumn"

	assert.EqualValues(t, SnowflakeObject{&databaseName, &schemaName, &tableName, &columnName}, ParseFullName(`"db🫘"."🛟schema"."ta🥹ble"."c🫶olumn"`))

	databaseName = "db🫘"
	schemaName = `🛟sche"ma`
	tableName = "ta🥹ble"
	columnName = "c🫶olumn"
	assert.Equal(t, SnowflakeObject{&databaseName, &schemaName, &tableName, &columnName}, ParseFullName(`"db🫘"."🛟sche"ma"."ta🥹ble"."c🫶olumn"`))

	// databaseName = "d``''b🫘"
	// schemaName = `🛟sc"he"ma`
	// tableName = "ta🥹b...le"
	// columnName = `c🫶o,?lu"mn`
	// assert.Equal(t, SnowflakeObject{&databaseName, &schemaName, &tableName, &columnName}, ParseFullName("\"d``''b🫘\".\"🛟sc\"he\"ma\".\"ta🥹b...le\".\"c🫶o,?lu\"mn\""))

	databaseName = "db"
	schemaName = `schema`
	tableName = "table"
	columnName = `column`
	assert.Equal(t, SnowflakeObject{&databaseName, &schemaName, &tableName, &columnName}, ParseFullName(`db.schema.table.column`))

	// databaseName = "db"
	// schemaName = `schema`
	// tableName = `."table`
	// columnName = `column`
	// assert.Equal(t, SnowflakeObject{&databaseName, &schemaName, &tableName, &columnName}, ParseFullName(`"db"."schema"."."table"."column"`))

	databaseName = "db🫘"
	schemaName = `🛟schema`
	tableName = `table`
	assert.Equal(t, SnowflakeObject{&databaseName, &schemaName, &tableName, nil}, ParseFullName(`"db🫘"."🛟schema".table`))

	// TODO; cover every possible case
	// databaseName = "db"
	// schemaName = `schema`
	// tableName = `."t"."able`
	// columnName = `column`
	// assert.Equal(t, SnowflakeObject{&databaseName, &schemaName, &tableName, &columnName}, ParseQuotedFullName(`"db"."schema"."."."t"."able"."column"`))
}
