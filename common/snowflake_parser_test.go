package common

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
	assert.Equal(t, "db", object.GetFullName(false))
	assert.Equal(t, `"db"`, object.GetFullName(true))

	object.Schema = &schemaName
	assert.Equal(t, "db.schema", object.GetFullName(false))
	assert.Equal(t, `"db"."schema"`, object.GetFullName(true))

	object.Table = &tableName
	assert.Equal(t, "db.schema.table", object.GetFullName(false))
	assert.Equal(t, `"db"."schema"."table"`, object.GetFullName(true))

	object.Column = &columnName
	assert.Equal(t, "db.schema.table.column", object.GetFullName(false))
	assert.Equal(t, `"db"."schema"."table"."column"`, object.GetFullName(true))
}

func TestFullNameWithSpecialChars(t *testing.T) {

	databaseName := "db🫘"
	schemaName := "🛟schema"
	tableName := "ta🥹ble"
	columnName := "c🫶olumn"

	object := SnowflakeObject{&databaseName, nil, nil, nil}
	assert.Equal(t, "db🫘", object.GetFullName(false))
	assert.Equal(t, `"db🫘"`, object.GetFullName(true))

	object.Schema = &schemaName
	assert.Equal(t, "db🫘.🛟schema", object.GetFullName(false))
	assert.Equal(t, `"db🫘"."🛟schema"`, object.GetFullName(true))

	object.Table = &tableName
	assert.Equal(t, "db🫘.🛟schema.ta🥹ble", object.GetFullName(false))
	assert.Equal(t, `"db🫘"."🛟schema"."ta🥹ble"`, object.GetFullName(true))

	object.Column = &columnName
	assert.Equal(t, "db🫘.🛟schema.ta🥹ble.c🫶olumn", object.GetFullName(false))
	assert.Equal(t, `"db🫘"."🛟schema"."ta🥹ble"."c🫶olumn"`, object.GetFullName(true))
}

func TestFullNameParser(t *testing.T) {

	// parsing logic: https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html
	// single data objects (database, schema, table, ...) can be double quoted or not
	// if not double quoted, no special characters allowed. Keep in mind you can have a full name with some fields quoted, others not
	// if double quoted, all unicode characters are allowed. A double quote in the name (`"`) is encoded as a double double quote (`""`),
	// therefore, double quotes are allowed at the beginning and end, but otherwise they always need to come in pairs (`""`).
	// Dots are ignored as a data object/field separator until the field-delimiting double quote has passed.

	var databaseName, schemaName, tableName, columnName, fullName string
	// var databaseName, schemaName, tableName, fullName string

	fullName = `"db🫘"."🛟schema"."ta🥹ble"."c🫶olumn"`
	databaseName = "db🫘"
	schemaName = "🛟schema"
	tableName = "ta🥹ble"
	columnName = "c🫶olumn"

	assert.EqualValues(t, SnowflakeObject{&databaseName, &schemaName, &tableName, &columnName}, ParseFullName(fullName))

	fullName = `"db🫘"."🛟sche"ma"."ta🥹ble"."c🫶olumn"`
	databaseName = "db🫘"
	schemaName = `🛟sche"ma`
	tableName = "ta🥹ble"
	columnName = "c🫶olumn"
	assert.EqualValues(t, SnowflakeObject{&databaseName, &schemaName, &tableName, &columnName}, ParseFullName(fullName))

	fullName = `db.schema.table.column`
	databaseName = "db"
	schemaName = `schema`
	tableName = "table"
	columnName = `column`
	assert.EqualValues(t, SnowflakeObject{&databaseName, &schemaName, &tableName, &columnName}, ParseFullName(fullName))

	fullName = `"db🫘"."🛟schema".table`
	databaseName = "db🫘"
	schemaName = `🛟schema`
	tableName = `table`
	assert.EqualValues(t, SnowflakeObject{&databaseName, &schemaName, &tableName, nil}, ParseFullName(fullName))

	fullName = `"EXTERNAL_TEST🩻"."""PUBLIC_DATASETS"""."ADULT"`
	databaseName = "EXTERNAL_TEST🩻"
	schemaName = `"PUBLIC_DATASETS"`
	tableName = `ADULT`
	assert.EqualValues(t, SnowflakeObject{&databaseName, &schemaName, &tableName, nil}, ParseFullName(fullName))

	fullName = `"EXTERNAL_TEST🩻".PUBLIC_DATASETS."AD""ULT"`
	databaseName = "EXTERNAL_TEST🩻"
	schemaName = `PUBLIC_DATASETS`
	tableName = `AD"ULT`
	res := ParseFullName(fullName)
	assert.EqualValues(t, SnowflakeObject{&databaseName, &schemaName, &tableName, nil}, res)

	// TODO, more difficult cases

	// fullName = `"EXTERNAL_TEST🩻".PUBLIC_DATASETS."C1.""C2"".C3"".""END."`
	// databaseName = "EXTERNAL_TEST🩻"
	// schemaName = `PUBLIC_DATASETS`
	// tableName = `C1."C2".C3"."END.`
	// assert.EqualValues(t, SnowflakeObject{&databaseName, &schemaName, &tableName, nil}, ParseFullName(fullName))

	// fullName = `"EXTERNAL_TEST🩻".PUBLIC_DATASETS."ADULT"".""_TABLE"`
	// databaseName = "EXTERNAL_TEST🩻"
	// schemaName = `PUBLIC_DATASETS`
	// tableName = `ADULT"."_TABLE`
	// assert.EqualValues(t, SnowflakeObject{&databaseName, &schemaName, &tableName, nil}, ParseFullName(fullName))

	// databaseName = "d``''b🫘"
	// schemaName = `🛟sc"he"ma`
	// tableName = "ta🥹b...le"
	// columnName = `c🫶o,?lu"mn`
	// assert.EqualValues(t, SnowflakeObject{&databaseName, &schemaName, &tableName, &columnName}, ParseFullName("\"d``''b🫘\".\"🛟sc\"he\"ma\".\"ta🥹b...le\".\"c🫶o,?lu\"mn\""))

}
