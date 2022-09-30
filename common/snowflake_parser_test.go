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

	var databaseName, schemaName, tableName, columnName, fullName string
	// var databaseName, schemaName, tableName, fullName string

	fullName = `"db🫘"."🛟schema"."ta🥹ble"."c🫶olumn"`
	databaseName = "db🫘"
	schemaName = "🛟schema"
	tableName = "ta🥹ble"
	columnName = "c🫶olumn"

	assert.EqualValues(t, SnowflakeObject{&databaseName, &schemaName, &tableName, &columnName}, ParseFullName(fullName))

	fullName = `"db🫘"."🛟sche""ma"."ta🥹ble"."c🫶olumn"`
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

	fullName = `"EXTERNAL_TEST🩻".PUBLIC_DATASETS."C1.""C2"".C3"".""END."`
	databaseName = "EXTERNAL_TEST🩻"
	schemaName = `PUBLIC_DATASETS`
	tableName = `C1."C2".C3"."END.`
	assert.EqualValues(t, SnowflakeObject{&databaseName, &schemaName, &tableName, nil}, ParseFullName(fullName))

	fullName = `"EXTERNAL_TEST🩻".PUBLIC_DATASETS."ADULT"".""_TABLE"`
	databaseName = "EXTERNAL_TEST🩻"
	schemaName = `PUBLIC_DATASETS`
	tableName = `ADULT"."_TABLE`
	assert.EqualValues(t, SnowflakeObject{&databaseName, &schemaName, &tableName, nil}, ParseFullName(fullName))

	fullName = "\"d``''b🫘\".\"🛟sc\"\"he\"\"ma\".\"ta🥹b...le\".\"c🫶o,?lu\"\"mn\""
	databaseName = "d``''b🫘"
	schemaName = `🛟sc"he"ma`
	tableName = "ta🥹b...le"
	columnName = `c🫶o,?lu"mn`
	assert.EqualValues(t, SnowflakeObject{&databaseName, &schemaName, &tableName, &columnName}, ParseFullName(fullName))

}

func TestSplit(t *testing.T) {

	var test string
	var expected, res []string
	var err error

	test = "A.B.C.D.E.F"
	expected = []string{"A", "B", "C", "D", "E", "F"}
	res, err = splitFullName(test, nil, nil)
	assert.Nil(t, err)
	assert.ElementsMatch(t, expected, res)

	test = `ONE."TWO".THREE."FOUR".FIVE`
	expected = []string{"ONE", `"TWO"`, "THREE", `"FOUR"`, "FIVE"}
	res, err = splitFullName(test, nil, nil)
	assert.Nil(t, err)
	assert.ElementsMatch(t, expected, res)

	test = `ONE."TWO".THREE."FOUR".FIVE."""SIX"."SEVEN""""EIGHT"""`
	expected = []string{"ONE", `"TWO"`, "THREE", `"FOUR"`, "FIVE", `"""SIX"`, `"SEVEN""""EIGHT"""`}
	res, err = splitFullName(test, nil, nil)
	assert.Nil(t, err)
	assert.ElementsMatch(t, expected, res)

	test = `ONE."TWO".THREE."FOUR"."""...""""."".""."""""".".FIVE."""SIX"."SEVEN""""EIGHT"""`
	expected = []string{"ONE", `"TWO"`, "THREE", `"FOUR"`, `"""..."""".""."".""""""."`, "FIVE", `"""SIX"`, `"SEVEN""""EIGHT"""`}
	res, err = splitFullName(test, nil, nil)
	assert.Nil(t, err)
	assert.ElementsMatch(t, expected, res)

	test = `ONE."TWO".THREE."FOUR".""".,.""|""."".""."""""".".FIVE."""SIX"."SEVEN""""EIGHT"""`
	expected = []string{"ONE", `"TWO"`, "THREE", `"FOUR"`, `""".,.""|"".""."".""""""."`, "FIVE", `"""SIX"`, `"SEVEN""""EIGHT"""`}
	res, err = splitFullName(test, nil, nil)
	assert.Nil(t, err)
	assert.ElementsMatch(t, expected, res)

	test = `"db🫘"."🛟schema"."ta🥹ble"."c🫶olumn"`
	expected = []string{`"db🫘"`, `"🛟schema"`, `"ta🥹ble"`, `"c🫶olumn"`}
	res, err = splitFullName(test, nil, nil)
	assert.Nil(t, err)
	assert.ElementsMatch(t, expected, res)

	// Badly formatted Snowflake strings

	test = "A.B.C.D.E.F."
	expected = []string{"A", "B", "C", "D", "E", "F"}
	res, err = splitFullName(test, nil, nil)
	assert.NotNil(t, err)

	test = "A.B.C.D.E.\"F"
	expected = []string{"A", "B", "C", "D", "E", "F"}
	res, err = splitFullName(test, nil, nil)
	assert.NotNil(t, err)

	test = `A.B.C.D.E."LAST"aaa`
	expected = []string{"A", "B", "C", "D", "E", `"LAST"aaa`}
	res, err = splitFullName(test, nil, nil)
	assert.NotNil(t, err)

	test = `A.B.C.D.E."LAST"aaa.a`
	expected = []string{"A", "B", "C", "D", "E", `"LAST"aaa.a`}
	res, err = splitFullName(test, nil, nil)
	assert.NotNil(t, err)

}

func TestFindNextQuote(t *testing.T) {

	res := findNextStandaloneChar(`dkdkd"""."ADULT"`, `"`)
	assert.Equal(t, 7, res)

	res = findNextStandaloneChar(`dkdkd"""""."ADULT"`, `"`)
	assert.Equal(t, 9, res)

	res = findNextStandaloneChar(`d ""kdkd"""."ADULT"`, `"`)
	assert.Equal(t, 10, res)

	res = findNextStandaloneChar(`d ""kdkddf"."ADULT"`, `"`)
	assert.Equal(t, 10, res)

	res = findNextStandaloneChar(`d ""kdkddf.`, `"`)
	assert.Equal(t, -1, res)

	res = findNextStandaloneChar(`d ""kdkddf."`, `"`)
	assert.Equal(t, 11, res)
}

func TestSimpleSnowflakeName(t *testing.T) {
	var test []string

	// simple names
	test = []string{"abcd", "_ab_cd", "AAA_ab_cd"}

	for _, testName := range test {
		assert.True(t, isSimpleSnowflakeName(testName))
	}

	// non-simple names
	test = []string{"12AAA_ab_cd", "AAAA!", "test-this", `"tst_something`}

	for _, testName := range test {
		assert.False(t, isSimpleSnowflakeName(testName))
	}
}
