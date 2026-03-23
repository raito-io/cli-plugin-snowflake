package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFullNameWithoutSpecialChars(t *testing.T) {

	databaseName := "db"
	schemaName := "schema"
	tableName := "table"
	columnName := "column"

	object := SnowflakeObject{&databaseName, nil, nil, nil}
	assert.Equal(t, "db", object.GetFullName(false))
	assert.Equal(t, `db`, object.GetFullName(true))

	object.Schema = &schemaName
	assert.Equal(t, "db.schema", object.GetFullName(false))
	assert.Equal(t, `db.schema`, object.GetFullName(true))

	object.Table = &tableName
	assert.Equal(t, "db.schema.table", object.GetFullName(false))
	assert.Equal(t, `db.schema.table`, object.GetFullName(true))

	object.Column = &columnName
	assert.Equal(t, "db.schema.table.column", object.GetFullName(false))
	assert.Equal(t, `db.schema.table.column`, object.GetFullName(true))

	columnName = `column"_$123`
	object.Column = &columnName
	assert.Equal(t, `db.schema.table.column"_$123`, object.GetFullName(false))
	assert.Equal(t, `db.schema.table."column""_$123"`, object.GetFullName(true))
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

	fullName = `MASTER_DATA.PUBLIC."DECRYPTIT(VAL VARCHAR, ENCRYPTIONTYPE VARCHAR):VARCHAR(16777216)"`
	databaseName = "MASTER_DATA"
	schemaName = `PUBLIC`
	tableName = `DECRYPTIT(VAL VARCHAR, ENCRYPTIONTYPE VARCHAR):VARCHAR(16777216)`
	assert.EqualValues(t, SnowflakeObject{&databaseName, &schemaName, &tableName, nil}, ParseFullName(fullName))

	fullName = "\"d``''b🫘\".\"🛟sc\"\"he\"\"ma\".\"ta🥹b...le\".\"c🫶o,?lu\"\"mn\""
	databaseName = "d``''b🫘"
	schemaName = `🛟sc"he"ma`
	tableName = "ta🥹b...le"
	columnName = `c🫶o,?lu"mn`
	assert.EqualValues(t, SnowflakeObject{&databaseName, &schemaName, &tableName, &columnName}, ParseFullName(fullName))

}

func TestSplit(t *testing.T) {
	type testCase struct {
		input    string
		expected []string
		hasError bool
	}

	tests := []testCase{
		{
			input:    "A.B.C.D.E.F",
			expected: []string{"A", "B", "C", "D", "E", "F"},
		},
		{
			input:    `ONE."TWO".THREE."FOUR".FIVE`,
			expected: []string{"ONE", `"TWO"`, "THREE", `"FOUR"`, "FIVE"},
		},
		{
			input:    `ONE."TWO".THREE."FOUR".FIVE."""SIX"."SEVEN""""EIGHT"""`,
			expected: []string{"ONE", `"TWO"`, "THREE", `"FOUR"`, "FIVE", `"""SIX"`, `"SEVEN""""EIGHT"""`},
		},
		{
			input:    `ONE."TWO".THREE."FOUR"."""...""""."".""."""""".".FIVE."""SIX"."SEVEN""""EIGHT"""`,
			expected: []string{"ONE", `"TWO"`, "THREE", `"FOUR"`, `"""..."""".""."".""""""."`, "FIVE", `"""SIX"`, `"SEVEN""""EIGHT"""`},
		},
		{
			input:    `ONE."TWO".THREE."FOUR".""".,.""|""."".""."""""".".FIVE."""SIX"."SEVEN""""EIGHT"""`,
			expected: []string{"ONE", `"TWO"`, "THREE", `"FOUR"`, `""".,.""|"".""."".""""""."`, "FIVE", `"""SIX"`, `"SEVEN""""EIGHT"""`},
		},
		{
			input:    `"db🫘"."🛟schema"."ta🥹ble"."c🫶olumn"`,
			expected: []string{`"db🫘"`, `"🛟schema"`, `"ta🥹ble"`, `"c🫶olumn"`},
		},
		// Quoted function name with arg list
		{
			input:    `RAITO_DATABASE.ORDERING."decrypt"(VARCHAR)`,
			expected: []string{"RAITO_DATABASE", "ORDERING", `"decrypt"(VARCHAR)`},
		},
		// Unquoted function name with arg list (regression)
		{
			input:    `RAITO_DATABASE.ORDERING.decrypt(VARCHAR)`,
			expected: []string{"RAITO_DATABASE", "ORDERING", "decrypt(VARCHAR)"},
		},
		// Nested parens in arg list
		{
			input:    `DB.SCHEMA."func"(TABLE(VARCHAR), NUMBER)`,
			expected: []string{"DB", "SCHEMA", `"func"(TABLE(VARCHAR), NUMBER)`},
		},
		// Quoted function + more tokens after
		{
			input:    `DB.SCHEMA."func"(VARCHAR).col`,
			expected: []string{"DB", "SCHEMA", `"func"(VARCHAR)`, "col"},
		},
		// Badly formatted Snowflake strings
		{
			input:    "A.B.C.D.E.F.",
			hasError: true,
		},
		{
			input:    "A.B.C.D.E.\"F",
			hasError: true,
		},
		{
			input:    `A.B.C.D.E."LAST"aaa`,
			hasError: true,
		},
		{
			input:    `A.B.C.D.E."LAST"aaa.a`,
			hasError: true,
		},
		// Unbalanced parens in arg list
		{
			input:    `DB.SCHEMA."func"(VARCHAR`,
			hasError: true,
		},
	}

	for _, tc := range tests {
		res, err := splitFullName(tc.input, nil)
		if tc.hasError {
			require.Error(t, err, "input: %s", tc.input)
		} else {
			require.NoError(t, err, "input: %s", tc.input)
			assert.ElementsMatch(t, tc.expected, res, "input: %s", tc.input)
		}
	}
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
	test = []string{"12AAA_ab_cd", "AAAA!", "test-this", `"tst_something`, `testRole"."Yes!`}

	for _, testName := range test {
		assert.False(t, isSimpleSnowflakeName(testName))
	}
}
