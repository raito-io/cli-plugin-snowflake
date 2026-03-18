package snowflake

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchemaQuery(t *testing.T) {
	databaseName := "db🫘"
	assert.Equal(t, `SELECT * FROM "db🫘".INFORMATION_SCHEMA.SCHEMATA`, getSchemasInDatabaseQuery(databaseName))
}

func TestTableQuery(t *testing.T) {
	databaseName := "db🫘"
	schemaName := "🛟schema"
	assert.Equal(t, `SELECT * FROM "db🫘".INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '🛟schema'`, getTablesInDatabaseQuery(databaseName, schemaName))
}

func TestColumnsQuery(t *testing.T) {
	databaseName := "db🫘"
	assert.Equal(t, `SELECT * FROM "db🫘".INFORMATION_SCHEMA.COLUMNS`, getColumnsInDatabaseQuery(databaseName))
}

func TestParseFunctionOrProcedureSignature(t *testing.T) {
	tt := []struct {
		name           string
		signature      string
		expectedResult string
		errorContains  string
	}{
		{
			name:           "simple function",
			signature:      "FUNCTION_NAME()",
			expectedResult: "()",
		},
		{
			name:           "function with parameters",
			signature:      "SUM3(NUMBER, NUMBER, NUMBER) RETURN NUMBER",
			expectedResult: "(NUMBER, NUMBER, NUMBER)",
		},
		{
			name:           "function with nested parentheses",
			signature:      "SUM3(NUMBER(38,0), NUMBER(38,0), VARCHAR(255)) RETURN NUMBER",
			expectedResult: "(NUMBER(38,0), NUMBER(38,0), VARCHAR(255))",
		},
		{
			name:          "no signature",
			signature:     "FUNCTION_NAME",
			errorContains: "signature has no opening parenthesis",
		},
		{
			name:          "signature not properly closed",
			signature:     "FUNCTION_NAME(VARCHAR() RETURN NUMBER",
			errorContains: "no matching closing parenthesis",
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			result, err := parseFunctionOrProcedureSignature(tc.signature)
			if tc.errorContains != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errorContains)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedResult, result)
			}
		})
	}
}