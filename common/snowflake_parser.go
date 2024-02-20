package common

import (
	"fmt"
	"regexp"
	"strings"
)

// SnowflakeObject represents (parsed) Snowflake objects
type SnowflakeObject struct {
	Database *string `json:"database"`
	Schema   *string `json:"schema"`
	Table    *string `json:"table"`
	Column   *string `json:"column"`
}

// Retrieve the 'full name' of the Snowflake object (i.e. database.schema.table). In some cases
// they need to be quoted, e.g. Snowflake queries, but in other cases they never need the outer
// double quotes, e.g. Raito Cloud. `withQuotes` controls this.
func (s SnowflakeObject) GetFullName(withQuotes bool) string {
	fullName := ""
	formatString := "%s.%s"

	if s.Database == nil {
		return fullName
	}

	fullName = convertToValidSnowflakeResource(*s.Database, withQuotes)

	if s.Schema == nil {
		return fullName
	}

	fullName = fmt.Sprintf(formatString, fullName, convertToValidSnowflakeResource(*s.Schema, withQuotes))

	if s.Table == nil {
		return fullName
	}

	fullName = fmt.Sprintf(formatString, fullName, convertToValidSnowflakeResource(*s.Table, withQuotes))

	if s.Column == nil {
		return fullName
	}

	fullName = fmt.Sprintf(formatString, fullName, convertToValidSnowflakeResource(*s.Column, withQuotes))

	return fullName
}

func convertToValidSnowflakeResource(name string, withQuotes bool) string {
	if isSimpleSnowflakeName(name) {
		return name
	}

	if withQuotes {
		name = strings.ReplaceAll(name, `"`, `""`)
		//nolint // %q would break correct formatting for Unicode characters
		name = fmt.Sprintf(`"%s"`, name)
	}

	return name
}

// Check if the resource name needs to be quoted, from: https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html. Simple names are
// - Start with a letter (A-Z, a-z) or an underscore (“_”).
// - Contain only letters, underscores, decimal digits (0-9), and dollar signs (“$”).
// - Are stored and resolved as uppercase characters (e.g. id is stored and resolved as ID).
func isSimpleSnowflakeName(name string) bool {
	startRegex := regexp.MustCompile("[a-zA-Z_]")
	contentRegex := regexp.MustCompile("[a-zA-Z0-9_$]")

	if startRegex.ReplaceAllString(fmt.Sprintf("%c", name[0]), "") != "" {
		return false
	}

	if contentRegex.ReplaceAllString(name, "") != "" {
		return false
	}

	return true
}

// Wrapper around fmt.Sprintf to properly format queries for Snowflake
func FormatQuery(query string, objects ...string) string {
	newObjects := []interface{}{}

	for _, obj := range objects {
		formattedObject := obj
		if !isSimpleSnowflakeName(formattedObject) {
			//nolint // Using %q would interfere with the required formatting
			formattedObject = fmt.Sprintf(`"%s"`, strings.ReplaceAll(formattedObject, `"`, `""`))
		}

		newObjects = append(newObjects, formattedObject)
	}

	return fmt.Sprintf(query, newObjects...)
}

func trimCircumfix(name string, circumfix string) string {
	name = strings.TrimPrefix(name, circumfix)
	name = strings.TrimSuffix(name, circumfix)

	return name
}

// Parse the fully-qualitied Snowflake resource name in a SnowflakeObject: https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html
func ParseFullName(fullName string) SnowflakeObject {
	split, err := splitFullName(fullName, nil, nil)
	if err != nil {
		return SnowflakeObject{}
	}
	parts := []string{}

	for i := range split {
		newPart := split[i]
		newPart = trimCircumfix(newPart, `"`)
		newPart = strings.ReplaceAll(newPart, `""`, `"`)
		parts = append(parts, newPart)
	}

	var database, schema, table, column *string

	if len(parts) > 0 {
		dbTemp := parts[0]
		database = &dbTemp
	}

	if len(parts) > 1 {
		schemaTemp := parts[1]
		schema = &schemaTemp
	}

	if len(parts) > 2 {
		tableTemp := parts[2]
		table = &tableTemp
	}

	if len(parts) > 3 {
		columnTemp := parts[3]
		column = &columnTemp
	}

	return SnowflakeObject{database, schema, table, column}
}

// Split a fully-qualitied Snowflake resource object into individual objects.
// Single data objects (database, schema, table, ...) can be double-quoted or not.
// If not double-quoted, no special characters allowed. Keep in mind you can have a `fullName` with some fields quoted, others not
// Else all unicode characters are allowed. A double quote in the name (`"`) is encoded as a double-double quote (`""`),
// therefore, double quotes are allowed at the beginning and end, but otherwise they always need to come in pairs (`""`).
// Dots are ignored as a data object/field separator until the field-delimiting double quote has passed.
func splitFullName(fullName string, currentResults []string, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}

	if fullName == "" {
		return currentResults, nil
	}

	startsWithDoubleQuote := strings.HasPrefix(fullName, `"`)

	// the simple case where the data object is not double-quoted
	if !startsWithDoubleQuote {
		i := strings.Index(fullName, `.`)
		if i == -1 {
			// no dot found, last entry in the list
			return append(currentResults, fullName), nil
		} else {
			currentResults = append(currentResults, fullName[:i])
			if i+1 < len(fullName) {
				return splitFullName(fullName[i+1:], currentResults, nil)
			} else {
				// if the last char is a dot (malformed through)
				return currentResults, fmt.Errorf("malformed fullName, last char can't be a dot if no double quote is used")
			}
		}
	} else { // the more complicated case where the data object is double-quoted
		i_quote := findNextStandaloneChar(fullName[1:], `"`)
		if i_quote == -1 {
			// This actually points to a malformed fullName (every beginning " should have a corresponding ending one)
			return append(currentResults, fullName), fmt.Errorf("no corresponding ending \" found for %s", fullName)
		}

		i_quote++
		subStr := fullName[i_quote:]

		i_dot := strings.Index(subStr, `.`)
		if i_dot > -1 {
			i_dot += i_quote
		}

		if i_dot == -1 && i_quote == len(fullName)-1 {
			// no dot found -> last entry in the list
			currentResults = append(currentResults, fullName)
			return currentResults, nil
		} else if i_dot == -1 {
			return nil, fmt.Errorf("badly-formatted fullName, should end with \"")
		} else if i_dot == i_quote+1 {
			// dot should follow " to have a next entry
			currentResults = append(currentResults, fullName[:i_quote+1])
			return splitFullName(fullName[i_dot+1:], currentResults, nil)
		} else {
			// This actually points to a malformed fullName
			return append(currentResults, fullName), fmt.Errorf("badly-formatted fullName, dot should follow \"")
		}
	}
}

// Function similar to `strings.Index()`, but it only returns a hit for a single instance of `searchChar`.
// E.g. findNextStandaloneChar(`ngaaba`, `a`) will return `5` versuses `2` for strings.Index()
func findNextStandaloneChar(fullName string, searchChar string) int {
	for i := 0; i < len(fullName); i++ {
		char := fullName[i]
		if strings.EqualFold(fmt.Sprintf("%c", char), searchChar) {
			if i+1 == len(fullName) {
				return i
			} else if i+1 < len(fullName) && strings.EqualFold(fmt.Sprintf("%c", fullName[i+1]), searchChar) {
				i++
				continue
			} else {
				return i
			}
		}
	}

	return -1
}
