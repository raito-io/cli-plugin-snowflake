package common

import (
	"fmt"
	"regexp"
	"strings"
)

type SnowflakeObject struct {
	Database *string `json:"database"`
	Schema   *string `json:"schema"`
	Table    *string `json:"table"`
	Column   *string `json:"column"`
}

func (s SnowflakeObject) GetFullName(withQuotes bool) string {
	fullName := ""
	formatString := "%s.%s"

	if withQuotes {
		formatString = `%s."%s"`
	}

	if s.Database == nil {
		return fullName
	}

	fullName = fmt.Sprintf(strings.Split(formatString, ".")[1], *s.Database)

	if s.Schema == nil {
		return fullName
	}

	fullName = fmt.Sprintf(formatString, fullName, *s.Schema)

	if s.Table == nil {
		return fullName
	}

	fullName = fmt.Sprintf(formatString, fullName, *s.Table)

	if s.Column == nil {
		return fullName
	}

	fullName = fmt.Sprintf(formatString, fullName, *s.Column)

	return fullName
}

func FormatQuery(query string, objects ...string) string {
	newObjects := []interface{}{}

	for ind := range objects {
		objects[ind] = strings.ReplaceAll(objects[ind], `"`, `""`)
		//nolint // this interferes with proper formatting
		objects[ind] = fmt.Sprintf(`"%s"`, objects[ind])
		newObjects = append(newObjects, objects[ind])
	}

	return fmt.Sprintf(query, newObjects...)
}

func trimCircumfix(name string, circumfix string) string {
	name = strings.TrimPrefix(name, circumfix)
	name = strings.TrimSuffix(name, circumfix)

	return name
}

func ParseFullName(fullName string) SnowflakeObject {
	// TODO: add more difficult cases (see tests) where name contains quotes
	re := regexp.MustCompile(`"?\."?`)
	fullName = trimCircumfix(fullName, `"`)
	split := re.Split(fullName, -1)
	parts := []string{}

	for i := range split {
		newPart := split[i]
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
