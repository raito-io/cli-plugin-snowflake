package common

import (
	"fmt"
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

func splitFullName(fullName string, currentResults []string, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}

	if fullName == "" {
		return currentResults, nil
	}

	startsWithDoubleQuote := strings.HasPrefix(fullName, `"`)

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
	} else {
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
