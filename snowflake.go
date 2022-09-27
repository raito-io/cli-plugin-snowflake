package main

import (
	"database/sql"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	e "github.com/raito-io/cli/base/util/error"
	_ "github.com/snowflakedb/gosnowflake"
)

const SfLimit = 10000

type SnowflakeObject struct {
	Database *string `json:"database"`
	Schema   *string `json:"schema"`
	Table    *string `json:"table"`
	Column   *string `json:"column"`
}

func (s SnowflakeObject) getFullName(withQuotes bool) string {
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
	for ind, _ := range objects {
		objects[ind] = strings.ReplaceAll(objects[ind], `"`, `""`)
		objects[ind] = fmt.Sprintf(`"%s"`, objects[ind])
		newObjects = append(newObjects, objects[ind])
	}
	return fmt.Sprintf(query, newObjects...)
}

// func FormatQueryFullName(query string, fullName string) string {
// 	newObjects := []interface{}{}
// 	for ind, _ := range objects {
// 		objects[ind] = strings.ReplaceAll(objects[ind], `"`, `""`)
// 		objects[ind] = fmt.Sprintf(`"%s"`, objects[ind])
// 		newObjects = append(newObjects, objects[ind])
// 	}
// 	return fmt.Sprintf(query, newObjects...)
// }

func ConvertFullNameToDoubleQuotedName(objectName string) string {
	parts := strings.Split(objectName, ".")
	newParts := []string{}
	for i := 0; i < len(parts); i++ {
		partName := parts[i]
		partName = strings.ReplaceAll(partName, `"`, `""`)
		partName = fmt.Sprintf(`"%s"`, partName)
		newParts = append(newParts, partName)
	}
	return strings.Join(newParts, ".")
}

func removeQuotesFromBeginningOrEnd(name string) string {
	newName := name
	if strings.HasPrefix(newName, `"`) {
		newName = strings.TrimPrefix(newName, `"`)
	}

	if strings.HasSuffix(newName, `"`) {
		newName = strings.TrimSuffix(newName, `"`)
	}

	return newName
}

func ParseFullName(fullName string) SnowflakeObject {
	parts := strings.Split(fullName, ".")
	// otherParts := strings.Split(fullName, `"."`)

	re := regexp.MustCompile(`["]?\.["]?`)
	split := re.Split(fullName, -1)
	otherParts := []string{}
	for i := range split {
		otherParts = append(otherParts, split[i])
	}

	if len(otherParts) > 1 {
		parts = otherParts
	}
	var database, schema, table, column *string

	if len(parts) > 0 {
		dbTemp := removeQuotesFromBeginningOrEnd(parts[0])
		database = &dbTemp
	}

	if len(parts) > 1 {
		schemaTemp := removeQuotesFromBeginningOrEnd(parts[1])
		schema = &schemaTemp
	}

	if len(parts) > 2 {
		tableTemp := removeQuotesFromBeginningOrEnd(parts[2])
		table = &tableTemp
	}

	if len(parts) > 3 {
		columnTemp := removeQuotesFromBeginningOrEnd(parts[3])
		column = &columnTemp
	}

	return SnowflakeObject{database, schema, table, column}
}

func ConnectToSnowflake(params map[string]interface{}, role string) (*sql.DB, error) {
	snowflakeUser := params[SfUser]
	if snowflakeUser == nil {
		return nil, e.CreateMissingInputParameterError(SfUser)
	}

	snowflakePassword := params[SfPassword]
	if snowflakePassword == nil {
		return nil, e.CreateMissingInputParameterError(SfPassword)
	}

	snowflakeAccount := params[SfAccount]
	if snowflakeAccount == nil {
		return nil, e.CreateMissingInputParameterError(SfAccount)
	}

	if role == "" {
		if v, ok := params[SfRole]; ok && v != nil {
			role = v.(string)
		}
	}

	if role == "" {
		role = "ACCOUNTADMIN"
	}

	urlUser := url.UserPassword(snowflakeUser.(string), snowflakePassword.(string))

	connectionString := fmt.Sprintf("%s@%s?role=%s", urlUser, snowflakeAccount, role)
	censoredConnectionString := fmt.Sprintf("%s:%s@%s?role=%s", snowflakeUser, "**censured**", snowflakeAccount, role)
	logger.Debug(fmt.Sprintf("Using connection string: %s", censoredConnectionString))
	conn, err := sql.Open("snowflake", connectionString)

	if err != nil {
		return nil, e.CreateSourceConnectionError(censoredConnectionString, err.Error())
	}

	return conn, nil
}

func QuerySnowflake(conn *sql.DB, query string) (*sql.Rows, error) {
	rows, err := conn.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error while querying Snowflake with query '%s': %s", query, err.Error())
	}

	return rows, nil
}

func ConnectAndQuery(params map[string]interface{}, role, query string) (*sql.Rows, error) {
	conn, err := ConnectToSnowflake(params, role)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	return QuerySnowflake(conn, query)
}

func CheckSFLimitExceeded(query string, size int) error {
	if size >= SfLimit {
		return fmt.Errorf("query (%s) exceeded the maximum of %d elements supported by Snowflake. This will lead to unexpected and faulty behavior. You may need to use another integration method or this is simply currently not supported", query, size)
	}

	return nil
}
