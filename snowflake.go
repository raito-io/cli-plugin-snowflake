package main

import (
	"database/sql"
	"fmt"
	"github.com/raito-io/cli/common/api"
	_ "github.com/snowflakedb/gosnowflake"
)

const SF_LIMIT = 10000

func ConnectToSnowflake(params map[string]interface{}, role string) (*sql.DB, error) {
	snowflakeUser := params[SfUser]
	if snowflakeUser == nil {
		return nil, api.CreateMissingInputParameterError(SfUser)
	}
	snowflakePassword := params[SfPassword]
	if snowflakePassword == nil {
		return nil, api.CreateMissingInputParameterError(SfPassword)
	}
	snowflakeAccount := params[SfAccount]
	if snowflakeAccount == nil {
		return nil, api.CreateMissingInputParameterError(SfAccount)
	}
	snowflakeDatabase := params[SfDatabase]
	if snowflakeDatabase == nil {
		return nil, api.CreateMissingInputParameterError(SfDatabase)
	}

	if role == "" {
		if v, ok := params[SfRole]; ok && v != nil {
			role = v.(string)
		}
	}
	if role == "" {
		role = "ACCOUNTADMIN"
	}

	connectionString := fmt.Sprintf("%s:%s@%s/%s?role=%s", snowflakeUser, snowflakePassword, snowflakeAccount, snowflakeDatabase, role)
	logger.Debug(fmt.Sprintf("Using connection string: %s:%s@%s/%s?role=%s", snowflakeUser, "**censured**", snowflakeAccount, snowflakeDatabase, role))
	conn, err := sql.Open("snowflake", connectionString)
	if err != nil {
		return nil, api.CreateSourceConnectionError(fmt.Sprintf("%s:%s@%s/%s?role=%s", snowflakeUser, "**censured**", snowflakeAccount, snowflakeDatabase, role), err.Error())
	}
	return conn, nil
}

func QuerySnowflake(conn *sql.DB, query string) (*sql.Rows, error) {
	rows, err := conn.Query(query)
	if err != nil {
		return nil, fmt.Errorf("Error while querying Snowflake: %s", err.Error())
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
	if size >= SF_LIMIT {
		return fmt.Errorf("Query (%s) exceeded the maximum of %d elements supported by Snowflake. This will lead to unexpected and faulty behavior. You may need to use another integration method or this is simply currently not supported", query, size)
	}
	return nil
}
