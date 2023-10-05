package snowflake

import (
	"database/sql"
	"fmt"
	"net/url"

	e "github.com/raito-io/cli/base/util/error"
	_ "github.com/snowflakedb/gosnowflake"
)

const SfLimit = 10000
const ConnectionStringIdentifier = "Raito_CLI"

func ConnectToSnowflake(params map[string]string, role string) (*sql.DB, string, error) {
	snowflakeUser, found := params[SfUser]
	if !found {
		return nil, "", e.CreateMissingInputParameterError(SfUser)
	}

	snowflakePassword, found := params[SfPassword]
	if !found {
		return nil, "", e.CreateMissingInputParameterError(SfPassword)
	}

	snowflakeAccount, found := params[SfAccount]
	if !found {
		return nil, "", e.CreateMissingInputParameterError(SfAccount)
	}

	if role == "" {
		if v, ok := params[SfRole]; ok {
			role = v
		}
	}

	if role == "" {
		role = "ACCOUNTADMIN"
	}

	urlUser := url.UserPassword(snowflakeUser, snowflakePassword)

	connectionString := fmt.Sprintf("%s@%s?role=%s&application=%s", urlUser, snowflakeAccount, role, ConnectionStringIdentifier)
	censoredConnectionString := fmt.Sprintf("%s:%s@%s?role=%s", snowflakeUser, "**censured**", snowflakeAccount, role)
	logger.Debug(fmt.Sprintf("Using connection string: %s", censoredConnectionString))
	conn, err := sql.Open("snowflake", connectionString)

	if err != nil {
		return nil, "", e.CreateSourceConnectionError(censoredConnectionString, err.Error())
	}

	return conn, role, nil
}

func QuerySnowflake(conn *sql.DB, query string, args ...any) (*sql.Rows, error) {
	rows, err := conn.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("error while querying Snowflake with query '%s': %s", query, err.Error())
	}

	return rows, nil
}

func CheckSFLimitExceeded(query string, size int) error {
	if size >= SfLimit {
		return fmt.Errorf("query (%s) exceeded the maximum of %d elements supported by Snowflake. This will lead to unexpected and faulty behavior. You may need to use another integration method or this is simply currently not supported", query, size)
	}

	return nil
}
