package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	e "github.com/raito-io/cli/base/util/error"
	sf "github.com/snowflakedb/gosnowflake"
)

const SfLimit = 10000
const ConnectionStringIdentifier = "Raito_CLI"

func ConnectToSnowflake(params map[string]string, role string) (*sql.DB, string, error) {
	sfUser, found := params[SfUser]
	if !found {
		return nil, "", e.CreateMissingInputParameterError(SfUser)
	}

	sfPassword, foundPassword := params[SfPassword]
	sfPrivateKey, foundPrivateKey := params[SfPrivateKey]
	sfPrivateKeyPassphrase := params[SfPrivateKeyPassphrase]

	if (!foundPassword || sfPassword == "") && (!foundPrivateKey || sfPrivateKey == "") {
		return nil, "", fmt.Errorf("either parameter %q or %q need to be specified", SfPassword, SfPrivateKey)
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

	insecure := false
	if v, ok := params[SfDriverInsecureMode]; ok && strings.EqualFold(v, "true") {
		insecure = true
	}

	dsnConfig := sf.Config{
		Account:      snowflakeAccount,
		User:         sfUser,
		Role:         role,
		Application:  ConnectionStringIdentifier,
		InsecureMode: insecure,
	}

	if foundPrivateKey && sfPrivateKey != "" {
		privateKey, err := LoadPrivateKeyFromFile(sfPrivateKey, sfPrivateKeyPassphrase)
		if err != nil {
			return nil, "", e.CreateBadInputParameterError(SfPrivateKey, sfPrivateKey, fmt.Sprintf("Failed to parse private key: %s", err.Error()))
		}

		dsnConfig.PrivateKey = privateKey
		dsnConfig.Authenticator = sf.AuthTypeJwt
	} else {
		dsnConfig.Password = sfPassword
	}

	if sfWarehouse, foundWh := params[SfWarehouse]; foundWh {
		if sfWarehouse != "" {
			dsnConfig.Warehouse = sfWarehouse
		}
	}

	dsn, err := sf.DSN(&dsnConfig)
	if err != nil {
		return nil, "", fmt.Errorf("snowflake DSN: %w", err)
	}

	censoredConnectionString := fmt.Sprintf("%s:%s@%s?role=%s", sfUser, "**censured**", snowflakeAccount, role)
	Logger.Debug(fmt.Sprintf("Using connection string: %s", censoredConnectionString))
	conn, err := sql.Open("snowflake", dsn)

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

func ExecuteSnowflake(ctx context.Context, conn *sql.DB, query string, args ...any) error {
	_, err := conn.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("error while executing Snowflake with query '%s': %s", query, err.Error())
	}

	return nil
}

func CheckSFLimitExceeded(query string, size int) error {
	if size >= SfLimit {
		return fmt.Errorf("query (%s) exceeded the maximum of %d elements supported by Snowflake. This will lead to unexpected and faulty behavior. You may need to use another integration method or this is simply currently not supported", query, size)
	}

	return nil
}
