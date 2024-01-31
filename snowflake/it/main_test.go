//go:build integration

package it

import (
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/blockloop/scan"

	"github.com/raito-io/cli-plugin-snowflake/snowflake"
)

var testId string

var (
	snowflakeUserName string
)

const (
	snowflakeWarehouse string = "TESTING_WAREHOUSE"
)

func setup() error {
	randomSource := rand.NewSource(time.Now().UnixNano())
	random := rand.New(randomSource)

	randomNr := random.Intn(65535)
	testId = strings.ToUpper(fmt.Sprintf("IT%x", randomNr))

	snowflakeUserName = strings.ToUpper(fmt.Sprintf("%s_snowflakeuser", testId))

	config := readDatabaseConfig()

	_, err := connectAndQuery(config.Parameters, "", fmt.Sprintf("CREATE USER IF NOT EXISTS %s", snowflakeUserName))
	if err != nil {
		return err
	}

	_, err = connectAndQuery(config.Parameters, "", fmt.Sprintf("CREATE WAREHOUSE IF NOT EXISTS %s with warehouse_size='X-SMALL'", snowflakeWarehouse))
	if err != nil {
		return err
	}

	_, err = connectAndQuery(config.Parameters, "", fmt.Sprintf("ALTER USER %s SET DEFAULT_WAREHOUSE=%s", config.Parameters[snowflake.SfUser], snowflakeWarehouse))
	if err != nil {
		return err
	}

	return nil
}

func teardown() error {
	config := readDatabaseConfig()

	roles, err := connectAndQuery(config.Parameters, "", fmt.Sprintf("SHOW ROLES"))
	if err != nil {
		return err
	}

	var roleEntities []snowflake.RoleEntity

	err = scan.Rows(&roleEntities, roles)
	if err != nil {
		return err
	}

	for _, role := range roleEntities {
		if strings.HasPrefix(role.Name, testId) {
			_, err = connectAndQuery(config.Parameters, "", fmt.Sprintf("DROP ROLE IF EXISTS %s", role.Name))
			if err != nil {
				return err
			}
		}
	}

	database := "SNOWFLAKE_INTEGRATION_TEST"
	databaseRoles, err := connectAndQuery(config.Parameters, "", fmt.Sprintf("SHOW DATABASE ROLES IN DATABASE %s", database))
	if err != nil {
		return err
	}

	var databaseRoleEntities []snowflake.RoleEntity

	err = scan.Rows(&databaseRoleEntities, databaseRoles)
	if err != nil {
		return err
	}

	for _, role := range databaseRoleEntities {
		if strings.HasPrefix(role.Name, testId) {
			_, err = connectAndQuery(config.Parameters, "", fmt.Sprintf("DROP DATABASE ROLE IF EXISTS %s.%s", database, role.Name))
			if err != nil {
				return err
			}
		}
	}

	_, err = connectAndQuery(config.Parameters, "", fmt.Sprintf("DROP USER IF EXISTS %s", snowflakeUserName))
	if err != nil {
		return err
	}

	return nil
}

func TestMain(m *testing.M) {
	testResult := -1
	err := setup()

	if err != nil {
		panic(err)
	}

	testResult = m.Run()

	err = teardown()

	if err != nil {
		panic(err)
	}

	os.Exit(testResult)
}

func connectAndQuery(params map[string]string, role, query string) (*sql.Rows, error) {
	conn, _, err := snowflake.ConnectToSnowflake(params, role)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	return snowflake.QuerySnowflake(conn, query)
}
