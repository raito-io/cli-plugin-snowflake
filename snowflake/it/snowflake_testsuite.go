//go:build integration

package it

import (
	"os"
	"sync"

	"github.com/raito-io/cli/base/util/config"
	"github.com/stretchr/testify/suite"

	"github.com/raito-io/cli-plugin-snowflake/snowflake"
)

var (
	sfAccount         string
	sfUser            string
	sfPassword        string
	sfRole            string
	sfStandardEdition string
	lock              = &sync.Mutex{}
)

func readDatabaseConfig() *config.ConfigMap {
	lock.Lock()
	defer lock.Unlock()

	if sfAccount == "" {
		sfAccount = os.Getenv("SF_ACCOUNT")
		sfUser = os.Getenv("SF_USER")
		sfPassword = os.Getenv("SF_PASSWORD")
		sfRole = os.Getenv("SF_ROLE")
		sfStandardEdition = os.Getenv("SF_STANDARD_EDITION")
	}

	return &config.ConfigMap{
		Parameters: map[string]interface{}{
			snowflake.SfAccount:         sfAccount,
			snowflake.SfUser:            sfUser,
			snowflake.SfPassword:        sfPassword,
			snowflake.SfRole:            sfRole,
			snowflake.SfStandardEdition: sfStandardEdition,
		},
	}
}

type SnowflakeTestSuite struct {
	suite.Suite
}

func (s *SnowflakeTestSuite) getConfig() *config.ConfigMap {
	return readDatabaseConfig()
}
