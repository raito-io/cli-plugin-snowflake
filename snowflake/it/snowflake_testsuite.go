//go:build integration

package it

import (
	"os"
	"strconv"
	"sync"

	"github.com/raito-io/cli/base/util/config"
	"github.com/stretchr/testify/suite"

	"github.com/raito-io/cli-plugin-snowflake/snowflake"
)

var (
	sfAccount              string
	sfOrganization         string
	sfUser                 string
	sfPassword             string
	sfPrivateKey           string
	sfPrivateKeyPassphrase string
	sfRole                 string

	sfStandardEdition bool
	lock              = &sync.Mutex{}
)

func readDatabaseConfig() *config.ConfigMap {
	lock.Lock()
	defer lock.Unlock()

	if sfAccount == "" {
		sfAccount = os.Getenv("SF_ACCOUNT")
		sfOrganization = os.Getenv("SF_ORGANIZATION")
		sfUser = os.Getenv("SF_USER")
		sfPassword = os.Getenv("SF_PASSWORD")
		sfRole = os.Getenv("SF_ROLE")
		sfPrivateKey = os.Getenv("SF_PRIVATE_KEY")
		sfPrivateKeyPassphrase = os.Getenv("SF_PRIVATE_KEY_PASSPHRASE")

		if sfStandardStr, sfStandardSet := os.LookupEnv("SF_STANDARD_EDITION"); sfStandardSet {
			var err error
			sfStandardEdition, err = strconv.ParseBool(sfStandardStr)

			if err != nil {
				panic(err)
			}
		}
	}

	return &config.ConfigMap{
		Parameters: map[string]string{
			snowflake.SfAccount:              sfOrganization + "-" + sfAccount,
			snowflake.SfUser:                 sfUser,
			snowflake.SfPassword:             sfPassword,
			snowflake.SfPrivateKey:           sfPrivateKey,
			snowflake.SfPrivateKeyPassphrase: sfPrivateKeyPassphrase,
			snowflake.SfRole:                 sfRole,
			snowflake.SfStandardEdition:      strconv.FormatBool(sfStandardEdition),
			snowflake.SfDatabaseRoles:        "true",
			snowflake.SfApplications:         "true",
			snowflake.SfRoleOwnerEmailTag:    "RAITO_DATABASE.PUBLIC.\"raito.io:owner\"",
		},
	}
}

type SnowflakeTestSuite struct {
	suite.Suite
}

func (s *SnowflakeTestSuite) getConfig() *config.ConfigMap {
	return readDatabaseConfig()
}
