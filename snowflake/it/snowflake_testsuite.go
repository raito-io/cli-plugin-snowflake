//go:build integration

package it

import (
	"os"

	"github.com/raito-io/cli/base/util/config"
	"github.com/stretchr/testify/suite"

	"github.com/raito-io/cli-plugin-snowflake/snowflake"
)

type SnowflakeTestSuite struct {
	suite.Suite
	sfAccount  string
	sfUser     string
	sfPassword string
	sfRole     string
	sfDataBase string
}

func (s *SnowflakeTestSuite) SetupSuite() {
	s.sfAccount = os.Getenv("SF_ACCOUNT")
	s.sfUser = os.Getenv("SF_USER")
	s.sfPassword = os.Getenv("SF_PASSWORD")
	s.sfRole = os.Getenv("SF_ROLE")
}

func (s *SnowflakeTestSuite) getConfig() *config.ConfigMap {
	return &config.ConfigMap{
		Parameters: map[string]interface{}{
			snowflake.SfAccount:  s.sfAccount,
			snowflake.SfUser:     s.sfUser,
			snowflake.SfPassword: s.sfPassword,
			snowflake.SfRole:     s.sfRole,
		},
	}
}
