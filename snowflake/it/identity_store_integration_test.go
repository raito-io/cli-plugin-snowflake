//go:build integration

package it

import (
	"context"
	"testing"

	"github.com/raito-io/cli/base/identity_store"
	"github.com/raito-io/cli/base/wrappers/mocks"
	"github.com/stretchr/testify/suite"

	"github.com/raito-io/cli-plugin-snowflake/snowflake"
)

type IdentityStoreTestSuite struct {
	SnowflakeTestSuite
}

func TestIdentityStoreTestSuite(t *testing.T) {
	ts := IdentityStoreTestSuite{}
	suite.Run(t, &ts)
}

func (s *IdentityStoreTestSuite) TestIdentityStoreSync() {
	//Given
	identityHandler := mocks.NewSimpleIdentityStoreIdentityHandler(s.T(), 1)
	identityStoreSyncer := snowflake.NewIdentityStoreSyncer()

	//When
	err := identityStoreSyncer.SyncIdentityStore(context.Background(), identityHandler, s.getConfig())

	//Then
	s.NoError(err)

	s.True(len(identityHandler.Users) >= 3)
	s.Contains(identityHandler.Users, identity_store.User{
		ExternalId:       "SNOWFLAKE",
		Name:             "SNOWFLAKE",
		UserName:         "SNOWFLAKE",
		Email:            "",
		GroupExternalIds: nil,
		Tags:             nil,
	})
	s.Contains(identityHandler.Users, identity_store.User{
		ExternalId:       snowflakeUserName,
		Name:             snowflakeUserName,
		UserName:         snowflakeUserName,
		Email:            "",
		GroupExternalIds: nil,
		Tags:             nil,
	})

	s.Empty(identityHandler.Groups)
}
