//go:build integration

package it

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/smithy-go/ptr"
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

	startTime := time.Now()

	//When
	err := identityStoreSyncer.SyncIdentityStore(context.Background(), identityHandler, s.getConfig())

	fmt.Printf("Time taken to sync identity store: %v\n", time.Since(startTime))

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
		IsMachine:        ptr.Bool(false),
	})
	s.Contains(identityHandler.Users, identity_store.User{
		ExternalId:       snowflakeUserName,
		Name:             snowflakeUserName,
		UserName:         snowflakeUserName,
		Email:            "",
		GroupExternalIds: nil,
		Tags:             nil,
		IsMachine:        ptr.Bool(false),
	})
	s.Contains(identityHandler.Users, identity_store.User{
		ExternalId:       "ATKISON_A",
		Name:             "Angelica Abbot Atkinson",
		UserName:         "atkison_a",
		Email:            "a_abbotatkinson7576@raito.io",
		GroupExternalIds: nil,
		Tags:             nil,
		IsMachine:        ptr.Bool(true),
	})

	s.Empty(identityHandler.Groups)
}
