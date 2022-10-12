package snowflake

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestIdentityStoreSyncer_SyncIdentityStore(t *testing.T) {
	//Given
	configMap := &config.ConfigMap{
		Parameters: map[string]interface{}{SfExcludedOwners: "excludeOwner"},
	}

	repoMock := newMockIdentityStoreRepository(t)
	identityHandlerMock := mocks.NewSimpleIdentityStoreIdentityHandler(t, 1)

	repoMock.EXPECT().Close().Return(nil)
	repoMock.EXPECT().TotalQueryTime().Return(time.Second)
	repoMock.EXPECT().GetUsers().Return([]UserEntity{
		{
			Name:        "UserName1",
			DisplayName: "user1",
		},
		{
			Name:        "Owned Excluded User",
			DisplayName: "ownedUser",
			Owner:       "excludeOwner",
		},
		{
			Name:        "UserName2",
			DisplayName: "user2",
		},
	}, nil)

	syncer := IdentityStoreSyncer{
		repoProvider: func(params map[string]interface{}, role string) (identityStoreRepository, error) {
			return repoMock, nil
		},
	}

	//When
	err := syncer.SyncIdentityStore(context.Background(), identityHandlerMock, configMap)

	//Then
	assert.NoError(t, err)
	assert.Len(t, identityHandlerMock.Users, 2)
	assert.Len(t, identityHandlerMock.Groups, 0)

	identityHandlerMock.AssertNumberOfCalls(t, "AddUsers", 2)
	identityHandlerMock.AssertNotCalled(t, "AddGroups")
}

func TestNewIdentityStoreSyncer_RepoError(t *testing.T) {
	//Given
	configMap := &config.ConfigMap{
		Parameters: map[string]interface{}{SfExcludedOwners: "excludeOwner"},
	}

	repoMock := newMockIdentityStoreRepository(t)
	identityHandlerMock := mocks.NewSimpleIdentityStoreIdentityHandler(t, 1)

	repoMock.EXPECT().Close().Return(nil)
	repoMock.EXPECT().TotalQueryTime().Return(time.Second)
	repoMock.EXPECT().GetUsers().Return(nil, fmt.Errorf("boom"))

	syncer := IdentityStoreSyncer{
		repoProvider: func(params map[string]interface{}, role string) (identityStoreRepository, error) {
			return repoMock, nil
		},
	}

	//When
	err := syncer.SyncIdentityStore(context.Background(), identityHandlerMock, configMap)

	//Then
	assert.Error(t, err)
	assert.Len(t, identityHandlerMock.Users, 0)
	assert.Len(t, identityHandlerMock.Groups, 0)

	identityHandlerMock.AssertNotCalled(t, "AddUsers")
	identityHandlerMock.AssertNotCalled(t, "AddGroups")
}

func TestNewIdentityStoreSyncer_AddUserError(t *testing.T) {
	//Given
	configMap := &config.ConfigMap{
		Parameters: map[string]interface{}{SfExcludedOwners: "excludeOwner"},
	}

	repoMock := newMockIdentityStoreRepository(t)
	identityHandlerMock := mocks.NewIdentityStoreIdentityHandler(t)
	identityHandlerMock.EXPECT().AddUsers(mock.Anything).Return(fmt.Errorf("boom"))

	repoMock.EXPECT().Close().Return(nil)
	repoMock.EXPECT().TotalQueryTime().Return(time.Second)
	repoMock.EXPECT().GetUsers().Return([]UserEntity{
		{
			Name:        "UserName1",
			DisplayName: "user1",
		},
		{
			Name:        "Owned Excluded User",
			DisplayName: "ownedUser",
			Owner:       "excludeOwner",
		},
		{
			Name:        "UserName2",
			DisplayName: "user2",
		},
	}, nil)

	syncer := IdentityStoreSyncer{
		repoProvider: func(params map[string]interface{}, role string) (identityStoreRepository, error) {
			return repoMock, nil
		},
	}

	//When
	err := syncer.SyncIdentityStore(context.Background(), identityHandlerMock, configMap)

	//Then
	assert.Error(t, err)

	identityHandlerMock.AssertNumberOfCalls(t, "AddUsers", 1)
	identityHandlerMock.AssertNotCalled(t, "AddGroups")
}
