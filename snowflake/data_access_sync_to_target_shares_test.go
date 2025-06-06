package snowflake

import (
	"testing"

	"github.com/aws/smithy-go/ptr"
	importer "github.com/raito-io/cli/base/access_provider/sync_to_target"
	"github.com/raito-io/cli/base/access_provider/types"
	"github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers/mocks"
	"github.com/stretchr/testify/assert"
)

func TestAccessSyncer_ProcessAccessProviderSharesToTarget(t *testing.T) {
	t.Run("Basic", processAccessProviderSharesToTargetBasic)
	t.Run("Delete", processAccessProviderSharesToTargetDelete)
}

func processAccessProviderSharesToTargetBasic(t *testing.T) {
	// Given
	configParams := config.ConfigMap{
		Parameters: map[string]string{"key": "value"},
	}

	repoMock := newMockDataAccessRepository(t)
	fileCreator := mocks.NewSimpleAccessProviderFeedbackHandler(t)
	repoMock.EXPECT().CreateShare("RAITO_SHARE1").Return(nil).Once()
	repoMock.EXPECT().CreateShare("RAITO_SHARE2").Return(nil).Once()

	repoMock.EXPECT().GetGrantsToShare("RAITO_SHARE2").Return([]GrantToRole{
		{
			Privilege: "USAGE",
			GrantedOn: "DATABASE",
			Name:      "DB_RANDOM",
		},
	}, nil).Once()

	repoMock.EXPECT().ExecuteGrantOnShare("USAGE", "DATABASE DB1", "RAITO_SHARE1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnShare("USAGE", "SCHEMA DB1.Schema1", "RAITO_SHARE1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnShare("SELECT", "TABLE DB1.Schema1.Table1", "RAITO_SHARE1").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrantOnShare("USAGE", "DATABASE DB1", "RAITO_SHARE2").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnShare("USAGE", "SCHEMA DB1.Schema1", "RAITO_SHARE2").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnShare("SELECT", "TABLE DB1.Schema1.Table2", "RAITO_SHARE2").Return(nil).Once()
	repoMock.EXPECT().ExecuteRevokeOnShare("USAGE", "DATABASE DB_RANDOM", "RAITO_SHARE2").Return(nil).Once()

	repoMock.EXPECT().SetShareAccounts("RAITO_SHARE1", []string{"acc1", "acc2"}).Return(nil).Once()
	repoMock.EXPECT().SetShareAccounts("RAITO_SHARE2", []string{"acc2"}).Return(nil).Once()

	syncer := createBasicToTargetSyncer(repoMock, nil, fileCreator, &configParams)

	toProcessApIds := []string{"ShareId1", "ShareId2"}

	apsById := map[string]*ApSyncToTargetItem{
		"ShareId1": {
			accessProvider: &importer.AccessProvider{
				Id:         "ShareId1",
				Name:       "Share1",
				NamingHint: "Share1",
				Who: importer.WhoItem{
					Recipients: []string{"acc1", "acc2"},
				},
				What: []importer.WhatItem{
					{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table1", Type: "table"}, Permissions: []string{"SELECT"}},
				},
				Action: types.Share,
			},
			calculatedExternalId: "RAITO_SHARE1",
			mutationAction:       ApMutationActionCreate,
		},
		"ShareId2": {
			accessProvider: &importer.AccessProvider{
				Id:         "ShareId2",
				Name:       "Share2",
				NamingHint: "Share2",
				Who: importer.WhoItem{
					Recipients: []string{"acc2"},
				},
				What: []importer.WhatItem{
					{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table2", Type: "table"}, Permissions: []string{"SELECT"}},
				},
				ExternalId: ptr.String("RAITO_SHARE2"),
				Action:     types.Share,
			},
			calculatedExternalId: "RAITO_SHARE2",
			mutationAction:       ApMutationActionUpdate,
		},
	}

	// When
	err := syncer.processSharesToTarget(toProcessApIds, apsById)

	// Then
	assert.NoError(t, err)
	assert.Len(t, fileCreator.AccessProviderFeedback, 2)
}

func processAccessProviderSharesToTargetDelete(t *testing.T) {
	// Given
	configParams := config.ConfigMap{
		Parameters: map[string]string{"key": "value"},
	}

	repoMock := newMockDataAccessRepository(t)
	fileCreator := mocks.NewSimpleAccessProviderFeedbackHandler(t)
	repoMock.EXPECT().CreateShare("RAITO_SHARE1").Return(nil).Once()
	repoMock.EXPECT().GetGrantsToShare("RAITO_SHARE1").Return([]GrantToRole{}, nil).Once()
	repoMock.EXPECT().ExecuteGrantOnShare("USAGE", "DATABASE DB1", "RAITO_SHARE1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnShare("USAGE", "SCHEMA DB1.Schema1", "RAITO_SHARE1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnShare("SELECT", "TABLE DB1.Schema1.Table1", "RAITO_SHARE1").Return(nil).Once()
	repoMock.EXPECT().SetShareAccounts("RAITO_SHARE1", []string{"acc1", "acc2"}).Return(nil).Once()

	repoMock.EXPECT().DropShare("SHARE2").Return(nil).Once()

	syncer := createBasicToTargetSyncer(repoMock, nil, fileCreator, &configParams)

	toProcessApIds := []string{"ShareId1", "ShareId2"}

	apsById := map[string]*ApSyncToTargetItem{
		"ShareId1": {
			accessProvider: &importer.AccessProvider{
				Id:         "ShareId1",
				Name:       "Share1",
				NamingHint: "Share1",
				ExternalId: ptr.String("RAITO_SHARE1"),
				Who: importer.WhoItem{
					Recipients: []string{"acc1", "acc2"},
				},
				What: []importer.WhatItem{
					{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table1", Type: "table"}, Permissions: []string{"SELECT"}},
				},
				Action: types.Share,
			},
			calculatedExternalId: "RAITO_SHARE1",
			mutationAction:       ApMutationActionCreate,
		},
		"ShareId2": {
			accessProvider: &importer.AccessProvider{
				Id:         "ShareId2",
				Name:       "Share2",
				NamingHint: "Share2",
				ExternalId: ptr.String("RAITO_SHARE2"),
				Who: importer.WhoItem{
					Recipients: []string{"acc2"},
				},
				What: []importer.WhatItem{
					{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table2", Type: "table"}, Permissions: []string{"SELECT"}},
				},
				Action: types.Share,
			},
			calculatedExternalId: "RAITO_SHARE2",
			mutationAction:       ApMutationActionDelete,
		},
	}

	// When
	err := syncer.processSharesToTarget(toProcessApIds, apsById)

	// Then
	assert.NoError(t, err)
	assert.Len(t, fileCreator.AccessProviderFeedback, 2)
}
