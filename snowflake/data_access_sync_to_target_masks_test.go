package snowflake

import (
	"testing"

	"github.com/aws/smithy-go/ptr"
	"github.com/raito-io/cli/base/access_provider/types"
	"github.com/raito-io/cli/base/data_source"
	"github.com/stretchr/testify/mock"

	importer "github.com/raito-io/cli/base/access_provider/sync_to_target"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers/mocks"
	"github.com/stretchr/testify/assert"
)

func TestAccessSyncer_ProcessAccessProviderMasksToTarget(t *testing.T) {
	t.Run("Basic", processAccessProviderMasksToTargetBasic)
	t.Run("SF Standard Edition", processAccessProviderMasksToTargetHandlesStandardEdition)
}

func processAccessProviderMasksToTargetBasic(t *testing.T) {
	// Given
	configParams := config.ConfigMap{
		Parameters: map[string]string{"key": "value"},
	}

	repoMock := newMockDataAccessRepository(t)
	fileCreator := mocks.NewSimpleAccessProviderFeedbackHandler(t)

	repoMock.EXPECT().GetPoliciesLike("MASKING", "RAITO_MASK1%").Return(nil, nil).Once() // No existing masks
	repoMock.EXPECT().CreateMaskPolicy("DB1", "Schema1", mock.AnythingOfType("string"), []string{"DB1.Schema1.Table1.Column1"}, ptr.String("SHA256"), &MaskingBeneficiaries{Users: []string{"User1", "User2"}, Roles: []string{"Role1", "Role2"}}).Return(nil)
	repoMock.EXPECT().CreateMaskPolicy("DB1", "Schema2", mock.AnythingOfType("string"), []string{"DB1.Schema2.Table1.Column1"}, ptr.String("SHA256"), &MaskingBeneficiaries{Users: []string{"User1", "User2"}, Roles: []string{"Role1", "Role2"}}).Return(nil)

	repoMock.EXPECT().GetPoliciesLike("MASKING", "RAITO_MASK2%").Return([]PolicyEntity{{Name: "RAITO_MASK2_OLD_TEXT", SchemaName: "Schema1", DatabaseName: "DB1"}}, nil).Once()
	repoMock.EXPECT().CreateMaskPolicy("DB1", "Schema1", mock.AnythingOfType("string"), []string{"DB1.Schema1.Table3.Column1"}, (*string)(nil), &MaskingBeneficiaries{Users: []string{"User1"}}).Return(nil)
	repoMock.EXPECT().DropMaskingPolicy("DB1", "Schema1", "RAITO_MASK2_OLD").Return(nil)

	repoMock.EXPECT().GetPoliciesLike("MASKING", "RAITO_MASKTOREMOVE1%").Return([]PolicyEntity{{Name: "RAITO_maskToRemove1_TEXT", SchemaName: "Schema3", DatabaseName: "DB1"}, {Name: "RAITO_maskToRemove1_INT", SchemaName: "Schema1", DatabaseName: "DB1"}}, nil).Once()
	repoMock.EXPECT().DropMaskingPolicy("DB1", "Schema3", "RAITO_MASKTOREMOVE1").Return(nil)
	repoMock.EXPECT().DropMaskingPolicy("DB1", "Schema1", "RAITO_MASKTOREMOVE1").Return(nil)

	syncer := createBasicToTargetSyncer(repoMock, nil, fileCreator, &configParams)

	toProcessApIds := []string{"MaskId1", "MaskId2", "xxx"}

	apsById := map[string]*ApSyncToTargetItem{
		"MaskId1": {
			accessProvider: &importer.AccessProvider{
				Id:   "MaskId1",
				Name: "Mask1",
				Who: importer.WhoItem{
					Users:       []string{"User1", "User2"},
					InheritFrom: []string{"Role1", "ID:Role2-Id"},
				},
				What: []importer.WhatItem{
					{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table1.Column1", Type: "column"}},
					{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema2.Table1.Column1", Type: "column"}},
				},
				Action: types.Mask,
				Type:   ptr.String("SHA256"),
			},
			calculatedExternalId: "Mask1",
			mutationAction:       ApMutationActionCreate,
		},
		"MaskId2": {
			accessProvider: &importer.AccessProvider{
				Id:   "MaskId2",
				Name: "Mask2",
				Who: importer.WhoItem{
					Users: []string{"User1"},
				},
				What: []importer.WhatItem{
					{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table3.Column1", Type: "column"}},
				},
				Action: types.Mask,
			},
			calculatedExternalId: "Mask2",
			mutationAction:       ApMutationActionCreate,
		},
		"xxx": {
			accessProvider: &importer.AccessProvider{
				Id:         "xxx",
				ActualName: ptr.String("RAITO_MASKTOREMOVE1"),
				Action:     types.Mask,
			},
			calculatedExternalId: "RAITO_MASKTOREMOVE1",
			mutationAction:       ApMutationActionDelete,
		},
	}

	// When
	err := syncer.processMasksToTarget(toProcessApIds, apsById, map[string]string{"Role2-Id": "Role2"})

	// Then
	assert.NoError(t, err)
	assert.Len(t, fileCreator.AccessProviderFeedback, 3)
}

func processAccessProviderMasksToTargetHandlesStandardEdition(t *testing.T) {
	// Given
	configParams := config.ConfigMap{
		Parameters: map[string]string{"key": "value", SfDatabaseRoles: "true"},
	}

	repoMock := newMockDataAccessRepository(t)
	feedbackHandler := mocks.NewSimpleAccessProviderFeedbackHandler(t)

	syncer := createBasicToTargetSyncer(repoMock, &importer.AccessProviderImport{
		AccessProviders: []*importer.AccessProvider{
			{
				Id:     "id1",
				Action: types.Mask,
			},
		},
	}, feedbackHandler, &configParams)

	err := syncer.processMasksToTarget([]string{"id1"}, map[string]*ApSyncToTargetItem{}, map[string]string{})

	assert.NoError(t, err)
}
