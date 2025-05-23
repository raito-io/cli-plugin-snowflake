package snowflake

import (
	"fmt"
	"testing"

	"github.com/raito-io/cli/base/access_provider/types"

	"github.com/aws/smithy-go/ptr"
	importer "github.com/raito-io/cli/base/access_provider/sync_to_target"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers"
	"github.com/raito-io/cli/base/wrappers/mocks"
	"github.com/raito-io/golang-set/set"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestAccessSyncer_SplitByType(t *testing.T) {
	t.Run("Basic", splitByTypeBasic)
	t.Run("Multiple Types", splitByTypeMultipleTypes)
	t.Run("Unsupported action", splitByTypeUnsupportedAction)
	t.Run("Handle empty aps", splitByTypeHandlesEmptyAccessProviders)
	t.Run("Multiple mutation actions", splitByTypeMultipleMutationActions)
	t.Run("Deletes", splitByTypeDeletes)
	t.Run("Rename auto retry", splitByTypeRenameAutoRetry)
}

func splitByTypeBasic(t *testing.T) {
	// Given
	configParams := config.ConfigMap{
		Parameters: map[string]string{"key": "value", SfDatabaseRoles: "true"},
	}

	repoMock := newMockDataAccessRepository(t)
	feedbackHandler := mocks.NewSimpleAccessProviderFeedbackHandler(t)

	syncer := createBasicToTargetSyncer(repoMock, &importer.AccessProviderImport{
		AccessProviders: []*importer.AccessProvider{
			{
				Id:     "AP2",
				Action: types.Grant,
			},
			{
				Id:     "AP1",
				Action: types.Grant,
			},
			{
				Id:     "AP3",
				Action: types.Grant,
			},
		},
	}, feedbackHandler, &configParams)

	supportedActions := []types.Action{types.Mask, types.Filtered, types.Share, types.Grant}
	existingRoles := set.Set[string]{}
	toProcessSortedApIdsByAction, toProcessApsById, err := syncer.splitItemsByAccessProviderAction(supportedActions, existingRoles)

	assert.NoError(t, err)
	assert.Len(t, toProcessSortedApIdsByAction, 1)
	assert.Len(t, toProcessApsById, 3)

	assert.Equal(t, "AP1", toProcessSortedApIdsByAction[types.Grant][1])
	assert.Equal(t, "AP2", toProcessSortedApIdsByAction[types.Grant][0])
	assert.Equal(t, "AP3", toProcessSortedApIdsByAction[types.Grant][2])
}

func splitByTypeMultipleTypes(t *testing.T) {
	// Given
	configParams := config.ConfigMap{
		Parameters: map[string]string{"key": "value", SfDatabaseRoles: "true"},
	}

	repoMock := newMockDataAccessRepository(t)
	feedbackHandler := mocks.NewSimpleAccessProviderFeedbackHandler(t)

	syncer := createBasicToTargetSyncer(repoMock, &importer.AccessProviderImport{
		AccessProviders: []*importer.AccessProvider{
			{
				Id:     "AP1",
				Action: types.Grant,
			},
			{
				Id:     "filter1",
				Action: types.Filtered,
			},
			{
				Id:     "AP2",
				Action: types.Grant,
			},
			{
				Id:     "aaaaa",
				Action: types.Filtered,
			},
			{
				Id:     "Mask1",
				Action: types.Mask,
			},
			{
				Id:     "AP3",
				Action: types.Grant,
			},
		},
	}, feedbackHandler, &configParams)

	supportedActions := []types.Action{types.Mask, types.Filtered, types.Share, types.Grant}
	existingRoles := set.Set[string]{}
	toProcessSortedApIdsByAction, toProcessApsById, err := syncer.splitItemsByAccessProviderAction(supportedActions, existingRoles)

	assert.NoError(t, err)
	assert.Len(t, toProcessSortedApIdsByAction, 3)
	assert.Len(t, toProcessApsById, 6)

	assert.Len(t, toProcessSortedApIdsByAction[types.Grant], 3)
	assert.Equal(t, "AP1", toProcessSortedApIdsByAction[types.Grant][0])
	assert.Equal(t, "AP2", toProcessSortedApIdsByAction[types.Grant][1])
	assert.Equal(t, "AP3", toProcessSortedApIdsByAction[types.Grant][2])

	assert.Len(t, toProcessSortedApIdsByAction[types.Filtered], 2)
	assert.Equal(t, "filter1", toProcessSortedApIdsByAction[types.Filtered][0])
	assert.Equal(t, "aaaaa", toProcessSortedApIdsByAction[types.Filtered][1])

	assert.Len(t, toProcessSortedApIdsByAction[types.Mask], 1)
	assert.Equal(t, "Mask1", toProcessSortedApIdsByAction[types.Mask][0])
}

func splitByTypeUnsupportedAction(t *testing.T) {
	// Given
	configParams := config.ConfigMap{
		Parameters: map[string]string{"key": "value", SfDatabaseRoles: "true"},
	}

	repoMock := newMockDataAccessRepository(t)
	feedbackHandler := mocks.NewSimpleAccessProviderFeedbackHandler(t)

	feedbackHandler.AccessProviderFeedbackHandler.EXPECT().AddAccessProviderFeedback(importer.AccessProviderSyncFeedback{
		AccessProvider: "BOGUS",
		Errors:         []string{"Unsupported action mask"},
	})

	syncer := createBasicToTargetSyncer(repoMock, &importer.AccessProviderImport{
		AccessProviders: []*importer.AccessProvider{
			{
				Id:     "AP1",
				Action: types.Grant,
			},
			{
				Id:     "BOGUS",
				Action: types.Mask,
			},
		},
	}, feedbackHandler, &configParams)

	supportedActions := []types.Action{types.Grant}
	existingRoles := set.Set[string]{}
	toProcessSortedApIdsByAction, toProcessApsById, err := syncer.splitItemsByAccessProviderAction(supportedActions, existingRoles)

	assert.NoError(t, err)
	assert.Len(t, toProcessSortedApIdsByAction, 1)
	assert.Len(t, toProcessApsById, 1)

	assert.Len(t, toProcessSortedApIdsByAction[types.Grant], 1)
	assert.Equal(t, "AP1", toProcessSortedApIdsByAction[types.Grant][0])
}

func splitByTypeHandlesEmptyAccessProviders(t *testing.T) {
	// Given
	configParams := config.ConfigMap{
		Parameters: map[string]string{"key": "value", SfDatabaseRoles: "true"},
	}

	repoMock := newMockDataAccessRepository(t)
	feedbackHandler := mocks.NewSimpleAccessProviderFeedbackHandler(t)

	syncer := createBasicToTargetSyncer(repoMock, &importer.AccessProviderImport{
		AccessProviders: []*importer.AccessProvider{},
	}, feedbackHandler, &configParams)

	supportedActions := []types.Action{types.Mask, types.Filtered, types.Share, types.Grant}
	existingRoles := set.Set[string]{}

	// When
	toProcessSortedApIdsByAction, toProcessApsById, err := syncer.splitItemsByAccessProviderAction(supportedActions, existingRoles)

	// Then
	assert.NoError(t, err)
	assert.Empty(t, toProcessSortedApIdsByAction)
	assert.Empty(t, toProcessApsById)
	assert.Empty(t, feedbackHandler.AccessProviderFeedback)
}

func splitByTypeMultipleMutationActions(t *testing.T) {
	// Given
	configParams := config.ConfigMap{
		Parameters: map[string]string{"key": "value", SfDatabaseRoles: "true"},
	}

	repoMock := newMockDataAccessRepository(t)
	feedbackHandler := mocks.NewSimpleAccessProviderFeedbackHandler(t)

	syncer := createBasicToTargetSyncer(repoMock, &importer.AccessProviderImport{
		AccessProviders: []*importer.AccessProvider{
			{
				Id:         "AP_Delete_1",
				Action:     types.Grant,
				Delete:     true,
				ExternalId: ptr.String("AP_Delete_1"),
			},
			{
				Id:         "AP_Update_2",
				Action:     types.Grant,
				NamingHint: "AP_UPDATE_2",
				ExternalId: ptr.String("AP_UPDATE_2"),
			},
			{
				Id:     "AP_Create_3",
				Action: types.Grant,
			},
			{
				Id:         "AP_Rename_4",
				Action:     types.Grant,
				NamingHint: "RENAME_AP",
				ExternalId: ptr.String("AP_Rename_4"),
			},
		},
	}, feedbackHandler, &configParams)

	supportedActions := []types.Action{types.Grant}
	existingRoles := set.Set[string]{}
	toProcessSortedApIdsByAction, toProcessApsById, err := syncer.splitItemsByAccessProviderAction(supportedActions, existingRoles)

	assert.NoError(t, err)
	assert.Len(t, toProcessSortedApIdsByAction, 1)
	assert.Len(t, toProcessApsById, 4)

	assert.Len(t, toProcessSortedApIdsByAction[types.Grant], 4)
	assert.Equal(t, "AP_Delete_1", toProcessSortedApIdsByAction[types.Grant][0])
	assert.Equal(t, "AP_Update_2", toProcessSortedApIdsByAction[types.Grant][1])
	assert.Equal(t, "AP_Create_3", toProcessSortedApIdsByAction[types.Grant][2])
	assert.Equal(t, "AP_Rename_4", toProcessSortedApIdsByAction[types.Grant][3])

	assert.Equal(t, ApMutationActionDelete, toProcessApsById["AP_Delete_1"].mutationAction)
	assert.Equal(t, ApMutationActionUpdate, toProcessApsById["AP_Update_2"].mutationAction)
	assert.Equal(t, ApMutationActionCreate, toProcessApsById["AP_Create_3"].mutationAction)
	assert.Equal(t, ApMutationActionRename, toProcessApsById["AP_Rename_4"].mutationAction)
}

func splitByTypeDeletes(t *testing.T) {
	// Given
	configParams := config.ConfigMap{
		Parameters: map[string]string{"key": "value", SfDatabaseRoles: "true"},
	}

	repoMock := newMockDataAccessRepository(t)
	feedbackHandler := mocks.NewSimpleAccessProviderFeedbackHandler(t)

	feedbackHandler.EXPECT().AddAccessProviderFeedback(importer.AccessProviderSyncFeedback{
		AccessProvider: "AP_Delete_2",
	})

	syncer := createBasicToTargetSyncer(repoMock, &importer.AccessProviderImport{
		AccessProviders: []*importer.AccessProvider{
			{
				Id:         "AP_Delete_1",
				Action:     types.Grant,
				Delete:     true,
				ExternalId: ptr.String("AP_Delete_1"),
			},
			{
				Id:     "AP_Delete_2",
				Action: types.Grant,
				Delete: true,
			},
		},
	}, feedbackHandler, &configParams)

	supportedActions := []types.Action{types.Grant}
	existingRoles := set.Set[string]{}
	toProcessSortedApIdsByAction, toProcessApsById, err := syncer.splitItemsByAccessProviderAction(supportedActions, existingRoles)

	assert.NoError(t, err)
	assert.Len(t, toProcessSortedApIdsByAction, 1)
	assert.Len(t, toProcessApsById, 1)

	assert.Len(t, toProcessSortedApIdsByAction[types.Grant], 1)
	assert.Equal(t, "AP_Delete_1", toProcessSortedApIdsByAction[types.Grant][0])
}

func splitByTypeRenameAutoRetry(t *testing.T) {
	// Given
	configParams := config.ConfigMap{
		Parameters: map[string]string{"key": "value", SfDatabaseRoles: "true"},
	}

	repoMock := newMockDataAccessRepository(t)
	feedbackHandler := mocks.NewSimpleAccessProviderFeedbackHandler(t)

	syncer := createBasicToTargetSyncer(repoMock, &importer.AccessProviderImport{
		AccessProviders: []*importer.AccessProvider{
			{
				Id:         "APid1",
				Action:     types.Grant,
				NamingHint: "AP2",
				ExternalId: ptr.String("AP1"),
			},
			{
				Id:         "APid2",
				Action:     types.Grant,
				NamingHint: "AP2",
				ExternalId: ptr.String("APhere"),
			},
			{
				Id:         "APid3",
				Action:     types.Grant,
				NamingHint: "AP4",
				ExternalId: ptr.String("AP3"),
			},
		},
	}, feedbackHandler, &configParams)

	supportedActions := []types.Action{types.Grant}
	existingRoles := set.Set[string]{}
	existingRoles.Add("AP1", "AP2", "AP3", "AP4", "AP4__0")
	toProcessSortedApIdsByAction, toProcessApsById, err := syncer.splitItemsByAccessProviderAction(supportedActions, existingRoles)

	assert.NoError(t, err)
	assert.Len(t, toProcessSortedApIdsByAction, 1)
	assert.Len(t, toProcessApsById, 3)

	assert.Equal(t, "AP2__0", toProcessApsById["APid1"].calculatedExternalId)
	assert.Equal(t, "AP2__1", toProcessApsById["APid2"].calculatedExternalId)
	assert.Equal(t, "AP4__1", toProcessApsById["APid3"].calculatedExternalId)

	assert.Equal(t, ApMutationActionRename, toProcessApsById["APid1"].mutationAction)
	assert.Equal(t, ApMutationActionRename, toProcessApsById["APid2"].mutationAction)
	assert.Equal(t, ApMutationActionRename, toProcessApsById["APid3"].mutationAction)
}

func TestAccessSyncer_RetrieveMappedGrantsById(t *testing.T) {
	t.Run("Basic", retrieveMappedGrantsByIdBasic)
	t.Run("Missing APs", retrieveMappedGrantsByIdHandlesMissingAccessProvider)
	t.Run("Multiple Actions", retrieveMappedGrantsByIdHandleListWithMultipleActions)
}

func retrieveMappedGrantsByIdBasic(t *testing.T) {
	toProcessSortedApIdsByAction := map[types.Action][]string{
		types.Grant: {"ap1", "ap2"},
	}
	toProcessApsById := map[string]*ApSyncToTargetItem{
		"ap1": {accessProvider: &importer.AccessProvider{Id: "id1"}, calculatedExternalId: "ext1"},
		"ap2": {accessProvider: &importer.AccessProvider{Id: "id2"}, calculatedExternalId: "ext2"},
	}

	syncer := &AccessToTargetSyncer{}
	result := syncer.mapCalculatedExternalIdByApId(toProcessSortedApIdsByAction, toProcessApsById)

	assert.Equal(t, map[string]string{"id1": "ext1", "id2": "ext2"}, result)
}

func retrieveMappedGrantsByIdHandlesMissingAccessProvider(t *testing.T) {
	toProcessSortedApIdsByAction := map[types.Action][]string{
		types.Grant: {"ap1", "ap2"},
	}
	toProcessApsById := map[string]*ApSyncToTargetItem{
		"ap1": {accessProvider: &importer.AccessProvider{Id: "id1"}, calculatedExternalId: "ext1"},
	}

	syncer := &AccessToTargetSyncer{}
	result := syncer.mapCalculatedExternalIdByApId(toProcessSortedApIdsByAction, toProcessApsById)

	assert.Equal(t, map[string]string{"id1": "ext1"}, result)
}

func retrieveMappedGrantsByIdHandleListWithMultipleActions(t *testing.T) {
	toProcessSortedApIdsByAction := map[types.Action][]string{
		types.Grant: {"ap1", "ap2"},
		types.Mask:  {"mask1"},
	}
	toProcessApsById := map[string]*ApSyncToTargetItem{
		"ap1": {accessProvider: &importer.AccessProvider{Id: "id1"}, calculatedExternalId: "ext1"},
		"ap2": {accessProvider: &importer.AccessProvider{Id: "id2"}, calculatedExternalId: "ext2"},
	}

	syncer := &AccessToTargetSyncer{}
	result := syncer.mapCalculatedExternalIdByApId(toProcessSortedApIdsByAction, toProcessApsById)

	assert.Equal(t, map[string]string{"id1": "ext1", "id2": "ext2"}, result)
}

func TestAccessSyncer_RetrieveItemsForMutationActions(t *testing.T) {
	t.Run("Multiple Actions", retrieveItemsForMutationActionsReturnsItemsMatchingActions)
	t.Run("Missing APs", retrieveItemsForMutationActionsSkipsMissingOrNilItems)
	t.Run("No matches", retrieveItemsForMutationActionsReturnsEmptyIfNoMatches)
}

func retrieveItemsForMutationActionsReturnsItemsMatchingActions(t *testing.T) {

	ap1 := &ApSyncToTargetItem{mutationAction: ApMutationActionCreate}
	ap2 := &ApSyncToTargetItem{mutationAction: ApMutationActionUpdate}
	ap3 := &ApSyncToTargetItem{mutationAction: ApMutationActionDelete}

	apsById := map[string]*ApSyncToTargetItem{
		"id1": ap1,
		"id2": ap2,
		"id3": ap3,
	}
	toProcessApIds := []string{"id1", "id2", "id3"}
	actions := []ApMutationAction{ApMutationActionCreate, ApMutationActionUpdate}

	syncer := &AccessToTargetSyncer{}
	result := syncer.accessProvidersForMutationActions(toProcessApIds, apsById, actions)

	assert.Len(t, result, 2)
	assert.Equal(t, ap1, result[0])
	assert.Equal(t, ap2, result[1])
}

func retrieveItemsForMutationActionsSkipsMissingOrNilItems(t *testing.T) {
	apsById := map[string]*ApSyncToTargetItem{
		"id1": {mutationAction: ApMutationActionCreate},
		"id2": nil,
	}
	toProcessApIds := []string{"id1", "id2", "id3"}
	actions := []ApMutationAction{ApMutationActionCreate}

	syncer := &AccessToTargetSyncer{}
	result := syncer.accessProvidersForMutationActions(toProcessApIds, apsById, actions)

	assert.Len(t, result, 1)
	assert.Contains(t, result, apsById["id1"])
}

func retrieveItemsForMutationActionsReturnsEmptyIfNoMatches(t *testing.T) {
	apsById := map[string]*ApSyncToTargetItem{
		"id1": {mutationAction: ApMutationActionCreate},
		"id2": {mutationAction: ApMutationActionUpdate},
	}
	toProcessApIds := []string{"id1", "id2"}
	actions := []ApMutationAction{ApMutationActionDelete}

	syncer := &AccessToTargetSyncer{}
	result := syncer.accessProvidersForMutationActions(toProcessApIds, apsById, actions)

	assert.Empty(t, result)
}

// -----------------------------------------------------------------------
// Helper functions
// -----------------------------------------------------------------------
func createBasicToTargetSyncer(repo dataAccessRepository, accessProviders *importer.AccessProviderImport, accessProviderFeedbackHandler wrappers.AccessProviderFeedbackHandler, configMap *config.ConfigMap) *AccessToTargetSyncer {
	as := AccessSyncer{
		repoProvider: func(params map[string]string, role string) (dataAccessRepository, error) {
			return repo, nil
		},
		repo:              repo,
		namingConstraints: RoleNameConstraints,
	}

	tts := NewAccessToTargetSyncer(&as, RoleNameConstraints, repo, accessProviders, accessProviderFeedbackHandler, configMap)
	tts.databaseRoleSupportEnabled = true
	return tts
}

func expectGrantUsersToRole(repoMock *mockDataAccessRepository, roleName string, users ...string) {
	expectedUsersList := make([]string, 0, len(users))
	expectedUsersList = append(expectedUsersList, users...)
	grantedUsers := make(map[string]struct{})

	expectedUsers := func(user string) bool {

		if _, f := grantedUsers[user]; f {
			return false
		}

		for _, expectedUser := range expectedUsersList {
			if expectedUser == user {
				grantedUsers[user] = struct{}{}
				return true
			}
		}
		return false
	}

	arguments := make([]interface{}, 0, len(users))
	for range users {
		arguments = append(arguments, mock.MatchedBy(expectedUsers))
	}

	repoMock.EXPECT().GrantUsersToAccountRole(mock.Anything, roleName, arguments...).Return(nil).Once()
}

func expectGrantAccountOrDatabaseRolesToDatabaseRole(repoMock *mockDataAccessRepository, expectDatabaseRoles bool, database string, roleName string, roles ...string) {
	expectedRolesList := make([]string, 0, len(roles))

	for _, expectedRole := range roles {
		if isDatabaseRoleByExternalId(expectedRole) {
			_, expectedRole, _ = parseDatabaseRoleExternalId(expectedRole)
		}

		expectedRolesList = append(expectedRolesList, expectedRole)
	}

	grantedInheritFromList := make(map[string]struct{})

	expectedAccountRoles := func(accountRole string) bool {
		if _, f := grantedInheritFromList[accountRole]; f {
			return false
		}

		for _, expectedAccountRole := range expectedRolesList {
			if expectedAccountRole == accountRole {
				grantedInheritFromList[accountRole] = struct{}{}
				return true
			}
		}
		return false
	}

	arguments := make([]interface{}, 0, len(roles))
	for range roles {
		arguments = append(arguments, mock.MatchedBy(expectedAccountRoles))
	}

	if expectDatabaseRoles {
		repoMock.EXPECT().GrantDatabaseRolesToDatabaseRole(mock.Anything, database, roleName, arguments...).Return(nil).Once()

	} else {
		repoMock.EXPECT().GrantAccountRolesToDatabaseRole(mock.Anything, database, roleName, arguments...).Return(nil).Once()

	}
}

type dummyFeedbackHandler struct {
}

func (d *dummyFeedbackHandler) AddAccessProviderFeedback(accessProviderFeedback importer.AccessProviderSyncFeedback) error {
	if len(accessProviderFeedback.Errors) > 0 {
		for _, err := range accessProviderFeedback.Errors {
			Logger.Error(fmt.Sprintf("error during syncing of access provider %q; %s", accessProviderFeedback.AccessProvider, err))
		}
	}

	return nil
}
