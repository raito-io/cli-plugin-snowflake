package snowflake

import (
	"context"
	"fmt"
	"testing"

	"github.com/raito-io/golang-set/set"
	"github.com/stretchr/testify/require"

	"github.com/aws/smithy-go/ptr"
	"github.com/raito-io/cli/base/access_provider"
	importer "github.com/raito-io/cli/base/access_provider/sync_to_target"
	"github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestAccessSyncer_SyncAccessProviderRolesToTarget(t *testing.T) {
	t.Run("Basic", syncAccessProviderRolesToTargetBasic)
	t.Run("Application Roles", syncAccessProviderRolesToTargetApplicationRoles)
}

func syncAccessProviderRolesToTargetBasic(t *testing.T) {
	// Given
	configParams := config.ConfigMap{
		Parameters: map[string]string{"key": "value", SfDatabaseRoles: "true"},
	}

	repoMock := newMockDataAccessRepository(t)
	feedbackHandler := mocks.NewSimpleAccessProviderFeedbackHandler(t)

	repoMock.EXPECT().GetApplications().Return(nil, nil)

	repoMock.EXPECT().DropAccountRole("ToRemove1").Return(nil).Once()
	repoMock.EXPECT().DropAccountRole("ToRemove2").Return(nil).Once()
	repoMock.EXPECT().DropDatabaseRole("TEST_DB", "ToRemoveDatabaseRole1").Return(nil).Once()

	repoMock.EXPECT().GetAccountRolesWithPrefix("").Return([]RoleEntity{
		{Name: "ExistingRole1", GrantedToRoles: 2, GrantedRoles: 3, AssignedToUsers: 2, Owner: "Owner"},
		{Name: "ExistingRole2", GrantedToRoles: 2, GrantedRoles: 3, AssignedToUsers: 2, Owner: "Owner"},
	}, nil).Once()
	repoMock.EXPECT().GetInboundShares().Return([]DbEntity{}, nil).Once()
	repoMock.EXPECT().GetDatabases().Return([]DbEntity{
		{Name: "SNOWFLAKE"},
		{Name: "TEST_DB"},
	}, nil).Once()
	repoMock.EXPECT().GetDatabaseRolesWithPrefix("SNOWFLAKE", "").Return([]RoleEntity{}, nil).Once()
	repoMock.EXPECT().GetDatabaseRolesWithPrefix("TEST_DB", "").Return([]RoleEntity{
		{Name: "DatabaseRole1", AssignedToUsers: 0, GrantedRoles: 0, GrantedToRoles: 1, Owner: "Owner"},
	}, nil).Once()

	repoMock.EXPECT().CreateAccountRole("RoleName1").Return(nil).Once()
	repoMock.EXPECT().CreateAccountRole("RoleName3").Return(nil).Once()
	repoMock.EXPECT().CreateAccountRole("RoleNameInheritance1").Return(nil).Once()
	repoMock.EXPECT().CreateDatabaseRole("TEST_DB", "DatabaseRole2").Return(nil).Once()
	repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "RoleName1").Return(nil).Once()
	repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "RoleName3").Return(nil).Once()
	repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "RoleNameInheritance1").Return(nil).Once()

	repoMock.EXPECT().CommentDatabaseRoleIfExists(mock.Anything, "TEST_DB", "DatabaseRole1").Return(nil).Once()
	repoMock.EXPECT().CommentDatabaseRoleIfExists(mock.Anything, "TEST_DB", "DatabaseRole2").Return(nil).Once()

	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")

	expectGrantAccountOrDatabaseRolesToDatabaseRole(repoMock, true, "TEST_DB", "DatabaseRole1", "DATABASEROLE###DATABASE:TEST_DB###ROLE:DatabaseRole2")
	expectGrantAccountOrDatabaseRolesToDatabaseRole(repoMock, false, "TEST_DB", "DatabaseRole1")

	repoMock.EXPECT().GrantAccountRolesToAccountRole(mock.Anything, "RoleNameInheritance1", "RoleName3").Return(nil).Once()

	repoMock.EXPECT().GetGrantsOfAccountRole("ExistingRole1").Return([]GrantOfRole{}, nil).Once()
	repoMock.EXPECT().GetGrantsToAccountRole("ExistingRole1").Return([]GrantToRole{}, nil).Once()

	repoMock.EXPECT().GetGrantsOfDatabaseRole("TEST_DB", "DatabaseRole1").Return([]GrantOfRole{}, nil).Once()
	repoMock.EXPECT().GetGrantsToDatabaseRole("TEST_DB", "DatabaseRole1").Return([]GrantToRole{}, nil).Once()

	repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "ExistingRole1").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "DATABASE DB1", "RoleName1", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "SCHEMA DB1.Schema1", "RoleName1", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "TABLE DB1.Schema1.Table1", "RoleName1", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "DATABASE DB1", "ExistingRole1", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "SCHEMA DB1.Schema1", "ExistingRole1", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "TABLE DB1.Schema1.Table2", "ExistingRole1", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "DATABASE DB1", "RoleName3", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "SCHEMA DB1.Schema2", "RoleName3", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "TABLE DB1.Schema2.Table1", "RoleName3", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "DATABASE DB1", "RoleNameInheritance1", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "SCHEMA DB1.Schema2", "RoleNameInheritance1", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "TABLE DB1.Schema2.Table1", "RoleNameInheritance1", false).Return(nil).Once()

	repoMock.EXPECT().ExecuteGrantOnDatabaseRole("USAGE", "DATABASE TEST_DB", "TEST_DB", "DatabaseRole1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnDatabaseRole("USAGE", "SCHEMA TEST_DB.Schema2", "TEST_DB", "DatabaseRole1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnDatabaseRole("SELECT", "TABLE TEST_DB.Schema2.Table1", "TEST_DB", "DatabaseRole1").Return(nil).Once()

	ap1 := &importer.AccessProvider{
		Id:         "AccessProviderId1",
		Name:       "AccessProvider1",
		ActualName: ptr.String("AccessProvider1"),
		Who: importer.WhoItem{
			Users: []string{"User1", "User2"},
		},
		What: []importer.WhatItem{
			{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table1", Type: "table"}, Permissions: []string{"SELECT"}},
		},
	}

	ap2 := &importer.AccessProvider{
		Id:   "AccessProviderId2",
		Name: "AccessProvider2",
		Who: importer.WhoItem{
			Groups: []string{"Group1"},
		},
		What: []importer.WhatItem{
			{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table2", Type: "table"}, Permissions: []string{"SELECT"}},
		},
	}

	ap3 := &importer.AccessProvider{
		Id:   "AccessProviderId3",
		Name: "AccessProvider3",
		Who: importer.WhoItem{
			Groups: []string{"User1"},
		},
		What: []importer.WhatItem{
			{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema2.Table1", Type: "table"}, Permissions: []string{"SELECT"}},
		},
	}

	apDatabaseRole1 := &importer.AccessProvider{
		Id:   "AccessProviderId4",
		Name: "TEST_DB.DatabaseRole1",
		Type: ptr.String("databaseRole"),
		Who: importer.WhoItem{
			InheritFrom: []string{"DATABASEROLE###DATABASE:TEST_DB###ROLE:DatabaseRole2"},
		},
		What: []importer.WhatItem{
			{DataObject: &data_source.DataObjectReference{FullName: "TEST_DB.Schema2.Table1", Type: "table"}, Permissions: []string{"SELECT"}},
		},
	}

	apDatabaseRole2 := &importer.AccessProvider{
		Id:   "AccessProviderId5",
		Name: "TEST_DB.DatabaseRole2",
		Type: ptr.String("databaseRole"),
		Who:  importer.WhoItem{},
		What: []importer.WhatItem{},
	}

	apWithInheritance := &importer.AccessProvider{
		Id:   "AccessProviderId6",
		Name: "AccessProvider6",
		Who: importer.WhoItem{
			InheritFrom: []string{"ID:AccessProviderId3"},
		},
		What: []importer.WhatItem{
			{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema2.Table1", Type: "table"}, Permissions: []string{"SELECT"}},
		},
	}

	apRemove1 := &importer.AccessProvider{
		Id:   "xxx",
		Type: ptr.String(access_provider.Role),
	}

	apRemove2 := &importer.AccessProvider{
		Id:   "yyy",
		Type: ptr.String(access_provider.Role),
	}
	apDatabaseRoleRemove1 := &importer.AccessProvider{
		Id:   "zzz",
		Type: ptr.String("databaseRole"),
	}

	toProcessApIds := []string{
		"AccessProviderId1", "AccessProviderId2", "AccessProviderId3", "AccessProviderId4", "AccessProviderId5", "AccessProviderId6", "xxx", "yyy", "zzz",
	}

	apsById := map[string]*ApSyncToTargetItem{
		"AccessProviderId1": {
			accessProvider:       ap1,
			mutationAction:       ApMutationActionCreate,
			calculatedExternalId: "RoleName1",
		},
		"AccessProviderId2": {
			accessProvider:       ap2,
			mutationAction:       ApMutationActionUpdate,
			calculatedExternalId: "ExistingRole1",
		},
		"AccessProviderId3": {
			accessProvider:       ap3,
			mutationAction:       ApMutationActionCreate,
			calculatedExternalId: "RoleName3",
		},
		"AccessProviderId4": {
			accessProvider:       apDatabaseRole1,
			mutationAction:       ApMutationActionUpdate,
			calculatedExternalId: "DATABASEROLE###DATABASE:TEST_DB###ROLE:DatabaseRole1",
		},
		"AccessProviderId5": {
			accessProvider:       apDatabaseRole2,
			mutationAction:       ApMutationActionCreate,
			calculatedExternalId: "DATABASEROLE###DATABASE:TEST_DB###ROLE:DatabaseRole2",
		},
		"AccessProviderId6": {
			accessProvider:       apWithInheritance,
			mutationAction:       ApMutationActionCreate,
			calculatedExternalId: "RoleNameInheritance1",
		},
		"xxx": {
			accessProvider:       apRemove1,
			mutationAction:       ApMutationActionDelete,
			calculatedExternalId: "ToRemove1",
		},
		"yyy": {
			accessProvider:       apRemove2,
			mutationAction:       ApMutationActionDelete,
			calculatedExternalId: "ToRemove2",
		},
		"zzz": {
			accessProvider:       apDatabaseRoleRemove1,
			mutationAction:       ApMutationActionDelete,
			calculatedExternalId: "DATABASEROLE###DATABASE:TEST_DB###ROLE:ToRemoveDatabaseRole1",
		},
	}

	syncer := createBasicToTargetSyncer(repoMock, nil, feedbackHandler, &configParams)

	// When
	err := syncer.syncGrantsToTarget(context.Background(), toProcessApIds, apsById)

	// Then
	assert.NoError(t, err)
	assert.ElementsMatch(t, []importer.AccessProviderSyncFeedback{{
		AccessProvider: "xxx",
		ExternalId:     ptr.String("ToRemove1"),
	},
		{
			AccessProvider: "yyy",
			ExternalId:     ptr.String("ToRemove2"),
		},
		{
			AccessProvider: "zzz",
			ExternalId:     ptr.String("DATABASEROLE###DATABASE:TEST_DB###ROLE:ToRemoveDatabaseRole1"),
		},
		{
			AccessProvider: "AccessProviderId2",
			ActualName:     "ExistingRole1",
			ExternalId:     ptr.String("ExistingRole1"),
			Type:           ptr.String(access_provider.Role),
		},
		{
			AccessProvider: "AccessProviderId3",
			ActualName:     "RoleName3",
			ExternalId:     ptr.String("RoleName3"),
			Type:           ptr.String(access_provider.Role),
		},
		{
			AccessProvider: "AccessProviderId1",
			ActualName:     "RoleName1",
			ExternalId:     ptr.String("RoleName1"),
			Type:           ptr.String(access_provider.Role),
		},
		{
			AccessProvider: "AccessProviderId4",
			ActualName:     "DatabaseRole1",
			ExternalId:     ptr.String("DATABASEROLE###DATABASE:TEST_DB###ROLE:DatabaseRole1"),
			Type:           ptr.String("databaseRole"),
		},
		{
			AccessProvider: "AccessProviderId5",
			ActualName:     "DatabaseRole2",
			ExternalId:     ptr.String("DATABASEROLE###DATABASE:TEST_DB###ROLE:DatabaseRole2"),
			Type:           ptr.String("databaseRole"),
		},
		{
			AccessProvider: "AccessProviderId6",
			ActualName:     "RoleNameInheritance1",
			ExternalId:     ptr.String("RoleNameInheritance1"),
			Type:           ptr.String(access_provider.Role),
		},
	}, feedbackHandler.AccessProviderFeedback)
}

func syncAccessProviderRolesToTargetApplicationRoles(t *testing.T) {
	// Given
	configParams := config.ConfigMap{
		Parameters: map[string]string{"key": "value", SfDatabaseRoles: "true"},
	}

	repoMock := newMockDataAccessRepository(t)
	feedbackHandler := mocks.NewSimpleAccessProviderFeedbackHandler(t)

	repoMock.EXPECT().GetApplications().Return([]ApplictionEntity{
		{
			Name:      "TEST_APP",
			IsDefault: "N",
			IsCurrent: "N",
		},
	}, nil)
	repoMock.EXPECT().GetApplicationRoles("TEST_APP").Return([]ApplicationRoleEntity{
		{
			Name: "AccessProvider2",
		}, {
			Name: "APP_ROLE_TO_REMOVE",
		},
	}, nil)
	repoMock.EXPECT().GetGrantsOfApplicationRole("TEST_APP", "AccessProvider2").Return([]GrantOfRole{
		{
			GrantedTo:   "ROLE",
			GranteeName: "ROLE_TO_REMOVE",
		},
		{
			GrantedTo:   "ROLE",
			GranteeName: "EXISTING_ROLE",
		},
		{
			GrantedTo:   "APPLICATION_ROLE",
			GranteeName: "TEST_APP.APP_ROLE_TO_REMOVE",
		},
	}, nil)

	repoMock.EXPECT().GrantApplicationRolesToApplicationRole(mock.Anything, "TEST_APP", "AccessProvider2").Return(nil).Once()
	repoMock.EXPECT().GrantAccountRolesToApplicationRole(mock.Anything, "TEST_APP", "AccessProvider2", "AccessProvider1").Return(nil)
	repoMock.EXPECT().RevokeAccountRolesFromApplicationRole(mock.Anything, "TEST_APP", "AccessProvider2", "ROLE_TO_REMOVE").Return(nil).Once()
	repoMock.EXPECT().RevokeApplicationRolesFromApplicationRole(mock.Anything, "TEST_APP", "AccessProvider2", "APP_ROLE_TO_REMOVE").Return(nil).Once()

	repoMock.EXPECT().DropAccountRole("ToRemove1").Return(nil).Once()

	repoMock.EXPECT().CreateAccountRole("RoleName1").Return(nil).Once()
	repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "RoleName1").Return(nil).Once()

	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")

	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "DATABASE DB1", "RoleName1", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "SCHEMA DB1.Schema1", "RoleName1", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "TABLE DB1.Schema1.Table1", "RoleName1", false).Return(nil).Once()

	repoMock.EXPECT().GetAccountRolesWithPrefix("").Return([]RoleEntity{
		{Name: "ExistingRole1", GrantedToRoles: 2, GrantedRoles: 3, AssignedToUsers: 2, Owner: "Owner"},
		{Name: "ROLE_TO_REMOVE", GrantedToRoles: 2, GrantedRoles: 3, AssignedToUsers: 2, Owner: "Owner"},
	}, nil).Once()
	repoMock.EXPECT().GetInboundShares().Return([]DbEntity{}, nil).Once()
	repoMock.EXPECT().GetDatabases().Return([]DbEntity{
		{Name: "SNOWFLAKE"},
		{Name: "TEST_DB"},
	}, nil).Once()
	repoMock.EXPECT().GetDatabaseRolesWithPrefix("SNOWFLAKE", "").Return([]RoleEntity{}, nil).Once()
	repoMock.EXPECT().GetDatabaseRolesWithPrefix("TEST_DB", "").Return([]RoleEntity{
		{Name: "DatabaseRole1", AssignedToUsers: 0, GrantedRoles: 0, GrantedToRoles: 1, Owner: "Owner"},
	}, nil).Once()

	ap1 := &importer.AccessProvider{
		Id:         "AccessProviderId1",
		Name:       "AccessProvider1",
		ActualName: ptr.String("AccessProvider1"),
		Who: importer.WhoItem{
			Users: []string{"User1", "User2"},
		},
		What: []importer.WhatItem{
			{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table1", Type: "table"}, Permissions: []string{"SELECT"}},
		},
	}

	appRole1 := &importer.AccessProvider{
		Id:         "AccessProviderId2",
		Name:       "AccessProvider2",
		ActualName: ptr.String("AccessProvider2"),
		ExternalId: ptr.String("share:AccessProvider2"),
		Type:       ptr.String("applicationRole"),
		Who: importer.WhoItem{
			InheritFrom: []string{"AccessProvider1", "EXISTING_ROLE"},
		},
		WhoLocked:    ptr.Bool(true),
		WhatLocked:   ptr.Bool(true),
		DeleteLocked: ptr.Bool(true),
	}

	apRemove1 := &importer.AccessProvider{
		Id:   "xxx",
		Type: ptr.String(access_provider.Role),
	}

	toProcessApIds := []string{
		"AccessProviderId1", "AccessProviderId2", "xxx",
	}

	apsById := map[string]*ApSyncToTargetItem{
		"AccessProviderId1": {
			accessProvider:       ap1,
			mutationAction:       ApMutationActionCreate,
			calculatedExternalId: "RoleName1",
		},
		"AccessProviderId2": {
			accessProvider:       appRole1,
			mutationAction:       ApMutationActionUpdate,
			calculatedExternalId: "APPLICATIONROLE###APPLICATION:TEST_APP###ROLE:AccessProvider2",
		},
		"xxx": {
			accessProvider:       apRemove1,
			mutationAction:       ApMutationActionDelete,
			calculatedExternalId: "ToRemove1",
		},
	}

	syncer := createBasicToTargetSyncer(repoMock, nil, feedbackHandler, &configParams)

	// When
	err := syncer.syncGrantsToTarget(context.Background(), toProcessApIds, apsById)

	// Then
	assert.NoError(t, err)
	assert.ElementsMatch(t, []importer.AccessProviderSyncFeedback{{
		AccessProvider: "xxx",
		ExternalId:     ptr.String("ToRemove1"),
	},
		{
			AccessProvider: "AccessProviderId1",
			ActualName:     "RoleName1",
			ExternalId:     ptr.String("RoleName1"),
			Type:           ptr.String(access_provider.Role),
		},
		{
			AccessProvider: "AccessProviderId2",
			ActualName:     "AccessProvider2",
			ExternalId:     ptr.String("APPLICATIONROLE###APPLICATION:TEST_APP###ROLE:AccessProvider2"),
			Type:           ptr.String("applicationRole"),
		},
	}, feedbackHandler.AccessProviderFeedback)
}

func TestAccessSyncer_RemoveRolesToRemove(t *testing.T) {
	t.Run("Basic", removeRolesToRemoveBasic)
	t.Run("No roles", removeRolesToRemoveNoRoles)
	t.Run("Error", removeRolesToRemoveError)
}

func removeRolesToRemoveBasic(t *testing.T) {
	// Given
	repo := newMockDataAccessRepository(t)
	rolesToRemove := []*ApSyncToTargetItem{
		{
			accessProvider: &importer.AccessProvider{
				Id: "xxx",
			},
			calculatedExternalId: "Role1",
		},
		{
			accessProvider: &importer.AccessProvider{
				Id: "yyy",
			},
			calculatedExternalId: "Role2",
		},
		{
			accessProvider: &importer.AccessProvider{
				Id: "zzz",
			},
			calculatedExternalId: "Role3",
		},
	}

	repo.EXPECT().DropAccountRole(mock.MatchedBy(func(roleName string) bool {
		for _, possibleRole := range rolesToRemove {
			if possibleRole.calculatedExternalId == roleName {
				return true
			}
		}
		return false
	})).Return(nil).Times(len(rolesToRemove))

	syncer := createBasicToTargetSyncer(repo, nil, &dummyFeedbackHandler{}, nil)

	// When
	err := syncer.grantsRemoveAll(rolesToRemove)

	// Then
	assert.NoError(t, err)
}

func removeRolesToRemoveNoRoles(t *testing.T) {
	// Given
	repo := newMockDataAccessRepository(t)

	syncer := createBasicToTargetSyncer(repo, nil, nil, nil)

	// When
	err := syncer.grantsRemoveAll([]*ApSyncToTargetItem{})

	// Then
	assert.NoError(t, err)
}

func removeRolesToRemoveError(t *testing.T) {
	// Given
	repo := newMockDataAccessRepository(t)
	rolesToRemove := []*ApSyncToTargetItem{
		{
			accessProvider: &importer.AccessProvider{
				Id: "xxx",
			},
			calculatedExternalId: "Role1",
		},
		{
			accessProvider: &importer.AccessProvider{
				Id: "yyy",
			},
			calculatedExternalId: "Role2",
		},
	}

	repo.EXPECT().DropAccountRole("Role1").Return(nil).Once()
	repo.EXPECT().DropAccountRole("Role2").Return(fmt.Errorf("BOOM")).Once()

	feedbackHandler := mocks.NewSimpleAccessProviderFeedbackHandler(t)
	syncer := createBasicToTargetSyncer(repo, nil, feedbackHandler, nil)

	// When
	err := syncer.grantsRemoveAll(rolesToRemove)

	assert.Len(t, feedbackHandler.AccessProviderFeedback, 2)
	assert.ElementsMatch(t, feedbackHandler.AccessProviderFeedback, []importer.AccessProviderSyncFeedback{
		{
			AccessProvider: "xxx",
			ExternalId:     ptr.String("Role1"),
		},
		{
			AccessProvider: "yyy",
			ExternalId:     ptr.String("Role2"),
			Errors:         []string{"unable to drop role \"Role2\": BOOM"},
		},
	})

	// Then
	assert.NoError(t, err)
}

func TestAccessSyncer_GrantsHandleAllUpdatesOrCreates(t *testing.T) {
	t.Run("Table", grantsHandleAllUpdatesOrCreatesTable)
	t.Run("View", grantsHandleAllUpdatesOrCreatesView)
	t.Run("Schema", grantsHandleAllUpdatesOrCreatesSchema)
	t.Run("Schema - No verify", grantsHandleAllUpdatesOrCreatesSchemaNoVerify)
	t.Run("Schema - Existing", grantsHandleAllUpdatesOrCreatesSchemaExisting)
	t.Run("Shared-database", grantsHandleAllUpdatesOrCreatesSharedDatabase)
	t.Run("Database", grantsHandleAllUpdatesOrCreatesDatabase)
	t.Run("Datasource", grantsHandleAllUpdatesOrCreatesDatasource)
	t.Run("Database - Existing", grantsHandleAllUpdatesOrCreatesDatabaseExisting)
	t.Run("Warehouse", grantsHandleAllUpdatesOrCreatesWarehouse)
	t.Run("Integration", grantsHandleAllUpdatesOrCreatesIntegration)
	t.Run("Existing Role", grantsHandleAllUpdatesOrCreatesExistingRole)
	t.Run("Inheritance", grantsHandleAllUpdatesOrCreatesInheritance)
	t.Run("Rename role", grantsHandleAllUpdatesOrCreatesRename)
	t.Run("Rename role - Existing", grantsHandleAllUpdatesOrCreatesRenameNewExists)
	t.Run("Rename role - Old ExternalId already taken", grantsHandleAllUpdatesOrCreatesRenameOldAlreadyTaken)
}

func grantsHandleAllUpdatesOrCreatesTable(t *testing.T) {
	// Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CreateAccountRole("RoleName1").Return(nil).Once()
	repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "RoleName1").Return(nil).Once()
	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")

	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "DATABASE DB1", "RoleName1", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "SCHEMA DB1.Schema1", "RoleName1", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "TABLE DB1.Schema1.Table1", "RoleName1", false).Return(nil).Once()

	toUpdateOrCreateItems := []*ApSyncToTargetItem{
		{
			accessProvider: &importer.AccessProvider{
				Id:   "AccessProviderId1",
				Name: "AccessProvider1",
				Who: importer.WhoItem{
					Users: []string{"User1", "User2"},
				},
				What: []importer.WhatItem{
					{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table1", Type: "table"}, Permissions: []string{"SELECT"}},
				},
			},
			calculatedExternalId: "RoleName1",
			mutationAction:       ApMutationActionCreate,
		},
	}

	syncer := createBasicToTargetSyncer(repoMock, nil, &dummyFeedbackHandler{}, &config.ConfigMap{})

	// When
	err := syncer.grantsCreateOrUpdateAll(context.Background(), toUpdateOrCreateItems, map[string]*ApSyncToTargetItem{}, set.NewSet[string]())

	// Then
	assert.NoError(t, err)
}

func grantsHandleAllUpdatesOrCreatesView(t *testing.T) {
	// Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CreateAccountRole("RoleName1").Return(nil).Once()
	repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "RoleName1").Return(nil).Once()
	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")

	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "DATABASE DB1", "RoleName1", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "SCHEMA DB1.Schema1", "RoleName1", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "VIEW DB1.Schema1.Table2", "RoleName1", false).Return(nil).Once()

	toUpdateOrCreateItems := []*ApSyncToTargetItem{
		{
			accessProvider: &importer.AccessProvider{
				Id:   "AccessProviderId1",
				Name: "AccessProvider1",
				Who: importer.WhoItem{
					Users: []string{"User1", "User2"},
				},
				What: []importer.WhatItem{
					{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table2", Type: "view"}, Permissions: []string{"SELECT"}},
				},
			},
			calculatedExternalId: "RoleName1",
			mutationAction:       ApMutationActionCreate,
		},
	}

	syncer := createBasicToTargetSyncer(repoMock, nil, &dummyFeedbackHandler{}, &config.ConfigMap{})

	// When
	err := syncer.grantsCreateOrUpdateAll(context.Background(), toUpdateOrCreateItems, map[string]*ApSyncToTargetItem{}, set.NewSet[string]())

	// Then
	assert.NoError(t, err)
}

func grantsHandleAllUpdatesOrCreatesSchema(t *testing.T) {
	// Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CreateAccountRole("RoleName1").Return(nil).Once()
	repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "RoleName1").Return(nil).Once()
	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")

	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "DATABASE DB1", "RoleName1", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "SCHEMA DB1.Schema2", "RoleName1", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "TABLE DB1.Schema2.Table3", "RoleName1", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "VIEW DB1.Schema2.View3", "RoleName1", false).Return(nil).Once()

	database := "DB1"
	schema := "Schema2"

	repoMock.EXPECT().GetTablesInDatabase(database, schema, mock.Anything).RunAndReturn(func(s string, s2 string, handler EntityHandler) error {
		handler(&TableEntity{Database: s, Schema: s2, Name: "Table3", TableType: "BASE TABLE"})
		handler(&TableEntity{Database: s, Schema: s2, Name: "View3", TableType: "VIEW"})
		return nil
	}).Once()

	repoMock.EXPECT().GetFunctionsInDatabase(database, mock.Anything).RunAndReturn(func(s string, handler EntityHandler) error {
		handler(&FunctionEntity{Database: s, Schema: "Schema2", Name: "Decrypt", ArgumentSignature: "(VAL VARCHAR)"})
		return nil
	}).Once()

	repoMock.EXPECT().GetProceduresInDatabase(database, mock.Anything).RunAndReturn(func(s string, handler EntityHandler) error {
		return nil
	}).Once()

	toUpdateOrCreateItems := []*ApSyncToTargetItem{
		{
			accessProvider: &importer.AccessProvider{
				Id:   "AccessProviderId1",
				Name: "AccessProvider1",
				Who: importer.WhoItem{
					Users: []string{"User1", "User2"},
				},
				What: []importer.WhatItem{
					{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema2", Type: "schema"}, Permissions: []string{"SELECT"}},
				},
			},
			calculatedExternalId: "RoleName1",
			mutationAction:       ApMutationActionCreate,
		},
	}

	syncer := createBasicToTargetSyncer(repoMock, nil, &dummyFeedbackHandler{}, &config.ConfigMap{})

	// When
	err := syncer.grantsCreateOrUpdateAll(context.Background(), toUpdateOrCreateItems, map[string]*ApSyncToTargetItem{}, set.NewSet[string]())

	// Then
	assert.NoError(t, err)
}

func grantsHandleAllUpdatesOrCreatesSchemaNoVerify(t *testing.T) {
	// Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CreateAccountRole("RoleName1").Return(nil).Once()
	repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "RoleName1").Return(nil).Once()
	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")

	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "DATABASE DB1", "RoleName1", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "SCHEMA DB1.Schema2", "RoleName1", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("CREATE TABLE", "SCHEMA DB1.Schema2", "RoleName1", false).Return(nil).Once()

	toUpdateOrCreateItems := []*ApSyncToTargetItem{
		{
			accessProvider: &importer.AccessProvider{
				Id:   "AccessProviderId1",
				Name: "AccessProvider1",
				Who: importer.WhoItem{
					Users: []string{"User1", "User2"},
				},
				What: []importer.WhatItem{
					{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema2", Type: "schema"}, Permissions: []string{"CREATE TABLE"}},
				},
			},
			calculatedExternalId: "RoleName1",
			mutationAction:       ApMutationActionCreate,
		},
	}

	syncer := createBasicToTargetSyncer(repoMock, nil, &dummyFeedbackHandler{}, &config.ConfigMap{})

	// When
	err := syncer.grantsCreateOrUpdateAll(context.Background(), toUpdateOrCreateItems, map[string]*ApSyncToTargetItem{}, set.NewSet[string]())

	// Then
	assert.NoError(t, err)
}

func grantsHandleAllUpdatesOrCreatesSchemaExisting(t *testing.T) {
	// Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CommentAccountRoleIfExists(mock.AnythingOfType("string"), "RoleName1").Return(nil).Once()

	repoMock.EXPECT().GetGrantsOfAccountRole(mock.Anything).Return([]GrantOfRole{}, nil)
	repoMock.EXPECT().GetGrantsToAccountRole(mock.Anything).Return([]GrantToRole{}, nil)

	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")

	//ToDo: validate it's ok to add this call
	repoMock.EXPECT().ExecuteRevokeOnAccountRole("ALL", "FUTURE TABLES IN SCHEMA \"DB1.Schema2\"", "RoleName1", false).Return(nil).Once()

	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "DATABASE DB1", "RoleName1", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "SCHEMA DB1.Schema2", "RoleName1", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "TABLE DB1.Schema2.Table3", "RoleName1", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "VIEW DB1.Schema2.View3", "RoleName1", false).Return(nil).Once()

	database := "DB1"
	schema := "Schema2"
	repoMock.EXPECT().GetTablesInDatabase(database, schema, mock.Anything).RunAndReturn(func(s string, s2 string, handler EntityHandler) error {
		handler(&TableEntity{Database: s, Schema: s2, Name: "Table3", TableType: "BASE TABLE"})
		handler(&TableEntity{Database: s, Schema: s2, Name: "View3", TableType: "VIEW"})
		return nil
	}).Once()

	repoMock.EXPECT().GetFunctionsInDatabase(database, mock.Anything).RunAndReturn(func(s string, handler EntityHandler) error {
		return nil
	}).Once()

	repoMock.EXPECT().GetProceduresInDatabase(database, mock.Anything).RunAndReturn(func(s string, handler EntityHandler) error {
		return nil
	}).Once()

	toUpdateOrCreateItems := []*ApSyncToTargetItem{
		{
			accessProvider: &importer.AccessProvider{
				Id:   "AccessProviderId1",
				Name: "AccessProvider1",
				Who: importer.WhoItem{
					Users: []string{"User1", "User2"},
				},
				What: []importer.WhatItem{
					{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema2", Type: "schema"}, Permissions: []string{"SELECT"}},
				},
			},
			calculatedExternalId: "RoleName1",
			mutationAction:       ApMutationActionUpdate,
		},
	}

	syncer := createBasicToTargetSyncer(repoMock, nil, &dummyFeedbackHandler{}, &config.ConfigMap{})

	// When
	err := syncer.grantsCreateOrUpdateAll(context.Background(), toUpdateOrCreateItems, map[string]*ApSyncToTargetItem{}, set.NewSet[string]("RoleName1"))

	// Then
	assert.NoError(t, err)
}

func grantsHandleAllUpdatesOrCreatesSharedDatabase(t *testing.T) {
	// Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CreateAccountRole("RoleName1").Return(nil).Once()
	repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "RoleName1").Return(nil).Once()
	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")

	repoMock.EXPECT().ExecuteGrantOnAccountRole("IMPORTED PRIVILEGES", "DATABASE DB2", "RoleName1", false).Return(nil).Once()

	toUpdateOrCreateItems := []*ApSyncToTargetItem{
		{
			accessProvider: &importer.AccessProvider{
				Id:   "AccessProviderId1",
				Name: "AccessProvider1",
				Who: importer.WhoItem{
					Users: []string{"User1", "User2"},
				},
				What: []importer.WhatItem{
					{DataObject: &data_source.DataObjectReference{FullName: "DB2", Type: "shared-database"}, Permissions: []string{"IMPORTED PRIVILEGES"}},
				},
			},
			calculatedExternalId: "RoleName1",
			mutationAction:       ApMutationActionCreate,
		},
	}

	syncer := createBasicToTargetSyncer(repoMock, nil, &dummyFeedbackHandler{}, &config.ConfigMap{})

	// When
	err := syncer.grantsCreateOrUpdateAll(context.Background(), toUpdateOrCreateItems, map[string]*ApSyncToTargetItem{}, set.NewSet[string]())

	// Then
	assert.NoError(t, err)
}

func grantsHandleAllUpdatesOrCreatesDatabase(t *testing.T) {
	// Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CreateAccountRole("RoleName1").Return(nil).Once()
	repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "RoleName1").Return(nil).Once()
	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")

	database := "DB1"
	schema := "Schema2"
	repoMock.EXPECT().GetTablesInDatabase(database, schema, mock.Anything).RunAndReturn(func(s string, s2 string, handler EntityHandler) error {
		handler(&TableEntity{Database: s, Schema: s2, Name: "Table3", TableType: "BASE TABLE"})
		handler(&TableEntity{Database: s, Schema: s2, Name: "View3", TableType: "VIEW"})
		return nil
	}).Once()

	repoMock.EXPECT().GetSchemasInDatabase("DB1", mock.Anything).RunAndReturn(func(s string, handler EntityHandler) error {
		handler(&SchemaEntity{Database: s, Name: "Schema2"})
		return nil
	}).Once()

	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "DATABASE DB1", "RoleName1", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "SCHEMA DB1.Schema2", "RoleName1", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "TABLE DB1.Schema2.Table3", "RoleName1", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "VIEW DB1.Schema2.View3", "RoleName1", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "PROCEDURE DB1.Schema2.\"procMe\"(VARCHAR)", "RoleName1", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "FUNCTION DB1.Schema2.\"Decrypt\"(VARCHAR)", "RoleName1", false).Return(nil).Once()

	repoMock.EXPECT().GetFunctionsInDatabase(database, mock.Anything).RunAndReturn(func(s string, handler EntityHandler) error {
		handler(&FunctionEntity{Database: s, Schema: "Schema2", Name: "Decrypt", ArgumentSignature: "(VAL VARCHAR)"})
		return nil
	}).Once()

	repoMock.EXPECT().GetProceduresInDatabase(database, mock.Anything).RunAndReturn(func(s string, handler EntityHandler) error {
		handler(&ProcedureEntity{Database: s, Schema: "Schema2", Name: "procMe", ArgumentSignature: "(VAL VARCHAR)"})
		return nil
	}).Once()

	toUpdateOrCreateItems := []*ApSyncToTargetItem{
		{
			accessProvider: &importer.AccessProvider{
				Id:   "AccessProviderId1",
				Name: "AccessProvider1",
				Who: importer.WhoItem{
					Users: []string{"User1", "User2"},
				},
				What: []importer.WhatItem{
					{DataObject: &data_source.DataObjectReference{FullName: "DB1", Type: "database"}, Permissions: []string{"SELECT", "USAGE"}},
				},
			},
			calculatedExternalId: "RoleName1",
			mutationAction:       ApMutationActionCreate,
		},
	}

	syncer := createBasicToTargetSyncer(repoMock, nil, &dummyFeedbackHandler{}, &config.ConfigMap{})

	// When
	err := syncer.grantsCreateOrUpdateAll(context.Background(), toUpdateOrCreateItems, map[string]*ApSyncToTargetItem{}, set.NewSet[string]())

	// Then
	assert.NoError(t, err)
}

func grantsHandleAllUpdatesOrCreatesDatabaseExisting(t *testing.T) {
	// Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CommentAccountRoleIfExists(mock.AnythingOfType("string"), "RoleName1").Return(nil).Once()

	repoMock.EXPECT().GetGrantsOfAccountRole(mock.Anything).Return([]GrantOfRole{}, nil)
	repoMock.EXPECT().GetGrantsToAccountRole(mock.Anything).Return([]GrantToRole{}, nil)

	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")

	database := "DB1"
	schema := "Schema2"
	repoMock.EXPECT().GetTablesInDatabase(database, schema, mock.Anything).RunAndReturn(func(s string, s2 string, handler EntityHandler) error {
		handler(&TableEntity{Database: s, Schema: s2, Name: "Table3", TableType: "BASE TABLE"})
		handler(&TableEntity{Database: s, Schema: s2, Name: "View3", TableType: "VIEW"})
		return nil
	}).Once()

	repoMock.EXPECT().GetSchemasInDatabase("DB1", mock.Anything).RunAndReturn(func(s string, handler EntityHandler) error {
		handler(&SchemaEntity{Database: s, Name: "Schema2"})
		return nil
	}).Once()

	repoMock.EXPECT().GetFunctionsInDatabase(database, mock.Anything).RunAndReturn(func(s string, handler EntityHandler) error {
		handler(&FunctionEntity{Database: s, Schema: "Schema2", Name: "Decrypt", ArgumentSignature: "(VAL VARCHAR)"})
		return nil
	}).Once()

	repoMock.EXPECT().GetProceduresInDatabase(database, mock.Anything).RunAndReturn(func(s string, handler EntityHandler) error {
		return nil
	}).Once()

	repoMock.EXPECT().ExecuteRevokeOnAccountRole("ALL", "FUTURE SCHEMAS IN DATABASE DB1", "RoleName1", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteRevokeOnAccountRole("ALL", "FUTURE TABLES IN DATABASE DB1", "RoleName1", false).Return(nil).Once()

	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "DATABASE DB1", "RoleName1", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "SCHEMA DB1.Schema2", "RoleName1", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "TABLE DB1.Schema2.Table3", "RoleName1", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "VIEW DB1.Schema2.View3", "RoleName1", false).Return(nil).Once()

	repoMock.EXPECT().CommentDatabaseRoleIfExists(mock.AnythingOfType("string"), "DB1", "DatabaseRole1").Return(nil).Once()

	repoMock.EXPECT().GetGrantsOfDatabaseRole("DB1", "DatabaseRole1").Return([]GrantOfRole{}, nil)
	repoMock.EXPECT().GetGrantsToDatabaseRole("DB1", "DatabaseRole1").Return([]GrantToRole{}, nil)

	repoMock.EXPECT().ExecuteRevokeOnDatabaseRole("ALL", "FUTURE SCHEMAS IN DATABASE DB1", "DB1", "DatabaseRole1").Return(nil).Once()
	repoMock.EXPECT().ExecuteRevokeOnDatabaseRole("ALL", "FUTURE TABLES IN DATABASE DB1", "DB1", "DatabaseRole1").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrantOnDatabaseRole("USAGE", "DATABASE DB1", "DB1", "DatabaseRole1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnDatabaseRole("USAGE", "SCHEMA DB1.Schema2", "DB1", "DatabaseRole1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnDatabaseRole("SELECT", "TABLE DB1.Schema2.Table3", "DB1", "DatabaseRole1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnDatabaseRole("SELECT", "VIEW DB1.Schema2.View3", "DB1", "DatabaseRole1").Return(nil).Once()

	toUpdateOrCreateItems := []*ApSyncToTargetItem{
		{
			accessProvider: &importer.AccessProvider{
				Id:   "AccessProviderId1",
				Name: "AccessProvider1",
				Who: importer.WhoItem{
					Users: []string{"User1", "User2"},
				},
				What: []importer.WhatItem{
					{DataObject: &data_source.DataObjectReference{FullName: "DB1", Type: "database"}, Permissions: []string{"SELECT"}},
				},
			},
			calculatedExternalId: "RoleName1",
			mutationAction:       ApMutationActionUpdate,
		},
		{
			accessProvider: &importer.AccessProvider{
				Id:   "DB1_DatabaseRole1",
				Name: "DatabaseRole1",
				Type: ptr.String("databaseRole"),
				Who:  importer.WhoItem{},
				What: []importer.WhatItem{
					{DataObject: &data_source.DataObjectReference{FullName: "DB1", Type: "database"}, Permissions: []string{"SELECT"}},
				},
			},
			calculatedExternalId: "DATABASEROLE###DATABASE:DB1###ROLE:DatabaseRole1",
			mutationAction:       ApMutationActionUpdate,
		},
	}

	syncer := createBasicToTargetSyncer(repoMock, nil, &dummyFeedbackHandler{}, &config.ConfigMap{})

	// When
	err := syncer.grantsCreateOrUpdateAll(context.Background(), toUpdateOrCreateItems, map[string]*ApSyncToTargetItem{}, set.NewSet[string]("RoleName1", "DATABASEROLE###DATABASE:DB1###ROLE:DatabaseRole1"))

	// Then
	assert.NoError(t, err)
}

func grantsHandleAllUpdatesOrCreatesWarehouse(t *testing.T) {
	// Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CreateAccountRole("RoleName1").Return(nil).Once()
	repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "RoleName1").Return(nil).Once()
	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")

	repoMock.EXPECT().ExecuteGrantOnAccountRole("MONITOR", "WAREHOUSE WH1", "RoleName1", false).Return(nil).Once()

	toUpdateOrCreateItems := []*ApSyncToTargetItem{
		{
			accessProvider: &importer.AccessProvider{
				Id:   "AccessProviderId1",
				Name: "AccessProvider1",
				Who: importer.WhoItem{
					Users: []string{"User1", "User2"},
				},
				What: []importer.WhatItem{
					{DataObject: &data_source.DataObjectReference{FullName: "WH1", Type: "warehouse"}, Permissions: []string{"MONITOR"}},
				},
			},
			calculatedExternalId: "RoleName1",
			mutationAction:       ApMutationActionCreate,
		},
	}

	syncer := createBasicToTargetSyncer(repoMock, nil, &dummyFeedbackHandler{}, &config.ConfigMap{})

	// When
	err := syncer.grantsCreateOrUpdateAll(context.Background(), toUpdateOrCreateItems, map[string]*ApSyncToTargetItem{}, set.NewSet[string]())

	// Then
	assert.NoError(t, err)
}

func grantsHandleAllUpdatesOrCreatesIntegration(t *testing.T) {
	// Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CreateAccountRole("RoleName1").Return(nil).Once()
	repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "RoleName1").Return(nil).Once()
	expectGrantUsersToRole(repoMock, "RoleName1", "User1")

	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "INTEGRATION I1", "RoleName1", false).Return(nil).Once()

	toUpdateOrCreateItems := []*ApSyncToTargetItem{
		{
			accessProvider: &importer.AccessProvider{
				Id:   "AccessProviderId1",
				Name: "AccessProvider1",
				Who: importer.WhoItem{
					Users: []string{"User1"},
				},
				What: []importer.WhatItem{
					{DataObject: &data_source.DataObjectReference{FullName: "I1", Type: "integration"}, Permissions: []string{"USAGE"}},
				},
			},
			calculatedExternalId: "RoleName1",
			mutationAction:       ApMutationActionCreate,
		},
	}

	syncer := createBasicToTargetSyncer(repoMock, nil, &dummyFeedbackHandler{}, &config.ConfigMap{})

	// When
	err := syncer.grantsCreateOrUpdateAll(context.Background(), toUpdateOrCreateItems, map[string]*ApSyncToTargetItem{}, set.NewSet[string]())

	// Then
	assert.NoError(t, err)
}

func grantsHandleAllUpdatesOrCreatesDatasource(t *testing.T) {
	// Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CreateAccountRole("RoleName1").Return(nil).Once()
	repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "RoleName1").Return(nil).Once()
	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")
	repoMock.EXPECT().GetInboundShares().Return([]DbEntity{}, nil).Once()
	repoMock.EXPECT().GetDatabases().Return([]DbEntity{}, nil).Once()

	toUpdateOrCreateItems := []*ApSyncToTargetItem{
		{
			accessProvider: &importer.AccessProvider{
				Id:   "AccessProviderId1",
				Name: "AccessProvider1",
				Who: importer.WhoItem{
					Users: []string{"User1", "User2"},
				},
				What: []importer.WhatItem{
					{DataObject: &data_source.DataObjectReference{FullName: "DS1", Type: "datasource"}, Permissions: []string{"READ"}},
				},
			},
			calculatedExternalId: "RoleName1",
			mutationAction:       ApMutationActionCreate,
		},
	}

	syncer := createBasicToTargetSyncer(repoMock, nil, &dummyFeedbackHandler{}, &config.ConfigMap{})

	// When
	err := syncer.grantsCreateOrUpdateAll(context.Background(), toUpdateOrCreateItems, map[string]*ApSyncToTargetItem{}, set.NewSet[string]())

	// Then
	assert.NoError(t, err)
}

func grantsHandleAllUpdatesOrCreatesExistingRole(t *testing.T) {
	// Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CommentAccountRoleIfExists(mock.AnythingOfType("string"), "existingRole1").Return(nil).Once()
	repoMock.EXPECT().GetGrantsOfAccountRole("existingRole1").Return([]GrantOfRole{
		{GrantedTo: "USER", GranteeName: "User1"},
		{GrantedTo: "USER", GranteeName: "User3"},
		{GrantedTo: "Role", GranteeName: "Role1"},
		{GrantedTo: "Role", GranteeName: "Role3"},
	}, nil).Once()

	repoMock.EXPECT().GetGrantsToAccountRole("existingRole1").Return([]GrantToRole{}, nil).Once()

	expectGrantUsersToRole(repoMock, "existingRole1", "User2")
	repoMock.EXPECT().GrantAccountRolesToAccountRole(mock.Anything, "existingRole1", "Role2").Return(nil).Once()
	repoMock.EXPECT().RevokeAccountRolesFromAccountRole(mock.Anything, "existingRole1", "Role3").Return(nil).Once()
	repoMock.EXPECT().RevokeUsersFromAccountRole(mock.Anything, "existingRole1", "User3").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "DATABASE DB1", "existingRole1", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "SCHEMA DB1.Schema1", "existingRole1", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "TABLE DB1.Schema1.Table1", "existingRole1", false).Return(nil).Once()

	repoMock.EXPECT().CommentDatabaseRoleIfExists(mock.AnythingOfType("string"), "TEST_DB", "existingDBRole1").Return(nil).Once()
	repoMock.EXPECT().GetGrantsOfDatabaseRole("TEST_DB", "existingDBRole1").Return([]GrantOfRole{
		{GrantedTo: GrantTypeDatabaseRole, GranteeName: "TEST_DB.Role2"},
		{GrantedTo: GrantTypeDatabaseRole, GranteeName: "TEST_DB.Role3"},
	}, nil).Once()

	repoMock.EXPECT().GetGrantsToDatabaseRole("TEST_DB", "existingDBRole1").Return([]GrantToRole{}, nil).Once()

	expectGrantAccountOrDatabaseRolesToDatabaseRole(repoMock, true, "TEST_DB", "existingDBRole1", "DATABASEROLE###DATABASE:TEST_DB###ROLE:Role1")
	expectGrantAccountOrDatabaseRolesToDatabaseRole(repoMock, false, "TEST_DB", "existingDBRole1")
	repoMock.EXPECT().RevokeDatabaseRolesFromDatabaseRole(mock.Anything, "TEST_DB", "existingDBRole1", "Role3").Return(nil).Once()
	repoMock.EXPECT().RevokeAccountRolesFromDatabaseRole(mock.Anything, "TEST_DB", "existingDBRole1").Return(nil).Once()

	toUpdateOrCreateItems := []*ApSyncToTargetItem{
		{
			accessProvider: &importer.AccessProvider{
				Id:   "AccessProviderId1",
				Name: "AccessProvider1",
				Who: importer.WhoItem{
					Users:       []string{"User1", "User2"},
					InheritFrom: []string{"Role1", "Role2"},
				},
				What: []importer.WhatItem{
					{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table1", Type: "table"}, Permissions: []string{"SELECT"}},
				},
			},
			calculatedExternalId: "existingRole1",
			mutationAction:       ApMutationActionUpdate,
		},
		{
			accessProvider: &importer.AccessProvider{
				Id:         "TEST_DB_existingDBRole1",
				Name:       "existingDBRole1",
				ActualName: ptr.String(""),
				Who: importer.WhoItem{
					InheritFrom: []string{"DATABASEROLE###DATABASE:TEST_DB###ROLE:Role1", "DATABASEROLE###DATABASE:TEST_DB###ROLE:Role2"},
				},
				What: []importer.WhatItem{},
				Type: ptr.String("databaseRole"),
			},
			calculatedExternalId: "DATABASEROLE###DATABASE:TEST_DB###ROLE:existingDBRole1",
			mutationAction:       ApMutationActionUpdate,
		},
	}

	syncer := createBasicToTargetSyncer(repoMock, nil, &dummyFeedbackHandler{}, &config.ConfigMap{})

	// When
	err := syncer.grantsCreateOrUpdateAll(context.Background(), toUpdateOrCreateItems, map[string]*ApSyncToTargetItem{}, set.NewSet[string]("existingRole1", "DATABASEROLE###DATABASE:TEST_DB###ROLE:existingDBRole1"))

	// Then
	assert.NoError(t, err)
}

func grantsHandleAllUpdatesOrCreatesInheritance(t *testing.T) {
	// Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CreateAccountRole("RoleName1").Return(nil).Once()
	repoMock.EXPECT().CreateAccountRole("RoleName2").Return(nil).Once()
	repoMock.EXPECT().CreateAccountRole("RoleName3").Return(nil).Once()
	repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "RoleName1").Return(nil).Once()
	repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "RoleName2").Return(nil).Once()
	repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "RoleName3").Return(nil).Once()
	expectGrantUsersToRole(repoMock, "RoleName3", "User1")
	repoMock.EXPECT().GrantAccountRolesToAccountRole(mock.Anything, "RoleName1", "RoleName2").Return(nil).Once()
	repoMock.EXPECT().GrantAccountRolesToAccountRole(mock.Anything, "RoleName2", "RoleName3").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "DATABASE DB1", "RoleName1", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "SCHEMA DB1.Schema1", "RoleName1", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "TABLE DB1.Schema1.Table1", "RoleName1", false).Return(nil).Once()

	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "DATABASE DB1", "RoleName2", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "SCHEMA DB1.Schema1", "RoleName2", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "TABLE DB1.Schema1.Table2", "RoleName2", false).Return(nil).Once()

	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "DATABASE DB1", "RoleName3", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "SCHEMA DB1.Schema1", "RoleName3", false).Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "TABLE DB1.Schema1.Table3", "RoleName3", false).Return(nil).Once()

	toUpdateOrCreateItems := []*ApSyncToTargetItem{
		{
			accessProvider: &importer.AccessProvider{
				Id:   "AccessProviderId1",
				Name: "AccessProvider1",
				Who: importer.WhoItem{
					InheritFrom: []string{"ID:AccessProviderId2"},
				},
				What: []importer.WhatItem{
					{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table1", Type: "table"}, Permissions: []string{"SELECT"}},
				},
			},
			calculatedExternalId: "RoleName1",
			mutationAction:       ApMutationActionCreate,
		},
		{
			accessProvider: &importer.AccessProvider{
				Id:   "AccessProviderId2",
				Name: "AccessProvider2",
				Who: importer.WhoItem{
					InheritFrom: []string{"RoleName3"},
				},
				What: []importer.WhatItem{
					{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table2", Type: "table"}, Permissions: []string{"SELECT"}},
				},
			},
			calculatedExternalId: "RoleName2",
			mutationAction:       ApMutationActionCreate,
		},
		{
			accessProvider: &importer.AccessProvider{
				Id:   "AccessProviderId3",
				Name: "AccessProvider3",
				Who: importer.WhoItem{
					Users: []string{"User1"},
				},
				What: []importer.WhatItem{
					{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table3", Type: "table"}, Permissions: []string{"SELECT"}},
				},
			},
			calculatedExternalId: "RoleName3",
			mutationAction:       ApMutationActionCreate,
		},
	}

	syncer := createBasicToTargetSyncer(repoMock, nil, &dummyFeedbackHandler{}, &config.ConfigMap{})

	// When
	err := syncer.grantsCreateOrUpdateAll(context.Background(), toUpdateOrCreateItems, map[string]*ApSyncToTargetItem{
		"AccessProviderId1": toUpdateOrCreateItems[0],
		"AccessProviderId2": toUpdateOrCreateItems[1],
		"AccessProviderId3": toUpdateOrCreateItems[2],
	}, set.NewSet[string]())

	// Then
	assert.NoError(t, err)
}

// Testing the normal rename case where we need to rename the role to the new name and then update stuff.
func grantsHandleAllUpdatesOrCreatesRename(t *testing.T) {
	// Given
	repoMock := newMockDataAccessRepository(t)
	feedbackHandler := mocks.NewSimpleAccessProviderFeedbackHandler(t)

	expectGrantUsersToRole(repoMock, "NewRoleName", "User1")
	repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "NewRoleName").Return(nil).Once()
	repoMock.EXPECT().RenameAccountRole("OldRoleName", "NewRoleName").Return(nil).Once()
	repoMock.EXPECT().GetGrantsOfAccountRole("NewRoleName").Return([]GrantOfRole{}, nil).Once()
	repoMock.EXPECT().GetGrantsToAccountRole("NewRoleName").Return([]GrantToRole{}, nil).Once()

	repoMock.EXPECT().CommentDatabaseRoleIfExists(mock.AnythingOfType("string"), "TEST_DB", "newDBRole").Return(nil).Once()
	repoMock.EXPECT().RenameDatabaseRole("TEST_DB", "oldDBRole", "newDBRole").Return(nil).Once()
	repoMock.EXPECT().GetGrantsOfDatabaseRole("TEST_DB", "newDBRole").Return([]GrantOfRole{
		{GrantedTo: GrantTypeDatabaseRole, GranteeName: "TEST_DB.Role1"},
		{GrantedTo: GrantTypeDatabaseRole, GranteeName: "TEST_DB.Role2"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsToDatabaseRole("TEST_DB", "newDBRole").Return([]GrantToRole{}, nil).Once()

	toUpdateOrCreateItems := []*ApSyncToTargetItem{
		{
			accessProvider: &importer.AccessProvider{
				Id:   "AccessProviderId",
				Name: "AccessProvider",
				Who: importer.WhoItem{
					Users: []string{"User1"},
				},
				ExternalId: ptr.String("OldRoleName"),
			},
			calculatedExternalId: "NewRoleName",
			mutationAction:       ApMutationActionRename,
		},
		{
			accessProvider: &importer.AccessProvider{
				Id:         "TEST_DB_DBRole",
				ActualName: ptr.String("anActualName"),
				ExternalId: ptr.String("DATABASEROLE###DATABASE:TEST_DB###ROLE:oldDBRole"),
				Name:       "TEST_DB.DBRole",
				Who: importer.WhoItem{
					InheritFrom: []string{"DATABASEROLE###DATABASE:TEST_DB###ROLE:Role1", "DATABASEROLE###DATABASE:TEST_DB###ROLE:Role2"},
				},
				What: []importer.WhatItem{},
				Type: ptr.String("databaseRole"),
			},
			calculatedExternalId: "DATABASEROLE###DATABASE:TEST_DB###ROLE:newDBRole",
			mutationAction:       ApMutationActionRename,
		},
	}

	syncer := createBasicToTargetSyncer(repoMock, nil, feedbackHandler, &config.ConfigMap{})

	// When
	err := syncer.grantsCreateOrUpdateAll(context.Background(), toUpdateOrCreateItems, map[string]*ApSyncToTargetItem{}, set.NewSet[string]("OldRoleName", "DATABASEROLE###DATABASE:TEST_DB###ROLE:oldDBRole"))

	// Then
	assert.NoError(t, err)
	assert.Len(t, feedbackHandler.AccessProviderFeedback, 2)
	assert.ElementsMatch(t, feedbackHandler.AccessProviderFeedback, []importer.AccessProviderSyncFeedback{
		{
			AccessProvider: "AccessProviderId",
			ActualName:     "NewRoleName",
			ExternalId:     ptr.String("NewRoleName"),
			Type:           ptr.String(access_provider.Role),
		}, {
			AccessProvider: "TEST_DB_DBRole",
			ActualName:     "newDBRole",
			ExternalId:     ptr.String("DATABASEROLE###DATABASE:TEST_DB###ROLE:newDBRole"),
			Type:           ptr.String("databaseRole"),
		},
	})
}

// Testing the rename of APs where the new role name already exists (but not needed), so the old should get dropped
func grantsHandleAllUpdatesOrCreatesRenameNewExists(t *testing.T) {
	// Given
	repoMock := newMockDataAccessRepository(t)
	feedbackHandler := mocks.NewSimpleAccessProviderFeedbackHandler(t)

	expectGrantUsersToRole(repoMock, "NewRoleName", "User1")
	repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "NewRoleName").Return(nil).Once()
	repoMock.EXPECT().DropAccountRole("OldRoleName").Return(nil).Once()
	repoMock.EXPECT().GetGrantsOfAccountRole("NewRoleName").Return([]GrantOfRole{}, nil).Once()
	repoMock.EXPECT().GetGrantsToAccountRole("NewRoleName").Return([]GrantToRole{}, nil).Once()

	toUpdateOrCreateItems := []*ApSyncToTargetItem{
		{
			accessProvider: &importer.AccessProvider{
				Id:         "AccessProviderId",
				Name:       "AccessProvider",
				ExternalId: ptr.String("OldRoleName"),
				Type:       ptr.String(access_provider.Role),
				Who: importer.WhoItem{
					Users: []string{"User1"},
				},
			},
			calculatedExternalId: "NewRoleName",
			mutationAction:       ApMutationActionRename,
		},
	}

	syncer := createBasicToTargetSyncer(repoMock, nil, feedbackHandler, &config.ConfigMap{})

	// When
	err := syncer.grantsCreateOrUpdateAll(context.Background(), toUpdateOrCreateItems, map[string]*ApSyncToTargetItem{}, set.NewSet[string]("OldRoleName", "NewRoleName"))

	// Then
	assert.NoError(t, err)
	assert.Len(t, feedbackHandler.AccessProviderFeedback, 1)
	assert.ElementsMatch(t, feedbackHandler.AccessProviderFeedback, []importer.AccessProviderSyncFeedback{
		{
			AccessProvider: "AccessProviderId",
			ActualName:     "NewRoleName",
			ExternalId:     ptr.String("NewRoleName"),
			Type:           ptr.String(access_provider.Role),
		},
	})
}

// Testing the rename of APs where the old role name is already taken by another AP in the meanwhile.
// So it should not get dropped but updated instead.
// The new role name should be created from scratch.
func grantsHandleAllUpdatesOrCreatesRenameOldAlreadyTaken(t *testing.T) {
	// Given
	repoMock := newMockDataAccessRepository(t)
	feedbackHandler := mocks.NewSimpleAccessProviderFeedbackHandler(t)

	repoMock.EXPECT().CreateAccountRole("NewRoleName").Return(nil).Once()
	expectGrantUsersToRole(repoMock, "NewRoleName", "User1")
	repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "NewRoleName").Return(nil).Once()
	repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "OldRoleName").Return(nil).Once()
	repoMock.EXPECT().GetGrantsOfAccountRole("OldRoleName").Return([]GrantOfRole{}, nil).Once()
	repoMock.EXPECT().GetGrantsToAccountRole("OldRoleName").Return([]GrantToRole{}, nil).Once()

	toUpdateOrCreateItems := []*ApSyncToTargetItem{
		{
			accessProvider: &importer.AccessProvider{
				Id:         "AccessProviderId",
				ExternalId: ptr.String("OldRoleName"),
				Name:       "AccessProvider",
				Who: importer.WhoItem{
					Users: []string{"User1"},
				},
			},
			calculatedExternalId: "NewRoleName",
			mutationAction:       ApMutationActionRename,
		},
		{
			accessProvider: &importer.AccessProvider{
				Id:   "AccessProviderId2",
				Name: "AccessProvider2",
			},
			calculatedExternalId: "OldRoleName",
			mutationAction:       ApMutationActionCreate,
		},
	}

	syncer := createBasicToTargetSyncer(repoMock, nil, feedbackHandler, &config.ConfigMap{})

	// When
	err := syncer.grantsCreateOrUpdateAll(context.Background(), toUpdateOrCreateItems, map[string]*ApSyncToTargetItem{}, set.NewSet[string]("OldRoleName"))

	// Then
	assert.NoError(t, err)
	assert.Len(t, feedbackHandler.AccessProviderFeedback, 2)
	assert.ElementsMatch(t, feedbackHandler.AccessProviderFeedback, []importer.AccessProviderSyncFeedback{
		{
			AccessProvider: "AccessProviderId",
			ActualName:     "NewRoleName",
			ExternalId:     ptr.String("NewRoleName"),
			Type:           ptr.String(access_provider.Role),
		},
		{
			AccessProvider: "AccessProviderId2",
			ActualName:     "OldRoleName",
			ExternalId:     ptr.String("OldRoleName"),
			Type:           ptr.String(access_provider.Role),
		},
	})
}

func TestAccessSyncer_GrantRolesToRole(t *testing.T) {
	t.Run("Database filtering", grantRolesToRoleDatabaseFiltering)
	t.Run("Account filtering", grantRolesToRoleAccountFiltering)
}

func grantRolesToRoleDatabaseFiltering(t *testing.T) {
	repoMock := newMockDataAccessRepository(t)
	syncer := createBasicToTargetSyncer(repoMock, nil, &dummyFeedbackHandler{}, &config.ConfigMap{})
	syncer.ignoreLinksToRole = []string{"My.+"}

	repoMock.EXPECT().GrantDatabaseRolesToDatabaseRole(mock.Anything, "DB1", "TargetRole", "AnotherDBRole").Return(nil).Once()
	repoMock.EXPECT().GrantAccountRolesToDatabaseRole(mock.Anything, "DB1", "TargetRole", "AnotherRole").Return(nil).Once()

	dbRoleType := apTypeDatabaseRole
	err := syncer.grantRolesToRole(context.Background(), databaseRoleExternalIdGenerator("DB1", "TargetRole"), &dbRoleType, "MyRole1", "AnotherRole", databaseRoleExternalIdGenerator("DB1", "MyDBRole"), databaseRoleExternalIdGenerator("DB1", "AnotherDBRole"))
	assert.NoError(t, err)
}

func grantRolesToRoleAccountFiltering(t *testing.T) {
	repoMock := newMockDataAccessRepository(t)
	syncer := createBasicToTargetSyncer(repoMock, nil, &dummyFeedbackHandler{}, &config.ConfigMap{})
	syncer.ignoreLinksToRole = []string{"My.+"}

	repoMock.EXPECT().GrantAccountRolesToAccountRole(mock.Anything, "TargetRole", "AnotherRole").Return(nil).Once()

	err := syncer.grantRolesToRole(context.Background(), "TargetRole", nil, "MyRole1", "AnotherRole")
	assert.NoError(t, err)
}

func TestAccessSyncer_RevokeRolesFromRole(t *testing.T) {
	t.Run("Database filtering", revokeRolesFromRoleDatabaseFiltering)
	t.Run("Account filtering", revokeRolesFromRoleAccountFiltering)
}

func revokeRolesFromRoleDatabaseFiltering(t *testing.T) {
	repoMock := newMockDataAccessRepository(t)
	syncer := createBasicToTargetSyncer(repoMock, nil, &dummyFeedbackHandler{}, &config.ConfigMap{})
	syncer.ignoreLinksToRole = []string{"My.+"}

	repoMock.EXPECT().RevokeDatabaseRolesFromDatabaseRole(mock.Anything, "DB1", "TargetRole", "AnotherDBRole").Return(nil).Once()
	repoMock.EXPECT().RevokeAccountRolesFromDatabaseRole(mock.Anything, "DB1", "TargetRole", "AnotherRole").Return(nil).Once()

	dbRoleType := apTypeDatabaseRole
	err := syncer.revokeRolesFromRole(context.Background(), databaseRoleExternalIdGenerator("DB1", "TargetRole"), &dbRoleType, "MyRole1", "AnotherRole", databaseRoleExternalIdGenerator("DB1", "MyDBRole"), databaseRoleExternalIdGenerator("DB1", "AnotherDBRole"))
	assert.NoError(t, err)
}

func revokeRolesFromRoleAccountFiltering(t *testing.T) {
	repoMock := newMockDataAccessRepository(t)
	syncer := createBasicToTargetSyncer(repoMock, nil, &dummyFeedbackHandler{}, &config.ConfigMap{})
	syncer.ignoreLinksToRole = []string{"My.+"}

	repoMock.EXPECT().RevokeAccountRolesFromAccountRole(mock.Anything, "TargetRole", "AnotherRole").Return(nil).Once()

	err := syncer.revokeRolesFromRole(context.Background(), "TargetRole", nil, "MyRole1", "AnotherRole")
	assert.NoError(t, err)
}

func TestAccessSyncer_RenameRole(t *testing.T) {
	apTypeAccountRole := ptr.String(access_provider.Role)
	apTypeDatabaseRole := ptr.String("databaseRole")

	type fields struct {
		setup func(repoMock *mockDataAccessRepository, feedbackHandlerMock *mocks.SimpleAccessProviderFeedbackHandler)
	}
	type args struct {
		oldName string
		newName string
		apType  *string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "account roles - basic",
			fields: fields{
				setup: func(repoMock *mockDataAccessRepository, feedbackHandlerMock *mocks.SimpleAccessProviderFeedbackHandler) {
					repoMock.EXPECT().RenameAccountRole("test", "test2").Return(nil).Once()
				},
			},
			args: args{
				oldName: "test",
				newName: "test2",
				apType:  apTypeAccountRole,
			},
			wantErr: require.NoError,
		},
		{
			name: "database roles - basic",
			fields: fields{
				setup: func(repoMock *mockDataAccessRepository, feedbackHandlerMock *mocks.SimpleAccessProviderFeedbackHandler) {
					repoMock.EXPECT().RenameDatabaseRole("TEST_DB", "oldDBRole", "newDBRole").Return(nil).Once()
				},
			},
			args: args{
				oldName: "DATABASEROLE###DATABASE:TEST_DB###ROLE:oldDBRole",
				newName: "DATABASEROLE###DATABASE:TEST_DB###ROLE:newDBRole",
				apType:  apTypeDatabaseRole,
			},
			wantErr: require.NoError,
		},
		{
			name: "database roles - not same target databases",
			fields: fields{
				setup: func(repoMock *mockDataAccessRepository, feedbackHandlerMock *mocks.SimpleAccessProviderFeedbackHandler) {
				},
			},
			args: args{
				oldName: "TEST_DB1.oldDBRole",
				newName: "TEST_DB2.newDBRole",
				apType:  apTypeDatabaseRole,
			},
			wantErr: require.Error,
		},
		{
			name: "database roles - invalidate oldName",
			fields: fields{
				setup: func(repoMock *mockDataAccessRepository, feedbackHandlerMock *mocks.SimpleAccessProviderFeedbackHandler) {
				},
			},
			args: args{
				oldName: "oldDBRole",
				newName: "DATABASEROLE###DATABASE:TEST_DB###ROLE:newDBRole",
				apType:  apTypeDatabaseRole,
			},
			wantErr: require.Error,
		},
		{
			name: "database roles - invalidate newName",
			fields: fields{
				setup: func(repoMock *mockDataAccessRepository, feedbackHandlerMock *mocks.SimpleAccessProviderFeedbackHandler) {
				},
			},
			args: args{
				oldName: "DATABASEROLE###DATABASE:TEST_DB###ROLE:oldDBRole",
				newName: "newDBRole",
				apType:  apTypeDatabaseRole,
			},
			wantErr: require.Error,
		},
		{
			name: "database roles - only oldName is a databaseRole",
			fields: fields{
				setup: func(repoMock *mockDataAccessRepository, feedbackHandlerMock *mocks.SimpleAccessProviderFeedbackHandler) {
				},
			},
			args: args{
				oldName: "DATABASEROLE###DATABASE:TEST_DB###ROLE:oldDBRole",
				newName: "newDBRole",
				apType:  apTypeDatabaseRole,
			},
			wantErr: require.Error,
		},
		{
			name: "database roles - only newName is a databaseRole",
			fields: fields{
				setup: func(repoMock *mockDataAccessRepository, feedbackHandlerMock *mocks.SimpleAccessProviderFeedbackHandler) {
				},
			},
			args: args{
				oldName: "oldDBRole",
				newName: "DATABASEROLE###DATABASE:TEST_DB###ROLE:newDBRole",
				apType:  apTypeDatabaseRole,
			},
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Given
			repoMock := newMockDataAccessRepository(t)
			feedbackHandler := mocks.NewSimpleAccessProviderFeedbackHandler(t)

			tt.fields.setup(repoMock, feedbackHandler)

			syncer := createBasicToTargetSyncer(repoMock, nil, nil, nil)

			// When
			err := syncer.renameRole(tt.args.oldName, tt.args.newName, tt.args.apType)

			// Then
			tt.wantErr(t, err)
		})
	}
}
