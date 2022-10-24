package snowflake

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/raito-io/cli/base/access_provider/sync_from_target"
	importer "github.com/raito-io/cli/base/access_provider/sync_to_target"
	"github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/raito-io/cli-plugin-snowflake/common"
)

func TestAccessSyncer_SyncAccessProvidersFromTarget(t *testing.T) {
	//Given
	configParams := config.ConfigMap{
		Parameters: map[string]interface{}{"key": "value", SfExcludedOwners: "OwnerToExclude1,OwnerToExclude2"},
	}

	repoMock := newMockDataAccessRepository(t)
	fileCreator := mocks.NewSimpleAccessProviderHandler(t, 1)

	repoMock.EXPECT().Close().Return(nil).Once()
	repoMock.EXPECT().TotalQueryTime().Return(time.Minute).Once()
	repoMock.EXPECT().GetShares().Return([]DbEntity{
		{Name: "Share1"}, {Name: "Share2"},
	}, nil).Once()
	repoMock.EXPECT().GetRoles().Return([]RoleEntity{
		{Name: "Role1", AssignedToUsers: 2, GrantedRoles: 3, GrantedToRoles: 1, Owner: "Owner1"},
		{Name: "Role2", AssignedToUsers: 3, GrantedRoles: 2, GrantedToRoles: 1, Owner: "Owner2"},
		{Name: "Role3", AssignedToUsers: 1, GrantedRoles: 1, GrantedToRoles: 1, Owner: "OwnerToExclude2"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsOfRole("Role1").Return([]GrantOfRole{
		{GrantedTo: "USER", GranteeName: "GranteeRole1Number1"},
		{GrantedTo: "ROLE", GranteeName: "GranteeRole1Number2"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsToRole("Role1").Return([]GrantToRole{
		{GrantedOn: "SCHEMA", Name: "Share2.GranteeRole1Schema", Privilege: "USAGE"},
		{GrantedOn: "SCHEMA", Name: "Share2.GranteeRole1Schema", Privilege: "READ"},
		{GrantedOn: "TABLE", Name: "DB1.GranteeRole1Table", Privilege: "USAGE"},
		{GrantedOn: "TABLE", Name: "DB1.GranteeRole1Table", Privilege: "SELECT"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsOfRole("Role2").Return([]GrantOfRole{
		{GrantedTo: "USER", GranteeName: "GranteeRole2"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsToRole("Role2").Return([]GrantToRole{
		{GrantedOn: "GrandOnRole2Number1", Name: "GranteeRole2", Privilege: "USAGE"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsOfRole("Role3").Return([]GrantOfRole{
		{GrantedTo: "ROLE", GranteeName: "GranteeRole3"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsToRole("Role3").Return([]GrantToRole{
		{GrantedOn: "GrandOnRole3Number1", Name: "GranteeRole3", Privilege: "WRITE"},
	}, nil).Once()
	repoMock.EXPECT().GetPolicies("MASKING").Return([]policyEntity{
		{Name: "MaskingPolicy1", SchemaName: "schema1", DatabaseName: "DB", Owner: "MaskingOwner", Kind: "MASKING_POLICY"},
	}, nil).Once()
	repoMock.EXPECT().GetPolicies("ROW ACCESS").Return([]policyEntity{
		{Name: "RowAccess1", SchemaName: "schema2", DatabaseName: "DB", Owner: "RowAccessOwner", Kind: "ROW_ACCESS_POLICY"},
	}, nil).Once()
	repoMock.EXPECT().DescribePolicy("MASKING", "DB", "schema1", "MaskingPolicy1").Return([]describePolicyEntity{
		{Name: "DescribePolicy1", Body: "PolicyBody 1"},
	}, nil).Once()
	repoMock.EXPECT().DescribePolicy("ROW ACCESS", "DB", "schema2", "RowAccess1").Return([]describePolicyEntity{
		{Name: "DescribePolicy2", Body: "Row Access Policy Body"},
	}, nil).Once()
	repoMock.EXPECT().GetPolicyReferences("DB", "schema1", "MaskingPolicy1").Return([]policyReferenceEntity{
		{POLICY_DB: "PolicyDB"},
	}, nil).Once()
	repoMock.EXPECT().GetPolicyReferences("DB", "schema2", "RowAccess1").Return([]policyReferenceEntity{
		{POLICY_DB: "PolicyDB"},
	}, nil).Once()

	syncer := &AccessSyncer{
		repoProvider: func(params map[string]interface{}, role string) (dataAccessRepository, error) {
			return repoMock, nil
		},
	}

	//When
	err := syncer.SyncAccessProvidersFromTarget(context.Background(), fileCreator, &configParams)

	//Then
	assert.NoError(t, err)
	assert.Equal(t, []sync_from_target.AccessProvider{
		{
			ExternalId:        "Role1",
			NotInternalizable: false,
			Name:              "Role1",
			NamingHint:        "Role1",
			Access: []*sync_from_target.Access{
				{
					ActualName: "Role1",
					Who: &sync_from_target.WhoItem{
						Users:           []string{"GranteeRole1Number1"},
						Groups:          []string{},
						AccessProviders: []string{"GranteeRole1Number2"},
					},
					What: []sync_from_target.WhatItem{
						{
							DataObject: &data_source.DataObjectReference{
								FullName: "Share2.GranteeRole1Schema",
								Type:     "SHARED-SCHEMA",
							},
							Permissions: []string{"READ"},
						},
						{
							DataObject: &data_source.DataObjectReference{
								FullName: "DB1.GranteeRole1Table",
								Type:     "TABLE",
							},
							Permissions: []string{"SELECT"},
						},
					},
				},
			},
			Action: 1,
			Policy: "",
		}, {
			ExternalId:        "Role2",
			NotInternalizable: false,
			Name:              "Role2",
			NamingHint:        "Role2",
			Access: []*sync_from_target.Access{
				{
					ActualName: "Role2",
					Who: &sync_from_target.WhoItem{
						Users:           []string{"GranteeRole2"},
						Groups:          []string{},
						AccessProviders: []string{},
					},
					What: []sync_from_target.WhatItem{},
				},
			},
			Action: 1,
			Policy: "",
		}, {
			ExternalId:        "Role3",
			NotInternalizable: true,
			Name:              "Role3",
			NamingHint:        "Role3",
			Access: []*sync_from_target.Access{
				{
					ActualName: "Role3",
					Who: &sync_from_target.WhoItem{
						Users:           []string{},
						Groups:          []string{},
						AccessProviders: []string{"GranteeRole3"},
					},
					What: []sync_from_target.WhatItem{},
				},
			},
			Action: 1,
			Policy: "",
		},
		{
			ExternalId:        "DB-schema1-MaskingPolicy1",
			NotInternalizable: true,
			Name:              "DB-schema1-MaskingPolicy1",
			NamingHint:        "MaskingPolicy1",
			Access: []*sync_from_target.Access{
				{
					ActualName: "MaskingPolicy1",
					Who:        nil,
					What:       []sync_from_target.WhatItem{},
				},
			},
			Action: 3,
			Policy: "PolicyBody 1",
		},
		{
			ExternalId:        "DB-schema2-RowAccess1",
			NotInternalizable: true,
			Name:              "DB-schema2-RowAccess1",
			NamingHint:        "RowAccess1",
			Access: []*sync_from_target.Access{
				{
					ActualName: "RowAccess1",
					Who:        nil,
					What:       []sync_from_target.WhatItem{},
				},
			},
			Action: 4,
			Policy: "Row Access Policy Body",
		},
	}, fileCreator.AccessProviders)
}

func TestAccessSyncer_SyncAccessProvidersFromTarget_StandardEdition(t *testing.T) {
	//Given
	configParams := config.ConfigMap{
		Parameters: map[string]interface{}{"key": "value", SfExcludedOwners: "OwnerToExclude1,OwnerToExclude2",
			SfStandardEdition: true},
	}

	repoMock := newMockDataAccessRepository(t)
	fileCreator := mocks.NewSimpleAccessProviderHandler(t, 1)

	repoMock.EXPECT().Close().Return(nil).Once()
	repoMock.EXPECT().TotalQueryTime().Return(time.Minute).Once()
	repoMock.EXPECT().GetShares().Return([]DbEntity{
		{Name: "Share1"}, {Name: "Share2"},
	}, nil).Once()
	repoMock.EXPECT().GetRoles().Return([]RoleEntity{
		{Name: "Role1", AssignedToUsers: 2, GrantedRoles: 3, GrantedToRoles: 1, Owner: "Owner1"},
		{Name: "Role2", AssignedToUsers: 3, GrantedRoles: 2, GrantedToRoles: 1, Owner: "Owner2"},
		{Name: "Role3", AssignedToUsers: 1, GrantedRoles: 1, GrantedToRoles: 1, Owner: "OwnerToExclude2"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsOfRole("Role1").Return([]GrantOfRole{
		{GrantedTo: "USER", GranteeName: "GranteeRole1Number1"},
		{GrantedTo: "ROLE", GranteeName: "GranteeRole1Number2"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsToRole("Role1").Return([]GrantToRole{
		{GrantedOn: "SCHEMA", Name: "Share2.GranteeRole1Schema", Privilege: "USAGE"},
		{GrantedOn: "SCHEMA", Name: "Share2.GranteeRole1Schema", Privilege: "READ"},
		{GrantedOn: "TABLE", Name: "DB1.GranteeRole1Table", Privilege: "USAGE"},
		{GrantedOn: "TABLE", Name: "DB1.GranteeRole1Table", Privilege: "SELECT"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsOfRole("Role2").Return([]GrantOfRole{
		{GrantedTo: "USER", GranteeName: "GranteeRole2"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsToRole("Role2").Return([]GrantToRole{
		{GrantedOn: "GrandOnRole2Number1", Name: "GranteeRole2", Privilege: "USAGE"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsOfRole("Role3").Return([]GrantOfRole{
		{GrantedTo: "ROLE", GranteeName: "GranteeRole3"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsToRole("Role3").Return([]GrantToRole{
		{GrantedOn: "GrandOnRole3Number1", Name: "GranteeRole3", Privilege: "WRITE"},
	}, nil).Once()

	syncer := &AccessSyncer{
		repoProvider: func(params map[string]interface{}, role string) (dataAccessRepository, error) {
			return repoMock, nil
		},
	}

	//When
	err := syncer.SyncAccessProvidersFromTarget(context.Background(), fileCreator, &configParams)

	//Then
	assert.NoError(t, err)
	assert.Equal(t, []sync_from_target.AccessProvider{
		{
			ExternalId:        "Role1",
			NotInternalizable: false,
			Name:              "Role1",
			NamingHint:        "Role1",
			Access: []*sync_from_target.Access{
				{
					ActualName: "Role1",
					Who: &sync_from_target.WhoItem{
						Users:           []string{"GranteeRole1Number1"},
						Groups:          []string{},
						AccessProviders: []string{"GranteeRole1Number2"},
					},
					What: []sync_from_target.WhatItem{
						{
							DataObject: &data_source.DataObjectReference{
								FullName: "Share2.GranteeRole1Schema",
								Type:     "SHARED-SCHEMA",
							},
							Permissions: []string{"READ"},
						},
						{
							DataObject: &data_source.DataObjectReference{
								FullName: "DB1.GranteeRole1Table",
								Type:     "TABLE",
							},
							Permissions: []string{"SELECT"},
						},
					},
				},
			},
			Action: 1,
			Policy: "",
		}, {
			ExternalId:        "Role2",
			NotInternalizable: false,
			Name:              "Role2",
			NamingHint:        "Role2",
			Access: []*sync_from_target.Access{
				{
					ActualName: "Role2",
					Who: &sync_from_target.WhoItem{
						Users:           []string{"GranteeRole2"},
						Groups:          []string{},
						AccessProviders: []string{},
					},
					What: []sync_from_target.WhatItem{},
				},
			},
			Action: 1,
			Policy: "",
		}, {
			ExternalId:        "Role3",
			NotInternalizable: true,
			Name:              "Role3",
			NamingHint:        "Role3",
			Access: []*sync_from_target.Access{
				{
					ActualName: "Role3",
					Who: &sync_from_target.WhoItem{
						Users:           []string{},
						Groups:          []string{},
						AccessProviders: []string{"GranteeRole3"},
					},
					What: []sync_from_target.WhatItem{},
				},
			},
			Action: 1,
			Policy: "",
		},
	}, fileCreator.AccessProviders)
	repoMock.AssertNotCalled(t, "GetPolicies", "MASKING")
	repoMock.AssertNotCalled(t, "GetPolicies", "ROW ACCESS")
	repoMock.AssertNotCalled(t, "DescribePolicy", "MASKING", mock.Anything, mock.Anything, mock.Anything)
	repoMock.AssertNotCalled(t, "GetPolicyReferences", mock.Anything, mock.Anything, mock.Anything)
}

func TestAccessSyncer_SyncAccessProvidersFromTarget_ErrorOnConnectingToRepo(t *testing.T) {
	//Given
	configParams := config.ConfigMap{
		Parameters: map[string]interface{}{"key": "value"},
	}

	fileCreator := mocks.NewSimpleAccessProviderHandler(t, 1)

	syncer := &AccessSyncer{
		repoProvider: func(params map[string]interface{}, role string) (dataAccessRepository, error) {
			return nil, fmt.Errorf("boom")
		},
	}

	//When
	err := syncer.SyncAccessProvidersFromTarget(context.Background(), fileCreator, &configParams)

	//Then
	assert.Error(t, err)
}

func TestAccessSyncer_SyncAccessProvidersToTarget(t *testing.T) {
	//Given
	configParams := config.ConfigMap{
		Parameters: map[string]interface{}{"key": "value"},
	}

	rolesToRemove := []string{"ToRemove1", "ToRemove2"}

	repoMock := newMockDataAccessRepository(t)
	feedbackHandler := mocks.NewSimpleAccessProviderFeedbackHandler(t, 2)

	repoMock.EXPECT().Close().Return(nil).Once()
	repoMock.EXPECT().TotalQueryTime().Return(time.Minute).Once()
	repoMock.EXPECT().DropRole("ToRemove1").Return(nil).Once()
	repoMock.EXPECT().DropRole("ToRemove2").Return(nil).Once()
	repoMock.EXPECT().GetRolesWithPrefix("").Return([]RoleEntity{
		{Name: "ExistingRole1", GrantedToRoles: 2, GrantedRoles: 3, AssignedToUsers: 2, Owner: "Owner"},
		{Name: "ExistingRole2", GrantedToRoles: 2, GrantedRoles: 3, AssignedToUsers: 2, Owner: "Owner"},
	}, nil).Once()

	repoMock.EXPECT().CreateRole("RoleName1", mock.Anything).Return(nil).Once()
	repoMock.EXPECT().CreateRole("RoleName3", mock.Anything).Return(nil).Once()

	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")
	expectGrantUsersToRole(repoMock, "RoleName3")

	repoMock.EXPECT().GrantRolesToRole(mock.Anything, "RoleName1").Return(nil).Once()
	repoMock.EXPECT().GrantRolesToRole(mock.Anything, "RoleName3").Return(nil).Once()

	repoMock.EXPECT().GetGrantsOfRole("ExistingRole1").Return([]GrantOfRole{}, nil).Once()
	repoMock.EXPECT().GetGrantsToRole("ExistingRole1").Return([]GrantToRole{}, nil).Once()

	repoMock.EXPECT().CommentIfExists(mock.Anything, "ROLE", "ExistingRole1").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrant("USAGE", "DATABASE DB1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("USAGE", "SCHEMA DB1.Schema1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("SELECT", "TABLE DB1.Schema1.Table1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("USAGE", "DATABASE DB1", "ExistingRole1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("USAGE", "SCHEMA DB1.Schema1", "ExistingRole1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("SELECT", "TABLE DB1.Schema1.Table2", "ExistingRole1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("USAGE", "DATABASE DB1", "RoleName3").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("USAGE", "SCHEMA DB1.Schema2", "RoleName3").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("SELECT", "TABLE DB1.Schema2.Table1", "RoleName3").Return(nil).Once()

	syncer := AccessSyncer{
		repoProvider: func(params map[string]interface{}, role string) (dataAccessRepository, error) {
			return repoMock, nil
		},
	}

	access1 := &importer.Access{
		Id: "Access1",
		Who: importer.WhoItem{
			Users: []string{"User1", "User2"},
		},
		What: []importer.WhatItem{
			{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table1", Type: "table"}, Permissions: []string{"SELECT"}},
		},
	}

	access2 := &importer.Access{
		Id: "Access2",
		Who: importer.WhoItem{
			Groups: []string{"Group1"},
		},
		What: []importer.WhatItem{
			{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table2", Type: "table"}, Permissions: []string{"SELECT"}},
		},
	}

	access3 := &importer.Access{
		Id: "Access3",
		Who: importer.WhoItem{
			Groups: []string{"User1"},
		},
		What: []importer.WhatItem{
			{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema2.Table1", Type: "table"}, Permissions: []string{"SELECT"}},
		},
	}

	ap1 := &importer.AccessProvider{
		Id:     "AccessProviderId1",
		Name:   "AccessProvider1",
		Access: []*importer.Access{access1},
	}

	ap2 := &importer.AccessProvider{
		Id:     "AccessProviderId2",
		Name:   "AccessProvider2",
		Access: []*importer.Access{access1},
	}

	access := map[string]importer.EnrichedAccess{
		"RoleName1": {
			AccessProvider: ap1,
			Access:         access1,
		},
		"ExistingRole1": {
			AccessProvider: ap1,
			Access:         access2,
		},
		"RoleName3": {
			AccessProvider: ap2,
			Access:         access3,
		},
	}

	//When
	err := syncer.SyncAccessProvidersToTarget(context.Background(), rolesToRemove, access, feedbackHandler, &configParams)

	//Then
	assert.NoError(t, err)
	assert.ElementsMatch(t, []importer.AccessSyncFeedbackInformation{{AccessId: "Access1", ActualName: "RoleName1"}, {AccessId: "Access2", ActualName: "ExistingRole1"}},
		feedbackHandler.AccessProviderFeedback["AccessProviderId1"])
	assert.ElementsMatch(t, []importer.AccessSyncFeedbackInformation{{AccessId: "Access3", ActualName: "RoleName3"}},
		feedbackHandler.AccessProviderFeedback["AccessProviderId2"])
}

func TestAccessSyncer_SyncAccessProvidersToTarget_ErrorOnConnectionToRepo(t *testing.T) {
	//Given
	configParams := config.ConfigMap{
		Parameters: map[string]interface{}{"key": "value"},
	}

	feedbackHandler := mocks.NewSimpleAccessProviderFeedbackHandler(t, 1)

	syncer := AccessSyncer{
		repoProvider: func(params map[string]interface{}, role string) (dataAccessRepository, error) {
			return nil, fmt.Errorf("boom")
		},
	}

	access1 := &importer.Access{
		Id: "Access1",
		Who: importer.WhoItem{
			Users: []string{"User1", "User2"},
		},
		What: []importer.WhatItem{
			{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table1", Type: "TABLE"}, Permissions: []string{"SELECT"}},
		},
	}

	access := map[string]importer.EnrichedAccess{
		"RoleName1": {
			AccessProvider: &importer.AccessProvider{
				Id:     "AccessProviderId1",
				Name:   "AccessProvider1",
				Access: []*importer.Access{access1},
			},
			Access: access1,
		},
	}

	//When
	err := syncer.SyncAccessProvidersToTarget(context.Background(), []string{"roleToRemove1"}, access, feedbackHandler, &configParams)

	//Then
	assert.Error(t, err)
}

func TestAccessSyncer_SyncAccessAsCodeToTarget(t *testing.T) {
	//Given
	configParams := config.ConfigMap{
		Parameters: map[string]interface{}{"key": "value"},
	}

	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().Close().Return(nil).Once()
	repoMock.EXPECT().TotalQueryTime().Return(time.Minute).Once()
	repoMock.EXPECT().DropRole("R_ToRemove1").Return(nil).Once()
	repoMock.EXPECT().DropRole("R_ToRemove2").Return(nil).Once()
	repoMock.EXPECT().GetRolesWithPrefix("R_").Return([]RoleEntity{
		{Name: "R_ToRemove1", GrantedToRoles: 2, GrantedRoles: 3, AssignedToUsers: 2, Owner: "Owner"},
		{Name: "R_ToRemove2", GrantedToRoles: 2, GrantedRoles: 3, AssignedToUsers: 2, Owner: "Owner"},
	}, nil).Once()

	repoMock.EXPECT().CreateRole("RoleName1", mock.Anything).Return(nil).Once()
	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")
	repoMock.EXPECT().GrantRolesToRole(mock.Anything, "RoleName1").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrant("USAGE", "DATABASE DB1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("USAGE", "SCHEMA DB1.Schema1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("SELECT", "TABLE DB1.Schema1.Table1", "RoleName1").Return(nil).Once()

	syncer := AccessSyncer{
		repoProvider: func(params map[string]interface{}, role string) (dataAccessRepository, error) {
			return repoMock, nil
		},
	}

	access1 := &importer.Access{
		Id: "Access1",
		Who: importer.WhoItem{
			Users: []string{"User1", "User2"},
		},
		What: []importer.WhatItem{
			{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table1", Type: "table"}, Permissions: []string{"SELECT"}},
		},
	}

	access := map[string]importer.EnrichedAccess{
		"RoleName1": {
			AccessProvider: &importer.AccessProvider{
				Id:     "AccessProviderId1",
				Name:   "AccessProvider1",
				Access: []*importer.Access{access1},
			},
			Access: access1,
		},
	}

	//When
	err := syncer.SyncAccessAsCodeToTarget(context.Background(), access, "R_", &configParams)

	//Then
	assert.NoError(t, err)
}

func TestAccessSyncer_SyncAccessAsCodeToTarget_ErrorOnRepoConnection(t *testing.T) {
	//Given
	configParams := config.ConfigMap{
		Parameters: map[string]interface{}{"key": "value"},
	}

	syncer := AccessSyncer{
		repoProvider: func(params map[string]interface{}, role string) (dataAccessRepository, error) {
			return nil, fmt.Errorf("boom")
		},
	}

	access1 := &importer.Access{
		Id: "Access1",
		Who: importer.WhoItem{
			Users: []string{"User1", "User2"},
		},
		What: []importer.WhatItem{
			{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table1", Type: "TABLE"}, Permissions: []string{"SELECT"}},
		},
	}

	access := map[string]importer.EnrichedAccess{
		"RoleName1": {
			AccessProvider: &importer.AccessProvider{
				Id:     "AccessProviderId1",
				Name:   "AccessProvider1",
				Access: []*importer.Access{access1},
			},
			Access: access1,
		},
	}

	//When
	err := syncer.SyncAccessAsCodeToTarget(context.Background(), access, "R_", &configParams)

	//Then
	assert.Error(t, err)
}

func TestAccessSyncer_removeRolesToRemove_NoRoles(t *testing.T) {
	//Given
	repo := newMockDataAccessRepository(t)

	syncer := AccessSyncer{
		repoProvider: func(params map[string]interface{}, role string) (dataAccessRepository, error) {
			return nil, nil
		},
	}

	//When
	err := syncer.removeRolesToRemove([]string{}, repo)

	//Then
	assert.NoError(t, err)
}

func TestAccessSyncer_removeRolesToRemove(t *testing.T) {
	//Given
	repo := newMockDataAccessRepository(t)

	rolesToRemove := []string{"Role1", "Role2", "Role3"}

	repo.EXPECT().DropRole(mock.MatchedBy(func(roleName string) bool {
		for _, possibleRole := range rolesToRemove {
			if possibleRole == roleName {
				return true
			}
		}
		return false
	})).Return(nil).Times(len(rolesToRemove))

	syncer := AccessSyncer{
		repoProvider: func(params map[string]interface{}, role string) (dataAccessRepository, error) {
			return nil, nil
		},
	}

	//When
	err := syncer.removeRolesToRemove(rolesToRemove, repo)

	//Then
	assert.NoError(t, err)
}

func TestAccessSyncer_removeRolesToRemove_error(t *testing.T) {
	//Given
	repo := newMockDataAccessRepository(t)

	rolesToRemove := []string{"Role1", "Role2"}

	repo.EXPECT().DropRole("Role1").Return(nil).Once()
	repo.EXPECT().DropRole("Role2").Return(fmt.Errorf("BOOM")).Once()

	syncer := AccessSyncer{
		repoProvider: func(params map[string]interface{}, role string) (dataAccessRepository, error) {
			return nil, nil
		},
	}

	//When
	err := syncer.removeRolesToRemove(rolesToRemove, repo)

	//Then
	assert.Error(t, err)
}

func TestAccessSyncer_importPoliciesOfType(t *testing.T) {
	//Given
	repoMock := newMockDataAccessRepository(t)
	fileCreator := mocks.NewSimpleAccessProviderHandler(t, 1)

	policyType := "policyType"

	repoMock.EXPECT().GetPolicies(policyType).Return([]policyEntity{
		{
			Name:         "Policy1",
			Owner:        "PolicyOwner",
			Kind:         policyType,
			DatabaseName: "DB1",
			SchemaName:   "Schema1",
		},
		{
			Name:         "Policy2",
			Kind:         policyType,
			DatabaseName: "DB1",
			SchemaName:   "Schema2",
		},
		{
			Name:         "Policy3",
			Kind:         "OtherKind",
			DatabaseName: "DB1",
			SchemaName:   "Schema2",
		},
	}, nil).Once()

	repoMock.EXPECT().DescribePolicy(policyType, "DB1", "Schema1", "Policy1").Return([]describePolicyEntity{
		{
			Name: "Policy1",
			Body: "PolicyBody1",
		},
	}, nil).Once()

	repoMock.EXPECT().DescribePolicy(policyType, "DB1", "Schema2", "Policy2").Return([]describePolicyEntity{
		{
			Name: "Policy2",
			Body: "PolicyBody2",
		},
	}, nil).Once()

	repoMock.EXPECT().GetPolicyReferences("DB1", "Schema1", "Policy1").Return([]policyReferenceEntity{
		{
			POLICY_STATUS:     "Active",
			REF_COLUMN_NAME:   NullString{String: "ColumnName1", Valid: true},
			REF_DATABASE_NAME: "DB1",
			REF_SCHEMA_NAME:   "Schema1",
			REF_ENTITY_NAME:   "EntityName1",
		},
	}, nil).Once()

	repoMock.EXPECT().GetPolicyReferences("DB1", "Schema2", "Policy2").Return([]policyReferenceEntity{
		{
			POLICY_STATUS:     "Active",
			REF_COLUMN_NAME:   NullString{Valid: false},
			REF_DATABASE_NAME: "DB1",
			REF_SCHEMA_NAME:   "Schema1",
			REF_ENTITY_NAME:   "EntityName1",
		},
	}, nil).Once()

	syncer := AccessSyncer{
		repoProvider: func(params map[string]interface{}, role string) (dataAccessRepository, error) {
			return nil, nil
		},
	}

	//When
	err := syncer.importPoliciesOfType(fileCreator, repoMock, policyType, sync_from_target.Grant)

	//Then
	assert.NoError(t, err)
	assert.Equal(t, []sync_from_target.AccessProvider{
		{
			ExternalId:        "DB1-Schema1-Policy1",
			NotInternalizable: true,
			Name:              "DB1-Schema1-Policy1",
			NamingHint:        "Policy1",
			Access: []*sync_from_target.Access{
				{
					ActualName: "Policy1",
					Who:        nil,
					What: []sync_from_target.WhatItem{
						{
							DataObject: &data_source.DataObjectReference{
								FullName: "DB1.Schema1.EntityName1.ColumnName1",
								Type:     "COLUMN",
							},
							Permissions: []string{},
						},
					},
				},
			},
			Action: sync_from_target.Grant,
			Policy: "PolicyBody1",
		},
		{
			ExternalId:        "DB1-Schema2-Policy2",
			NotInternalizable: true,
			Name:              "DB1-Schema2-Policy2",
			NamingHint:        "Policy2",
			Access: []*sync_from_target.Access{
				{
					ActualName: "Policy2",
					Who:        nil,
					What: []sync_from_target.WhatItem{
						{
							DataObject: &data_source.DataObjectReference{
								FullName: "DB1.Schema1.EntityName1",
								Type:     "TABLE",
							},
							Permissions: []string{},
						},
					},
				},
			},
			Action: sync_from_target.Grant,
			Policy: "PolicyBody2",
		},
	}, fileCreator.AccessProviders)

}

func TestAccessSyncer_importPoliciesOfType_ErrorOnDescribePolicy(t *testing.T) {
	//Given
	repoMock := newMockDataAccessRepository(t)
	fileCreator := mocks.NewSimpleAccessProviderHandler(t, 1)

	policyType := "policyType"

	repoMock.EXPECT().GetPolicies(policyType).Return([]policyEntity{
		{
			Name:         "Policy1",
			Owner:        "PolicyOwner",
			Kind:         policyType,
			DatabaseName: "DB1",
			SchemaName:   "Schema1",
		},
	}, nil).Once()

	repoMock.EXPECT().DescribePolicy(policyType, "DB1", "Schema1", "Policy1").Return([]describePolicyEntity{
		{
			Name: "Policy1",
			Body: "PolicyBody1",
		},
		{
			Name: "BadPolicy1",
			Body: "PolicyBody1",
		},
	}, nil).Once()

	syncer := AccessSyncer{
		repoProvider: func(params map[string]interface{}, role string) (dataAccessRepository, error) {
			return nil, nil
		},
	}

	//When
	err := syncer.importPoliciesOfType(fileCreator, repoMock, policyType, sync_from_target.Grant)

	//Then
	assert.Error(t, err)
	assert.Empty(t, fileCreator.AccessProviders)
}

func generateAccessControls_table(t *testing.T) {
	//Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CreateRole("RoleName1", mock.Anything).Return(nil).Once()
	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")
	repoMock.EXPECT().GrantRolesToRole(mock.Anything, "RoleName1").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrant("USAGE", "DATABASE DB1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("USAGE", "SCHEMA DB1.Schema1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("SELECT", "TABLE DB1.Schema1.Table1", "RoleName1").Return(nil).Once()

	syncer := AccessSyncer{
		repoProvider: func(params map[string]interface{}, role string) (dataAccessRepository, error) {
			return nil, nil
		},
	}

	access1 := &importer.Access{
		Id: "Access1",
		Who: importer.WhoItem{
			Users: []string{"User1", "User2"},
		},
		What: []importer.WhatItem{
			{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table1", Type: "table"}, Permissions: []string{"SELECT"}},
		},
	}

	access := map[string]importer.EnrichedAccess{
		"RoleName1": {
			AccessProvider: &importer.AccessProvider{
				Id:     "AccessProviderId1",
				Name:   "AccessProvider1",
				Access: []*importer.Access{access1},
			},
			Access: access1,
		},
	}

	//When
	err := syncer.generateAccessControls(context.Background(), access, map[string]bool{}, repoMock)

	//Then
	assert.NoError(t, err)
}

func generateAccessControls_view(t *testing.T) {
	//Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CreateRole("RoleName1", mock.Anything).Return(nil).Once()
	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")
	repoMock.EXPECT().GrantRolesToRole(mock.Anything, "RoleName1").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrant("USAGE", "DATABASE DB1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("USAGE", "SCHEMA DB1.Schema1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("SELECT", "VIEW DB1.Schema1.Table2", "RoleName1").Return(nil).Once()

	syncer := AccessSyncer{
		repoProvider: func(params map[string]interface{}, role string) (dataAccessRepository, error) {
			return nil, nil
		},
	}

	access1 := &importer.Access{
		Id: "Access1",
		Who: importer.WhoItem{
			Users: []string{"User1", "User2"},
		},
		What: []importer.WhatItem{
			{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table2", Type: "view"}, Permissions: []string{"SELECT"}},
		},
	}

	access := map[string]importer.EnrichedAccess{
		"RoleName1": {
			AccessProvider: &importer.AccessProvider{
				Id:     "AccessProviderId1",
				Name:   "AccessProvider1",
				Access: []*importer.Access{access1},
			},
			Access: access1,
		},
	}

	//When
	err := syncer.generateAccessControls(context.Background(), access, map[string]bool{}, repoMock)

	//Then
	assert.NoError(t, err)
}

func generateAccessControls_schema(t *testing.T) {
	//Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CreateRole("RoleName1", mock.Anything).Return(nil).Once()
	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")
	repoMock.EXPECT().GrantRolesToRole(mock.Anything, "RoleName1").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrant("USAGE", "DATABASE DB1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("USAGE", "SCHEMA DB1.Schema2", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("SELECT", "TABLE DB1.Schema2.Table3", "RoleName1").Return(nil).Once()

	database := "DB1"
	schema := "Schema2"
	repoMock.EXPECT().GetTablesInSchema(&common.SnowflakeObject{
		Database: &database,
		Schema:   &schema,
	}).Return([]DbEntity{
		{Name: "Table3"},
	}, nil).Once()

	syncer := AccessSyncer{
		repoProvider: func(params map[string]interface{}, role string) (dataAccessRepository, error) {
			return nil, nil
		},
	}

	access1 := &importer.Access{
		Id: "Access1",
		Who: importer.WhoItem{
			Users: []string{"User1", "User2"},
		},
		What: []importer.WhatItem{
			{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema2", Type: "schema"}, Permissions: []string{"READ"}},
		},
	}

	access := map[string]importer.EnrichedAccess{
		"RoleName1": {
			AccessProvider: &importer.AccessProvider{
				Id:     "AccessProviderId1",
				Name:   "AccessProvider1",
				Access: []*importer.Access{access1},
			},
			Access: access1,
		},
	}

	//When
	err := syncer.generateAccessControls(context.Background(), access, map[string]bool{}, repoMock)

	//Then
	assert.NoError(t, err)
}

func generateAccessControls_existing_schema(t *testing.T) {
	//Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CommentIfExists(mock.AnythingOfType("string"), "ROLE", "RoleName1").Return(nil).Once()

	repoMock.EXPECT().GetGrantsOfRole(mock.Anything).Return([]GrantOfRole{}, nil)
	repoMock.EXPECT().GetGrantsToRole(mock.Anything).Return([]GrantToRole{}, nil)

	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")

	repoMock.EXPECT().ExecuteRevoke("ALL", "FUTURE TABLES IN SCHEMA DB1.Schema2", "RoleName1").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrant("USAGE", "DATABASE DB1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("USAGE", "SCHEMA DB1.Schema2", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("SELECT", "TABLE DB1.Schema2.Table3", "RoleName1").Return(nil).Once()

	database := "DB1"
	schema := "Schema2"
	repoMock.EXPECT().GetTablesInSchema(&common.SnowflakeObject{
		Database: &database,
		Schema:   &schema,
	}).Return([]DbEntity{
		{Name: "Table3"},
	}, nil).Once()

	syncer := AccessSyncer{
		repoProvider: func(params map[string]interface{}, role string) (dataAccessRepository, error) {
			return nil, nil
		},
	}

	access1 := &importer.Access{
		Id: "Access1",
		Who: importer.WhoItem{
			Users: []string{"User1", "User2"},
		},
		What: []importer.WhatItem{
			{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema2", Type: "schema"}, Permissions: []string{"READ"}},
		},
	}

	access := map[string]importer.EnrichedAccess{
		"RoleName1": {
			AccessProvider: &importer.AccessProvider{
				Id:     "AccessProviderId1",
				Name:   "AccessProvider1",
				Access: []*importer.Access{access1},
			},
			Access: access1,
		},
	}

	//When
	err := syncer.generateAccessControls(context.Background(), access, map[string]bool{"RoleName1": true}, repoMock)

	//Then
	assert.NoError(t, err)
}

func generateAccessControls_sharedDatabase(t *testing.T) {
	//Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CreateRole("RoleName1", mock.Anything).Return(nil).Once()
	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")
	repoMock.EXPECT().GrantRolesToRole(mock.Anything, "RoleName1").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrant("DELETE", "DATABASE DB2", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("INSERT", "DATABASE DB2", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("UPDATE", "DATABASE DB2", "RoleName1").Return(nil).Once()

	syncer := AccessSyncer{
		repoProvider: func(params map[string]interface{}, role string) (dataAccessRepository, error) {
			return nil, nil
		},
	}

	access1 := &importer.Access{
		Id: "Access1",
		Who: importer.WhoItem{
			Users: []string{"User1", "User2"},
		},
		What: []importer.WhatItem{
			{DataObject: &data_source.DataObjectReference{FullName: "DB2", Type: "shared-database"}, Permissions: []string{"WRITE"}},
		},
	}

	access := map[string]importer.EnrichedAccess{
		"RoleName1": {
			AccessProvider: &importer.AccessProvider{
				Id:     "AccessProviderId1",
				Name:   "AccessProvider1",
				Access: []*importer.Access{access1},
			},
			Access: access1,
		},
	}

	//When
	err := syncer.generateAccessControls(context.Background(), access, map[string]bool{}, repoMock)

	//Then
	assert.NoError(t, err)
}

func generateAccessControls_database(t *testing.T) {
	//Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CreateRole("RoleName1", mock.Anything).Return(nil).Once()
	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")
	repoMock.EXPECT().GrantRolesToRole(mock.Anything, "RoleName1").Return(nil).Once()

	database := "DB1"
	schema := "Schema2"
	repoMock.EXPECT().GetTablesInSchema(&common.SnowflakeObject{
		Database: &database,
		Schema:   &schema,
	}).Return([]DbEntity{
		{Name: "Table3"},
	}, nil).Once()

	repoMock.EXPECT().GetSchemaInDatabase("DB1").Return([]DbEntity{
		{Name: "Schema2"},
	}, nil).Once()

	repoMock.EXPECT().ExecuteGrant("USAGE", "DATABASE DB1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("SELECT", "DATABASE DB1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("USAGE", "SCHEMA DB1.Schema2", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("SELECT", "TABLE \"DB1.Schema2.Table3\"", "RoleName1").Return(nil).Once()

	syncer := AccessSyncer{
		repoProvider: func(params map[string]interface{}, role string) (dataAccessRepository, error) {
			return nil, nil
		},
	}

	access1 := &importer.Access{
		Id: "Access1",
		Who: importer.WhoItem{
			Users: []string{"User1", "User2"},
		},
		What: []importer.WhatItem{
			{DataObject: &data_source.DataObjectReference{FullName: "DB1", Type: "database"}, Permissions: []string{"READ"}},
		},
	}

	access := map[string]importer.EnrichedAccess{
		"RoleName1": {
			AccessProvider: &importer.AccessProvider{
				Id:     "AccessProviderId1",
				Name:   "AccessProvider1",
				Access: []*importer.Access{access1},
			},
			Access: access1,
		},
	}

	//When
	err := syncer.generateAccessControls(context.Background(), access, map[string]bool{}, repoMock)

	//Then
	assert.NoError(t, err)
}

func generateAccessControls_existing_database(t *testing.T) {
	//Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CommentIfExists(mock.AnythingOfType("string"), "ROLE", "RoleName1").Return(nil).Once()

	repoMock.EXPECT().GetGrantsOfRole(mock.Anything).Return([]GrantOfRole{}, nil)
	repoMock.EXPECT().GetGrantsToRole(mock.Anything).Return([]GrantToRole{}, nil)

	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")

	database := "DB1"
	schema := "Schema2"
	repoMock.EXPECT().GetTablesInSchema(&common.SnowflakeObject{
		Database: &database,
		Schema:   &schema,
	}).Return([]DbEntity{
		{Name: "Table3"},
	}, nil).Once()

	repoMock.EXPECT().GetSchemaInDatabase("DB1").Return([]DbEntity{
		{Name: "Schema2"},
	}, nil).Once()

	repoMock.EXPECT().ExecuteRevoke("ALL", "FUTURE SCHEMAS IN DATABASE DB1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteRevoke("ALL", "FUTURE TABLES IN DATABASE DB1", "RoleName1").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrant("USAGE", "DATABASE DB1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("SELECT", "DATABASE DB1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("USAGE", "SCHEMA DB1.Schema2", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("SELECT", "TABLE \"DB1.Schema2.Table3\"", "RoleName1").Return(nil).Once()

	syncer := AccessSyncer{
		repoProvider: func(params map[string]interface{}, role string) (dataAccessRepository, error) {
			return nil, nil
		},
	}

	access1 := &importer.Access{
		Id: "Access1",
		Who: importer.WhoItem{
			Users: []string{"User1", "User2"},
		},
		What: []importer.WhatItem{
			{DataObject: &data_source.DataObjectReference{FullName: "DB1", Type: "database"}, Permissions: []string{"READ"}},
		},
	}

	access := map[string]importer.EnrichedAccess{
		"RoleName1": {
			AccessProvider: &importer.AccessProvider{
				Id:     "AccessProviderId1",
				Name:   "AccessProvider1",
				Access: []*importer.Access{access1},
			},
			Access: access1,
		},
	}

	//When
	err := syncer.generateAccessControls(context.Background(), access, map[string]bool{"RoleName1": true}, repoMock)

	//Then
	assert.NoError(t, err)
}

func generateAccessControls_warehouse(t *testing.T) {
	//Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CreateRole("RoleName1", mock.Anything).Return(nil).Once()
	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")
	repoMock.EXPECT().GrantRolesToRole(mock.Anything, "RoleName1").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrant("USAGE", "WAREHOUSE WH1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("SELECT", "WAREHOUSE WH1", "RoleName1").Return(nil).Once()

	syncer := AccessSyncer{
		repoProvider: func(params map[string]interface{}, role string) (dataAccessRepository, error) {
			return nil, nil
		},
	}

	access1 := &importer.Access{
		Id: "Access1",
		Who: importer.WhoItem{
			Users: []string{"User1", "User2"},
		},
		What: []importer.WhatItem{
			{DataObject: &data_source.DataObjectReference{FullName: "WH1", Type: "warehouse"}, Permissions: []string{"READ"}},
		},
	}

	access := map[string]importer.EnrichedAccess{
		"RoleName1": {
			AccessProvider: &importer.AccessProvider{
				Id:     "AccessProviderId1",
				Name:   "AccessProvider1",
				Access: []*importer.Access{access1},
			},
			Access: access1,
		},
	}

	//When
	err := syncer.generateAccessControls(context.Background(), access, map[string]bool{}, repoMock)

	//Then
	assert.NoError(t, err)
}

func generateAccessControls_datasource(t *testing.T) {
	//Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CreateRole("RoleName1", mock.Anything).Return(nil).Once()
	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")
	repoMock.EXPECT().GrantRolesToRole(mock.Anything, "RoleName1").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrant("SELECT", "ACCOUNT", "RoleName1").Return(nil).Once()

	syncer := AccessSyncer{
		repoProvider: func(params map[string]interface{}, role string) (dataAccessRepository, error) {
			return nil, nil
		},
	}

	access1 := &importer.Access{
		Id: "Access1",
		Who: importer.WhoItem{
			Users: []string{"User1", "User2"},
		},
		What: []importer.WhatItem{
			{DataObject: &data_source.DataObjectReference{FullName: "DS1", Type: "datasource"}, Permissions: []string{"READ"}},
		},
	}

	access := map[string]importer.EnrichedAccess{
		"RoleName1": {
			AccessProvider: &importer.AccessProvider{
				Id:     "AccessProviderId1",
				Name:   "AccessProvider1",
				Access: []*importer.Access{access1},
			},
			Access: access1,
		},
	}

	//When
	err := syncer.generateAccessControls(context.Background(), access, map[string]bool{}, repoMock)

	//Then
	assert.NoError(t, err)
}

func TestAccessSyncer_generateAccessControls(t *testing.T) {
	t.Run("Table", generateAccessControls_table)
	t.Run("View", generateAccessControls_view)
	t.Run("Schema", generateAccessControls_schema)
	t.Run("Existing Schema", generateAccessControls_existing_schema)
	t.Run("Shared-database", generateAccessControls_sharedDatabase)
	t.Run("Database", generateAccessControls_database)
	t.Run("Existing Database", generateAccessControls_existing_database)
	t.Run("Warehouse", generateAccessControls_warehouse)
	t.Run("Datasource", generateAccessControls_datasource)
}

func TestAccessSyncer_generateAccessControls_existingRole(t *testing.T) {
	//Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CommentIfExists(mock.AnythingOfType("string"), "ROLE", "existingRole1").Return(nil).Once()
	repoMock.EXPECT().GetGrantsOfRole("existingRole1").Return([]GrantOfRole{
		{GrantedTo: "USER", GranteeName: "User1"},
		{GrantedTo: "USER", GranteeName: "User3"},
		{GrantedTo: "Role", GranteeName: "Role1"},
		{GrantedTo: "Role", GranteeName: "Role3"},
	}, nil).Once()

	repoMock.EXPECT().GetGrantsToRole("existingRole1").Return([]GrantToRole{}, nil).Once()

	expectGrantUsersToRole(repoMock, "existingRole1", "User2")

	repoMock.EXPECT().GrantRolesToRole(mock.Anything, "existingRole1", "Role2").Return(nil).Once()
	repoMock.EXPECT().RevokeRolesFromRole(mock.Anything, "existingRole1", "Role3").Return(nil).Once()
	repoMock.EXPECT().RevokeUsersFromRole(mock.Anything, "existingRole1", "User3").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrant("USAGE", "DATABASE DB1", "existingRole1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("USAGE", "SCHEMA DB1.Schema1", "existingRole1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("SELECT", "TABLE DB1.Schema1.Table1", "existingRole1").Return(nil).Once()

	syncer := AccessSyncer{
		repoProvider: func(params map[string]interface{}, role string) (dataAccessRepository, error) {
			return nil, nil
		},
	}

	access1 := &importer.Access{
		Id: "Access1",
		Who: importer.WhoItem{
			Users:       []string{"User1", "User2"},
			InheritFrom: []string{"Role1", "Role2"},
		},
		What: []importer.WhatItem{
			{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table1", Type: "table"}, Permissions: []string{"SELECT"}},
		},
	}

	access := map[string]importer.EnrichedAccess{
		"existingRole1": {
			AccessProvider: &importer.AccessProvider{
				Id:     "AccessProviderId1",
				Name:   "AccessProvider1",
				Access: []*importer.Access{access1},
			},
			Access: access1,
		},
	}

	//When
	err := syncer.generateAccessControls(context.Background(), access, map[string]bool{"existingRole1": true}, repoMock)

	//Then
	assert.NoError(t, err)
}

func TestAccessSyncer_generateAccessControls_inheritance(t *testing.T) {
	//Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CreateRole("RoleName1", mock.Anything).Return(nil).Once()
	repoMock.EXPECT().CreateRole("RoleName2", mock.Anything).Return(nil).Once()
	repoMock.EXPECT().CreateRole("RoleName3", mock.Anything).Return(nil).Once()
	expectGrantUsersToRole(repoMock, "RoleName1")
	expectGrantUsersToRole(repoMock, "RoleName2")
	expectGrantUsersToRole(repoMock, "RoleName3", "User1")
	repoMock.EXPECT().GrantRolesToRole(mock.Anything, "RoleName1", "RoleName2").Return(nil).Once()
	repoMock.EXPECT().GrantRolesToRole(mock.Anything, "RoleName2", "RoleName3").Return(nil).Once()
	repoMock.EXPECT().GrantRolesToRole(mock.Anything, "RoleName3").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrant("USAGE", "DATABASE DB1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("USAGE", "SCHEMA DB1.Schema1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("SELECT", "TABLE DB1.Schema1.Table1", "RoleName1").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrant("USAGE", "DATABASE DB1", "RoleName2").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("USAGE", "SCHEMA DB1.Schema1", "RoleName2").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("SELECT", "TABLE DB1.Schema1.Table2", "RoleName2").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrant("USAGE", "DATABASE DB1", "RoleName3").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("USAGE", "SCHEMA DB1.Schema1", "RoleName3").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("SELECT", "TABLE DB1.Schema1.Table3", "RoleName3").Return(nil).Once()

	syncer := AccessSyncer{
		repoProvider: func(params map[string]interface{}, role string) (dataAccessRepository, error) {
			return nil, nil
		},
	}

	access1 := &importer.Access{
		Id: "Access1",
		Who: importer.WhoItem{
			InheritFrom: []string{"ID:AccessProviderId2"},
		},
		What: []importer.WhatItem{
			{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table1", Type: "table"}, Permissions: []string{"SELECT"}},
		},
	}

	access2 := &importer.Access{
		Id: "Access2",
		Who: importer.WhoItem{
			InheritFrom: []string{"RoleName3"},
		},
		What: []importer.WhatItem{
			{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table2", Type: "table"}, Permissions: []string{"SELECT"}},
		},
	}

	access3 := &importer.Access{
		Id: "Access3",
		Who: importer.WhoItem{
			Users: []string{"User1"},
		},
		What: []importer.WhatItem{
			{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table3", Type: "table"}, Permissions: []string{"SELECT"}},
		},
	}

	access := map[string]importer.EnrichedAccess{
		"RoleName1": {
			AccessProvider: &importer.AccessProvider{
				Id:     "AccessProviderId1",
				Name:   "AccessProvider1",
				Access: []*importer.Access{access1},
			},
			Access: access1,
		},
		"RoleName2": {
			AccessProvider: &importer.AccessProvider{
				Id:     "AccessProviderId2",
				Name:   "AccessProvider2",
				Access: []*importer.Access{access2},
			},
			Access: access2,
		},
		"RoleName3": {
			AccessProvider: &importer.AccessProvider{
				Id:     "AccessProviderId3",
				Name:   "AccessProvider3",
				Access: []*importer.Access{access3},
			},
			Access: access3,
		},
	}

	//When
	err := syncer.generateAccessControls(context.Background(), access, map[string]bool{}, repoMock)

	//Then
	assert.NoError(t, err)
}

func expectGrantUsersToRole(repoMock *mockDataAccessRepository, roleName string, users ...string) {
	expectedUsersList := make([]string, 0, len(users))
	expectedUsersList = append(expectedUsersList, users...)
	grandedUsers := make(map[string]struct{})

	expectedUsers := func(user string) bool {

		if _, f := grandedUsers[user]; f {
			return false
		}

		for _, expectedUser := range expectedUsersList {
			if expectedUser == user {
				grandedUsers[user] = struct{}{}
				return true
			}
		}
		return false
	}

	arguments := make([]interface{}, 0, len(users))
	for range users {
		arguments = append(arguments, mock.MatchedBy(expectedUsers))
	}

	repoMock.EXPECT().GrantUsersToRole(mock.Anything, roleName, arguments...).Return(nil).Once()

}
