package snowflake

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/smithy-go/ptr"

	"github.com/raito-io/cli/base/access_provider/sync_from_target"
	importer "github.com/raito-io/cli/base/access_provider/sync_to_target"
	"github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestAccessSyncer_SyncAccessProvidersFromTarget(t *testing.T) {
	//Given
	configParams := config.ConfigMap{
		Parameters: map[string]string{"key": "value", SfExternalIdentityStoreOwners: "ExternalOwner1,ExternalOwner2"},
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
		{Name: "Role3", AssignedToUsers: 1, GrantedRoles: 1, GrantedToRoles: 1, Owner: "ExternalOwner1"},
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
		{GrantedOn: "MATERIALIZED_VIEW", Name: "DB1.GranteeRole1MatView", Privilege: "SELECT"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsOfRole("Role2").Return([]GrantOfRole{
		{GrantedTo: "USER", GranteeName: "GranteeRole2"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsToRole("Role2").Return([]GrantToRole{
		{GrantedOn: "GrandOnRole2Number1", Name: "GranteeRole2", Privilege: "USAGE"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsOfRole("Role3").Return([]GrantOfRole{
		{GrantedTo: "ROLE", GranteeName: "\"GranteeRole.3\""},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsToRole("Role3").Return([]GrantToRole{
		{GrantedOn: "GrandOnRole3Number1", Name: "GranteeRole3", Privilege: "WRITE"},
	}, nil).Once()
	repoMock.EXPECT().GetPolicies("MASKING").Return([]PolicyEntity{
		{Name: "MaskingPolicy1", SchemaName: "schema1", DatabaseName: "DB", Owner: "MaskingOwner", Kind: "MASKING_POLICY"},
	}, nil).Once()
	repoMock.EXPECT().GetPolicies("ROW ACCESS").Return([]PolicyEntity{
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
		repoProvider: func(params map[string]string, role string) (dataAccessRepository, error) {
			return repoMock, nil
		},
		tablesPerSchemaCache:    make(map[string][]TableEntity),
		schemasPerDataBaseCache: make(map[string][]SchemaEntity),
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
			Who: &sync_from_target.WhoItem{
				Users:           []string{"GranteeRole1Number1"},
				Groups:          []string{},
				AccessProviders: []string{"GranteeRole1Number2"},
			},
			ActualName: "Role1",
			What: []sync_from_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "Share2.GranteeRole1Schema",
						Type:     "",
					},
					Permissions: []string{"READ"},
				},
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "DB1.GranteeRole1Table",
						Type:     "",
					},
					Permissions: []string{"SELECT"},
				},
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "DB1.GranteeRole1MatView",
						Type:     "",
					},
					Permissions: []string{"SELECT"},
				},
			},
			Action: 1,
			Policy: "",
		}, {
			ExternalId:        "Role2",
			NotInternalizable: false,
			Name:              "Role2",
			NamingHint:        "Role2",
			Who: &sync_from_target.WhoItem{
				Users:           []string{"GranteeRole2"},
				Groups:          []string{},
				AccessProviders: []string{},
			},
			ActualName: "Role2",
			What:       []sync_from_target.WhatItem{},
			Action:     1,
			Policy:     "",
		}, {
			ExternalId:        "Role3",
			NotInternalizable: true,
			Name:              "Role3",
			NamingHint:        "Role3",
			Who: &sync_from_target.WhoItem{
				Users:           []string{},
				Groups:          []string{},
				AccessProviders: []string{"GranteeRole.3"},
			},
			ActualName: "Role3",
			What:       []sync_from_target.WhatItem{},
			Action:     1,
			Policy:     "",
		},
		{
			ExternalId:        "DB-schema1-MaskingPolicy1",
			NotInternalizable: true,
			Name:              "DB-schema1-MaskingPolicy1",
			NamingHint:        "MaskingPolicy1",
			Who:               nil,
			ActualName:        "DB-schema1-MaskingPolicy1",
			What:              []sync_from_target.WhatItem{},
			Action:            3,
			Policy:            "PolicyBody 1",
		},
		{
			ExternalId:        "DB-schema2-RowAccess1",
			NotInternalizable: true,
			Name:              "DB-schema2-RowAccess1",
			NamingHint:        "RowAccess1",
			Who:               nil,
			ActualName:        "DB-schema2-RowAccess1",
			What:              []sync_from_target.WhatItem{},
			Action:            4,
			Policy:            "Row Access Policy Body",
		},
	}, fileCreator.AccessProviders)
}

func TestAccessSyncer_SyncAccessProvidersFromTarget_NoUnpack(t *testing.T) {
	//Given
	configParams := config.ConfigMap{
		Parameters: map[string]string{"key": "value", SfExternalIdentityStoreOwners: "ExternalOwner1,ExternalOwner2", SfLinkToExternalIdentityStoreGroups: "true"},
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
		{Name: "Role3", AssignedToUsers: 1, GrantedRoles: 1, GrantedToRoles: 1, Owner: "ExternalOwner1"},
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
	repoMock.EXPECT().GetGrantsToRole("Role3").Return([]GrantToRole{
		{GrantedOn: "GrandOnRole3Number1", Name: "GranteeRole3", Privilege: "WRITE"},
	}, nil).Once()
	repoMock.EXPECT().GetPolicies("MASKING").Return([]PolicyEntity{
		{Name: "MaskingPolicy1", SchemaName: "schema1", DatabaseName: "DB", Owner: "MaskingOwner", Kind: "MASKING_POLICY"},
	}, nil).Once()
	repoMock.EXPECT().GetPolicies("ROW ACCESS").Return([]PolicyEntity{
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
		repoProvider: func(params map[string]string, role string) (dataAccessRepository, error) {
			return repoMock, nil
		},
		tablesPerSchemaCache:    make(map[string][]TableEntity),
		schemasPerDataBaseCache: make(map[string][]SchemaEntity),
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
			Who: &sync_from_target.WhoItem{
				Users:           []string{"GranteeRole1Number1"},
				Groups:          []string{},
				AccessProviders: []string{"GranteeRole1Number2"},
			},
			ActualName: "Role1",
			What: []sync_from_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "Share2.GranteeRole1Schema",
						Type:     "",
					},
					Permissions: []string{"READ"},
				},
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "DB1.GranteeRole1Table",
						Type:     "",
					},
					Permissions: []string{"SELECT"},
				},
			},
			Action: 1,
			Policy: "",
		}, {
			ExternalId:              "Role3",
			NotInternalizable:       false,
			WhoLocked:               ptr.Bool(true),
			InheritanceLocked:       ptr.Bool(true),
			NameLocked:              ptr.Bool(true),
			DeleteLocked:            ptr.Bool(true),
			WhoLockedReason:         ptr.String(whoLockedReason),
			InheritanceLockedReason: ptr.String(inheritanceLockedReason),
			NameLockedReason:        ptr.String(nameLockedReason),
			DeleteLockedReason:      ptr.String(deleteLockedReason),
			Name:                    "Role3",
			NamingHint:              "Role3",
			Who: &sync_from_target.WhoItem{
				Users:           []string{},
				Groups:          []string{"Role3"},
				AccessProviders: []string{},
			},
			ActualName: "Role3",
			What:       []sync_from_target.WhatItem{},
			Action:     1,
			Policy:     "",
		},
		{
			ExternalId:        "DB-schema1-MaskingPolicy1",
			NotInternalizable: true,
			Name:              "DB-schema1-MaskingPolicy1",
			NamingHint:        "MaskingPolicy1",
			Who:               nil,
			ActualName:        "DB-schema1-MaskingPolicy1",
			What:              []sync_from_target.WhatItem{},
			Action:            3,
			Policy:            "PolicyBody 1",
		},
		{
			ExternalId:        "DB-schema2-RowAccess1",
			NotInternalizable: true,
			Name:              "DB-schema2-RowAccess1",
			NamingHint:        "RowAccess1",
			Who:               nil,
			ActualName:        "DB-schema2-RowAccess1",
			What:              []sync_from_target.WhatItem{},
			Action:            4,
			Policy:            "Row Access Policy Body",
		},
	}, fileCreator.AccessProviders)
}

func TestAccessSyncer_SyncAccessProvidersFromTarget_StandardEdition(t *testing.T) {
	//Given
	configParams := config.ConfigMap{
		Parameters: map[string]string{"key": "value", SfExternalIdentityStoreOwners: "ExternalOwner1,ExternalOwner2",
			SfStandardEdition: "true"},
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
		{Name: "Role3", AssignedToUsers: 1, GrantedRoles: 1, GrantedToRoles: 1, Owner: "ExternalOwner2"},
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
		repoProvider: func(params map[string]string, role string) (dataAccessRepository, error) {
			return repoMock, nil
		},
		tablesPerSchemaCache:    make(map[string][]TableEntity),
		schemasPerDataBaseCache: make(map[string][]SchemaEntity),
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
			Who: &sync_from_target.WhoItem{
				Users:           []string{"GranteeRole1Number1"},
				Groups:          []string{},
				AccessProviders: []string{"GranteeRole1Number2"},
			},
			ActualName: "Role1",
			What: []sync_from_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "Share2.GranteeRole1Schema",
						Type:     "",
					},
					Permissions: []string{"READ"},
				},
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "DB1.GranteeRole1Table",
						Type:     "",
					},
					Permissions: []string{"SELECT"},
				},
			},
			Action: 1,
			Policy: "",
		}, {
			ExternalId:        "Role2",
			NotInternalizable: false,
			Name:              "Role2",
			NamingHint:        "Role2",
			Who: &sync_from_target.WhoItem{
				Users:           []string{"GranteeRole2"},
				Groups:          []string{},
				AccessProviders: []string{},
			},
			ActualName: "Role2",
			What:       []sync_from_target.WhatItem{},
			Action:     1,
			Policy:     "",
		}, {
			ExternalId:        "Role3",
			NotInternalizable: true,
			Name:              "Role3",
			NamingHint:        "Role3",
			Who: &sync_from_target.WhoItem{
				Users:           []string{},
				Groups:          []string{},
				AccessProviders: []string{"GranteeRole3"},
			},
			ActualName: "Role3",
			What:       []sync_from_target.WhatItem{},
			Action:     1,
			Policy:     "",
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
		Parameters: map[string]string{"key": "value"},
	}

	fileCreator := mocks.NewSimpleAccessProviderHandler(t, 1)

	syncer := &AccessSyncer{
		repoProvider: func(params map[string]string, role string) (dataAccessRepository, error) {
			return nil, fmt.Errorf("boom")
		},
		tablesPerSchemaCache:    make(map[string][]TableEntity),
		schemasPerDataBaseCache: make(map[string][]SchemaEntity),
	}

	//When
	err := syncer.SyncAccessProvidersFromTarget(context.Background(), fileCreator, &configParams)

	//Then
	assert.Error(t, err)
}

func TestAccessSyncer_SyncAccessProviderRolesToTarget(t *testing.T) {
	//Given
	configParams := config.ConfigMap{
		Parameters: map[string]string{"key": "value"},
	}

	rolesToRemove := map[string]*importer.AccessProvider{"ToRemove1": {Id: "xxx"}, "ToRemove2": {Id: "yyy"}}

	repoMock := newMockDataAccessRepository(t)
	feedbackHandler := mocks.NewSimpleAccessProviderFeedbackHandler(t)

	repoMock.EXPECT().Close().Return(nil).Once()
	repoMock.EXPECT().TotalQueryTime().Return(time.Minute).Once()
	repoMock.EXPECT().DropRole("ToRemove1").Return(nil).Once()
	repoMock.EXPECT().DropRole("ToRemove2").Return(nil).Once()
	repoMock.EXPECT().GetRolesWithPrefix("").Return([]RoleEntity{
		{Name: "ExistingRole1", GrantedToRoles: 2, GrantedRoles: 3, AssignedToUsers: 2, Owner: "Owner"},
		{Name: "ExistingRole2", GrantedToRoles: 2, GrantedRoles: 3, AssignedToUsers: 2, Owner: "Owner"},
	}, nil).Once()

	repoMock.EXPECT().CreateRole("RoleName1").Return(nil).Once()
	repoMock.EXPECT().CreateRole("RoleName3").Return(nil).Once()
	repoMock.EXPECT().CommentRoleIfExists(mock.Anything, "RoleName1").Return(nil).Once()
	repoMock.EXPECT().CommentRoleIfExists(mock.Anything, "RoleName3").Return(nil).Once()

	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")
	expectGrantUsersToRole(repoMock, "RoleName3")

	repoMock.EXPECT().GrantRolesToRole(mock.Anything, "RoleName1").Return(nil).Once()
	repoMock.EXPECT().GrantRolesToRole(mock.Anything, "RoleName3").Return(nil).Once()

	repoMock.EXPECT().GetGrantsOfRole("ExistingRole1").Return([]GrantOfRole{}, nil).Once()
	repoMock.EXPECT().GetGrantsToRole("ExistingRole1").Return([]GrantToRole{}, nil).Once()

	repoMock.EXPECT().CommentRoleIfExists(mock.Anything, "ExistingRole1").Return(nil).Once()

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
		repoProvider: func(params map[string]string, role string) (dataAccessRepository, error) {
			return repoMock, nil
		},
		tablesPerSchemaCache:    make(map[string][]TableEntity),
		schemasPerDataBaseCache: make(map[string][]SchemaEntity),
	}

	ap1 := &importer.AccessProvider{
		Id:   "AccessProviderId1",
		Name: "AccessProvider1",
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

	access := map[string]*importer.AccessProvider{
		"RoleName1":     ap1,
		"ExistingRole1": ap2,
		"RoleName3":     ap3,
	}

	//When
	err := syncer.SyncAccessProviderRolesToTarget(context.Background(), rolesToRemove, access, feedbackHandler, &configParams)

	//Then
	assert.NoError(t, err)
	assert.ElementsMatch(t, []importer.AccessProviderSyncFeedback{{
		AccessProvider: "xxx",
		ActualName:     "ToRemove1",
		ExternalId:     ptr.String("ToRemove1"),
	},
		{
			AccessProvider: "yyy",
			ActualName:     "ToRemove2",
			ExternalId:     ptr.String("ToRemove2"),
		},
		{
			AccessProvider: "AccessProviderId2",
			ActualName:     "ExistingRole1",
			ExternalId:     ptr.String("ExistingRole1"),
			Type:           ptr.String("role"),
		},
		{
			AccessProvider: "AccessProviderId3",
			ActualName:     "RoleName3",
			ExternalId:     ptr.String("RoleName3"),
			Type:           ptr.String("role"),
		},
		{
			AccessProvider: "AccessProviderId1",
			ActualName:     "RoleName1",
			ExternalId:     ptr.String("RoleName1"),
			Type:           ptr.String("role"),
		},
	}, feedbackHandler.AccessProviderFeedback)

}

func TestAccessSyncer_SyncAccessProvidersToTarget_ErrorOnConnectionToRepo(t *testing.T) {
	//Given
	configParams := config.ConfigMap{
		Parameters: map[string]string{"key": "value"},
	}

	feedbackHandler := mocks.NewSimpleAccessProviderFeedbackHandler(t)

	syncer := AccessSyncer{
		repoProvider: func(params map[string]string, role string) (dataAccessRepository, error) {
			return nil, fmt.Errorf("boom")
		},
		tablesPerSchemaCache:    make(map[string][]TableEntity),
		schemasPerDataBaseCache: make(map[string][]SchemaEntity),
	}

	access := map[string]*importer.AccessProvider{
		"RoleName1": {
			Id:   "AccessProviderId1",
			Name: "AccessProvider1",
			Who: importer.WhoItem{
				Users: []string{"User1", "User2"},
			},
			What: []importer.WhatItem{
				{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table1", Type: "TABLE"}, Permissions: []string{"SELECT"}},
			},
		},
	}

	//When
	err := syncer.SyncAccessProviderRolesToTarget(context.Background(), map[string]*importer.AccessProvider{"roleToRemove1": {Id: "xxx"}}, access, feedbackHandler, &configParams)

	//Then
	assert.Error(t, err)
}

func TestAccessSyncer_SyncAccessAsCodeToTarget(t *testing.T) {
	//Given
	configParams := config.ConfigMap{
		Parameters: map[string]string{"key": "value"},
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

	repoMock.EXPECT().CreateRole("AccessProvider1").Return(nil).Once()
	repoMock.EXPECT().CommentRoleIfExists(mock.Anything, "AccessProvider1").Return(nil).Once()
	expectGrantUsersToRole(repoMock, "AccessProvider1", "User1", "User2")
	repoMock.EXPECT().GrantRolesToRole(mock.Anything, "AccessProvider1").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrant("USAGE", "DATABASE DB1", "AccessProvider1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("USAGE", "SCHEMA DB1.Schema1", "AccessProvider1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("SELECT", "TABLE DB1.Schema1.Table1", "AccessProvider1").Return(nil).Once()

	syncer := AccessSyncer{
		repoProvider: func(params map[string]string, role string) (dataAccessRepository, error) {
			return repoMock, nil
		},
		tablesPerSchemaCache:    make(map[string][]TableEntity),
		schemasPerDataBaseCache: make(map[string][]SchemaEntity),
	}

	access := map[string]*importer.AccessProvider{
		"AccessProvider1": {
			Id:   "AccessProviderId1",
			Name: "AccessProvider1",
			Who: importer.WhoItem{
				Users: []string{"User1", "User2"},
			},
			What: []importer.WhatItem{
				{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table1", Type: "table"}, Permissions: []string{"SELECT"}},
			},
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
		Parameters: map[string]string{"key": "value"},
	}

	syncer := AccessSyncer{
		repoProvider: func(params map[string]string, role string) (dataAccessRepository, error) {
			return nil, fmt.Errorf("boom")
		},
		tablesPerSchemaCache:    make(map[string][]TableEntity),
		schemasPerDataBaseCache: make(map[string][]SchemaEntity),
	}

	access := map[string]*importer.AccessProvider{
		"RoleName1": {
			Id:   "AccessProviderId1",
			Name: "AccessProvider1",
			Who: importer.WhoItem{
				Users: []string{"User1", "User2"},
			},
			What: []importer.WhatItem{
				{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table1", Type: "TABLE"}, Permissions: []string{"SELECT"}},
			},
		},
	}

	//When
	err := syncer.SyncAccessAsCodeToTarget(context.Background(), access, "R_", &configParams)

	//Then
	assert.Error(t, err)
}

func TestAccessSyncer_SyncAccessProviderMasksToTarget(t *testing.T) {
	// Given
	configParams := config.ConfigMap{
		Parameters: map[string]string{"key": "value"},
	}

	repoMock := newMockDataAccessRepository(t)
	fileCreator := mocks.NewSimpleAccessProviderFeedbackHandler(t)

	masksToRemove := map[string]*importer.AccessProvider{"RAITO_MASKTOREMOVE1": {Id: "xxx", ActualName: ptr.String("RAITO_MASKTOREMOVE1")}}
	masks := map[string]*importer.AccessProvider{
		"Mask1": {
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
			Action: importer.Mask,
		},
		"Mask2": {
			Id:   "MaskId2",
			Name: "Mask2",
			Who: importer.WhoItem{
				Users: []string{"User1"},
			},
			What: []importer.WhatItem{
				{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table3.Column1", Type: "column"}},
			},
			Action: importer.Mask,
		},
	}

	repoMock.EXPECT().GetPoliciesLike("MASKING", "RAITO_MASK1%").Return(nil, nil).Once() //No existing masks
	repoMock.EXPECT().CreateMaskPolicy("DB1", "Schema1", mock.AnythingOfType("string"), []string{"DB1.Schema1.Table1.Column1"}, (*string)(nil), &MaskingBeneficiaries{Users: []string{"User1", "User2"}, Roles: []string{"Role1", "Role2"}}).Return(nil)
	repoMock.EXPECT().CreateMaskPolicy("DB1", "Schema2", mock.AnythingOfType("string"), []string{"DB1.Schema2.Table1.Column1"}, (*string)(nil), &MaskingBeneficiaries{Users: []string{"User1", "User2"}, Roles: []string{"Role1", "Role2"}}).Return(nil)

	repoMock.EXPECT().GetPoliciesLike("MASKING", "RAITO_MASK2%").Return([]PolicyEntity{{Name: "RAITO_MASK2_OLD_TEXT", SchemaName: "Schema1", DatabaseName: "DB1"}}, nil).Once()
	repoMock.EXPECT().CreateMaskPolicy("DB1", "Schema1", mock.AnythingOfType("string"), []string{"DB1.Schema1.Table3.Column1"}, (*string)(nil), &MaskingBeneficiaries{Users: []string{"User1"}}).Return(nil)
	repoMock.EXPECT().DropMaskingPolicy("DB1", "Schema1", "RAITO_MASK2_OLD").Return(nil)

	repoMock.EXPECT().GetPoliciesLike("MASKING", "RAITO_MASKTOREMOVE1%").Return([]PolicyEntity{{Name: "RAITO_maskToRemove1_TEXT", SchemaName: "Schema3", DatabaseName: "DB1"}, {Name: "RAITO_maskToRemove1_INT", SchemaName: "Schema1", DatabaseName: "DB1"}}, nil).Once()
	repoMock.EXPECT().DropMaskingPolicy("DB1", "Schema3", "RAITO_MASKTOREMOVE1").Return(nil)
	repoMock.EXPECT().DropMaskingPolicy("DB1", "Schema1", "RAITO_MASKTOREMOVE1").Return(nil)

	syncer := &AccessSyncer{
		repoProvider: func(params map[string]string, role string) (dataAccessRepository, error) {
			return repoMock, nil
		},
	}

	// When
	err := syncer.SyncAccessProviderMasksToTarget(context.Background(), masksToRemove, masks, map[string]string{"Role2-Id": "Role2"}, fileCreator, &configParams)

	// Then
	assert.NoError(t, err)
	assert.Len(t, fileCreator.AccessProviderFeedback, 3)
}

func TestAccessSyncer_removeRolesToRemove_NoRoles(t *testing.T) {
	//Given
	repo := newMockDataAccessRepository(t)

	syncer := AccessSyncer{
		repoProvider: func(params map[string]string, role string) (dataAccessRepository, error) {
			return nil, nil
		},
		tablesPerSchemaCache:    make(map[string][]TableEntity),
		schemasPerDataBaseCache: make(map[string][]SchemaEntity),
	}

	//When
	err := syncer.removeRolesToRemove(map[string]*importer.AccessProvider{}, repo, nil)

	//Then
	assert.NoError(t, err)
}

func TestAccessSyncer_removeRolesToRemove(t *testing.T) {
	//Given
	repo := newMockDataAccessRepository(t)

	rolesToRemove := map[string]*importer.AccessProvider{"Role1": {Id: "xxx"}, "Role2": {Id: "yyy"}, "Role3": {Id: "zzz"}}

	repo.EXPECT().DropRole(mock.MatchedBy(func(roleName string) bool {
		for possibleRole := range rolesToRemove {
			if possibleRole == roleName {
				return true
			}
		}
		return false
	})).Return(nil).Times(len(rolesToRemove))

	syncer := AccessSyncer{
		repoProvider: func(params map[string]string, role string) (dataAccessRepository, error) {
			return nil, nil
		},
		tablesPerSchemaCache:    make(map[string][]TableEntity),
		schemasPerDataBaseCache: make(map[string][]SchemaEntity),
	}

	//When
	err := syncer.removeRolesToRemove(rolesToRemove, repo, &dummyFeedbackHandler{})

	//Then
	assert.NoError(t, err)
}

func TestAccessSyncer_removeRolesToRemove_error(t *testing.T) {
	//Given
	repo := newMockDataAccessRepository(t)

	rolesToRemove := map[string]*importer.AccessProvider{"Role1": {Id: "xxx"}, "Role2": {Id: "yyy"}}

	repo.EXPECT().DropRole("Role1").Return(nil).Once()
	repo.EXPECT().DropRole("Role2").Return(fmt.Errorf("BOOM")).Once()

	syncer := AccessSyncer{
		repoProvider: func(params map[string]string, role string) (dataAccessRepository, error) {
			return nil, nil
		},
		tablesPerSchemaCache:    make(map[string][]TableEntity),
		schemasPerDataBaseCache: make(map[string][]SchemaEntity),
	}

	feedbackHandler := mocks.NewSimpleAccessProviderFeedbackHandler(t)

	//When
	err := syncer.removeRolesToRemove(rolesToRemove, repo, feedbackHandler)

	assert.Len(t, feedbackHandler.AccessProviderFeedback, 2)
	assert.ElementsMatch(t, feedbackHandler.AccessProviderFeedback, []importer.AccessProviderSyncFeedback{
		{
			AccessProvider: "xxx",
			ActualName:     "Role1",
			ExternalId:     ptr.String("Role1"),
		},
		{
			AccessProvider: "yyy",
			ActualName:     "Role2",
			ExternalId:     ptr.String("Role2"),
			Errors:         []string{"unable to drop role \"Role2\": BOOM"},
		},
	})

	//Then
	assert.NoError(t, err)
}

func TestAccessSyncer_importPoliciesOfType(t *testing.T) {
	//Given
	repoMock := newMockDataAccessRepository(t)
	fileCreator := mocks.NewSimpleAccessProviderHandler(t, 1)

	policyType := "policyType"

	repoMock.EXPECT().GetPolicies(policyType).Return([]PolicyEntity{
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
		repoProvider: func(params map[string]string, role string) (dataAccessRepository, error) {
			return nil, nil
		},
		tablesPerSchemaCache:    make(map[string][]TableEntity),
		schemasPerDataBaseCache: make(map[string][]SchemaEntity),
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
			Who:               nil,
			ActualName:        "DB1-Schema1-Policy1",
			What: []sync_from_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "DB1.Schema1.EntityName1.ColumnName1",
						Type:     "COLUMN",
					},
					Permissions: []string{},
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
			Who:               nil,
			ActualName:        "DB1-Schema2-Policy2",
			What: []sync_from_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "DB1.Schema1.EntityName1",
						Type:     "TABLE",
					},
					Permissions: []string{},
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

	repoMock.EXPECT().GetPolicies(policyType).Return([]PolicyEntity{
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
		repoProvider: func(params map[string]string, role string) (dataAccessRepository, error) {
			return nil, nil
		},
		tablesPerSchemaCache:    make(map[string][]TableEntity),
		schemasPerDataBaseCache: make(map[string][]SchemaEntity),
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

	repoMock.EXPECT().CreateRole("RoleName1").Return(nil).Once()
	repoMock.EXPECT().CommentRoleIfExists(mock.Anything, "RoleName1").Return(nil).Once()
	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")
	repoMock.EXPECT().GrantRolesToRole(mock.Anything, "RoleName1").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrant("USAGE", "DATABASE DB1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("USAGE", "SCHEMA DB1.Schema1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("SELECT", "TABLE DB1.Schema1.Table1", "RoleName1").Return(nil).Once()

	syncer := AccessSyncer{
		repoProvider: func(params map[string]string, role string) (dataAccessRepository, error) {
			return nil, nil
		},
		tablesPerSchemaCache:    make(map[string][]TableEntity),
		schemasPerDataBaseCache: make(map[string][]SchemaEntity),
	}

	access := map[string]*importer.AccessProvider{
		"RoleName1": {
			Id:   "AccessProviderId1",
			Name: "AccessProvider1",
			Who: importer.WhoItem{
				Users: []string{"User1", "User2"},
			},
			What: []importer.WhatItem{
				{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table1", Type: "table"}, Permissions: []string{"SELECT"}},
			},
		},
	}

	//When
	err := syncer.generateAccessControls(context.Background(), access, map[string]bool{}, repoMock, &config.ConfigMap{}, &dummyFeedbackHandler{})

	//Then
	assert.NoError(t, err)
}

func generateAccessControls_view(t *testing.T) {
	//Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CreateRole("RoleName1").Return(nil).Once()
	repoMock.EXPECT().CommentRoleIfExists(mock.Anything, "RoleName1").Return(nil).Once()
	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")
	repoMock.EXPECT().GrantRolesToRole(mock.Anything, "RoleName1").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrant("USAGE", "DATABASE DB1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("USAGE", "SCHEMA DB1.Schema1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("SELECT", "VIEW DB1.Schema1.Table2", "RoleName1").Return(nil).Once()

	syncer := AccessSyncer{
		repoProvider: func(params map[string]string, role string) (dataAccessRepository, error) {
			return nil, nil
		},
		tablesPerSchemaCache:    make(map[string][]TableEntity),
		schemasPerDataBaseCache: make(map[string][]SchemaEntity),
	}

	access := map[string]*importer.AccessProvider{
		"RoleName1": {
			Id:   "AccessProviderId1",
			Name: "AccessProvider1",
			Who: importer.WhoItem{
				Users: []string{"User1", "User2"},
			},
			What: []importer.WhatItem{
				{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table2", Type: "view"}, Permissions: []string{"SELECT"}},
			},
		},
	}

	//When
	err := syncer.generateAccessControls(context.Background(), access, map[string]bool{}, repoMock, &config.ConfigMap{}, &dummyFeedbackHandler{})

	//Then
	assert.NoError(t, err)
}

func generateAccessControls_schema(t *testing.T) {
	//Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CreateRole("RoleName1").Return(nil).Once()
	repoMock.EXPECT().CommentRoleIfExists(mock.Anything, "RoleName1").Return(nil).Once()
	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")
	repoMock.EXPECT().GrantRolesToRole(mock.Anything, "RoleName1").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrant("USAGE", "DATABASE DB1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("USAGE", "SCHEMA DB1.Schema2", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("SELECT", "TABLE DB1.Schema2.Table3", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("SELECT", "VIEW DB1.Schema2.View3", "RoleName1").Return(nil).Once()

	database := "DB1"
	schema := "Schema2"

	repoMock.EXPECT().GetTablesInDatabase(database, schema, mock.Anything).RunAndReturn(func(s string, s2 string, handler EntityHandler) error {
		handler(&TableEntity{Database: s, Schema: s2, Name: "Table3", TableType: "BASE TABLE"})
		handler(&TableEntity{Database: s, Schema: s2, Name: "View3", TableType: "VIEW"})
		return nil
	}).Once()

	syncer := AccessSyncer{
		repoProvider: func(params map[string]string, role string) (dataAccessRepository, error) {
			return nil, nil
		},
		tablesPerSchemaCache:    make(map[string][]TableEntity),
		schemasPerDataBaseCache: make(map[string][]SchemaEntity),
	}

	access := map[string]*importer.AccessProvider{
		"RoleName1": {
			Id:   "AccessProviderId1",
			Name: "AccessProvider1",
			Who: importer.WhoItem{
				Users: []string{"User1", "User2"},
			},
			What: []importer.WhatItem{
				{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema2", Type: "schema"}, Permissions: []string{"SELECT"}},
			},
		},
	}

	//When
	err := syncer.generateAccessControls(context.Background(), access, map[string]bool{}, repoMock, &config.ConfigMap{}, &dummyFeedbackHandler{})

	//Then
	assert.NoError(t, err)
}

func generateAccessControls_schema_noverify(t *testing.T) {
	//Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CreateRole("RoleName1").Return(nil).Once()
	repoMock.EXPECT().CommentRoleIfExists(mock.Anything, "RoleName1").Return(nil).Once()
	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")
	repoMock.EXPECT().GrantRolesToRole(mock.Anything, "RoleName1").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrant("USAGE", "DATABASE DB1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("USAGE", "SCHEMA DB1.Schema2", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("CREATE TABLE", "SCHEMA DB1.Schema2", "RoleName1").Return(nil).Once()

	syncer := AccessSyncer{
		repoProvider: func(params map[string]string, role string) (dataAccessRepository, error) {
			return nil, nil
		},
		tablesPerSchemaCache:    make(map[string][]TableEntity),
		schemasPerDataBaseCache: make(map[string][]SchemaEntity),
	}

	access := map[string]*importer.AccessProvider{
		"RoleName1": {
			Id:   "AccessProviderId1",
			Name: "AccessProvider1",
			Who: importer.WhoItem{
				Users: []string{"User1", "User2"},
			},
			What: []importer.WhatItem{
				{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema2", Type: "schema"}, Permissions: []string{"CREATE TABLE"}},
			},
		},
	}

	//When
	err := syncer.generateAccessControls(context.Background(), access, map[string]bool{}, repoMock, &config.ConfigMap{}, &dummyFeedbackHandler{})

	//Then
	assert.NoError(t, err)
}

func generateAccessControls_existing_schema(t *testing.T) {
	//Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CommentRoleIfExists(mock.AnythingOfType("string"), "RoleName1").Return(nil).Once()

	repoMock.EXPECT().GetGrantsOfRole(mock.Anything).Return([]GrantOfRole{}, nil)
	repoMock.EXPECT().GetGrantsToRole(mock.Anything).Return([]GrantToRole{}, nil)

	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")

	repoMock.EXPECT().ExecuteRevoke("ALL", "FUTURE TABLES IN SCHEMA DB1.Schema2", "RoleName1").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrant("USAGE", "DATABASE DB1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("USAGE", "SCHEMA DB1.Schema2", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("SELECT", "TABLE DB1.Schema2.Table3", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("SELECT", "VIEW DB1.Schema2.View3", "RoleName1").Return(nil).Once()

	database := "DB1"
	schema := "Schema2"
	repoMock.EXPECT().GetTablesInDatabase(database, schema, mock.Anything).RunAndReturn(func(s string, s2 string, handler EntityHandler) error {
		handler(&TableEntity{Database: s, Schema: s2, Name: "Table3", TableType: "BASE TABLE"})
		handler(&TableEntity{Database: s, Schema: s2, Name: "View3", TableType: "VIEW"})
		return nil
	}).Once()

	syncer := AccessSyncer{
		repoProvider: func(params map[string]string, role string) (dataAccessRepository, error) {
			return nil, nil
		},
		tablesPerSchemaCache:    make(map[string][]TableEntity),
		schemasPerDataBaseCache: make(map[string][]SchemaEntity),
	}

	access := map[string]*importer.AccessProvider{
		"RoleName1": {
			Id:   "AccessProviderId1",
			Name: "AccessProvider1",
			Who: importer.WhoItem{
				Users: []string{"User1", "User2"},
			},
			What: []importer.WhatItem{
				{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema2", Type: "schema"}, Permissions: []string{"SELECT"}},
			},
		},
	}

	//When
	err := syncer.generateAccessControls(context.Background(), access, map[string]bool{"RoleName1": true}, repoMock, &config.ConfigMap{}, &dummyFeedbackHandler{})

	//Then
	assert.NoError(t, err)
}

func generateAccessControls_sharedDatabase(t *testing.T) {
	//Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CreateRole("RoleName1").Return(nil).Once()
	repoMock.EXPECT().CommentRoleIfExists(mock.Anything, "RoleName1").Return(nil).Once()
	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")
	repoMock.EXPECT().GrantRolesToRole(mock.Anything, "RoleName1").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrant("IMPORTED PRIVILEGES", "DATABASE DB2", "RoleName1").Return(nil).Once()

	syncer := AccessSyncer{
		repoProvider: func(params map[string]string, role string) (dataAccessRepository, error) {
			return nil, nil
		},
		tablesPerSchemaCache:    make(map[string][]TableEntity),
		schemasPerDataBaseCache: make(map[string][]SchemaEntity),
	}

	access := map[string]*importer.AccessProvider{
		"RoleName1": {
			Id:   "AccessProviderId1",
			Name: "AccessProvider1",
			Who: importer.WhoItem{
				Users: []string{"User1", "User2"},
			},
			What: []importer.WhatItem{
				{DataObject: &data_source.DataObjectReference{FullName: "DB2", Type: "shared-database"}, Permissions: []string{"IMPORTED PRIVILEGES"}},
			},
		},
	}

	//When
	err := syncer.generateAccessControls(context.Background(), access, map[string]bool{}, repoMock, &config.ConfigMap{}, &dummyFeedbackHandler{})

	//Then
	assert.NoError(t, err)
}

func generateAccessControls_database(t *testing.T) {
	//Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CreateRole("RoleName1").Return(nil).Once()
	repoMock.EXPECT().CommentRoleIfExists(mock.Anything, "RoleName1").Return(nil).Once()
	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")
	repoMock.EXPECT().GrantRolesToRole(mock.Anything, "RoleName1").Return(nil).Once()

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

	repoMock.EXPECT().ExecuteGrant("USAGE", "DATABASE DB1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("USAGE", "SCHEMA DB1.Schema2", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("SELECT", "TABLE DB1.Schema2.Table3", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("SELECT", "VIEW DB1.Schema2.View3", "RoleName1").Return(nil).Once()

	syncer := AccessSyncer{
		repoProvider: func(params map[string]string, role string) (dataAccessRepository, error) {
			return nil, nil
		},
		tablesPerSchemaCache:    make(map[string][]TableEntity),
		schemasPerDataBaseCache: make(map[string][]SchemaEntity),
	}

	access := map[string]*importer.AccessProvider{
		"RoleName1": {
			Id:   "AccessProviderId1",
			Name: "AccessProvider1",
			Who: importer.WhoItem{
				Users: []string{"User1", "User2"},
			},
			What: []importer.WhatItem{
				{DataObject: &data_source.DataObjectReference{FullName: "DB1", Type: "database"}, Permissions: []string{"SELECT"}},
			},
		},
	}

	//When
	err := syncer.generateAccessControls(context.Background(), access, map[string]bool{}, repoMock, &config.ConfigMap{}, &dummyFeedbackHandler{})

	//Then
	assert.NoError(t, err)
}

func generateAccessControls_existing_database(t *testing.T) {
	//Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CommentRoleIfExists(mock.AnythingOfType("string"), "RoleName1").Return(nil).Once()

	repoMock.EXPECT().GetGrantsOfRole(mock.Anything).Return([]GrantOfRole{}, nil)
	repoMock.EXPECT().GetGrantsToRole(mock.Anything).Return([]GrantToRole{}, nil)

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

	repoMock.EXPECT().ExecuteRevoke("ALL", "FUTURE SCHEMAS IN DATABASE DB1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteRevoke("ALL", "FUTURE TABLES IN DATABASE DB1", "RoleName1").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrant("USAGE", "DATABASE DB1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("USAGE", "SCHEMA DB1.Schema2", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("SELECT", "TABLE DB1.Schema2.Table3", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("SELECT", "VIEW DB1.Schema2.View3", "RoleName1").Return(nil).Once()

	syncer := AccessSyncer{
		repoProvider: func(params map[string]string, role string) (dataAccessRepository, error) {
			return nil, nil
		},
		tablesPerSchemaCache:    make(map[string][]TableEntity),
		schemasPerDataBaseCache: make(map[string][]SchemaEntity),
	}

	access := map[string]*importer.AccessProvider{
		"RoleName1": {
			Id:   "AccessProviderId1",
			Name: "AccessProvider1",
			Who: importer.WhoItem{
				Users: []string{"User1", "User2"},
			},
			What: []importer.WhatItem{
				{DataObject: &data_source.DataObjectReference{FullName: "DB1", Type: "database"}, Permissions: []string{"SELECT"}},
			},
		},
	}

	//When
	err := syncer.generateAccessControls(context.Background(), access, map[string]bool{"RoleName1": true}, repoMock, &config.ConfigMap{}, &dummyFeedbackHandler{})

	//Then
	assert.NoError(t, err)
}

func generateAccessControls_warehouse(t *testing.T) {
	//Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CreateRole("RoleName1").Return(nil).Once()
	repoMock.EXPECT().CommentRoleIfExists(mock.Anything, "RoleName1").Return(nil).Once()
	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")
	repoMock.EXPECT().GrantRolesToRole(mock.Anything, "RoleName1").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrant("MONITOR", "WAREHOUSE WH1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrant("USAGE", "WAREHOUSE WH1", "RoleName1").Return(nil).Once()

	syncer := AccessSyncer{
		repoProvider: func(params map[string]string, role string) (dataAccessRepository, error) {
			return nil, nil
		},
		tablesPerSchemaCache:    make(map[string][]TableEntity),
		schemasPerDataBaseCache: make(map[string][]SchemaEntity),
	}

	access := map[string]*importer.AccessProvider{
		"RoleName1": {
			Id:   "AccessProviderId1",
			Name: "AccessProvider1",
			Who: importer.WhoItem{
				Users: []string{"User1", "User2"},
			},
			What: []importer.WhatItem{
				{DataObject: &data_source.DataObjectReference{FullName: "WH1", Type: "warehouse"}, Permissions: []string{"MONITOR"}},
			},
		},
	}

	//When
	err := syncer.generateAccessControls(context.Background(), access, map[string]bool{}, repoMock, &config.ConfigMap{}, &dummyFeedbackHandler{})

	//Then
	assert.NoError(t, err)
}

func generateAccessControls_datasource(t *testing.T) {
	//Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CreateRole("RoleName1").Return(nil).Once()
	repoMock.EXPECT().CommentRoleIfExists(mock.Anything, "RoleName1").Return(nil).Once()
	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")
	repoMock.EXPECT().GrantRolesToRole(mock.Anything, "RoleName1").Return(nil).Once()
	repoMock.EXPECT().GetShares().Return([]DbEntity{}, nil).Once()
	repoMock.EXPECT().GetDataBases().Return([]DbEntity{}, nil).Once()

	syncer := AccessSyncer{
		repoProvider: func(params map[string]string, role string) (dataAccessRepository, error) {
			return nil, nil
		},
		tablesPerSchemaCache:    make(map[string][]TableEntity),
		schemasPerDataBaseCache: make(map[string][]SchemaEntity),
	}

	access := map[string]*importer.AccessProvider{
		"RoleName1": {
			Id:   "AccessProviderId1",
			Name: "AccessProvider1",
			Who: importer.WhoItem{
				Users: []string{"User1", "User2"},
			},
			What: []importer.WhatItem{
				{DataObject: &data_source.DataObjectReference{FullName: "DS1", Type: "datasource"}, Permissions: []string{"READ"}},
			},
		},
	}

	//When
	err := syncer.generateAccessControls(context.Background(), access, map[string]bool{}, repoMock, &config.ConfigMap{}, &dummyFeedbackHandler{})

	//Then
	assert.NoError(t, err)
}

func TestAccessSyncer_generateAccessControls(t *testing.T) {
	t.Run("Table", generateAccessControls_table)
	t.Run("View", generateAccessControls_view)
	t.Run("Schema", generateAccessControls_schema)
	t.Run("Schema no verify", generateAccessControls_schema_noverify)
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

	repoMock.EXPECT().CommentRoleIfExists(mock.AnythingOfType("string"), "existingRole1").Return(nil).Once()
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
		repoProvider: func(params map[string]string, role string) (dataAccessRepository, error) {
			return nil, nil
		},
		tablesPerSchemaCache:    make(map[string][]TableEntity),
		schemasPerDataBaseCache: make(map[string][]SchemaEntity),
	}

	access := map[string]*importer.AccessProvider{
		"existingRole1": {
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
	}

	//When
	err := syncer.generateAccessControls(context.Background(), access, map[string]bool{"existingRole1": true}, repoMock, &config.ConfigMap{}, &dummyFeedbackHandler{})

	//Then
	assert.NoError(t, err)
}

func TestAccessSyncer_generateAccessControls_inheritance(t *testing.T) {
	//Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CreateRole("RoleName1").Return(nil).Once()
	repoMock.EXPECT().CreateRole("RoleName2").Return(nil).Once()
	repoMock.EXPECT().CreateRole("RoleName3").Return(nil).Once()
	repoMock.EXPECT().CommentRoleIfExists(mock.Anything, "RoleName1").Return(nil).Once()
	repoMock.EXPECT().CommentRoleIfExists(mock.Anything, "RoleName2").Return(nil).Once()
	repoMock.EXPECT().CommentRoleIfExists(mock.Anything, "RoleName3").Return(nil).Once()
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
		repoProvider: func(params map[string]string, role string) (dataAccessRepository, error) {
			return nil, nil
		},
		tablesPerSchemaCache:    make(map[string][]TableEntity),
		schemasPerDataBaseCache: make(map[string][]SchemaEntity),
	}

	access := map[string]*importer.AccessProvider{
		"RoleName1": {
			Id:   "AccessProviderId1",
			Name: "AccessProvider1",
			Who: importer.WhoItem{
				InheritFrom: []string{"ID:AccessProviderId2"},
			},
			What: []importer.WhatItem{
				{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table1", Type: "table"}, Permissions: []string{"SELECT"}},
			},
		},
		"RoleName2": {
			Id:   "AccessProviderId2",
			Name: "AccessProvider2",
			Who: importer.WhoItem{
				InheritFrom: []string{"RoleName3"},
			},
			What: []importer.WhatItem{
				{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table2", Type: "table"}, Permissions: []string{"SELECT"}},
			},
		},
		"RoleName3": {
			Id:   "AccessProviderId3",
			Name: "AccessProvider3",
			Who: importer.WhoItem{
				Users: []string{"User1"},
			},
			What: []importer.WhatItem{
				{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table3", Type: "table"}, Permissions: []string{"SELECT"}},
			},
		},
	}

	//When
	err := syncer.generateAccessControls(context.Background(), access, map[string]bool{}, repoMock, &config.ConfigMap{}, &dummyFeedbackHandler{})

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
