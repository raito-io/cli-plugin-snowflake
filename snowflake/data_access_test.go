package snowflake

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/smithy-go/ptr"
	"github.com/raito-io/cli/base/access_provider"
	"github.com/raito-io/cli/base/access_provider/sync_from_target"
	"github.com/raito-io/cli/base/access_provider/sync_to_target"
	importer "github.com/raito-io/cli/base/access_provider/sync_to_target"
	"github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestAccessSyncer_SyncAccessProvidersFromTarget(t *testing.T) {
	//Given
	configParams := config.ConfigMap{
		Parameters: map[string]string{"key": "value", SfExternalIdentityStoreOwners: "ExternalOwner1,ExternalOwner2"},
	}

	repoMock := newMockDataAccessRepository(t)
	fileCreator := mocks.NewSimpleAccessProviderHandler(t, 3)

	repoMock.EXPECT().Close().Return(nil).Once()
	repoMock.EXPECT().TotalQueryTime().Return(time.Minute).Once()
	repoMock.EXPECT().GetShares().Return([]DbEntity{
		{Name: "Share1"}, {Name: "Share2"},
	}, nil).Once()

	repoMock.EXPECT().GetAccountRoles().Return([]RoleEntity{
		{Name: "Role1", AssignedToUsers: 2, GrantedRoles: 3, GrantedToRoles: 1, Owner: "Owner1"},
		{Name: "Role2", AssignedToUsers: 3, GrantedRoles: 2, GrantedToRoles: 1, Owner: "Owner2"},
		{Name: "Role3", AssignedToUsers: 1, GrantedRoles: 1, GrantedToRoles: 1, Owner: "ExternalOwner1"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsOfAccountRole("Role1").Return([]GrantOfRole{
		{GrantedTo: "USER", GranteeName: "GranteeRole1Number1"},
		{GrantedTo: "ROLE", GranteeName: "GranteeRole1Number2"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsToAccountRole("Role1").Return([]GrantToRole{
		{GrantedOn: "SCHEMA", Name: "Share2.GranteeRole1Schema", Privilege: "USAGE"},
		{GrantedOn: "SCHEMA", Name: "Share2.GranteeRole1Schema", Privilege: "READ"},
		{GrantedOn: "TABLE", Name: "DB1.GranteeRole1Table", Privilege: "USAGE"},
		{GrantedOn: "TABLE", Name: "DB1.GranteeRole1Table", Privilege: "SELECT"},
		{GrantedOn: "MATERIALIZED_VIEW", Name: "DB1.GranteeRole1MatView", Privilege: "SELECT"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsOfAccountRole("Role2").Return([]GrantOfRole{
		{GrantedTo: "USER", GranteeName: "GranteeRole2"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsToAccountRole("Role2").Return([]GrantToRole{
		{GrantedOn: "GrandOnRole2Number1", Name: "GranteeRole2", Privilege: "USAGE"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsOfAccountRole("Role3").Return([]GrantOfRole{
		{GrantedTo: "ROLE", GranteeName: "\"GranteeRole.3\""},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsToAccountRole("Role3").Return([]GrantToRole{
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

	syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
		return repoMock, nil
	})

	//When
	err := syncer.SyncAccessProvidersFromTarget(context.Background(), fileCreator, &configParams)

	//Then
	assert.NoError(t, err)
	assert.ElementsMatch(t, []sync_from_target.AccessProvider{
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
			Type:       ptr.String(access_provider.Role),
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
			Type:              ptr.String(access_provider.Role),
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
			Type:              ptr.String(access_provider.Role),
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

func TestAccessSyncer_SyncAccessProvidersFromTarget_WithDatabaseRoleSupport(t *testing.T) {
	//Given
	configParams := config.ConfigMap{
		Parameters: map[string]string{"key": "value", SfExternalIdentityStoreOwners: "ExternalOwner1,ExternalOwner2", SfDatabaseRoles: "true"},
	}

	repoMock := newMockDataAccessRepository(t)
	fileCreator := mocks.NewSimpleAccessProviderHandler(t, 3)

	repoMock.EXPECT().Close().Return(nil).Once()
	repoMock.EXPECT().TotalQueryTime().Return(time.Minute).Once()
	repoMock.EXPECT().GetShares().Return([]DbEntity{
		{Name: "Share1"}, {Name: "Share2"},
	}, nil).Once()

	repoMock.EXPECT().GetDatabases().Return([]DbEntity{
		{Name: "SNOWFLAKE"},
		{Name: "TEST_DB"},
	}, nil).Once()
	repoMock.EXPECT().GetDatabaseRoles("TEST_DB").Return([]RoleEntity{
		{Name: "DatabaseRole1", AssignedToUsers: 0, GrantedRoles: 0, GrantedToRoles: 1, Owner: "Owner1"},
		{Name: "DatabaseRole2", AssignedToUsers: 0, GrantedRoles: 1, GrantedToRoles: 0, Owner: "Owner2"},
		{Name: "DatabaseRole3", AssignedToUsers: 0, GrantedRoles: 1, GrantedToRoles: 0, Owner: "Owner2"},
	}, nil).Once()

	repoMock.EXPECT().GetAccountRoles().Return([]RoleEntity{
		{Name: "Role1", AssignedToUsers: 2, GrantedRoles: 3, GrantedToRoles: 1, Owner: "Owner1"},
		{Name: "Role2", AssignedToUsers: 3, GrantedRoles: 2, GrantedToRoles: 1, Owner: "Owner2"},
		{Name: "Role3", AssignedToUsers: 1, GrantedRoles: 1, GrantedToRoles: 1, Owner: "ExternalOwner1"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsOfAccountRole("Role1").Return([]GrantOfRole{
		{GrantedTo: "USER", GranteeName: "GranteeRole1Number1"},
		{GrantedTo: "ROLE", GranteeName: "GranteeRole1Number2"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsToAccountRole("Role1").Return([]GrantToRole{
		{GrantedOn: "SCHEMA", Name: "Share2.GranteeRole1Schema", Privilege: "USAGE"},
		{GrantedOn: "SCHEMA", Name: "Share2.GranteeRole1Schema", Privilege: "READ"},
		{GrantedOn: "TABLE", Name: "DB1.GranteeRole1Table", Privilege: "USAGE"},
		{GrantedOn: "TABLE", Name: "DB1.GranteeRole1Table", Privilege: "SELECT"},
		{GrantedOn: "MATERIALIZED_VIEW", Name: "DB1.GranteeRole1MatView", Privilege: "SELECT"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsOfAccountRole("Role2").Return([]GrantOfRole{
		{GrantedTo: "USER", GranteeName: "GranteeRole2"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsToAccountRole("Role2").Return([]GrantToRole{
		{GrantedOn: "GrandOnRole2Number1", Name: "GranteeRole2", Privilege: "USAGE"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsOfAccountRole("Role3").Return([]GrantOfRole{
		{GrantedTo: "ROLE", GranteeName: "\"GranteeRole.3\""},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsToAccountRole("Role3").Return([]GrantToRole{
		{GrantedOn: "GrandOnRole3Number1", Name: "GranteeRole3", Privilege: "WRITE"},
	}, nil).Once()

	repoMock.EXPECT().GetGrantsOfDatabaseRole("TEST_DB", "DatabaseRole1").Return([]GrantOfRole{
		{GrantedTo: "ROLE", GranteeName: "GranteeDatabaseRole1Number2"},
		{GrantedTo: "DATABASE_ROLE", GranteeName: "TEST_DB.DatabaseRole2"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsToDatabaseRole("TEST_DB", "DatabaseRole1").Return([]GrantToRole{
		{GrantedOn: "TABLE", Name: "TEST_DB.GranteeRole1Table", Privilege: "USAGE"},
		{GrantedOn: "TABLE", Name: "TEST_DB.GranteeRole1Table", Privilege: "SELECT"},
		{GrantedOn: "MATERIALIZED_VIEW", Name: "TEST_DB.GranteeRole1MatView", Privilege: "SELECT"},
	}, nil).Once()

	repoMock.EXPECT().GetGrantsOfDatabaseRole("TEST_DB", "DatabaseRole2").Return([]GrantOfRole{}, nil).Once()
	repoMock.EXPECT().GetGrantsToDatabaseRole("TEST_DB", "DatabaseRole2").Return([]GrantToRole{
		{GrantedOn: "GrandOnDatabaseRole2Number1", Name: "GranteeDatabaseRole2", Privilege: "USAGE"},
	}, nil).Once()

	repoMock.EXPECT().GetGrantsOfDatabaseRole("TEST_DB", "DatabaseRole3").Return([]GrantOfRole{}, nil).Once()
	repoMock.EXPECT().GetGrantsToDatabaseRole("TEST_DB", "DatabaseRole3").Return([]GrantToRole{
		{GrantedOn: "MATERIALIZED_VIEW", Name: "TEST_DB.GranteeRole3MatView", Privilege: "SELECT"},
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

	syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
		return repoMock, nil
	})

	//When
	err := syncer.SyncAccessProvidersFromTarget(context.Background(), fileCreator, &configParams)

	//Then
	assert.NoError(t, err)
	assert.ElementsMatch(t, []sync_from_target.AccessProvider{
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
			Type:       ptr.String(access_provider.Role),
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
			Type:              ptr.String(access_provider.Role),
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
			Type:              ptr.String(access_provider.Role),
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
			ExternalId:        "DATABASEROLE###DATABASE:TEST_DB###ROLE:DatabaseRole1",
			NotInternalizable: false,
			Name:              "TEST_DB.DatabaseRole1",
			NamingHint:        "DatabaseRole1",
			ActualName:        "DatabaseRole1",
			Who: &sync_from_target.WhoItem{
				Users:           []string{},
				Groups:          []string{},
				AccessProviders: []string{"GranteeDatabaseRole1Number2", "DATABASEROLE###DATABASE:TEST_DB###ROLE:DatabaseRole2"},
			},
			What: []sync_from_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "TEST_DB.GranteeRole1Table",
						Type:     "",
					},
					Permissions: []string{"SELECT"},
				},
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "TEST_DB.GranteeRole1MatView",
						Type:     "",
					},
					Permissions: []string{"SELECT"},
				},
			},
			Action:           sync_from_target.Grant,
			Policy:           "",
			Type:             ptr.String("databaseRole"),
			WhoLocked:        ptr.Bool(true),
			WhoLockedReason:  ptr.String("The 'who' for this Snowflake role cannot be changed because we currently do not support database role changes"),
			WhatLocked:       ptr.Bool(true),
			WhatLockedReason: ptr.String("The 'what' for this Snowflake role cannot be changed because we currently do not support database role changes"),
		}, {
			ExternalId:        "DATABASEROLE###DATABASE:TEST_DB###ROLE:DatabaseRole2",
			NotInternalizable: false,
			Name:              "TEST_DB.DatabaseRole2",
			NamingHint:        "DatabaseRole2",
			ActualName:        "DatabaseRole2",
			Who: &sync_from_target.WhoItem{
				Users:           []string{},
				Groups:          []string{},
				AccessProviders: []string{},
			},
			What:             []sync_from_target.WhatItem{},
			Action:           1,
			Policy:           "",
			Type:             ptr.String("databaseRole"),
			WhoLocked:        ptr.Bool(true),
			WhoLockedReason:  ptr.String("The 'who' for this Snowflake role cannot be changed because we currently do not support database role changes"),
			WhatLocked:       ptr.Bool(true),
			WhatLockedReason: ptr.String("The 'what' for this Snowflake role cannot be changed because we currently do not support database role changes"),
		}, {
			ExternalId:        "DATABASEROLE###DATABASE:TEST_DB###ROLE:DatabaseRole3",
			NotInternalizable: false,
			Name:              "TEST_DB.DatabaseRole3",
			NamingHint:        "DatabaseRole3",
			ActualName:        "DatabaseRole3",
			Who: &sync_from_target.WhoItem{
				Users:           []string{},
				Groups:          []string{},
				AccessProviders: []string{},
			},
			What: []sync_from_target.WhatItem{

				{
					DataObject: &data_source.DataObjectReference{
						FullName: "TEST_DB.GranteeRole3MatView",
						Type:     "",
					},
					Permissions: []string{"SELECT"},
				},
			},
			Action:           1,
			Policy:           "",
			Type:             ptr.String("databaseRole"),
			WhoLocked:        ptr.Bool(true),
			WhoLockedReason:  ptr.String("The 'who' for this Snowflake role cannot be changed because we currently do not support database role changes"),
			WhatLocked:       ptr.Bool(true),
			WhatLockedReason: ptr.String("The 'what' for this Snowflake role cannot be changed because we currently do not support database role changes"),
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
	fileCreator := mocks.NewSimpleAccessProviderHandler(t, 2)

	repoMock.EXPECT().Close().Return(nil).Once()
	repoMock.EXPECT().TotalQueryTime().Return(time.Minute).Once()
	repoMock.EXPECT().GetShares().Return([]DbEntity{
		{Name: "Share1"}, {Name: "Share2"},
	}, nil).Once()
	repoMock.EXPECT().GetAccountRoles().Return([]RoleEntity{
		{Name: "Role1", AssignedToUsers: 2, GrantedRoles: 3, GrantedToRoles: 1, Owner: "Owner1"},
		{Name: "Role3", AssignedToUsers: 1, GrantedRoles: 1, GrantedToRoles: 1, Owner: "ExternalOwner1"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsOfAccountRole("Role1").Return([]GrantOfRole{
		{GrantedTo: "USER", GranteeName: "GranteeRole1Number1"},
		{GrantedTo: "ROLE", GranteeName: "GranteeRole1Number2"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsToAccountRole("Role1").Return([]GrantToRole{
		{GrantedOn: "SCHEMA", Name: "Share2.GranteeRole1Schema", Privilege: "USAGE"},
		{GrantedOn: "SCHEMA", Name: "Share2.GranteeRole1Schema", Privilege: "READ"},
		{GrantedOn: "TABLE", Name: "DB1.GranteeRole1Table", Privilege: "USAGE"},
		{GrantedOn: "TABLE", Name: "DB1.GranteeRole1Table", Privilege: "SELECT"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsToAccountRole("Role3").Return([]GrantToRole{
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

	syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
		return repoMock, nil
	})

	//When
	err := syncer.SyncAccessProvidersFromTarget(context.Background(), fileCreator, &configParams)

	//Then
	assert.NoError(t, err)
	assert.ElementsMatch(t, []sync_from_target.AccessProvider{
		{
			ExternalId:        "Role1",
			Type:              ptr.String(access_provider.Role),
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
			Type:                    ptr.String(access_provider.Role),
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
	fileCreator := mocks.NewSimpleAccessProviderHandler(t, 3)

	repoMock.EXPECT().Close().Return(nil).Once()
	repoMock.EXPECT().TotalQueryTime().Return(time.Minute).Once()
	repoMock.EXPECT().GetShares().Return([]DbEntity{
		{Name: "Share1"}, {Name: "Share2"},
	}, nil).Once()

	repoMock.EXPECT().GetAccountRoles().Return([]RoleEntity{
		{Name: "Role1", AssignedToUsers: 2, GrantedRoles: 3, GrantedToRoles: 1, Owner: "Owner1"},
		{Name: "Role2", AssignedToUsers: 3, GrantedRoles: 2, GrantedToRoles: 1, Owner: "Owner2"},
		{Name: "Role3", AssignedToUsers: 1, GrantedRoles: 1, GrantedToRoles: 1, Owner: "ExternalOwner2"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsOfAccountRole("Role1").Return([]GrantOfRole{
		{GrantedTo: "USER", GranteeName: "GranteeRole1Number1"},
		{GrantedTo: "ROLE", GranteeName: "GranteeRole1Number2"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsToAccountRole("Role1").Return([]GrantToRole{
		{GrantedOn: "SCHEMA", Name: "Share2.GranteeRole1Schema", Privilege: "USAGE"},
		{GrantedOn: "SCHEMA", Name: "Share2.GranteeRole1Schema", Privilege: "READ"},
		{GrantedOn: "TABLE", Name: "DB1.GranteeRole1Table", Privilege: "USAGE"},
		{GrantedOn: "TABLE", Name: "DB1.GranteeRole1Table", Privilege: "SELECT"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsOfAccountRole("Role2").Return([]GrantOfRole{
		{GrantedTo: "USER", GranteeName: "GranteeRole2"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsToAccountRole("Role2").Return([]GrantToRole{
		{GrantedOn: "GrandOnRole2Number1", Name: "GranteeRole2", Privilege: "USAGE"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsOfAccountRole("Role3").Return([]GrantOfRole{
		{GrantedTo: "ROLE", GranteeName: "GranteeRole3"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsToAccountRole("Role3").Return([]GrantToRole{
		{GrantedOn: "GrandOnRole3Number1", Name: "GranteeRole3", Privilege: "WRITE"},
	}, nil).Once()

	syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
		return repoMock, nil
	})

	//When
	err := syncer.SyncAccessProvidersFromTarget(context.Background(), fileCreator, &configParams)

	//Then
	assert.NoError(t, err)
	assert.ElementsMatch(t, []sync_from_target.AccessProvider{
		{
			ExternalId:        "Role1",
			Type:              ptr.String(access_provider.Role),
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
			Type:              ptr.String(access_provider.Role),
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
			Type:              ptr.String(access_provider.Role),
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

func TestAccessSyncer_SyncAccessProvidersFromTarget_Excludes(t *testing.T) {
	//Given
	configParams := config.ConfigMap{
		Parameters: map[string]string{SfExcludedRoles: "Role1,TEST_DB.DatabaseRole1", SfDatabaseRoles: "true", SfExcludedDatabases: "SNOWFLAKE,OTHER_DB"},
	}

	repoMock := newMockDataAccessRepository(t)
	fileCreator := mocks.NewSimpleAccessProviderHandler(t, 2)

	repoMock.EXPECT().Close().Return(nil).Once()
	repoMock.EXPECT().TotalQueryTime().Return(time.Minute).Once()
	repoMock.EXPECT().GetShares().Return([]DbEntity{}, nil).Once()

	repoMock.EXPECT().GetDatabases().Return([]DbEntity{
		{Name: "SNOWFLAKE"},
		{Name: "OTHER_DB"},
		{Name: "TEST_DB"},
	}, nil).Once()
	repoMock.EXPECT().GetDatabaseRoles("TEST_DB").Return([]RoleEntity{
		{Name: "DatabaseRole1", AssignedToUsers: 0, GrantedRoles: 0, GrantedToRoles: 1, Owner: "Owner1"},
		{Name: "DatabaseRole2", AssignedToUsers: 0, GrantedRoles: 1, GrantedToRoles: 0, Owner: "Owner2"},
	}, nil).Once()

	repoMock.EXPECT().GetAccountRoles().Return([]RoleEntity{
		{Name: "Role1", AssignedToUsers: 2, GrantedRoles: 3, GrantedToRoles: 1, Owner: "Owner1"},
		{Name: "Role2", AssignedToUsers: 3, GrantedRoles: 2, GrantedToRoles: 1, Owner: "Owner2"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsOfAccountRole("Role2").Return([]GrantOfRole{}, nil).Once()
	repoMock.EXPECT().GetGrantsToAccountRole("Role2").Return([]GrantToRole{}, nil).Once()

	repoMock.EXPECT().GetGrantsOfDatabaseRole("TEST_DB", "DatabaseRole2").Return([]GrantOfRole{}, nil).Once()
	repoMock.EXPECT().GetGrantsToDatabaseRole("TEST_DB", "DatabaseRole2").Return([]GrantToRole{}, nil).Once()

	repoMock.EXPECT().GetPolicies("MASKING").Return([]PolicyEntity{}, nil).Once()
	repoMock.EXPECT().GetPolicies("ROW ACCESS").Return([]PolicyEntity{}, nil).Once()

	syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
		return repoMock, nil
	})

	//When
	err := syncer.SyncAccessProvidersFromTarget(context.Background(), fileCreator, &configParams)

	//Then
	assert.NoError(t, err)
	assert.ElementsMatch(t, []sync_from_target.AccessProvider{
		{
			ExternalId:        "Role2",
			Type:              ptr.String(access_provider.Role),
			NotInternalizable: false,
			Name:              "Role2",
			NamingHint:        "Role2",
			Who: &sync_from_target.WhoItem{
				Users:           []string{},
				Groups:          []string{},
				AccessProviders: []string{},
			},
			ActualName: "Role2",
			What:       []sync_from_target.WhatItem{},
			Action:     1,
			Policy:     "",
		}, {
			ExternalId:        "DATABASEROLE###DATABASE:TEST_DB###ROLE:DatabaseRole2",
			Type:              ptr.String("databaseRole"),
			NotInternalizable: false,
			Name:              "TEST_DB.DatabaseRole2",
			NamingHint:        "DatabaseRole2",
			ActualName:        "DatabaseRole2",
			Who: &sync_from_target.WhoItem{
				Users:           []string{},
				Groups:          []string{},
				AccessProviders: []string{},
			},
			What:             []sync_from_target.WhatItem{},
			Action:           1,
			Policy:           "",
			WhoLocked:        ptr.Bool(true),
			WhoLockedReason:  ptr.String("The 'who' for this Snowflake role cannot be changed because we currently do not support database role changes"),
			WhatLocked:       ptr.Bool(true),
			WhatLockedReason: ptr.String("The 'what' for this Snowflake role cannot be changed because we currently do not support database role changes"),
		},
	}, fileCreator.AccessProviders)
}

func TestAccessSyncer_SyncAccessProvidersFromTarget_ErrorOnConnectingToRepo(t *testing.T) {
	//Given
	configParams := config.ConfigMap{
		Parameters: map[string]string{"key": "value"},
	}

	fileCreator := mocks.NewSimpleAccessProviderHandler(t, 1)

	syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
		return nil, fmt.Errorf("boom")
	})

	//When
	err := syncer.SyncAccessProvidersFromTarget(context.Background(), fileCreator, &configParams)

	//Then
	assert.Error(t, err)
}

func TestAccessSyncer_SyncAccessProvidersFromTarget_ErrorOnShares(t *testing.T) {
	//Given
	configParams := config.ConfigMap{
		Parameters: map[string]string{"key": "value"},
	}

	repoMock := newMockDataAccessRepository(t)
	fileCreator := mocks.NewSimpleAccessProviderHandler(t, 1)

	repoMock.EXPECT().Close().Return(nil).Once()
	repoMock.EXPECT().TotalQueryTime().Return(time.Minute).Once()
	repoMock.EXPECT().GetShares().Return(nil, fmt.Errorf("boom")).Once()
	syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
		return repoMock, nil
	})

	//When
	err := syncer.SyncAccessProvidersFromTarget(context.Background(), fileCreator, &configParams)

	//Then
	assert.Error(t, err)
}

func TestAccessSyncer_SyncAccessProviderToTarget(t *testing.T) {
	type fields struct {
		setup             func(repoMock *mockDataAccessRepository, feedbackHandlerMock *mocks.SimpleAccessProviderFeedbackHandler)
		repoProviderError error
	}
	type args struct {
		configMap       *config.ConfigMap
		accessProviders *sync_to_target.AccessProviderImport
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "basic - grants only",
			fields: fields{
				setup: func(repoMock *mockDataAccessRepository, feedbackHandlerMock *mocks.SimpleAccessProviderFeedbackHandler) {
					repoMock.EXPECT().Close().Return(nil).Once()
					repoMock.EXPECT().TotalQueryTime().Return(time.Minute).Once()
					repoMock.EXPECT().GetAccountRolesWithPrefix("").Return([]RoleEntity{}, nil).Once()

					repoMock.EXPECT().CreateAccountRole("ACCESS_PROVIDER1").Return(nil).Once()
					repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "ACCESS_PROVIDER1").Return(nil).Once()
					expectGrantUsersToRole(repoMock, "ACCESS_PROVIDER1", "User1", "User2")
					repoMock.EXPECT().GrantAccountRolesToAccountRole(mock.Anything, "ACCESS_PROVIDER1").Return(nil).Once()

					repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "DATABASE DB1", "ACCESS_PROVIDER1").Return(nil).Once()
					repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "SCHEMA DB1.Schema1", "ACCESS_PROVIDER1").Return(nil).Once()
					repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "TABLE DB1.Schema1.Table1", "ACCESS_PROVIDER1").Return(nil).Once()

					repoMock.EXPECT().CreateDatabaseRole("TEST_DB", "DATABASE_ROLE1").Return(nil).Once()
					repoMock.EXPECT().CommentDatabaseRoleIfExists(mock.Anything, "TEST_DB", "DATABASE_ROLE1").Return(nil).Once()
					expectGrantAccountOrDatabaseRolesToDatabaseRole(repoMock, false, "TEST_DB", "DATABASE_ROLE1", "AccessProviderId1")
					expectGrantAccountOrDatabaseRolesToDatabaseRole(repoMock, true, "TEST_DB", "DATABASE_ROLE1")

					repoMock.EXPECT().ExecuteGrantOnDatabaseRole("USAGE", "DATABASE TEST_DB", "TEST_DB", "DATABASE_ROLE1").Return(nil).Once()
					repoMock.EXPECT().ExecuteGrantOnDatabaseRole("USAGE", "SCHEMA TEST_DB.Schema1", "TEST_DB", "DATABASE_ROLE1").Return(nil).Once()
					repoMock.EXPECT().ExecuteGrantOnDatabaseRole("SELECT", "TABLE TEST_DB.Schema1.Table1", "TEST_DB", "DATABASE_ROLE1").Return(nil).Once()
				},
			},
			args: args{
				accessProviders: &sync_to_target.AccessProviderImport{
					AccessProviders: []*importer.AccessProvider{
						{
							Id:         "AccessProviderId1",
							Action:     importer.Grant,
							Type:       ptr.String(access_provider.Role),
							Name:       "AccessProvider1",
							NamingHint: "AccessProvider1",
							Who: importer.WhoItem{
								Users: []string{"User1", "User2"},
							},
							What: []importer.WhatItem{
								{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table1", Type: "table"}, Permissions: []string{"SELECT"}},
							},
						}, {
							Id:         "AccessProviderId2",
							Action:     importer.Grant,
							Type:       ptr.String("databaseRole"),
							NamingHint: "TEST_DB.DatabaseRole1",
							Who: importer.WhoItem{
								InheritFrom: []string{"AccessProviderId1"},
							},
							What: []importer.WhatItem{
								{DataObject: &data_source.DataObjectReference{FullName: "TEST_DB.Schema1.Table1", Type: "table"}, Permissions: []string{"SELECT"}},
							},
						},
					},
				},
				configMap: &config.ConfigMap{
					Parameters: map[string]string{},
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "basic - renaming grants",
			fields: fields{
				setup: func(repoMock *mockDataAccessRepository, feedbackHandlerMock *mocks.SimpleAccessProviderFeedbackHandler) {
					repoMock.EXPECT().Close().Return(nil).Once()
					repoMock.EXPECT().TotalQueryTime().Return(time.Minute).Once()
					repoMock.EXPECT().GetAccountRolesWithPrefix("").Return([]RoleEntity{
						{Name: "ACCESS_PROVIDER1_OLD"},
						{Name: "DATABASEROLE###DATABASE:TEST_DB###ROLE:DATABASE_ROLE1_OLD"},
					}, nil).Once()

					repoMock.EXPECT().RenameAccountRole("ACCESS_PROVIDER1_OLD", "ACCESS_PROVIDER1").Return(nil).Once()
					repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "ACCESS_PROVIDER1").Return(nil).Once()
					repoMock.EXPECT().GetGrantsOfAccountRole("ACCESS_PROVIDER1").Return([]GrantOfRole{}, nil).Once()
					repoMock.EXPECT().GetGrantsToAccountRole("ACCESS_PROVIDER1").Return([]GrantToRole{}, nil).Once()

					expectGrantUsersToRole(repoMock, "ACCESS_PROVIDER1", "User1", "User2")
					repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "DATABASE DB1", "ACCESS_PROVIDER1").Return(nil).Once()
					repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "SCHEMA DB1.Schema1", "ACCESS_PROVIDER1").Return(nil).Once()
					repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "TABLE DB1.Schema1.Table1", "ACCESS_PROVIDER1").Return(nil).Once()

					repoMock.EXPECT().RenameDatabaseRole("TEST_DB", "DATABASE_ROLE1_OLD", "DATABASE_ROLE1").Return(nil).Once()
					repoMock.EXPECT().CommentDatabaseRoleIfExists(mock.Anything, "TEST_DB", "DATABASE_ROLE1").Return(nil).Once()
					repoMock.EXPECT().GetGrantsOfDatabaseRole("TEST_DB", "DATABASE_ROLE1").Return([]GrantOfRole{}, nil).Once()
					repoMock.EXPECT().GetGrantsToDatabaseRole("TEST_DB", "DATABASE_ROLE1").Return([]GrantToRole{}, nil).Once()

					expectGrantAccountOrDatabaseRolesToDatabaseRole(repoMock, false, "TEST_DB", "DATABASE_ROLE1", "AccessProviderId1")
					expectGrantAccountOrDatabaseRolesToDatabaseRole(repoMock, true, "TEST_DB", "DATABASE_ROLE1")

					repoMock.EXPECT().ExecuteGrantOnDatabaseRole("USAGE", "DATABASE TEST_DB", "TEST_DB", "DATABASE_ROLE1").Return(nil).Once()
					repoMock.EXPECT().ExecuteGrantOnDatabaseRole("USAGE", "SCHEMA TEST_DB.Schema1", "TEST_DB", "DATABASE_ROLE1").Return(nil).Once()
					repoMock.EXPECT().ExecuteGrantOnDatabaseRole("SELECT", "TABLE TEST_DB.Schema1.Table1", "TEST_DB", "DATABASE_ROLE1").Return(nil).Once()
				},
			},
			args: args{
				accessProviders: &sync_to_target.AccessProviderImport{
					AccessProviders: []*importer.AccessProvider{
						{
							Id:         "AccessProviderId1",
							ExternalId: ptr.String("ACCESS_PROVIDER1_OLD"),
							Action:     importer.Grant,
							Type:       ptr.String(access_provider.Role),
							Name:       "AccessProvider1",
							NamingHint: "AccessProvider1",
							Who: importer.WhoItem{
								Users: []string{"User1", "User2"},
							},
							What: []importer.WhatItem{
								{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table1", Type: "table"}, Permissions: []string{"SELECT"}},
							},
						}, {
							Id:         "AccessProviderId2",
							Action:     importer.Grant,
							ExternalId: ptr.String("DATABASEROLE###DATABASE:TEST_DB###ROLE:DATABASE_ROLE1_OLD"),
							Type:       ptr.String("databaseRole"),
							NamingHint: "TEST_DB.DatabaseRole1",
							Who: importer.WhoItem{
								InheritFrom: []string{"AccessProviderId1"},
							},
							What: []importer.WhatItem{
								{DataObject: &data_source.DataObjectReference{FullName: "TEST_DB.Schema1.Table1", Type: "table"}, Permissions: []string{"SELECT"}},
							},
						},
					},
				},
				configMap: &config.ConfigMap{
					Parameters: map[string]string{},
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "basic - masks + filters on basic SF",
			fields: fields{
				setup: func(repoMock *mockDataAccessRepository, feedbackHandlerMock *mocks.SimpleAccessProviderFeedbackHandler) {
					repoMock.EXPECT().Close().Return(nil).Once()
					repoMock.EXPECT().TotalQueryTime().Return(time.Minute).Once()
					repoMock.EXPECT().GetAccountRolesWithPrefix("").Return([]RoleEntity{}, nil).Once()
				},
			},
			args: args{
				accessProviders: &sync_to_target.AccessProviderImport{
					AccessProviders: []*importer.AccessProvider{
						{
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
							Type:   ptr.String("SHA256"),
						}, {
							Id:     "RAITO_FILTER1",
							Name:   "RAITO_FILTER1",
							Action: importer.Filtered,
							What: []importer.WhatItem{
								{
									DataObject: &data_source.DataObjectReference{
										FullName: "DB1.Schema1.Table1",
										Type:     data_source.Table,
									},
								},
							},
							Who: importer.WhoItem{
								Users:       []string{"User1", "User2"},
								InheritFrom: []string{"Role1"},
							},
							PolicyRule: ptr.String("{state} = 'NJ'"),
						},
					},
				},
				configMap: &config.ConfigMap{
					Parameters: map[string]string{
						SfStandardEdition: "true",
					},
				},
			},
			wantErr: require.NoError,
		},

		{
			name: "repo connection error",
			fields: fields{
				setup: func(repoMock *mockDataAccessRepository, feedbackHandlerMock *mocks.SimpleAccessProviderFeedbackHandler) {

				},
				repoProviderError: fmt.Errorf("boom"),
			},
			args: args{
				accessProviders: &sync_to_target.AccessProviderImport{
					AccessProviders: []*importer.AccessProvider{},
				},
				configMap: &config.ConfigMap{
					Parameters: map[string]string{
						SfStandardEdition: "false",
					},
				},
			},
			wantErr: require.Error,
		},

		{
			name: "basic - masks + filters on non-basic SF",
			fields: fields{
				setup: func(repoMock *mockDataAccessRepository, feedbackHandlerMock *mocks.SimpleAccessProviderFeedbackHandler) {
					repoMock.EXPECT().Close().Return(nil).Once()
					repoMock.EXPECT().TotalQueryTime().Return(time.Minute).Once()
					repoMock.EXPECT().GetAccountRolesWithPrefix("").Return([]RoleEntity{}, nil).Once()

					repoMock.EXPECT().GetPoliciesLike("MASKING", "RAITO_MASK1%").Return(nil, nil).Once() //No existing masks
					repoMock.EXPECT().CreateMaskPolicy("DB1", "Schema1", mock.AnythingOfType("string"), []string{"DB1.Schema1.Table1.Column1"}, ptr.String("SHA256"), &MaskingBeneficiaries{Users: []string{"User1", "User2"}, Roles: []string{"Role1"}}).Return(nil)
					repoMock.EXPECT().CreateMaskPolicy("DB1", "Schema2", mock.AnythingOfType("string"), []string{"DB1.Schema2.Table1.Column1"}, ptr.String("SHA256"), &MaskingBeneficiaries{Users: []string{"User1", "User2"}, Roles: []string{"Role1"}}).Return(nil)
					repoMock.EXPECT().UpdateFilter("DB1", "Schema1", "Table1", mock.AnythingOfType("string"), mock.AnythingOfType("[]string"),
						mock.AnythingOfType("string")).RunAndReturn(func(_ string, _ string, _ string, filterName string, arguments []string, query string) error {
						assert.True(t, strings.HasPrefix(filterName, "raito_Schema1_Table1_"))
						assert.ElementsMatch(t, []string{"state"}, arguments)

						assert.Equal(t, "(current_user() IN ('User1', 'User2') OR current_role() IN ('Role1')) AND (state = 'NJ')", query)

						return nil
					})

				},
			},
			args: args{
				accessProviders: &sync_to_target.AccessProviderImport{
					AccessProviders: []*importer.AccessProvider{
						{
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
							Type:   ptr.String("SHA256"),
						}, {
							Id:     "RAITO_FILTER1",
							Name:   "RAITO_FILTER1",
							Action: importer.Filtered,
							What: []importer.WhatItem{
								{
									DataObject: &data_source.DataObjectReference{
										FullName: "DB1.Schema1.Table1",
										Type:     data_source.Table,
									},
								},
							},
							Who: importer.WhoItem{
								Users:       []string{"User1", "User2"},
								InheritFrom: []string{"Role1"},
							},
							PolicyRule: ptr.String("{state} = 'NJ'"),
						},
					},
				},
				configMap: &config.ConfigMap{
					Parameters: map[string]string{
						SfStandardEdition: "false",
					},
				},
			},
			wantErr: require.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Given
			repoMock := newMockDataAccessRepository(t)
			feedbackHandler := mocks.NewSimpleAccessProviderFeedbackHandler(t)

			tt.fields.setup(repoMock, feedbackHandler)

			syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
				return repoMock, tt.fields.repoProviderError
			})

			// When
			err := syncer.SyncAccessProviderToTarget(context.Background(), tt.args.accessProviders, feedbackHandler, tt.args.configMap)

			// Then
			tt.wantErr(t, err)
		})
	}
}

func TestAccessSyncer_SyncAccessAsCodeToTarget(t *testing.T) {
	//Given
	configParams := config.ConfigMap{
		Parameters: map[string]string{"key": "value", SfDatabaseRoles: "true"},
	}

	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().Close().Return(nil).Once()
	repoMock.EXPECT().TotalQueryTime().Return(time.Minute).Once()
	repoMock.EXPECT().DropAccountRole("R_ToRemove1").Return(nil).Once()
	repoMock.EXPECT().DropAccountRole("R_ToRemove2").Return(nil).Once()
	repoMock.EXPECT().DropDatabaseRole("TEST_DB", "R_ToRemoveDatabaseRole1").Return(nil).Once()
	repoMock.EXPECT().DropDatabaseRole("TEST_DB", "R_ToRemoveDatabaseRole2").Return(nil).Once()
	repoMock.EXPECT().GetAccountRolesWithPrefix("R_").Return([]RoleEntity{
		{Name: "R_ToRemove1", GrantedToRoles: 2, GrantedRoles: 3, AssignedToUsers: 2, Owner: "Owner"},
		{Name: "R_ToRemove2", GrantedToRoles: 2, GrantedRoles: 3, AssignedToUsers: 2, Owner: "Owner"},
	}, nil).Once()

	repoMock.EXPECT().GetDatabases().Return([]DbEntity{
		{Name: "TEST_DB"},
	}, nil).Once()
	repoMock.EXPECT().GetDatabaseRolesWithPrefix("TEST_DB", "R_").Return([]RoleEntity{
		{Name: "R_ToRemoveDatabaseRole1", AssignedToUsers: 0, GrantedRoles: 0, GrantedToRoles: 1, Owner: "Owner"},
		{Name: "R_ToRemoveDatabaseRole2", AssignedToUsers: 0, GrantedRoles: 1, GrantedToRoles: 0, Owner: "Owner"},
	}, nil).Once()

	repoMock.EXPECT().CreateAccountRole("R_ACCESS_PROVIDER1").Return(nil).Once()
	repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "R_ACCESS_PROVIDER1").Return(nil).Once()
	expectGrantUsersToRole(repoMock, "R_ACCESS_PROVIDER1", "User1", "User2")
	repoMock.EXPECT().GrantAccountRolesToAccountRole(mock.Anything, "R_ACCESS_PROVIDER1").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "DATABASE DB1", "R_ACCESS_PROVIDER1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "SCHEMA DB1.Schema1", "R_ACCESS_PROVIDER1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "TABLE DB1.Schema1.Table1", "R_ACCESS_PROVIDER1").Return(nil).Once()

	repoMock.EXPECT().CreateDatabaseRole("TEST_DB", "R_DATABASE_ROLE1").Return(nil).Once()
	repoMock.EXPECT().CommentDatabaseRoleIfExists(mock.Anything, "TEST_DB", "R_DATABASE_ROLE1").Return(nil).Once()
	expectGrantAccountOrDatabaseRolesToDatabaseRole(repoMock, false, "TEST_DB", "R_DATABASE_ROLE1", "AccessProviderId1")
	expectGrantAccountOrDatabaseRolesToDatabaseRole(repoMock, true, "TEST_DB", "R_DATABASE_ROLE1")

	repoMock.EXPECT().ExecuteGrantOnDatabaseRole("USAGE", "DATABASE TEST_DB", "TEST_DB", "R_DATABASE_ROLE1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnDatabaseRole("USAGE", "SCHEMA TEST_DB.Schema1", "TEST_DB", "R_DATABASE_ROLE1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnDatabaseRole("SELECT", "TABLE TEST_DB.Schema1.Table1", "TEST_DB", "R_DATABASE_ROLE1").Return(nil).Once()

	syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
		return repoMock, nil
	})

	apImport := &sync_to_target.AccessProviderImport{
		AccessProviders: []*importer.AccessProvider{
			{
				Id:         "AccessProviderId1",
				Type:       ptr.String(access_provider.Role),
				Name:       "AccessProvider1",
				NamingHint: "AccessProvider1",
				Who: importer.WhoItem{
					Users: []string{"User1", "User2"},
				},
				What: []importer.WhatItem{
					{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table1", Type: "table"}, Permissions: []string{"SELECT"}},
				},
			}, {
				Id:         "AccessProviderId2",
				Type:       ptr.String("databaseRole"),
				NamingHint: "TEST_DB.DatabaseRole1",
				Who: importer.WhoItem{
					InheritFrom: []string{"AccessProviderId1"},
				},
				What: []importer.WhatItem{
					{DataObject: &data_source.DataObjectReference{FullName: "TEST_DB.Schema1.Table1", Type: "table"}, Permissions: []string{"SELECT"}},
				},
			},
		},
	}

	//When
	err := syncer.SyncAccessAsCodeToTarget(context.Background(), apImport, "R_", &configParams)

	//Then
	assert.NoError(t, err)
}

func TestAccessSyncer_SyncAccessAsCodeToTarget_ErrorOnRepoConnection(t *testing.T) {
	//Given
	configParams := config.ConfigMap{
		Parameters: map[string]string{"key": "value"},
	}

	syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
		return nil, fmt.Errorf("boom")
	})

	apImport := &sync_to_target.AccessProviderImport{
		AccessProviders: []*importer.AccessProvider{
			{
				Id:   "AccessProviderId1",
				Name: "AccessProvider1",
				Who: importer.WhoItem{
					Users: []string{"User1", "User2"},
				},
				What: []importer.WhatItem{
					{DataObject: &data_source.DataObjectReference{FullName: "DB1.Schema1.Table1", Type: "TABLE"}, Permissions: []string{"SELECT"}},
				},
			},
		},
	}

	//When
	err := syncer.SyncAccessAsCodeToTarget(context.Background(), apImport, "R_", &configParams)

	//Then
	assert.Error(t, err)
}
