package snowflake

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/smithy-go/ptr"
	"github.com/raito-io/bexpression"
	"github.com/raito-io/bexpression/datacomparison"
	"github.com/raito-io/cli/base/access_provider"
	"github.com/raito-io/cli/base/access_provider/sync_from_target"
	importer "github.com/raito-io/cli/base/access_provider/sync_to_target"
	"github.com/raito-io/cli/base/access_provider/sync_to_target/naming_hint"
	"github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers/mocks"
	"github.com/raito-io/golang-set/set"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestAccessSyncer_SyncAccessProviderRolesToTarget(t *testing.T) {
	//Given
	configParams := config.ConfigMap{
		Parameters: map[string]string{"key": "value", SfDatabaseRoles: "true"},
	}

	rolesToRemove := map[string]*importer.AccessProvider{
		"ToRemove1": {Id: "xxx", Type: ptr.String(access_provider.Role)},
		"ToRemove2": {Id: "yyy", Type: ptr.String(access_provider.Role)},
		"DATABASEROLE###DATABASE:TEST_DB###ROLE:ToRemoveDatabaseRole1": {Id: "zzz", Type: ptr.String("databaseRole")},
	}

	repoMock := newMockDataAccessRepository(t)
	feedbackHandler := mocks.NewSimpleAccessProviderFeedbackHandler(t)

	repoMock.EXPECT().DropAccountRole("ToRemove1").Return(nil).Once()
	repoMock.EXPECT().DropAccountRole("ToRemove2").Return(nil).Once()
	repoMock.EXPECT().DropDatabaseRole("TEST_DB", "ToRemoveDatabaseRole1").Return(nil).Once()

	repoMock.EXPECT().GetAccountRolesWithPrefix("").Return([]RoleEntity{
		{Name: "ExistingRole1", GrantedToRoles: 2, GrantedRoles: 3, AssignedToUsers: 2, Owner: "Owner"},
		{Name: "ExistingRole2", GrantedToRoles: 2, GrantedRoles: 3, AssignedToUsers: 2, Owner: "Owner"},
	}, nil).Once()
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
	repoMock.EXPECT().CreateDatabaseRole("TEST_DB", "DatabaseRole2").Return(nil).Once()
	repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "RoleName1").Return(nil).Once()
	repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "RoleName3").Return(nil).Once()

	repoMock.EXPECT().CommentDatabaseRoleIfExists(mock.Anything, "TEST_DB", "DatabaseRole1").Return(nil).Once()
	repoMock.EXPECT().CommentDatabaseRoleIfExists(mock.Anything, "TEST_DB", "DatabaseRole2").Return(nil).Once()

	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")

	expectGrantAccountOrDatabaseRolesToDatabaseRole(repoMock, true, "TEST_DB", "DatabaseRole1", "DATABASEROLE###DATABASE:TEST_DB###ROLE:DatabaseRole2")
	expectGrantAccountOrDatabaseRolesToDatabaseRole(repoMock, false, "TEST_DB", "DatabaseRole1")

	expectGrantAccountOrDatabaseRolesToDatabaseRole(repoMock, true, "TEST_DB", "DatabaseRole2")
	expectGrantAccountOrDatabaseRolesToDatabaseRole(repoMock, false, "TEST_DB", "DatabaseRole2")

	repoMock.EXPECT().GrantAccountRolesToAccountRole(mock.Anything, "RoleName1").Return(nil).Once()
	repoMock.EXPECT().GrantAccountRolesToAccountRole(mock.Anything, "RoleName3").Return(nil).Once()

	repoMock.EXPECT().GetGrantsOfAccountRole("ExistingRole1").Return([]GrantOfRole{}, nil).Once()
	repoMock.EXPECT().GetGrantsToAccountRole("ExistingRole1").Return([]GrantToRole{}, nil).Once()

	repoMock.EXPECT().GetGrantsOfDatabaseRole("TEST_DB", "DatabaseRole1").Return([]GrantOfRole{}, nil).Once()
	repoMock.EXPECT().GetGrantsToDatabaseRole("TEST_DB", "DatabaseRole1").Return([]GrantToRole{}, nil).Once()

	repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "ExistingRole1").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "DATABASE DB1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "SCHEMA DB1.Schema1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "TABLE DB1.Schema1.Table1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "DATABASE DB1", "ExistingRole1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "SCHEMA DB1.Schema1", "ExistingRole1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "TABLE DB1.Schema1.Table2", "ExistingRole1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "DATABASE DB1", "RoleName3").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "SCHEMA DB1.Schema2", "RoleName3").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "TABLE DB1.Schema2.Table1", "RoleName3").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnDatabaseRole("USAGE", "DATABASE TEST_DB", "TEST_DB", "DatabaseRole1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnDatabaseRole("USAGE", "SCHEMA TEST_DB.Schema2", "TEST_DB", "DatabaseRole1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnDatabaseRole("SELECT", "TABLE TEST_DB.Schema2.Table1", "TEST_DB", "DatabaseRole1").Return(nil).Once()

	syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
		return repoMock, nil
	})

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

	access := map[string]*importer.AccessProvider{
		"RoleName1":     ap1,
		"ExistingRole1": ap2,
		"RoleName3":     ap3,
		"DATABASEROLE###DATABASE:TEST_DB###ROLE:DatabaseRole1": apDatabaseRole1,
		"DATABASEROLE###DATABASE:TEST_DB###ROLE:DatabaseRole2": apDatabaseRole2,
	}

	//When
	err := syncer.SyncAccessProviderRolesToTarget(context.Background(), rolesToRemove, access, feedbackHandler, &configParams, repoMock)

	//Then
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
		},
		{
			AccessProvider: "AccessProviderId3",
			ActualName:     "RoleName3",
			ExternalId:     ptr.String("RoleName3"),
		},
		{
			AccessProvider: "AccessProviderId1",
			ActualName:     "RoleName1",
			ExternalId:     ptr.String("RoleName1"),
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
	}, feedbackHandler.AccessProviderFeedback)

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
			Type:   ptr.String("SHA256"),
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
	repoMock.EXPECT().CreateMaskPolicy("DB1", "Schema1", mock.AnythingOfType("string"), []string{"DB1.Schema1.Table1.Column1"}, ptr.String("SHA256"), &MaskingBeneficiaries{Users: []string{"User1", "User2"}, Roles: []string{"Role1", "Role2"}}).Return(nil)
	repoMock.EXPECT().CreateMaskPolicy("DB1", "Schema2", mock.AnythingOfType("string"), []string{"DB1.Schema2.Table1.Column1"}, ptr.String("SHA256"), &MaskingBeneficiaries{Users: []string{"User1", "User2"}, Roles: []string{"Role1", "Role2"}}).Return(nil)

	repoMock.EXPECT().GetPoliciesLike("MASKING", "RAITO_MASK2%").Return([]PolicyEntity{{Name: "RAITO_MASK2_OLD_TEXT", SchemaName: "Schema1", DatabaseName: "DB1"}}, nil).Once()
	repoMock.EXPECT().CreateMaskPolicy("DB1", "Schema1", mock.AnythingOfType("string"), []string{"DB1.Schema1.Table3.Column1"}, (*string)(nil), &MaskingBeneficiaries{Users: []string{"User1"}}).Return(nil)
	repoMock.EXPECT().DropMaskingPolicy("DB1", "Schema1", "RAITO_MASK2_OLD").Return(nil)

	repoMock.EXPECT().GetPoliciesLike("MASKING", "RAITO_MASKTOREMOVE1%").Return([]PolicyEntity{{Name: "RAITO_maskToRemove1_TEXT", SchemaName: "Schema3", DatabaseName: "DB1"}, {Name: "RAITO_maskToRemove1_INT", SchemaName: "Schema1", DatabaseName: "DB1"}}, nil).Once()
	repoMock.EXPECT().DropMaskingPolicy("DB1", "Schema3", "RAITO_MASKTOREMOVE1").Return(nil)
	repoMock.EXPECT().DropMaskingPolicy("DB1", "Schema1", "RAITO_MASKTOREMOVE1").Return(nil)

	syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
		return repoMock, nil
	})

	// When
	err := syncer.SyncAccessProviderMasksToTarget(context.Background(), masksToRemove, masks, map[string]string{"Role2-Id": "Role2"}, fileCreator, &configParams, repoMock)

	// Then
	assert.NoError(t, err)
	assert.Len(t, fileCreator.AccessProviderFeedback, 3)
}

func TestAccessSyncer_SyncAccessProviderFiltersToTarget(t *testing.T) {
	// Given
	configParams := config.ConfigMap{
		Parameters: map[string]string{"key": "value"},
	}

	repo := newMockDataAccessRepository(t)
	fileCreator := mocks.NewSimpleAccessProviderFeedbackHandler(t)

	repo.EXPECT().UpdateFilter("DB1", "Schema1", "Table1", mock.AnythingOfType("string"), mock.AnythingOfType("[]string"),
		mock.AnythingOfType("string")).RunAndReturn(func(_ string, _ string, _ string, filterName string, arguments []string, query string) error {
		assert.True(t, strings.HasPrefix(filterName, "raito_Schema1_Table1_"))
		assert.ElementsMatch(t, []string{"Column1", "state"}, arguments)

		queryPart1 := "(current_user() IN ('User2') OR current_role() IN ('Role3')) AND ((100 >= Column1))"
		queryPart2 := "(current_user() IN ('User1', 'User2') OR current_role() IN ('Role1')) AND (state = 'NJ')"

		queryOption1 := fmt.Sprintf("%s OR %s", queryPart1, queryPart2)
		queryOption2 := fmt.Sprintf("%s OR %s", queryPart2, queryPart1)

		assert.True(t, query == queryOption1 || query == queryOption2)

		return nil
	})

	repo.EXPECT().UpdateFilter("DB1", "Schema2", "Table1", mock.AnythingOfType("string"), []string{}, "FALSE").RunAndReturn(func(_ string, _ string, _ string, filterName string, _ []string, _ string) error {
		assert.True(t, strings.HasPrefix(filterName, "raito_Schema2_Table1_"))

		return nil
	})

	repo.EXPECT().DropFilter("DB1", "Schema1", "Table3", "RAITO_FILTERTOREMOVE2").Return(nil)

	masksToRemove := map[string]*importer.AccessProvider{
		"RAITO_FILTERTOREMOVE1": {
			Id:         "FilterToRemove1",
			Action:     importer.Filtered,
			ActualName: ptr.String("RAITO_FILTERTOREMOVE1"),
			ExternalId: ptr.String("DB1.Schema1.RAITO_FILTERTOREMOVE1"),
			What: []importer.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "DB1.Schema1.Table1",
						Type:     data_source.Table,
					},
				},
			},
		},
		"RAITO_FILTERTOREMOVE2": {
			Id:         "FilterToRemove2",
			Action:     importer.Filtered,
			ActualName: ptr.String("RAITO_FILTERTOREMOVE2"),
			ExternalId: ptr.String("DB1.Schema1.Table3.RAITO_FILTERTOREMOVE2"),
			What: []importer.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "DB1.Schema1.Table3",
						Type:     data_source.Table,
					},
				},
			},
		},
	}

	apMap := map[string]*importer.AccessProvider{
		"RAITO_FILTER1": {
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
		"RAITO_FILTER2": {
			Id:     "RAITO_FILTER2",
			Name:   "RAITO_FILTER2",
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
				Users:       []string{"User2"},
				InheritFrom: []string{"ID:Role3-ID"},
			},
			FilterCriteria: &bexpression.DataComparisonExpression{
				Comparison: &datacomparison.DataComparison{
					Operator: datacomparison.ComparisonOperatorGreaterThanOrEqual,
					LeftOperand: datacomparison.Operand{
						Literal: &datacomparison.Literal{Int: ptr.Int(100)},
					},
					RightOperand: datacomparison.Operand{
						Reference: &datacomparison.Reference{
							EntityType: datacomparison.EntityTypeColumnReferenceByName,
							EntityID:   `Column1`,
						},
					},
				},
			},
		},
		"RAITO_FILTER3": {
			Id:     "RAITO_FILTER3",
			Name:   "RAITO_FILTER3",
			Action: importer.Filtered,
			What: []importer.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "DB1.Schema2.Table1",
						Type:     data_source.Table,
					},
				},
			},
			Who: importer.WhoItem{},
			FilterCriteria: &bexpression.DataComparisonExpression{
				Comparison: &datacomparison.DataComparison{
					Operator: datacomparison.ComparisonOperatorGreaterThanOrEqual,
					LeftOperand: datacomparison.Operand{
						Literal: &datacomparison.Literal{Int: ptr.Int(100)},
					},
					RightOperand: datacomparison.Operand{
						Reference: &datacomparison.Reference{
							EntityType: datacomparison.EntityTypeDataObject,
							EntityID:   `{"fullName":"DB1.Schema1.Table1.Column1","id":"JJGSpyjrssv94KPk9dNuI","type":"column"}`,
						},
					},
				},
			},
		},
	}

	syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
		return repo, nil
	})

	// When
	err := syncer.SyncAccessProviderFiltersToTarget(context.Background(), masksToRemove, apMap, map[string]string{"Role3-ID": "Role3"}, fileCreator, &configParams, repo)

	// Then
	require.NoError(t, err)
	assert.Len(t, fileCreator.AccessProviderFeedback, 5)
}

func TestAccessSyncer_removeRolesToRemove_NoRoles(t *testing.T) {
	//Given
	repo := newMockDataAccessRepository(t)

	syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
		return nil, nil
	})

	//When
	err := syncer.removeRolesToRemove(map[string]*importer.AccessProvider{}, repo, nil)

	//Then
	assert.NoError(t, err)
}

func TestAccessSyncer_removeRolesToRemove(t *testing.T) {
	//Given
	repo := newMockDataAccessRepository(t)

	rolesToRemove := map[string]*importer.AccessProvider{"Role1": {Id: "xxx"}, "Role2": {Id: "yyy"}, "Role3": {Id: "zzz"}}

	repo.EXPECT().DropAccountRole(mock.MatchedBy(func(roleName string) bool {
		for possibleRole := range rolesToRemove {
			if possibleRole == roleName {
				return true
			}
		}
		return false
	})).Return(nil).Times(len(rolesToRemove))

	syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
		return nil, nil
	})

	//When
	err := syncer.removeRolesToRemove(rolesToRemove, repo, &dummyFeedbackHandler{})

	//Then
	assert.NoError(t, err)
}

func TestAccessSyncer_removeRolesToRemove_error(t *testing.T) {
	//Given
	repo := newMockDataAccessRepository(t)

	rolesToRemove := map[string]*importer.AccessProvider{"Role1": {Id: "xxx"}, "Role2": {Id: "yyy"}}

	repo.EXPECT().DropAccountRole("Role1").Return(nil).Once()
	repo.EXPECT().DropAccountRole("Role2").Return(fmt.Errorf("BOOM")).Once()

	syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
		return nil, nil
	})

	feedbackHandler := mocks.NewSimpleAccessProviderFeedbackHandler(t)

	//When
	err := syncer.removeRolesToRemove(rolesToRemove, repo, feedbackHandler)

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

	repoMock.EXPECT().DescribePolicy(policyType, "DB1", "Schema1", "Policy1").Return([]DescribePolicyEntity{
		{
			Name: "Policy1",
			Body: "PolicyBody1",
		},
	}, nil).Once()

	repoMock.EXPECT().DescribePolicy(policyType, "DB1", "Schema2", "Policy2").Return([]DescribePolicyEntity{
		{
			Name: "Policy2",
			Body: "PolicyBody2",
		},
	}, nil).Once()

	repoMock.EXPECT().GetPolicyReferences("DB1", "Schema1", "Policy1").Return([]PolicyReferenceEntity{
		{
			POLICY_STATUS:     "Active",
			REF_COLUMN_NAME:   NullString{String: "ColumnName1", Valid: true},
			REF_DATABASE_NAME: "DB1",
			REF_SCHEMA_NAME:   "Schema1",
			REF_ENTITY_NAME:   "EntityName1",
		},
	}, nil).Once()

	repoMock.EXPECT().GetPolicyReferences("DB1", "Schema2", "Policy2").Return([]PolicyReferenceEntity{
		{
			POLICY_STATUS:     "Active",
			REF_COLUMN_NAME:   NullString{Valid: false},
			REF_DATABASE_NAME: "DB1",
			REF_SCHEMA_NAME:   "Schema1",
			REF_ENTITY_NAME:   "EntityName1",
		},
	}, nil).Once()

	syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
		return nil, nil
	})

	//When
	err := syncer.importPoliciesOfType(fileCreator, repoMock, policyType, sync_from_target.Grant)

	//Then
	assert.NoError(t, err)
	assert.ElementsMatch(t, []sync_from_target.AccessProvider{
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

func generateAccessControls_table(t *testing.T) {
	//Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CreateAccountRole("RoleName1").Return(nil).Once()
	repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "RoleName1").Return(nil).Once()
	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")
	repoMock.EXPECT().GrantAccountRolesToAccountRole(mock.Anything, "RoleName1").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "DATABASE DB1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "SCHEMA DB1.Schema1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "TABLE DB1.Schema1.Table1", "RoleName1").Return(nil).Once()

	syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
		return nil, nil
	})

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
	err := syncer.generateAccessControls(context.Background(), access, set.NewSet[string](), map[string]string{}, repoMock, &config.ConfigMap{}, &dummyFeedbackHandler{})

	//Then
	assert.NoError(t, err)
}

func generateAccessControls_view(t *testing.T) {
	//Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CreateAccountRole("RoleName1").Return(nil).Once()
	repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "RoleName1").Return(nil).Once()
	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")
	repoMock.EXPECT().GrantAccountRolesToAccountRole(mock.Anything, "RoleName1").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "DATABASE DB1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "SCHEMA DB1.Schema1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "VIEW DB1.Schema1.Table2", "RoleName1").Return(nil).Once()

	syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
		return nil, nil
	})

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
	err := syncer.generateAccessControls(context.Background(), access, set.NewSet[string](), map[string]string{}, repoMock, &config.ConfigMap{}, &dummyFeedbackHandler{})

	//Then
	assert.NoError(t, err)
}

func createBasicAccessSyncer(repoProvider func(params map[string]string, role string) (dataAccessRepository, error)) AccessSyncer {
	return AccessSyncer{
		repoProvider:                  repoProvider,
		tablesPerSchemaCache:          make(map[string][]TableEntity),
		schemasPerDataBaseCache:       make(map[string][]SchemaEntity),
		uniqueRoleNameGeneratorsCache: make(map[*string]naming_hint.UniqueGenerator),
		namingConstraints:             RoleNameConstraints,
	}
}

func generateAccessControls_schema(t *testing.T) {
	//Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CreateAccountRole("RoleName1").Return(nil).Once()
	repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "RoleName1").Return(nil).Once()
	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")
	repoMock.EXPECT().GrantAccountRolesToAccountRole(mock.Anything, "RoleName1").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "DATABASE DB1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "SCHEMA DB1.Schema2", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "TABLE DB1.Schema2.Table3", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "VIEW DB1.Schema2.View3", "RoleName1").Return(nil).Once()

	database := "DB1"
	schema := "Schema2"

	repoMock.EXPECT().GetTablesInDatabase(database, schema, mock.Anything).RunAndReturn(func(s string, s2 string, handler EntityHandler) error {
		handler(&TableEntity{Database: s, Schema: s2, Name: "Table3", TableType: "BASE TABLE"})
		handler(&TableEntity{Database: s, Schema: s2, Name: "View3", TableType: "VIEW"})
		return nil
	}).Once()

	syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
		return nil, nil
	})

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
	err := syncer.generateAccessControls(context.Background(), access, set.NewSet[string](), map[string]string{}, repoMock, &config.ConfigMap{}, &dummyFeedbackHandler{})

	//Then
	assert.NoError(t, err)
}

func generateAccessControls_schema_noverify(t *testing.T) {
	//Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CreateAccountRole("RoleName1").Return(nil).Once()
	repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "RoleName1").Return(nil).Once()
	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")
	repoMock.EXPECT().GrantAccountRolesToAccountRole(mock.Anything, "RoleName1").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "DATABASE DB1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "SCHEMA DB1.Schema2", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("CREATE TABLE", "SCHEMA DB1.Schema2", "RoleName1").Return(nil).Once()

	syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
		return nil, nil
	})

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
	err := syncer.generateAccessControls(context.Background(), access, set.NewSet[string](), map[string]string{}, repoMock, &config.ConfigMap{}, &dummyFeedbackHandler{})

	//Then
	assert.NoError(t, err)
}

func generateAccessControls_existing_schema(t *testing.T) {
	//Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CommentAccountRoleIfExists(mock.AnythingOfType("string"), "RoleName1").Return(nil).Once()

	repoMock.EXPECT().GetGrantsOfAccountRole(mock.Anything).Return([]GrantOfRole{}, nil)
	repoMock.EXPECT().GetGrantsToAccountRole(mock.Anything).Return([]GrantToRole{}, nil)

	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")

	repoMock.EXPECT().ExecuteRevokeOnAccountRole("ALL", "FUTURE TABLES IN SCHEMA DB1.Schema2", "RoleName1").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "DATABASE DB1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "SCHEMA DB1.Schema2", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "TABLE DB1.Schema2.Table3", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "VIEW DB1.Schema2.View3", "RoleName1").Return(nil).Once()

	database := "DB1"
	schema := "Schema2"
	repoMock.EXPECT().GetTablesInDatabase(database, schema, mock.Anything).RunAndReturn(func(s string, s2 string, handler EntityHandler) error {
		handler(&TableEntity{Database: s, Schema: s2, Name: "Table3", TableType: "BASE TABLE"})
		handler(&TableEntity{Database: s, Schema: s2, Name: "View3", TableType: "VIEW"})
		return nil
	}).Once()

	syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
		return nil, nil
	})

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
	err := syncer.generateAccessControls(context.Background(), access, set.NewSet[string]("RoleName1"), map[string]string{}, repoMock, &config.ConfigMap{}, &dummyFeedbackHandler{})

	//Then
	assert.NoError(t, err)
}

func generateAccessControls_sharedDatabase(t *testing.T) {
	//Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CreateAccountRole("RoleName1").Return(nil).Once()
	repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "RoleName1").Return(nil).Once()
	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")
	repoMock.EXPECT().GrantAccountRolesToAccountRole(mock.Anything, "RoleName1").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrantOnAccountRole("IMPORTED PRIVILEGES", "DATABASE DB2", "RoleName1").Return(nil).Once()

	syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
		return nil, nil
	})

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
	err := syncer.generateAccessControls(context.Background(), access, set.NewSet[string](), map[string]string{}, repoMock, &config.ConfigMap{}, &dummyFeedbackHandler{})

	//Then
	assert.NoError(t, err)
}

func generateAccessControls_database(t *testing.T) {
	//Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CreateAccountRole("RoleName1").Return(nil).Once()
	repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "RoleName1").Return(nil).Once()
	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")
	repoMock.EXPECT().GrantAccountRolesToAccountRole(mock.Anything, "RoleName1").Return(nil).Once()

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

	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "DATABASE DB1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "SCHEMA DB1.Schema2", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "TABLE DB1.Schema2.Table3", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "VIEW DB1.Schema2.View3", "RoleName1").Return(nil).Once()

	syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
		return nil, nil
	})

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
	err := syncer.generateAccessControls(context.Background(), access, set.NewSet[string](), map[string]string{}, repoMock, &config.ConfigMap{}, &dummyFeedbackHandler{})

	//Then
	assert.NoError(t, err)
}

func generateAccessControls_existing_database(t *testing.T) {
	//Given
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

	repoMock.EXPECT().ExecuteRevokeOnAccountRole("ALL", "FUTURE SCHEMAS IN DATABASE DB1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteRevokeOnAccountRole("ALL", "FUTURE TABLES IN DATABASE DB1", "RoleName1").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "DATABASE DB1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "SCHEMA DB1.Schema2", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "TABLE DB1.Schema2.Table3", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "VIEW DB1.Schema2.View3", "RoleName1").Return(nil).Once()

	repoMock.EXPECT().CommentDatabaseRoleIfExists(mock.AnythingOfType("string"), "DB1", "DatabaseRole1").Return(nil).Once()

	repoMock.EXPECT().GetGrantsOfDatabaseRole("DB1", "DatabaseRole1").Return([]GrantOfRole{}, nil)
	repoMock.EXPECT().GetGrantsToDatabaseRole("DB1", "DatabaseRole1").Return([]GrantToRole{}, nil)

	repoMock.EXPECT().ExecuteRevokeOnDatabaseRole("ALL", "FUTURE SCHEMAS IN DATABASE DB1", "DB1", "DatabaseRole1").Return(nil).Once()
	repoMock.EXPECT().ExecuteRevokeOnDatabaseRole("ALL", "FUTURE TABLES IN DATABASE DB1", "DB1", "DatabaseRole1").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrantOnDatabaseRole("USAGE", "DATABASE DB1", "DB1", "DatabaseRole1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnDatabaseRole("USAGE", "SCHEMA DB1.Schema2", "DB1", "DatabaseRole1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnDatabaseRole("SELECT", "TABLE DB1.Schema2.Table3", "DB1", "DatabaseRole1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnDatabaseRole("SELECT", "VIEW DB1.Schema2.View3", "DB1", "DatabaseRole1").Return(nil).Once()

	syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
		return nil, nil
	})

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
		"DATABASEROLE###DATABASE:DB1###ROLE:DatabaseRole1": {
			Id:   "DB1_DatabaseRole1",
			Name: "DatabaseRole1",
			Type: ptr.String("databaseRole"),
			Who:  importer.WhoItem{},
			What: []importer.WhatItem{
				{DataObject: &data_source.DataObjectReference{FullName: "DB1", Type: "database"}, Permissions: []string{"SELECT"}},
			},
		},
	}

	//When
	err := syncer.generateAccessControls(context.Background(), access, set.NewSet[string]("RoleName1", "DATABASEROLE###DATABASE:DB1###ROLE:DatabaseRole1"), map[string]string{}, repoMock, &config.ConfigMap{}, &dummyFeedbackHandler{})

	//Then
	assert.NoError(t, err)
}

func generateAccessControls_warehouse(t *testing.T) {
	//Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CreateAccountRole("RoleName1").Return(nil).Once()
	repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "RoleName1").Return(nil).Once()
	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")
	repoMock.EXPECT().GrantAccountRolesToAccountRole(mock.Anything, "RoleName1").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrantOnAccountRole("MONITOR", "WAREHOUSE WH1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "WAREHOUSE WH1", "RoleName1").Return(nil).Once()

	syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
		return nil, nil
	})

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
	err := syncer.generateAccessControls(context.Background(), access, set.NewSet[string](), map[string]string{}, repoMock, &config.ConfigMap{}, &dummyFeedbackHandler{})

	//Then
	assert.NoError(t, err)
}

func generateAccessControls_datasource(t *testing.T) {
	//Given
	repoMock := newMockDataAccessRepository(t)

	repoMock.EXPECT().CreateAccountRole("RoleName1").Return(nil).Once()
	repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "RoleName1").Return(nil).Once()
	expectGrantUsersToRole(repoMock, "RoleName1", "User1", "User2")
	repoMock.EXPECT().GrantAccountRolesToAccountRole(mock.Anything, "RoleName1").Return(nil).Once()
	repoMock.EXPECT().GetShares().Return([]DbEntity{}, nil).Once()
	repoMock.EXPECT().GetDatabases().Return([]DbEntity{}, nil).Once()

	syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
		return nil, nil
	})

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
	err := syncer.generateAccessControls(context.Background(), access, set.NewSet[string](), map[string]string{}, repoMock, &config.ConfigMap{}, &dummyFeedbackHandler{})

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

	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "DATABASE DB1", "existingRole1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "SCHEMA DB1.Schema1", "existingRole1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "TABLE DB1.Schema1.Table1", "existingRole1").Return(nil).Once()

	repoMock.EXPECT().CommentDatabaseRoleIfExists(mock.AnythingOfType("string"), "TEST_DB", "existingDBRole1").Return(nil).Once()
	repoMock.EXPECT().GetGrantsOfDatabaseRole("TEST_DB", "existingDBRole1").Return([]GrantOfRole{
		{GrantedTo: "DATABASE_ROLE", GranteeName: "TEST_DB.Role2"},
		{GrantedTo: "DATABASE_ROLE", GranteeName: "TEST_DB.Role3"},
	}, nil).Once()

	repoMock.EXPECT().GetGrantsToDatabaseRole("TEST_DB", "existingDBRole1").Return([]GrantToRole{}, nil).Once()

	expectGrantAccountOrDatabaseRolesToDatabaseRole(repoMock, true, "TEST_DB", "existingDBRole1", "DATABASEROLE###DATABASE:TEST_DB###ROLE:Role1")
	expectGrantAccountOrDatabaseRolesToDatabaseRole(repoMock, false, "TEST_DB", "existingDBRole1")
	repoMock.EXPECT().RevokeDatabaseRolesFromDatabaseRole(mock.Anything, "TEST_DB", "existingDBRole1", "Role3").Return(nil).Once()
	repoMock.EXPECT().RevokeAccountRolesFromDatabaseRole(mock.Anything, "TEST_DB", "existingDBRole1").Return(nil).Once()

	syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
		return nil, nil
	})

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
		"DATABASEROLE###DATABASE:TEST_DB###ROLE:existingDBRole1": {
			Id:         "TEST_DB_existingDBRole1",
			Name:       "existingDBRole1",
			ActualName: ptr.String(""),
			Who: importer.WhoItem{
				InheritFrom: []string{"DATABASEROLE###DATABASE:TEST_DB###ROLE:Role1", "DATABASEROLE###DATABASE:TEST_DB###ROLE:Role2"},
			},
			What: []importer.WhatItem{},
			Type: ptr.String("databaseRole"),
		},
	}

	//When
	err := syncer.generateAccessControls(context.Background(), access, set.NewSet[string]("existingRole1", "DATABASEROLE###DATABASE:TEST_DB###ROLE:existingDBRole1"), map[string]string{}, repoMock, &config.ConfigMap{}, &dummyFeedbackHandler{})

	//Then
	assert.NoError(t, err)
}

func TestAccessSyncer_generateAccessControls_inheritance(t *testing.T) {
	//Given
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
	repoMock.EXPECT().GrantAccountRolesToAccountRole(mock.Anything, "RoleName3").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "DATABASE DB1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "SCHEMA DB1.Schema1", "RoleName1").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "TABLE DB1.Schema1.Table1", "RoleName1").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "DATABASE DB1", "RoleName2").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "SCHEMA DB1.Schema1", "RoleName2").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "TABLE DB1.Schema1.Table2", "RoleName2").Return(nil).Once()

	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "DATABASE DB1", "RoleName3").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("USAGE", "SCHEMA DB1.Schema1", "RoleName3").Return(nil).Once()
	repoMock.EXPECT().ExecuteGrantOnAccountRole("SELECT", "TABLE DB1.Schema1.Table3", "RoleName3").Return(nil).Once()

	syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
		return nil, nil
	})

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
	err := syncer.generateAccessControls(context.Background(), access, set.NewSet[string](), map[string]string{}, repoMock, &config.ConfigMap{}, &dummyFeedbackHandler{})

	//Then
	assert.NoError(t, err)
}

// Testing the normal rename case where we need to rename the role to the new name and then update stuff.
func TestAccessSyncer_generateAccessControls_rename(t *testing.T) {
	//Given
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
		{GrantedTo: "DATABASE_ROLE", GranteeName: "TEST_DB.Role1"},
		{GrantedTo: "DATABASE_ROLE", GranteeName: "TEST_DB.Role2"},
	}, nil).Once()
	repoMock.EXPECT().GetGrantsToDatabaseRole("TEST_DB", "newDBRole").Return([]GrantToRole{}, nil).Once()

	syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
		return nil, nil
	})

	access := map[string]*importer.AccessProvider{
		"NewRoleName": {
			Id:   "AccessProviderId",
			Name: "AccessProvider",
			Who: importer.WhoItem{
				Users: []string{"User1"},
			},
			ExternalId: ptr.String("OldRoleName"),
		},
		"DATABASEROLE###DATABASE:TEST_DB###ROLE:newDBRole": {
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
	}

	//When
	err := syncer.generateAccessControls(context.Background(), access, set.NewSet[string]("OldRoleName", "DATABASEROLE###DATABASE:TEST_DB###ROLE:oldDBRole"), map[string]string{"NewRoleName": "OldRoleName", "DATABASEROLE###DATABASE:TEST_DB###ROLE:newDBRole": "DATABASEROLE###DATABASE:TEST_DB###ROLE:oldDBRole"}, repoMock, &config.ConfigMap{}, feedbackHandler)

	//Then
	assert.NoError(t, err)
	assert.Len(t, feedbackHandler.AccessProviderFeedback, 2)
	assert.ElementsMatch(t, feedbackHandler.AccessProviderFeedback, []importer.AccessProviderSyncFeedback{
		{
			AccessProvider: "AccessProviderId",
			ActualName:     "NewRoleName",
			ExternalId:     ptr.String("NewRoleName"),
		}, {
			AccessProvider: "TEST_DB_DBRole",
			ActualName:     "newDBRole",
			ExternalId:     ptr.String("DATABASEROLE###DATABASE:TEST_DB###ROLE:newDBRole"),
			Type:           ptr.String("databaseRole"),
		},
	})
}

// Testing the rename of APs where the new role name already exists (but not needed), so the old should get dropped
func TestAccessSyncer_generateAccessControls_renameNewExists(t *testing.T) {
	//Given
	repoMock := newMockDataAccessRepository(t)
	feedbackHandler := mocks.NewSimpleAccessProviderFeedbackHandler(t)

	expectGrantUsersToRole(repoMock, "NewRoleName", "User1")
	repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "NewRoleName").Return(nil).Once()
	repoMock.EXPECT().DropAccountRole("OldRoleName").Return(nil).Once()
	repoMock.EXPECT().GetGrantsOfAccountRole("NewRoleName").Return([]GrantOfRole{}, nil).Once()
	repoMock.EXPECT().GetGrantsToAccountRole("NewRoleName").Return([]GrantToRole{}, nil).Once()

	syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
		return nil, nil
	})

	access := map[string]*importer.AccessProvider{
		"NewRoleName": {
			Id:         "AccessProviderId",
			Name:       "AccessProvider",
			ExternalId: ptr.String("NewRoleName"),
			Type:       ptr.String(access_provider.Role),
			Who: importer.WhoItem{
				Users: []string{"User1"},
			},
		},
	}

	//When
	err := syncer.generateAccessControls(context.Background(), access, set.NewSet[string]("OldRoleName", "NewRoleName"), map[string]string{"NewRoleName": "OldRoleName"}, repoMock, &config.ConfigMap{}, feedbackHandler)

	//Then
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
func TestAccessSyncer_generateAccessControls_renameOldAlreadyTaken(t *testing.T) {
	//Given
	repoMock := newMockDataAccessRepository(t)
	feedbackHandler := mocks.NewSimpleAccessProviderFeedbackHandler(t)

	repoMock.EXPECT().CreateAccountRole("NewRoleName").Return(nil).Once()
	expectGrantUsersToRole(repoMock, "NewRoleName", "User1")
	repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "NewRoleName").Return(nil).Once()
	repoMock.EXPECT().CommentAccountRoleIfExists(mock.Anything, "OldRoleName").Return(nil).Once()
	repoMock.EXPECT().GetGrantsOfAccountRole("OldRoleName").Return([]GrantOfRole{}, nil).Once()
	repoMock.EXPECT().GetGrantsToAccountRole("OldRoleName").Return([]GrantToRole{}, nil).Once()
	repoMock.EXPECT().GrantAccountRolesToAccountRole(mock.Anything, "NewRoleName").Return(nil).Once()

	syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
		return nil, nil
	})

	access := map[string]*importer.AccessProvider{
		// This AP gets renewed from OldRoleName to NewRoleName
		"NewRoleName": {
			Id:         "AccessProviderId",
			ExternalId: ptr.String("OldRoleName"),
			Name:       "AccessProvider",
			Who: importer.WhoItem{
				Users: []string{"User1"},
			},
		},
		// In the meanwhile, there already is a new AP with the name OldRoleName
		"OldRoleName": {
			Id:   "AccessProviderId2",
			Name: "AccessProvider2",
		},
	}

	//When
	err := syncer.generateAccessControls(context.Background(), access, set.NewSet[string]("OldRoleName"), map[string]string{"NewRoleName": "OldRoleName"}, repoMock, &config.ConfigMap{}, feedbackHandler)

	//Then
	assert.NoError(t, err)
	assert.Len(t, feedbackHandler.AccessProviderFeedback, 2)
	assert.ElementsMatch(t, feedbackHandler.AccessProviderFeedback, []importer.AccessProviderSyncFeedback{
		{
			AccessProvider: "AccessProviderId",
			ActualName:     "NewRoleName",
			ExternalId:     ptr.String("NewRoleName"),
		},
		{
			AccessProvider: "AccessProviderId2",
			ActualName:     "OldRoleName",
			ExternalId:     ptr.String("OldRoleName"),
		},
	})
}

func TestAccessSyncer_updateOrCreateFilter(t *testing.T) {

}

func Test_filterExpressionOfPolicyRule(t *testing.T) {
	type args struct {
		policyRule string
	}
	tests := []struct {
		name  string
		args  args
		want  string
		want1 []string
	}{
		{
			name: "empty policy rule",
			args: args{
				policyRule: "",
			},
			want:  "",
			want1: []string{},
		},
		{
			name: "simple policy rule, without references",
			args: args{
				policyRule: "SELECT * FROM table1 WHERE column1 = 'value1'",
			},
			want:  "SELECT * FROM table1 WHERE column1 = 'value1'",
			want1: []string{},
		},
		{
			name: "simple policy rule, with references",
			args: args{
				policyRule: "{column1} = 'value1'",
			},
			want:  "column1 = 'value1'",
			want1: []string{"column1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := filterExpressionOfPolicyRule(tt.args.policyRule)
			assert.Equalf(t, tt.want, got, "filterExpressionOfPolicyRule(%v)", tt.args.policyRule)
			assert.Equalf(t, tt.want1, got1, "filterExpressionOfPolicyRule(%v)", tt.args.policyRule)
		})
	}
}

func Test_RenameRole(t *testing.T) {
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

			syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
				return nil, nil
			})

			// When
			err := syncer.renameRole(tt.args.oldName, tt.args.newName, tt.args.apType, repoMock)

			// Then
			tt.wantErr(t, err)
		})
	}
}

func TestGrantRolesToRole_DatabaseFiltering(t *testing.T) {
	repoMock := newMockDataAccessRepository(t)
	syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
		return repoMock, nil
	})
	syncer.ignoreLinksToRole = []string{"My.+"}

	repoMock.EXPECT().GrantDatabaseRolesToDatabaseRole(mock.Anything, "DB1", "TargetRole", "AnotherDBRole").Return(nil).Once()
	repoMock.EXPECT().GrantAccountRolesToDatabaseRole(mock.Anything, "DB1", "TargetRole", "AnotherRole").Return(nil).Once()

	dbRoleType := apTypeDatabaseRole
	err := syncer.grantRolesToRole(context.Background(), repoMock, databaseRoleExternalIdGenerator("DB1", "TargetRole"), &dbRoleType, "MyRole1", "AnotherRole", databaseRoleExternalIdGenerator("DB1", "MyDBRole"), databaseRoleExternalIdGenerator("DB1", "AnotherDBRole"))
	assert.NoError(t, err)
}

func TestGrantRolesToRole_AccountFiltering(t *testing.T) {
	repoMock := newMockDataAccessRepository(t)
	syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
		return repoMock, nil
	})
	syncer.ignoreLinksToRole = []string{"My.+"}

	repoMock.EXPECT().GrantAccountRolesToAccountRole(mock.Anything, "TargetRole", "AnotherRole").Return(nil).Once()

	err := syncer.grantRolesToRole(context.Background(), repoMock, "TargetRole", nil, "MyRole1", "AnotherRole")
	assert.NoError(t, err)
}

func TestRevokeRolesFromRole_DatabaseFiltering(t *testing.T) {
	repoMock := newMockDataAccessRepository(t)
	syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
		return repoMock, nil
	})
	syncer.ignoreLinksToRole = []string{"My.+"}

	repoMock.EXPECT().RevokeDatabaseRolesFromDatabaseRole(mock.Anything, "DB1", "TargetRole", "AnotherDBRole").Return(nil).Once()
	repoMock.EXPECT().RevokeAccountRolesFromDatabaseRole(mock.Anything, "DB1", "TargetRole", "AnotherRole").Return(nil).Once()

	dbRoleType := apTypeDatabaseRole
	err := syncer.revokeRolesFromRole(context.Background(), repoMock, databaseRoleExternalIdGenerator("DB1", "TargetRole"), &dbRoleType, "MyRole1", "AnotherRole", databaseRoleExternalIdGenerator("DB1", "MyDBRole"), databaseRoleExternalIdGenerator("DB1", "AnotherDBRole"))
	assert.NoError(t, err)
}

func TestRevokeRolesFromRole_AccountFiltering(t *testing.T) {
	repoMock := newMockDataAccessRepository(t)
	syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
		return repoMock, nil
	})
	syncer.ignoreLinksToRole = []string{"My.+"}

	repoMock.EXPECT().RevokeAccountRolesFromAccountRole(mock.Anything, "TargetRole", "AnotherRole").Return(nil).Once()

	err := syncer.revokeRolesFromRole(context.Background(), repoMock, "TargetRole", nil, "MyRole1", "AnotherRole")
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
			logger.Error(fmt.Sprintf("error during syncing of access provider %q; %s", accessProviderFeedback.AccessProvider, err))
		}
	}

	return nil
}
