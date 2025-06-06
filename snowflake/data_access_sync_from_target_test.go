package snowflake

import (
	"testing"

	"github.com/aws/smithy-go/ptr"
	"github.com/raito-io/cli/base/access_provider"
	"github.com/raito-io/cli/base/access_provider/sync_from_target"
	"github.com/raito-io/cli/base/access_provider/sync_to_target/naming_hint"
	"github.com/raito-io/cli/base/access_provider/types"
	"github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers"
	"github.com/raito-io/cli/base/wrappers/mocks"
	"github.com/stretchr/testify/assert"
)

func Test_IsNotInternalizableRole(t *testing.T) {
	apTypeAccountRole := ptr.String(access_provider.Role)
	apTypeDatabaseRole := ptr.String("databaseRole")

	type args struct {
		roleName string
		roleType *string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "account role - internalizable",
			args: args{
				roleName: "TEST",
				roleType: apTypeAccountRole,
			},
			want: false,
		},
		{
			name: "account role - not internalizable",
			args: args{
				roleName: "ORGADMIN",
				roleType: apTypeAccountRole,
			},
			want: true,
		},
		{
			name: "database role - internalizable",
			args: args{
				roleName: "DATABASEROLE###DATABASE:TEST_DB###ROLE:DatabaseRole1",
				roleType: apTypeDatabaseRole,
			},
			want: false,
		},
		{
			name: "application role - not internalizable",
			args: args{
				roleName: "APPLICATIONROLE###APPLICATION:TEST_APP###ROLE:ApplicationRole1",
				roleType: ptr.String(apTypeApplicationRole),
			},
			want: true,
		},
		{
			name: "database role - invalid",
			args: args{
				roleName: "BLAAT",
				roleType: apTypeDatabaseRole,
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isNotInternalizableRole(tt.args.roleName, tt.args.roleType)
			assert.Equalf(t, tt.want, got, "isNotInternalizableRole(%v)", tt.args.roleName)
		})
	}
}

func Test_ShouldRetrieveTags(t *testing.T) {
	type args struct {
		configMap config.ConfigMap
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "basic",

			args: args{
				configMap: config.ConfigMap{
					Parameters: map[string]string{
						SfStandardEdition: "false",
						SfSkipTags:        "false",
					},
				},
			},
			want: true,
		},
		{
			name: "on SF standard edition",

			args: args{
				configMap: config.ConfigMap{
					Parameters: map[string]string{
						SfStandardEdition: "true",
						SfSkipTags:        "false",
					},
				},
			},
			want: false,
		},
		{
			name: "skip tags enabled",

			args: args{
				configMap: config.ConfigMap{
					Parameters: map[string]string{
						SfStandardEdition: "false",
						SfSkipTags:        "true",
					},
				},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Given
			repoMock := newMockDataAccessRepository(t)

			syncer := createBasicFromTargetSyncer(repoMock, nil, &tt.args.configMap)

			// When
			shouldRetrieveTags := syncer.shouldRetrieveTags()

			// Then
			assert.Equal(t, tt.want, shouldRetrieveTags)
		})
	}
}

func TestAccessSyncer_GetFullNameFromGrant(t *testing.T) {
	accessSyncer := NewDataAccessSyncer(naming_hint.NamingConstraints{})
	assert.Equal(t, "DB1.Schema1.Entity1", accessSyncer.getFullNameFromGrant("DB1.Schema1.Entity1", "table"))
	assert.Equal(t, `MASTER_DATA.PUBLIC."DECRYPTIT"(VARCHAR, VARCHAR)`, accessSyncer.getFullNameFromGrant(`MASTER_DATA.PUBLIC."DECRYPTIT(VAL VARCHAR, ENCRYPTIONTYPE VARCHAR):VARCHAR(16777216)"`, Function))
	assert.Equal(t, `MASTER_DATA.PUBLIC."DECRYPTIT"(VARCHAR, VARCHAR)`, accessSyncer.getFullNameFromGrant(`MASTER_DATA.PUBLIC."DECRYPTIT(VAL VARCHAR, ENCRYPTIONTYPE VARCHAR):VARCHAR(16777216)"`, Procedure))
}

func TestAccessSyncer_ImportPoliciesOfType(t *testing.T) {
	// Given
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
			POLICY_KIND:       "MASKING_POLICY",
			REF_DATABASE_NAME: "DB1",
			REF_SCHEMA_NAME:   "Schema1",
			REF_ENTITY_NAME:   "EntityName1",
		},
	}, nil).Once()

	repoMock.EXPECT().GetPolicyReferences("DB1", "Schema2", "Policy2").Return([]PolicyReferenceEntity{
		{
			POLICY_STATUS:     "Active",
			REF_COLUMN_NAME:   NullString{Valid: false},
			POLICY_KIND:       "ROW_ACCESS_POLICY",
			REF_DATABASE_NAME: "DB1",
			REF_SCHEMA_NAME:   "Schema1",
			REF_ENTITY_NAME:   "EntityName1",
		},
	}, nil).Once()

	syncer := createBasicFromTargetSyncer(repoMock, fileCreator, nil)

	// When
	err := syncer.importPoliciesOfType(policyType, types.Grant)

	// Then
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
			Action: types.Grant,
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
			Action: types.Grant,
			Policy: "PolicyBody2",
		},
	}, fileCreator.AccessProviders)
}

func createBasicFromTargetSyncer(repo dataAccessRepository, accessProviderHandler wrappers.AccessProviderHandler, configMap *config.ConfigMap) *AccessFromTargetSyncer {
	as := AccessSyncer{
		repoProvider: func(params map[string]string, role string) (dataAccessRepository, error) {
			return repo, nil
		},
		repo:              repo,
		namingConstraints: RoleNameConstraints,
	}

	return NewAccessFromTargetSyncer(&as, repo, accessProviderHandler, configMap)
}
