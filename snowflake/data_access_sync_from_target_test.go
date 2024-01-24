package snowflake

import (
	"testing"

	"github.com/aws/smithy-go/ptr"
	"github.com/raito-io/cli/base/access_provider"
	"github.com/raito-io/cli/base/tag"
	"github.com/raito-io/cli/base/util/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	type fields struct {
		setup func(repoMock *mockDataAccessRepository)
	}
	type args struct {
		configMap config.ConfigMap
		tagDomain string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *tagApRetrievalConfig
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "basic",
			fields: fields{
				setup: func(repoMock *mockDataAccessRepository) {
					repoMock.EXPECT().GetTagsByDomain("ROLE").Return(map[string][]*tag.Tag{
						"Role1": {
							{Key: "a_key", Value: "override_name"},
							{Key: "an_other_key", Value: "...."},
						},
					}, nil).Once()
				},
			},
			args: args{
				configMap: config.ConfigMap{
					Parameters: map[string]string{
						SfStandardEdition:                     "false",
						SfSkipTags:                            "false",
						SfTagOverwriteKeyForAccessControlName: "a_key",
					},
				},
				tagDomain: "ROLE",
			},
			want: &tagApRetrievalConfig{
				enabled:           true,
				tagKeyDisplayName: "a_key",
				availableTags: map[string][]*tag.Tag{
					"Role1": {
						{Key: "a_key", Value: "override_name"},
						{Key: "an_other_key", Value: "...."},
					},
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "on SF standard edition",
			fields: fields{
				setup: func(repoMock *mockDataAccessRepository) {},
			},
			args: args{
				configMap: config.ConfigMap{
					Parameters: map[string]string{
						SfStandardEdition:                     "true",
						SfSkipTags:                            "false",
						SfTagOverwriteKeyForAccessControlName: "a_key",
					},
				},
				tagDomain: "ROLE",
			},
			want: &tagApRetrievalConfig{
				enabled:           false,
				tagKeyDisplayName: "a_key",
				availableTags:     map[string][]*tag.Tag{},
			},
			wantErr: require.NoError,
		},
		{
			name: "skip tags enabled",
			fields: fields{
				setup: func(repoMock *mockDataAccessRepository) {},
			},
			args: args{
				configMap: config.ConfigMap{
					Parameters: map[string]string{
						SfStandardEdition:                     "false",
						SfSkipTags:                            "true",
						SfTagOverwriteKeyForAccessControlName: "a_key",
					},
				},
				tagDomain: "ROLE",
			},
			want: &tagApRetrievalConfig{
				enabled:           false,
				tagKeyDisplayName: "a_key",
				availableTags:     map[string][]*tag.Tag{},
			},
			wantErr: require.NoError,
		},
		{
			name: "with no overwrite key defined ",
			fields: fields{
				setup: func(repoMock *mockDataAccessRepository) {},
			},
			args: args{
				configMap: config.ConfigMap{
					Parameters: map[string]string{
						SfStandardEdition: "false",
						SfSkipTags:        "false",
					},
				},
				tagDomain: "ROLE",
			},
			want: &tagApRetrievalConfig{
				enabled:           false,
				tagKeyDisplayName: "",
				availableTags:     map[string][]*tag.Tag{},
			},
			wantErr: require.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Given
			repoMock := newMockDataAccessRepository(t)

			tt.fields.setup(repoMock)

			syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
				return repoMock, nil
			})

			//When
			retrieveConfig, err := syncer.shouldRetrieveTags(&tt.args.configMap, repoMock, tt.args.tagDomain)

			// Then
			tt.wantErr(t, err)
			assert.Equal(t, tt.want, retrieveConfig)
		})
	}
}

func Test_ExtractNameFromTagss(t *testing.T) {
	type args struct {
		tags              []*tag.Tag
		tagKeyDisplayName string
	}
	tests := []struct {
		name string
		args args
		want *string
	}{
		{
			name: "key available",
			args: args{
				tags: []*tag.Tag{
					{Key: "a_key", Value: "override_name"},
					{Key: "an_other_key", Value: "...."},
				},
				tagKeyDisplayName: "a_key",
			},
			want: ptr.String("override_name"),
		},
		{
			name: "key not available",
			args: args{
				tags: []*tag.Tag{
					{Key: "a_key", Value: "override_name"},
					{Key: "an_other_key", Value: "...."},
				},
				tagKeyDisplayName: "blaat",
			},
			want: nil,
		},
		{
			name: "no tags available",
			args: args{
				tags:              []*tag.Tag{},
				tagKeyDisplayName: "a_key",
			},
			want: nil,
		},
		{
			name: "no tag key available",
			args: args{
				tags: []*tag.Tag{
					{Key: "a_key", Value: "override_name"},
					{Key: "an_other_key", Value: "...."}},
				tagKeyDisplayName: "",
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Given
			repoMock := newMockDataAccessRepository(t)

			syncer := createBasicAccessSyncer(func(params map[string]string, role string) (dataAccessRepository, error) {
				return repoMock, nil
			})

			//When
			possibleTagOverrideValue := syncer.extractNameFromTags(tt.args.tags, tt.args.tagKeyDisplayName)

			// Then
			assert.Equal(t, &tt.want, &possibleTagOverrideValue)
		})
	}
}
