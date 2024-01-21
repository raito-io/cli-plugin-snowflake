package snowflake

import (
	"testing"

	"github.com/aws/smithy-go/ptr"
	"github.com/raito-io/cli/base/access_provider"
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
