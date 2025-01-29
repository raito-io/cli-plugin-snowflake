package snowflake

import (
	"testing"

	"github.com/aws/smithy-go/ptr"
	ds "github.com/raito-io/cli/base/data_source"
	"github.com/stretchr/testify/assert"
)

func Test_TagEntity_GetFullName(t *testing.T) {
	type args struct {
		tagEntity TagEntity
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "domain DATABASE",
			args: args{
				tagEntity: TagEntity{
					Domain:   "database",
					Name:     "db1",
					Column:   ptr.String("column1"),
					TagName:  "tag1",
					TagValue: "value1",
				},
			},
			want: "db1",
		},
		{
			name: "domain SCHEMA",
			args: args{
				tagEntity: TagEntity{
					Domain:   "schema",
					Database: ptr.String("db1"),
					Schema:   ptr.String("schema1"),
					Name:     "resource1",
					Column:   ptr.String("column1"),
					TagName:  "tag1",
					TagValue: "value1",
				},
			},
			want: "db1.resource1",
		},
		{
			name: "domain TABLE",
			args: args{
				tagEntity: TagEntity{
					Domain:   "table",
					Database: ptr.String("db1"),
					Schema:   ptr.String("schema1"),
					Name:     "resource1",
					Column:   ptr.String("column1"),
					TagName:  "tag1",
					TagValue: "value1",
				},
			},
			want: "db1.schema1.resource1",
		},
		{
			name: "domain COLUMN",
			args: args{
				tagEntity: TagEntity{
					Domain:   "column",
					Database: ptr.String("db1"),
					Schema:   ptr.String("schema1"),
					Name:     "resource1",
					Column:   ptr.String("column1"),
					TagName:  "tag1",
					TagValue: "value1",
				},
			},
			want: "db1.schema1.resource1.column1",
		},
		{
			name: "domain BOGUS",
			args: args{
				tagEntity: TagEntity{
					Domain:   "bogus",
					Database: ptr.String("db1"),
					Schema:   ptr.String("schema1"),
					Name:     "resource1",
					Column:   ptr.String("column1"),
					TagName:  "tag1",
					TagValue: "value1",
				},
			},
			want: "",
		},
		{
			name: "domain ROLE",
			args: args{
				tagEntity: TagEntity{
					Domain:   "role",
					Database: ptr.String("db1"),
					Schema:   ptr.String("schema1"),
					Name:     "resource1",
					Column:   ptr.String("column1"),
					TagName:  "tag1",
					TagValue: "value1",
				},
			},
			want: "resource1",
		},
		{
			name: "domain DATABASE ROLE",
			args: args{
				tagEntity: TagEntity{
					Domain:   "database_role",
					Database: ptr.String("db1"),
					Schema:   ptr.String("schema1"),
					Name:     "resource1",
					Column:   ptr.String("column1"),
					TagName:  "tag1",
					TagValue: "value1",
				},
			},
			want: "db1.resource1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.args.tagEntity.GetFullName())
		})
	}
}

func TestGrantSet_Iterator(t *testing.T) {
	tests := []struct {
		name            string
		grantToAdd      []Grant
		elementsInOrder []Grant
	}{
		{
			name:            "empty",
			grantToAdd:      []Grant{},
			elementsInOrder: []Grant{},
		},
		{
			name: "order by dataobjects",
			grantToAdd: []Grant{
				{
					Permissions: "SELECT",
					OnType:      ds.Table,
					On:          "someId1",
				},
				{
					Permissions: "Usage",
					OnType:      ds.Schema,
					On:          "someId2",
				},
				{
					Permissions: "PERM",
					OnType:      ds.Column,
					On:          "someId3",
				},
				{
					Permissions: "USAGE",
					OnType:      ds.Database,
					On:          "someId4",
				},
			},
			elementsInOrder: []Grant{
				{
					Permissions: "USAGE",
					OnType:      ds.Database,
					On:          "someId4",
				},
				{
					Permissions: "Usage",
					OnType:      ds.Schema,
					On:          "someId2",
				},
				{
					Permissions: "SELECT",
					OnType:      ds.Table,
					On:          "someId1",
				},
				{
					Permissions: "PERM",
					OnType:      ds.Column,
					On:          "someId3",
				},
			},
		},
		{
			name: "Non expecting type at the end",
			grantToAdd: []Grant{
				{
					Permissions: "SELECT",
					OnType:      ds.Table,
					On:          "someId1",
				},
				{
					Permissions: "Usage",
					OnType:      ds.Schema,
					On:          "someId2",
				},
				{
					Permissions: "PERM",
					OnType:      "Non expecting type",
					On:          "someId5",
				},
				{
					Permissions: "PERM",
					OnType:      ds.Column,
					On:          "someId3",
				},
				{
					Permissions: "USAGE",
					OnType:      ds.Database,
					On:          "someId4",
				},
			},
			elementsInOrder: []Grant{
				{
					Permissions: "USAGE",
					OnType:      ds.Database,
					On:          "someId4",
				},
				{
					Permissions: "Usage",
					OnType:      ds.Schema,
					On:          "someId2",
				},
				{
					Permissions: "SELECT",
					OnType:      ds.Table,
					On:          "someId1",
				},
				{
					Permissions: "PERM",
					OnType:      ds.Column,
					On:          "someId3",
				},
				{
					Permissions: "PERM",
					OnType:      "Non expecting type",
					On:          "someId5",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gs := NewGrantSet()

			for _, grant := range tt.grantToAdd {
				gs.Add(grant)
			}

			order := gs.Slice()
			assert.Equal(t, tt.elementsInOrder, order)
		})
	}
}
