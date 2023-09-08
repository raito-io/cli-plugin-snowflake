package snowflake

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/raito-io/cli/base/tag"
)

// Implementation of Scanner interface for NullString
type NullString sql.NullString

// Data Source

type DbEntity struct {
	Name    string  `db:"name"`
	Comment *string `db:"comment"`
}

type SchemaEntity struct {
	Database string  `db:"CATALOG_NAME"`
	Name     string  `db:"SCHEMA_NAME"`
	Comment  *string `db:"COMMENT"`
}

type TagEntity struct {
	Database string  `db:"OBJECT_DATABASE"`
	Schema   *string `db:"OBJECT_SCHEMA"`
	Name     string  `db:"OBJECT_NAME"`
	Domain   string  `db:"DOMAIN"`
	TagName  string  `db:"TAG_NAME"`
	TagValue string  `db:"TAG_VALUE"`
	Column   *string `db:"COLUMN_NAME"`
}

func (t *TagEntity) CreateTag() *tag.Tag {
	return &tag.Tag{
		Key:    t.TagName,
		Value:  t.TagValue,
		Source: TagSource,
	}
}

func (t *TagEntity) GetFullName() string {
	switch strings.ToUpper(t.Domain) {
	case "DATABASE":
		return t.Database
	case "SCHEMA":
		return t.Database + "." + t.Name
	case "TABLE":
		return t.Database + "." + *t.Schema + "." + t.Name
	case "COLUMN":
		return t.Database + "." + *t.Schema + "." + t.Name + "." + *t.Column
	}

	return ""
}

type TableEntity struct {
	Database  string  `db:"TABLE_CATALOG"`
	Schema    string  `db:"TABLE_SCHEMA"`
	Name      string  `db:"TABLE_NAME"`
	TableType string  `db:"TABLE_TYPE"`
	Comment   *string `db:"COMMENT"`
}

type ColumnEntity struct {
	Database string  `db:"TABLE_CATALOG"`
	Schema   string  `db:"TABLE_SCHEMA"`
	Table    string  `db:"TABLE_NAME"`
	Name     string  `db:"COLUMN_NAME"`
	Comment  *string `db:"COMMENT"`
}

// Identity Store

type UserEntity struct {
	Name        string `db:"name"`
	LoginName   string `db:"login_name"`
	DisplayName string `db:"display_name"`
	Email       string `db:"email"`
	Owner       string `db:"owner"`
}

// Data Usage
type QueryDbEntities struct {
	ExternalId            string     `db:"QUERY_ID" useColumnName:"true"`
	Status                string     `db:"EXECUTION_STATUS" useColumnName:"true"`
	Query                 string     `db:"QUERY_TEXT" useColumnName:"true"`
	ErrorMessage          NullString `db:"ERROR_MESSAGE" useColumnName:"true"`
	DatabaseName          NullString `db:"DATABASE_NAME" useColumnName:"true"`
	SchemaName            NullString `db:"SCHEMA_NAME" useColumnName:"true"`
	User                  string     `db:"USER_NAME" useColumnName:"true"`
	Role                  NullString `db:"ROLE_NAME" useColumnName:"true"`
	StartTime             string     `db:"START_TIME" useColumnName:"true"`
	EndTime               string     `db:"END_TIME" useColumnName:"true"`
	BytesTranferred       int        `db:"OUTBOUND_DATA_TRANSFER_BYTES" useColumnName:"true"`
	RowsReturned          int        `db:"EXTERNAL_FUNCTION_TOTAL_SENT_ROWS" useColumnName:"true"`
	CloudCreditsUsed      float32    `db:"CREDITS_USED_CLOUD_SERVICES" useColumnName:"true"`
	AccessId              NullString `db:"QID"`
	DirectObjectsAccessed *string    `db:"DIRECT_OBJECTS_ACCESSED"`
	BaseObjectsAccessed   *string    `db:"BASE_OBJECTS_ACCESSED"`
	ObjectsModified       *string    `db:"OBJECTS_MODIFIED"`
}

func (entity QueryDbEntities) String() string {
	return fmt.Sprintf("ID: %s, Status: %s, SQL Query: %s, DatabaseName: %s, SchemaName: %s, UserName: %s, RoleName: %v, StartTime: %s, EndTime: %s, DirectObjectsAccessed: %v, BaseObjectsAccess: %v",
		entity.ExternalId, entity.Status, entity.Query, entity.DatabaseName.String, entity.SchemaName.String, entity.User, entity.Role, entity.StartTime, entity.EndTime, entity.DirectObjectsAccessed, entity.BaseObjectsAccessed)
}

// Data Access
type RoleEntity struct {
	Name            string `db:"name"`
	AssignedToUsers int    `db:"assigned_to_users"`
	GrantedToRoles  int    `db:"granted_to_roles"`
	GrantedRoles    int    `db:"granted_roles"`
	Owner           string `db:"owner"`
}

type GrantOfRole struct {
	GrantedTo   string `db:"granted_to"`
	GranteeName string `db:"grantee_name"`
}

type GrantToRole struct {
	Privilege string `db:"privilege"`
	GrantedOn string `db:"granted_on"`
	Name      string `db:"name"`
}

type Grant struct {
	Permissions string
	// OnType represents the raito data object type of the targeted object
	OnType string
	On     string
}

// GetGrantOnType returns the type to use in a GRANT query.
func (g *Grant) GetGrantOnType() string {
	if sfType, f := raitoTypeToSnowflakeGrantType[g.OnType]; f {
		return sfType
	}

	// By default, we take the uppercase version of the raito data object type
	return strings.ToUpper(g.OnType)
}

type policyEntity struct {
	Name         string `db:"name"`
	DatabaseName string `db:"database_name"`
	SchemaName   string `db:"schema_name"`
	Kind         string `db:"kind"`
	Owner        string `db:"owner"`
}

type describePolicyEntity struct {
	Name string `db:"name"`
	Body string `db:"body"`
}

type policyReferenceEntity struct {
	POLICY_DB            string     `db:"POLICY_DB"`
	POLICY_SCHEMA        string     `db:"POLICY_SCHEMA"`
	POLICY_NAME          string     `db:"POLICY_NAME"`
	POLICY_KIND          string     `db:"POLICY_KIND"`
	REF_DATABASE_NAME    string     `db:"REF_DATABASE_NAME"`
	REF_SCHEMA_NAME      string     `db:"REF_SCHEMA_NAME"`
	REF_ENTITY_NAME      string     `db:"REF_ENTITY_NAME"`
	REF_ENTITY_DOMAIN    string     `db:"REF_ENTITY_DOMAIN"`
	REF_COLUMN_NAME      NullString `db:"REF_COLUMN_NAME"`
	REF_ARG_COLUMN_NAMES NullString `db:"REF_ARG_COLUMN_NAMES"`
	TAG_DATABASE         NullString `db:"TAG_DATABASE"`
	TAG_SCHEMA           NullString `db:"TAG_SCHEMA"`
	TAG_NAME             NullString `db:"TAG_NAME"`
	POLICY_STATUS        string     `db:"POLICY_STATUS"`
}
