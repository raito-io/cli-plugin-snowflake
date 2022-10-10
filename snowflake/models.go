package snowflake

import (
	"database/sql"
	"fmt"
)

// Implementation of Scanner interface for NullString
type NullString sql.NullString

// Data Source
type dbEntity struct {
	Name    string  `db:"name"`
	Comment *string `db:"comment"`
}

// Identity Store
type userEntity struct {
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
	Role                  string     `db:"ROLE_NAME" useColumnName:"true"`
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
	return fmt.Sprintf("ID: %s, Status: %s, SQL Query: %s, DatabaseName: %s, SchemaName: %s, UserName: %s, RoleName: %s, StartTime: %s, EndTime: %s, DirectObjectsAccessed: %v, BaseObjectsAccess: %v",
		entity.ExternalId, entity.Status, entity.Query, entity.DatabaseName.String, entity.SchemaName.String, entity.User, entity.Role, entity.StartTime, entity.EndTime, entity.DirectObjectsAccessed, entity.BaseObjectsAccessed)
}

// Data Access
type roleEntity struct {
	Name            string `db:"name"`
	AssignedToUsers int    `db:"assigned_to_users"`
	GrantedToRoles  int    `db:"granted_to_roles"`
	GrantedRoles    int    `db:"granted_roles"`
	Owner           string `db:"owner"`
}

type grantOfRole struct {
	GrantedTo   string `db:"granted_to"`
	GranteeName string `db:"grantee_name"`
}

type grantToRole struct {
	Privilege string `db:"privilege"`
	GrantedOn string `db:"granted_on"`
	Name      string `db:"name"`
}

type Grant struct {
	Permissions string
	On          string
}

type policyEntity struct {
	Name         string `db:"name"`
	DatabaseName string `db:"database_name"`
	SchemaName   string `db:"schema_name"`
	Kind         string `db:"kind"`
	Owner        string `db:"owner"`
}

type desribePolicyEntity struct {
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