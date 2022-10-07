package snowflake

import "fmt"

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
