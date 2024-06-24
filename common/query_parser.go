package common

type SnowflakeColumn struct {
	Id   int    `json:"columnId"`
	Name string `json:"columnName"`
}
type SnowflakeAccessedObjects struct {
	Columns []SnowflakeColumn `json:"columns"`
	Domain  string            `json:"objectDomain"`
	Id      int               `json:"objectId"`
	Name    string            `json:"objectName"`
}

//go:generate go run github.com/raito-io/enumer -type=ModifiedObjectByDdlOperationType -json -transform=upper -trimprefix=ModifiedObjectByDdlOperationType
type ModifiedObjectByDdlOperationType int

const (
	ModifiedObjectByDdlOperationTypeUnknown ModifiedObjectByDdlOperationType = iota
	ModifiedObjectByDdlOperationTypeAlter
	ModifiedObjectByDdlOperationTypeCreate
	ModifiedObjectByDdlOperationTypeDrop
	ModifiedObjectByDdlOperationTypeReplace
	ModifiedObjectByDdlOperationTypeUndrop
)

type SnowflakeModifiedObjectsByDdl struct {
	ObjectDomain  string                           `json:"objectDomain"`
	ObjectId      int64                            `json:"objectId"`
	ObjectName    string                           `json:"objectName"`
	OperationType ModifiedObjectByDdlOperationType `json:"operationType"`
}
