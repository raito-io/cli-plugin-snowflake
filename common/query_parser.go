package common

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/raito-io/cli/base/data_usage"
	log "github.com/sirupsen/logrus"
	parser "github.com/xwb1989/sqlparser"
)

type SnowflakeAccess struct {
	Database    string   `json:"database"`
	Schema      string   `json:"schema"`
	Table       string   `json:"table"`
	Column      string   `json:"column"`
	Permissions []string `json:"permissions"`
}

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

// List of all Snowflake keywords: https://docs.snowflake.com/en/sql-reference/sql-all.html
var snowflakeKeywords = map[string]data_usage.ActionType{
	"ALTER":     data_usage.Admin,
	"BEGIN":     data_usage.UnknownAction,
	"CALL":      data_usage.UnknownAction,
	"COMMENT":   data_usage.Admin,
	"COMMIT":    data_usage.UnknownAction,
	"COPY INTO": data_usage.Write,
	"CREATE":    data_usage.Write,
	"DELETE":    data_usage.Write,
	"DESCRIBE":  data_usage.Read,
	"DROP":      data_usage.Admin,
	"EXECUTE":   data_usage.UnknownAction,
	"EXPLAIN":   data_usage.UnknownAction,
	"GET":       data_usage.Read,
	"GRANT":     data_usage.Admin,
	"INSERT":    data_usage.Write,
	"LIST":      data_usage.Read,
	"MERGE":     data_usage.Write,
	"PUT":       data_usage.Write,
	"REMOVE":    data_usage.Write,
	"REVOKE":    data_usage.Admin,
	"ROLLBACK":  data_usage.UnknownAction,
	"SELECT":    data_usage.Read,
	"SET":       data_usage.Admin,
	"SHOW":      data_usage.UnknownAction,
	"TRUNCATE":  data_usage.Write,
	"UNDROP":    data_usage.Admin,
	"UNSET":     data_usage.Admin,
	"UPDATE":    data_usage.Write,
	"USE":       data_usage.UnknownAction,
}

var snowflakeIdentifierRegex = regexp.MustCompile(`(?m)"([a-zA-Z0-9_$]*)"`)

func ExtractInfoFromQuery(query string, databaseName string, schemaName string) ([]data_usage.UsageDataObjectItem, error) {
	// TODO: make checking which keywords need to be parsed more efficient
	unsupportedKeywords := []string{
		"ALTER",
		"BEGIN",
		"CALL",
		"COMMENT",
		"COMMIT",
		"COPY INTO",
		// "CREATE",
		// "DELETE",
		"DESCRIBE",
		"DROP",
		"EXECUTE",
		"EXPLAIN",
		"GET",
		"GRANT",
		// "INSERT",
		"LIST",
		"MERGE",
		"PUT",
		"REMOVE",
		"REVOKE",
		"ROLLBACK",
		// "SELECT",
		"SET",
		"SHOW",
		"TRUNCATE",
		"UNDROP",
		"UNSET",
		// "UPDATE",
		"USE",
	}

	query = strings.ToUpper(query)
	query = strings.TrimLeft(query, " \t")
	query = snowflakeIdentifierRegex.ReplaceAllString(query, "$1") //Remove double quotes from identifiers

	for _, keyword := range unsupportedKeywords {
		returnEmpytObject := strings.HasPrefix(query, strings.ToUpper(keyword))
		if returnEmpytObject {
			return nil, nil
		}
	}

	stmt, err := parser.Parse(query)
	if err != nil {
		return nil, fmt.Errorf("error parsing query: %s", query)
	}

	if stmt == nil {
		return nil, fmt.Errorf("syntax tree was returned empty for query: %s", query)
	}

	parsedQueries := []SnowflakeAccess{}
	ParseSyntaxTree(stmt, &parsedQueries)

	return ConvertSnowflakeToGeneralDataObjects(parsedQueries, databaseName, schemaName), nil
}

func ConvertSnowflakeToGeneralDataObjects(snowflakeAccess []SnowflakeAccess, databaseName string, schemaName string) []data_usage.UsageDataObjectItem {
	generalAccess := make([]data_usage.UsageDataObjectItem, 0, len(snowflakeAccess))

	for _, obj := range snowflakeAccess {
		globalPermission := data_usage.UnknownAction
		for _, permission := range obj.Permissions {
			if action, found := snowflakeKeywords[permission]; found && action > globalPermission {
				globalPermission = action
			}
		}

		newItem := data_usage.UsageDataObjectItem{
			Permissions:      obj.Permissions,
			GlobalPermission: globalPermission,
		}

		if obj.Database == "" && databaseName != "" {
			obj.Database = strings.ToUpper(databaseName)
		}

		if obj.Schema == "" && schemaName != "" {
			obj.Schema = strings.ToUpper(schemaName)
		}

		if obj.Column == "" || obj.Column == "*" {
			newItem.DataObject = data_usage.UsageDataObjectReference{
				Type:     "table",
				FullName: fmt.Sprintf("%s.%s.%s", obj.Database, obj.Schema, obj.Table),
			}
		} else {
			newItem.DataObject = data_usage.UsageDataObjectReference{
				Type:     "column",
				FullName: fmt.Sprintf("%s.%s.%s.%s", obj.Database, obj.Schema, obj.Table, obj.Column),
			}
		}

		generalAccess = append(generalAccess, newItem)
	}

	return generalAccess
}

func ParseSyntaxTree(stmt parser.Statement, parsedQueries *[]SnowflakeAccess) {
	switch v := stmt.(type) {
	case *parser.Select:
		log.Debug("Parse select query")
		ParseSelectQuery(*v, parsedQueries)
	case *parser.DDL:
		log.Debug("Parse DDL query")
		ddlInfo := SnowflakeAccess{Table: v.NewName.Name.CompliantName(), Permissions: []string{strings.ToUpper(v.Action)}}
		*parsedQueries = append(*parsedQueries, ddlInfo)
	case *parser.Insert:
		log.Debug("Parse Insert query")
		insertInfo := SnowflakeAccess{Table: v.Table.Name.CompliantName(), Permissions: []string{strings.ToUpper(v.Action)}}
		*parsedQueries = append(*parsedQueries, insertInfo)
	case *parser.Update, *parser.Delete:
		statementType := fmt.Sprintf("%T", v)
		action := ""
		var tableExpressions parser.TableExprs

		if statementType == "*sqlparser.Delete" {
			action = "DELETE"
			tableExpressions = v.(*parser.Delete).TableExprs
		} else if statementType == "*sqlparser.Update" {
			action = "UPDATE"
			tableExpressions = v.(*parser.Update).TableExprs
		}

		log.Debug("Parse Update/delete query")
		var TableSchema = make(map[string]string)

		for _, expr := range tableExpressions {
			ExtractFromClauseInfo(expr, TableSchema)
		}

		for k := range TableSchema {
			updateInfo := SnowflakeAccess{Table: k, Permissions: []string{strings.ToUpper(action)}}
			*parsedQueries = append(*parsedQueries, updateInfo)
		}
	case parser.Statement:
		log.Debug("Generic statement, not implemented")
	default:
		log.Debug(fmt.Sprintf("Unknown type for %s", v))
	}
}

func (snowflakeObject SnowflakeAccess) String() string {
	return fmt.Sprintf("Schema: %s, Table: %s, Column: %s", snowflakeObject.Schema, snowflakeObject.Table, snowflakeObject.Column)
}

func ParseSelectQuery(stmt parser.Select, objectsFromQueries *[]SnowflakeAccess) {
	selectExpressions := stmt.SelectExprs
	fromClauses := stmt.From

	objectsFromThisQuery := []SnowflakeAccess{}
	for _, selectExpr := range selectExpressions {
		ParseSelectExpression(selectExpr, &objectsFromThisQuery)
	}

	var TableSchema = make(map[string]string)
	for _, expr := range fromClauses {
		ExtractFromClauseInfo(expr, TableSchema)
	}

	TableName := ""
	SchemaName := ""

	numberOfTables := len(TableSchema)
	if numberOfTables == 1 {
		for k := range TableSchema {
			TableName = k
			SchemaName = TableSchema[k]
		}
	}

	for i := 0; i < len(objectsFromThisQuery); i++ {
		ptr := &(objectsFromThisQuery[i])
		if numberOfTables == 1 && ptr.Table == "" {
			ptr.Table = TableName
			ptr.Schema = SchemaName
		}

		if val, ok := TableSchema[ptr.Table]; ok {
			ptr.Schema = val
		}
	}

	*objectsFromQueries = append(*objectsFromQueries, objectsFromThisQuery...)
}

func ParseSelectExpression(expr parser.SelectExpr, accessedSnowflakeObjects *[]SnowflakeAccess) {
	switch expr := expr.(type) {
	case *parser.AliasedExpr:
		aliasedSubExpr := expr.Expr
		switch aliasedExpr := aliasedSubExpr.(type) {
		case *parser.ColName:
			accessedSnowflakeObject := SnowflakeAccess{
				Column:      aliasedExpr.Name.String(),
				Table:       aliasedExpr.Qualifier.Name.String(),
				Permissions: []string{"SELECT"},
			}
			*accessedSnowflakeObjects = append(*accessedSnowflakeObjects, accessedSnowflakeObject)
		case *parser.FuncExpr:
			for _, subExpr := range aliasedExpr.Exprs {
				ParseSelectExpression(subExpr, accessedSnowflakeObjects)
			}
		case *parser.Subquery:
			if subQueryExpr, ok := aliasedExpr.Select.(*parser.Select); ok {
				ParseSelectQuery(*subQueryExpr, accessedSnowflakeObjects)
			}
		}
	case *parser.StarExpr:
		accessedSnowflakeObject := SnowflakeAccess{
			Column:      "*",
			Table:       expr.TableName.Name.String(),
			Permissions: []string{"SELECT"},
		}
		*accessedSnowflakeObjects = append(*accessedSnowflakeObjects, accessedSnowflakeObject)
	default:
		log.Debug(fmt.Sprintf("Unknown type for %s", expr))
	}
}

func ExtractFromClauseInfo(stmt parser.TableExpr, tableInfo map[string]string) {
	// cases to cover:
	//func (*AliasedTableExpr) iTableExpr() {}
	//func (*ParenTableExpr) iTableExpr()   {}
	//func (*JoinTableExpr) iTableExpr()    {}
	switch expr := stmt.(type) {
	case *parser.AliasedTableExpr:
		ExtractTableName(expr.Expr, tableInfo)
	case *parser.ParenTableExpr:
		expressions := expr.Exprs
		for _, subExpr := range expressions {
			ExtractFromClauseInfo(subExpr, tableInfo)
		}

	case *parser.JoinTableExpr:
		lefExpr := expr.LeftExpr
		ExtractFromClauseInfo(lefExpr, tableInfo)
		rightExpr := expr.RightExpr
		ExtractFromClauseInfo(rightExpr, tableInfo)
	default:
		log.Debug("Unknown Table expression type")
	}
}

func ExtractTableName(stmt parser.SimpleTableExpr, tableInfo map[string]string) {
	switch expr := stmt.(type) {
	case parser.TableName:
		tableInfo[expr.Name.CompliantName()] = expr.Qualifier.CompliantName()
	case *parser.Subquery:
		log.Debug("Subquery not implemented")
	}
}
