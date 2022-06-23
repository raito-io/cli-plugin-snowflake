package common

import (
	"fmt"
	"strings"

	"github.com/hashicorp/go-hclog"
	"github.com/raito-io/cli/base"
	ap "github.com/raito-io/cli/base/access_provider"
	"github.com/raito-io/cli/base/data_source"
	parser "github.com/xwb1989/sqlparser"
)

var logger hclog.Logger = base.Logger()

type SnowflakeAccess struct {
	Database    string   `json:"database"`
	Schema      string   `json:"schema"`
	Table       string   `json:"table"`
	Column      string   `json:"column"`
	Permissions []string `json:"permissions"`
}

func ExtractInfoFromQuery(query string, databaseName string, schemaName string) []ap.Access {
	// List of all Snowflake keywords: https://docs.snowflake.com/en/sql-reference/sql-all.html
	// TODO: make checking which keywords need to be parsed more efficient
	unsupportedKeywords := []string{
		"ALTER",
		"BEGIN",
		"CALL",
		"COMMENT",
		"COMMIT",
		"COPY INTO",
		"CREATE",
		"DELETE",
		"DESCRIBE",
		"DROP",
		"EXECUTE",
		"EXPLAIN",
		"GET",
		"GRANT",
		"INSERT",
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
		"UPDATE",
		"USE",
	}

	query = strings.ToUpper(query)
	query = strings.TrimLeft(query, " \t")

	for _, keyword := range unsupportedKeywords {
		returnEmpytObject := strings.HasPrefix(query, strings.ToUpper(keyword))
		if returnEmpytObject {
			return []ap.Access{
				{DataObjectReference: nil, Permissions: []string{strings.ToUpper(keyword)}},
			}
		}
	}

	stmt, err := parser.Parse(query)
	if err != nil {
		logger.Error(fmt.Sprintf("Error parsing SQL query: %s", query))
		return []ap.Access{{DataObjectReference: nil, Permissions: []string{"PARSE_ERROR"}}}
	}

	if stmt == nil {
		logger.Error(fmt.Sprintf("Empty syntax tree from query: %s", query))
		return []ap.Access{{DataObjectReference: nil, Permissions: []string{"EMPTY"}}}
	}

	parsedQueries := []SnowflakeAccess{}
	ParseSyntaxTree(stmt, &parsedQueries)

	return ConvertSnowflakeToGeneralDataObjects(parsedQueries, databaseName, schemaName)
}

func ConvertSnowflakeToGeneralDataObjects(snowflakeAccess []SnowflakeAccess, databaseName string, schemaName string) []ap.Access {
	generalAccess := []ap.Access{}

	for _, obj := range snowflakeAccess {
		newItem := ap.Access{}
		newItem.Permissions = obj.Permissions

		if obj.Database == "" && databaseName != "" {
			obj.Database = strings.ToUpper(databaseName)
		}

		if obj.Schema == "" && schemaName != "" {
			obj.Schema = strings.ToUpper(schemaName)
		}

		if obj.Column == "" || obj.Column == "*" {
			newItem.DataObjectReference = &data_source.DataObjectReference{
				Type:     "table",
				FullName: fmt.Sprintf("%s.%s.%s", obj.Database, obj.Schema, obj.Table),
			}
		} else {
			newItem.DataObjectReference = &data_source.DataObjectReference{
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
		logger.Debug("Parse select query")
		ParseSelectQuery(*v, parsedQueries)
	case parser.Statement:
		logger.Debug("Generic statement, not implemented")
	default:
		logger.Debug(fmt.Sprintf("Unknown type for %s", v))
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
		logger.Debug(fmt.Sprintf("Unknown type for %s", expr))
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
		logger.Debug("Unknown Table expression type")
	}
}

func ExtractTableName(stmt parser.SimpleTableExpr, tableInfo map[string]string) {
	switch expr := stmt.(type) {
	case parser.TableName:
		tableInfo[expr.Name.CompliantName()] = expr.Qualifier.CompliantName()
	case *parser.Subquery:
		logger.Debug("Subquery not implemented")
	}
}
