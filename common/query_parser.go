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
	Schema      string   `json:"schema"`
	Table       string   `json:"table"`
	Column      string   `json:"column"`
	Permissions []string `json:"permissions"`
}

func ExtractInfoFromQuery(query string) []ap.Access {

	// List of all Snowflake keywords: https://docs.snowflake.com/en/sql-reference/sql-all.html
	// supportedKeywords := []string{
	// 	"SELECT",
	// 	"SHOW",
	// 	"GRANT",
	// }
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

	query = strings.ToLower(query)
	query = strings.TrimLeft(query, " \t")
	for _, keyword := range unsupportedKeywords {
		returnEmpytObject := strings.HasPrefix(query, strings.ToLower(keyword))
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
	return ConvertSnowflakeToGeneralDataObjects(parsedQueries)
}

func ConvertSnowflakeToGeneralDataObjects(snowflakeAccess []SnowflakeAccess) []ap.Access {
	generalAccess := []ap.Access{}
	for _, obj := range snowflakeAccess {
		newItem := ap.Access{}
		newItem.Permissions = obj.Permissions
		if obj.Column == "" || obj.Column == "*" {
			newItem.DataObjectReference = &data_source.DataObjectReference{
				Type:     "table",
				FullName: fmt.Sprintf("%s.%s", obj.Schema, obj.Table),
			}
			newItem.DataObjectReference.Type = "table"
		} else {
			newItem.DataObjectReference = &data_source.DataObjectReference{
				Type:     "table",
				FullName: fmt.Sprintf("%s.%s.%s", obj.Schema, obj.Table, obj.Column),
			}
			newItem.DataObjectReference.Type = "column"

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
		ExtractFromClauseInfo(expr, &TableSchema)
	}

	numberOfTables := len(TableSchema)
	TableName := ""
	SchemaName := ""
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

func ParseSelectExpression(expr parser.SelectExpr, accessedsnowflakeObjects *[]SnowflakeAccess) {

	switch expr := expr.(type) {
	case *parser.AliasedExpr:
		aliasedSubExpr := expr.Expr
		switch aliasedExpr := aliasedSubExpr.(type) {
		case *parser.ColName:
			accessedsnowflakeObject := SnowflakeAccess{
				Column:      aliasedExpr.Name.String(),
				Table:       aliasedExpr.Qualifier.Name.String(),
				Permissions: []string{"SELECT"},
			}
			*accessedsnowflakeObjects = append(*accessedsnowflakeObjects, accessedsnowflakeObject)
		case *parser.FuncExpr:
			for _, subExpr := range aliasedExpr.Exprs {
				ParseSelectExpression(subExpr, accessedsnowflakeObjects)
			}
		case *parser.Subquery:
			switch subQueryExpr := aliasedExpr.Select.(type) {
			case *parser.Select:
				ParseSelectQuery(*subQueryExpr, accessedsnowflakeObjects)
			}
		}
	case *parser.StarExpr:
		accessedsnowflakeObject := SnowflakeAccess{
			Column:      "*",
			Table:       expr.TableName.Name.String(),
			Permissions: []string{"SELECT"},
		}
		*accessedsnowflakeObjects = append(*accessedsnowflakeObjects, accessedsnowflakeObject)
	default:
		logger.Debug(fmt.Sprintf("Unknown type for %s", expr))
	}
}

func ExtractFromClauseInfo(stmt parser.TableExpr, TableInfo *map[string]string) {
	// cases to cover:
	//func (*AliasedTableExpr) iTableExpr() {}
	//func (*ParenTableExpr) iTableExpr()   {}
	//func (*JoinTableExpr) iTableExpr()    {}
	switch expr := stmt.(type) {
	case *parser.AliasedTableExpr:
		ExtractTableName(expr.Expr, TableInfo)
	case *parser.ParenTableExpr:
		expressions := expr.Exprs
		for _, subExpr := range expressions {
			ExtractFromClauseInfo(subExpr, TableInfo)
		}
		fmt.Println(expressions)
	case *parser.JoinTableExpr:
		lefExpr := expr.LeftExpr
		ExtractFromClauseInfo(lefExpr, TableInfo)
		rightExpr := expr.RightExpr
		ExtractFromClauseInfo(rightExpr, TableInfo)
	default:
		logger.Debug("Unknown Table expression type")
	}
}

func ExtractTableName(stmt parser.SimpleTableExpr, TableInfo *map[string]string) {
	switch expr := stmt.(type) {
	case parser.TableName:
		(*TableInfo)[expr.Name.CompliantName()] = expr.Qualifier.CompliantName()
	case *parser.Subquery:
		logger.Debug("Subquery not implemented")
	}
}
