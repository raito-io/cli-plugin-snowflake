package common

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	ap "github.com/raito-io/cli/base/access_provider/exporter"
	"github.com/raito-io/cli/base/data_source"
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

// List of all Snowflake keywords: https://docs.snowflake.com/en/sql-reference/sql-all.html
var snowflakeKeywords = []string{"ALTER",
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
	"SELECT",
	"SET",
	"SHOW",
	"TRUNCATE",
	"UNDROP",
	"UNSET",
	"UPDATE",
	"USE"}

var containsKeywordRegex = regexp.MustCompile(fmt.Sprintf(`\b(%s)\b`, strings.Join(snowflakeKeywords, "|")))

func ParseSnowflakeInformation(query string, databaseName string, schemaName string, baseObjectsAccessed *string, directObjectAccessed *string, objectsModified *string) ([]ap.WhatItem, error) {
	numKeywords := 0
	detectedKeywords := []string{}

	res := containsKeywordRegex.FindAllStringSubmatch(query, -1)
	numKeywords = len(res)

	for i := range res {
		detectedKeywords = append(detectedKeywords, res[i][0])
	}

	if numKeywords == 1 {
		detectedObjects := []SnowflakeAccessedObjects{}

		if baseObjectsAccessed != nil {
			objects := []SnowflakeAccessedObjects{}
			err := json.Unmarshal([]byte(*baseObjectsAccessed), &objects)

			if err != nil {
				log.Error(err)
			}

			detectedObjects = append(detectedObjects, objects...)
		}

		if directObjectAccessed != nil {
			objects := []SnowflakeAccessedObjects{}
			err := json.Unmarshal([]byte(*directObjectAccessed), &objects)

			if err != nil {
				log.Error(err)
			}

			detectedObjects = append(detectedObjects, objects...)
		}

		if objectsModified != nil {
			objects := []SnowflakeAccessedObjects{}
			err := json.Unmarshal([]byte(*objectsModified), &objects)

			if err != nil {
				log.Error(err)
			}

			detectedObjects = append(detectedObjects, objects...)
		}

		if len(detectedObjects) > 0 {
			accessObjects := []ap.WhatItem{}

			for _, obj := range detectedObjects {
				accessObjects = append(accessObjects, ap.WhatItem{
					Permissions: []string{detectedKeywords[0]},
					DataObject: &data_source.DataObjectReference{
						FullName: strings.ToUpper(obj.Name), Type: strings.ToLower(obj.Domain)},
				})
			}

			return accessObjects, nil
		}
	} else if numKeywords == 0 {
		return []ap.WhatItem{}, nil
	}

	return ExtractInfoFromQuery(query, databaseName, schemaName)
}

func ExtractInfoFromQuery(query string, databaseName string, schemaName string) ([]ap.WhatItem, error) {
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

	for _, keyword := range unsupportedKeywords {
		returnEmpytObject := strings.HasPrefix(query, strings.ToUpper(keyword))
		if returnEmpytObject {
			return []ap.WhatItem{
				{DataObject: nil, Permissions: []string{strings.ToUpper(keyword)}},
			}, nil
		}
	}

	stmt, err := parser.Parse(query)
	if err != nil {
		return []ap.WhatItem{{DataObject: nil, Permissions: []string{"PARSE_ERROR"}}},
			fmt.Errorf("error parsing query: %s", query)
	}

	if stmt == nil {
		return []ap.WhatItem{{DataObject: nil, Permissions: []string{"EMPTY"}}},
			fmt.Errorf("syntax tree was returned empty for query: %s", query)
	}

	parsedQueries := []SnowflakeAccess{}
	ParseSyntaxTree(stmt, &parsedQueries)

	return ConvertSnowflakeToGeneralDataObjects(parsedQueries, databaseName, schemaName), nil
}

func ConvertSnowflakeToGeneralDataObjects(snowflakeAccess []SnowflakeAccess, databaseName string, schemaName string) []ap.WhatItem {
	generalAccess := []ap.WhatItem{}

	for _, obj := range snowflakeAccess {
		newItem := ap.WhatItem{}
		newItem.Permissions = obj.Permissions

		if obj.Database == "" && databaseName != "" {
			obj.Database = strings.ToUpper(databaseName)
		}

		if obj.Schema == "" && schemaName != "" {
			obj.Schema = strings.ToUpper(schemaName)
		}

		if obj.Column == "" || obj.Column == "*" {
			newItem.DataObject = &data_source.DataObjectReference{
				Type:     "table",
				FullName: fmt.Sprintf("%s.%s.%s", obj.Database, obj.Schema, obj.Table),
			}
		} else {
			newItem.DataObject = &data_source.DataObjectReference{
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
