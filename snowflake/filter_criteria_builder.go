package snowflake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/raito-io/bexpression"
	"github.com/raito-io/bexpression/base"
	"github.com/raito-io/bexpression/datacomparison"
	ds "github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/golang-set/set"
)

var _ base.Visitor = (*FilterCriteriaBuilder)(nil)

type FilterCriteriaBuilder struct {
	stringBuilder strings.Builder
	arguments     set.Set[string]
}

func NewFilterCriteriaBuilder() *FilterCriteriaBuilder {
	return &FilterCriteriaBuilder{
		stringBuilder: strings.Builder{},
		arguments:     set.NewSet[string](),
	}
}

func (f *FilterCriteriaBuilder) GetQueryAndArguments() (string, set.Set[string]) {
	return f.stringBuilder.String(), f.arguments
}

func (f *FilterCriteriaBuilder) EnterExpressionElement(_ context.Context, element base.VisitableElement) error {
	if node, ok := element.(*bexpression.DataComparisonExpression); ok && node.Literal == nil {
		f.stringBuilder.WriteString("(")
	}

	return nil
}

func (f *FilterCriteriaBuilder) LeaveExpressionElement(_ context.Context, element base.VisitableElement) {
	if node, ok := element.(*bexpression.DataComparisonExpression); ok && node.Literal == nil {
		f.stringBuilder.WriteString(")")
	}
}

func (f *FilterCriteriaBuilder) Literal(_ context.Context, l interface{}) error {
	switch node := l.(type) {
	case bool:
		if node {
			f.stringBuilder.WriteString("TRUE")
		} else {
			f.stringBuilder.WriteString("FALSE")
		}
	case int:
		f.stringBuilder.WriteString(fmt.Sprintf("%d", node))
	case float64:
		f.stringBuilder.WriteString(fmt.Sprintf("%f", node))
	case string:
		f.stringBuilder.WriteString(fmt.Sprintf("'%s'", node))
	case time.Time:
		return errors.New("time.Time is not supported yet")
	case datacomparison.ComparisonOperator:
		switch node {
		case datacomparison.ComparisonOperatorEqual:
			f.stringBuilder.WriteString(" = ")
		case datacomparison.ComparisonOperatorNotEqual:
			f.stringBuilder.WriteString(" != ")
		case datacomparison.ComparisonOperatorLessThan:
			f.stringBuilder.WriteString(" < ")
		case datacomparison.ComparisonOperatorLessThanOrEqual:
			f.stringBuilder.WriteString(" <= ")
		case datacomparison.ComparisonOperatorGreaterThan:
			f.stringBuilder.WriteString(" > ")
		case datacomparison.ComparisonOperatorGreaterThanOrEqual:
			f.stringBuilder.WriteString(" >= ")
		}
	case base.AggregatorOperator:
		switch node {
		case base.AggregatorOperatorAnd:
			f.stringBuilder.WriteString(" AND ")
		case base.AggregatorOperatorOr:
			f.stringBuilder.WriteString(" OR ")
		}
	case base.UnaryOperator:
		if node != base.UnaryOperatorNot {
			return errors.New("unsupported unary operator")
		}

		f.stringBuilder.WriteString("NOT ")
	case *datacomparison.Reference:
		if node.EntityType != datacomparison.EntityTypeDataObject {
			return fmt.Errorf("unsupported reference entity type: %s", node.EntityType)
		}

		var object ds.DataObjectReference

		err := json.Unmarshal([]byte(node.EntityID), &object)
		if err != nil {
			return fmt.Errorf("unmarshal reference entity id: %w", err)
		}

		if object.Type != ds.Column {
			return fmt.Errorf("unsupported reference entity type: %s", object.Type)
		}

		parsedDataObject := strings.SplitN(object.FullName, ".", 4)
		if len(parsedDataObject) != 4 {
			return fmt.Errorf("unsupported reference entity id: %s", object.FullName)
		}

		f.stringBuilder.WriteString(parsedDataObject[3])
		f.arguments.Add(parsedDataObject[3])
	}

	return nil
}
