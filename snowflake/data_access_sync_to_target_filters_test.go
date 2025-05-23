package snowflake

import (
	"context"
	"fmt"
	"testing"

	"strings"

	"github.com/aws/smithy-go/ptr"
	"github.com/raito-io/bexpression"
	"github.com/raito-io/bexpression/datacomparison"
	importer "github.com/raito-io/cli/base/access_provider/sync_to_target"
	"github.com/raito-io/cli/base/access_provider/types"
	"github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestAccessSyncer_SyncAccessProviderFiltersToTarget(t *testing.T) {
	// Given
	configParams := config.ConfigMap{
		Parameters: map[string]string{"key": "value"},
	}

	repo := newMockDataAccessRepository(t)
	fileCreator := mocks.NewSimpleAccessProviderFeedbackHandler(t)

	repo.EXPECT().UpdateFilter("DB1", "Schema1", "Table1", mock.AnythingOfType("string"), mock.AnythingOfType("[]string"),
		mock.AnythingOfType("string")).RunAndReturn(func(_ string, _ string, _ string, filterName string, arguments []string, query string) error {
		assert.True(t, strings.HasPrefix(filterName, "raito_Schema1_Table1_"))
		assert.ElementsMatch(t, []string{"Column1", "state"}, arguments)

		queryPart1 := "(current_user() IN ('User2') OR IS_ROLE_IN_SESSION('Role3')) AND ((100 >= Column1))"
		queryPart2 := "(current_user() IN ('User1', 'User2') OR IS_ROLE_IN_SESSION('Role1')) AND (state = 'NJ')"

		queryOption1 := fmt.Sprintf("%s OR %s", queryPart1, queryPart2)
		queryOption2 := fmt.Sprintf("%s OR %s", queryPart2, queryPart1)

		assert.True(t, query == queryOption1 || query == queryOption2)

		return nil
	})

	repo.EXPECT().UpdateFilter("DB1", "Schema2", "Table1", mock.AnythingOfType("string"), []string{}, "FALSE").RunAndReturn(func(_ string, _ string, _ string, filterName string, _ []string, _ string) error {
		assert.True(t, strings.HasPrefix(filterName, "raito_Schema2_Table1_"))

		return nil
	})

	repo.EXPECT().DropFilter("DB1", "Schema1", "Table3", "RAITO_FILTERTOREMOVE2").Return(nil)

	toProcessApIds := []string{"RAITO_FILTER1", "RAITO_FILTER2", "RAITO_FILTER3", "FilterToRemove1", "FilterToRemove2"}

	apsById := map[string]*ApSyncToTargetItem{
		"RAITO_FILTER1": {
			accessProvider: &importer.AccessProvider{
				Id:     "RAITO_FILTER1",
				Name:   "RAITO_FILTER1",
				Action: types.Filtered,
				What: []importer.WhatItem{
					{
						DataObject: &data_source.DataObjectReference{
							FullName: "DB1.Schema1.Table1",
							Type:     data_source.Table,
						},
					},
				},
				Who: importer.WhoItem{
					Users:       []string{"User1", "User2"},
					InheritFrom: []string{"Role1"},
				},
				PolicyRule: ptr.String("{state} = 'NJ'")},
			calculatedExternalId: "RAITO_FILTER1",
			mutationAction:       ApMutationActionCreate,
		},
		"RAITO_FILTER2": {
			accessProvider: &importer.AccessProvider{
				Id:     "RAITO_FILTER2",
				Name:   "RAITO_FILTER2",
				Action: types.Filtered,
				What: []importer.WhatItem{
					{
						DataObject: &data_source.DataObjectReference{
							FullName: "DB1.Schema1.Table1",
							Type:     data_source.Table,
						},
					},
				},
				Who: importer.WhoItem{
					Users:       []string{"User2"},
					InheritFrom: []string{"ID:Role3-ID"},
				},
				FilterCriteria: &bexpression.DataComparisonExpression{
					Comparison: &datacomparison.DataComparison{
						Operator: datacomparison.ComparisonOperatorGreaterThanOrEqual,
						LeftOperand: datacomparison.Operand{
							Literal: &datacomparison.Literal{Int: ptr.Int(100)},
						},
						RightOperand: datacomparison.Operand{
							Reference: &datacomparison.Reference{
								EntityType: datacomparison.EntityTypeColumnReferenceByName,
								EntityID:   `Column1`,
							},
						},
					},
				}},
			calculatedExternalId: "RAITO_FILTER2",
			mutationAction:       ApMutationActionCreate,
		},
		"RAITO_FILTER3": {
			accessProvider: &importer.AccessProvider{
				Id:     "RAITO_FILTER3",
				Name:   "RAITO_FILTER3",
				Action: types.Filtered,
				What: []importer.WhatItem{
					{
						DataObject: &data_source.DataObjectReference{
							FullName: "DB1.Schema2.Table1",
							Type:     data_source.Table,
						},
					},
				},
				Who: importer.WhoItem{},
				FilterCriteria: &bexpression.DataComparisonExpression{
					Comparison: &datacomparison.DataComparison{
						Operator: datacomparison.ComparisonOperatorGreaterThanOrEqual,
						LeftOperand: datacomparison.Operand{
							Literal: &datacomparison.Literal{Int: ptr.Int(100)},
						},
						RightOperand: datacomparison.Operand{
							Reference: &datacomparison.Reference{
								EntityType: datacomparison.EntityTypeDataObject,
								EntityID:   `{"fullName":"DB1.Schema1.Table1.Column1","id":"JJGSpyjrssv94KPk9dNuI","type":"column"}`,
							},
						},
					},
				}},
			calculatedExternalId: "RAITO_FILTER3",
			mutationAction:       ApMutationActionCreate,
		},
		"FilterToRemove1": {
			accessProvider: &importer.AccessProvider{
				Id:         "FilterToRemove1",
				Action:     types.Filtered,
				ActualName: ptr.String("RAITO_FILTERTOREMOVE1"),
				ExternalId: ptr.String("DB1.Schema1.Table1.RAITO_FILTERTOREMOVE1"),
				What: []importer.WhatItem{
					{
						DataObject: &data_source.DataObjectReference{
							FullName: "DB1.Schema1.Table1",
							Type:     data_source.Table,
						},
					},
				}},
			calculatedExternalId: "RAITO_FILTERTOREMOVE1",
			mutationAction:       ApMutationActionDelete,
		},
		"FilterToRemove2": {
			accessProvider: &importer.AccessProvider{
				Id:         "FilterToRemove2",
				Action:     types.Filtered,
				ActualName: ptr.String("RAITO_FILTERTOREMOVE2"),
				ExternalId: ptr.String("DB1.Schema1.Table3.RAITO_FILTERTOREMOVE2"),
				What: []importer.WhatItem{
					{
						DataObject: &data_source.DataObjectReference{
							FullName: "DB1.Schema1.Table3",
							Type:     data_source.Table,
						},
					},
				}},
			calculatedExternalId: "RAITO_FILTERTOREMOVE2",
			mutationAction:       ApMutationActionDelete,
		},
	}

	syncer := createBasicToTargetSyncer(repo, nil, fileCreator, &configParams)

	// When
	err := syncer.processFiltersToTarget(context.Background(), toProcessApIds, apsById, map[string]string{"Role3-ID": "Role3"})

	// Then
	require.NoError(t, err)
	assert.Len(t, fileCreator.AccessProviderFeedback, 5)
}

func TestAccessSyncer_FilterExpressionOfPolicyRule(t *testing.T) {
	type args struct {
		policyRule string
	}
	tests := []struct {
		name  string
		args  args
		want  string
		want1 []string
	}{
		{
			name: "empty policy rule",
			args: args{
				policyRule: "",
			},
			want:  "",
			want1: []string{},
		},
		{
			name: "simple policy rule, without references",
			args: args{
				policyRule: "SELECT * FROM table1 WHERE column1 = 'value1'",
			},
			want:  "SELECT * FROM table1 WHERE column1 = 'value1'",
			want1: []string{},
		},
		{
			name: "simple policy rule, with references",
			args: args{
				policyRule: "{column1} = 'value1'",
			},
			want:  "column1 = 'value1'",
			want1: []string{"column1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := filterExpressionOfPolicyRule(tt.args.policyRule)
			assert.Equalf(t, tt.want, got, "filterExpressionOfPolicyRule(%v)", tt.args.policyRule)
			assert.Equalf(t, tt.want1, got1, "filterExpressionOfPolicyRule(%v)", tt.args.policyRule)
		})
	}
}
