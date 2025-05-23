package snowflake

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/aws/smithy-go/ptr"
	"github.com/hashicorp/go-multierror"
	gonanoid "github.com/matoous/go-nanoid/v2"
	importer "github.com/raito-io/cli/base/access_provider/sync_to_target"
	ds "github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/wrappers"
	"github.com/raito-io/golang-set/set"
)

func (s *AccessToTargetSyncer) processFiltersToTarget(ctx context.Context, toProcessApIds []string, apsById map[string]*ApSyncToTargetItem, mappedGrantExternalIdById map[string]string) error {
	// Do combine update action to create, rename and update filters
	toUpdateItems := s.accessProvidersForMutationActions(toProcessApIds, apsById, []ApMutationAction{ApMutationActionCreate, ApMutationActionRename, ApMutationActionUpdate})
	toRemoveItems := s.accessProvidersForMutationActions(toProcessApIds, apsById, []ApMutationAction{ApMutationActionDelete})

	// Validate if we can set filters on this SF account
	if s.configMap.GetBoolWithDefault(SfStandardEdition, false) {
		if len(toUpdateItems) > 0 || len(toRemoveItems) > 0 {
			Logger.Error("Skipping filter policies due to Snowflake Standard Edition.")
		}

		return nil
	}

	Logger.Info(fmt.Sprintf("Configuring access provider as filters in Snowflake. Need to update %d filters and remove %d filters", len(toUpdateItems), len(toRemoveItems)))

	// Group create or update filters per table
	toUpdateItemsGroupedByTable, err := groupFiltersByTable(toUpdateItems, s.accessProviderFeedbackHandler)
	if err != nil {
		return fmt.Errorf("grouping updatable filters by table: %w", err)
	}

	unableToUpdateTables, err := s.filtersCreateOrUpdateAll(ctx, toUpdateItemsGroupedByTable, mappedGrantExternalIdById)
	if err != nil {
		return fmt.Errorf("updating filters: %w", err)
	}

	// Group to remove filters per table
	err = s.filtersRemoveAll(toRemoveItems, unableToUpdateTables, toUpdateItemsGroupedByTable)
	if err != nil {
		return fmt.Errorf("removing filters: %w", err)
	}

	return nil
}

func (s *AccessToTargetSyncer) filtersCreateOrUpdateAll(ctx context.Context, updateGroupedFilters map[string][]*importer.AccessProvider, mappedGrantExternalIdById map[string]string) (set.Set[string], error) {
	unableToUpdateTables := set.NewSet[string]()

	for table, filters := range updateGroupedFilters {
		filterName, externalId, createErr := s.filterCreateOrUpdateItem(ctx, table, filters, mappedGrantExternalIdById)

		feedbackErr := s.filterFeedbackProcessor(filters, filterName, externalId, createErr)
		if feedbackErr != nil {
			return nil, fmt.Errorf("failed to process feedback for filter %q: %w", *filterName, feedbackErr)
		}

		if createErr != nil {
			unableToUpdateTables.Add(table)
		}
	}

	return unableToUpdateTables, nil
}

func (s *AccessToTargetSyncer) filtersRemoveAll(toRemoveItems []*ApSyncToTargetItem, unableToUpdateTables set.Set[string], toUpdateItemsGroupedByTable map[string][]*importer.AccessProvider) error {
	removeGroupedFilters, err := groupFiltersByTable(toRemoveItems, s.accessProviderFeedbackHandler)
	if err != nil {
		return fmt.Errorf("grouping removable filters by table: %w", err)
	}

	for table, filters := range removeGroupedFilters {
		var apFeedbackErr error

		// Check if the filter is in the list of filters to be updated
		if _, found := toUpdateItemsGroupedByTable[table]; found && !unableToUpdateTables.Contains(table) {
			apFeedbackErr = fmt.Errorf("prevent deletion of filter because unable to create new filter in table %q", table)
		} else {
			apFeedbackErr = s.filterDeleteItem(table, filters)
		}

		feedbackErr := s.filterFeedbackProcessor(filters, nil, nil, apFeedbackErr)
		if feedbackErr != nil {
			return fmt.Errorf("failed to process feedback for filter %q: %w", table, feedbackErr)
		}
	}

	return nil
}

func groupFiltersByTable(aps []*ApSyncToTargetItem, feedbackHandler wrappers.AccessProviderFeedbackHandler) (map[string][]*importer.AccessProvider, error) {
	groupedFilters := make(map[string][]*importer.AccessProvider)

	for _, filter := range aps {
		if len(filter.accessProvider.What) != 1 || filter.accessProvider.What[0].DataObject.Type != ds.Table {
			err := feedbackHandler.AddAccessProviderFeedback(importer.AccessProviderSyncFeedback{
				AccessProvider: filter.accessProvider.Id,
				Errors:         []string{"Filters can only be applied to a single table."},
			})

			if err != nil {
				return nil, fmt.Errorf("failed to add access provider feedback: %w", err)
			}

			continue
		}

		table := filter.accessProvider.What[0].DataObject.FullName

		groupedFilters[table] = append(groupedFilters[table], filter.accessProvider)
	}

	return groupedFilters, nil
}

func (s *AccessToTargetSyncer) filterCreateOrUpdateItem(ctx context.Context, tableFullName string, aps []*importer.AccessProvider, roleNameMap map[string]string) (*string, *string, error) {
	tableFullNameSplit := strings.Split(tableFullName, ".")
	database := tableFullNameSplit[0]
	schema := tableFullNameSplit[1]
	table := tableFullNameSplit[2]

	filterExpressions := make([]string, 0, len(aps))
	arguments := set.NewSet[string]()

	for _, ap := range aps {
		fExpression, apArguments, err := filterExpression(ctx, ap)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to generate filter expression for access provider %s: %w", ap.Name, err)
		}

		whoExpressionPart, hasWho := filterWhoExpression(ap, roleNameMap)

		if !hasWho {
			continue
		}

		filterExpressions = append(filterExpressions, fmt.Sprintf("(%s) AND (%s)", whoExpressionPart, fExpression))

		arguments.Add(apArguments...)

		Logger.Info(fmt.Sprintf("Filter expression for access provider %s: %s (%+v)", ap.Name, fExpression, apArguments))
	}

	if len(filterExpressions) == 0 {
		// No filter expression for example when no who was defined for the filter
		Logger.Info("No filter expressions found for table %s.", tableFullName)

		filterExpressions = append(filterExpressions, "FALSE")
	}

	filterName := fmt.Sprintf("raito_%s_%s_%s_filter", schema, table, gonanoid.MustGenerate(idAlphabet, 8))

	err := s.repo.UpdateFilter(database, schema, table, filterName, arguments.Slice(), strings.Join(filterExpressions, " OR "))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to update filter %s: %w", filterName, err)
	}

	return ptr.String(filterName), ptr.String(fmt.Sprintf("%s.%s", tableFullName, filterName)), nil
}

func (s *AccessToTargetSyncer) filterFeedbackProcessor(aps []*importer.AccessProvider, actualName *string, externalId *string, err error) error {
	var feedbackErr error

	var errorMessages []string

	if err != nil {
		errorMessages = []string{err.Error()}
	}

	for _, ap := range aps {
		// Actually, actual name isn't even relevant for filters
		var actualNameStr string
		if actualName != nil {
			actualNameStr = *actualName
		}

		var apExternalId *string
		if externalId != nil {
			apExternalId = externalId
		} else {
			apExternalId = ap.ExternalId
		}

		apFeedbackErr := s.accessProviderFeedbackHandler.AddAccessProviderFeedback(importer.AccessProviderSyncFeedback{
			AccessProvider: ap.Id,
			ActualName:     actualNameStr,
			ExternalId:     apExternalId,
			Errors:         errorMessages,
		})
		if apFeedbackErr != nil {
			feedbackErr = multierror.Append(feedbackErr, apFeedbackErr)
		}
	}

	return feedbackErr
}

func (s *AccessToTargetSyncer) filterDeleteItem(tableFullName string, aps []*importer.AccessProvider) error {
	tableFullNameSplit := strings.Split(tableFullName, ".")
	database := tableFullNameSplit[0]
	schema := tableFullNameSplit[1]
	table := tableFullNameSplit[2]

	filterNames := set.NewSet[string]()

	for _, ap := range aps {
		if ap.ExternalId != nil {
			externalIdSplit := strings.Split(*ap.ExternalId, ".")
			filterNames.Add(externalIdSplit[3])
		}
	}

	var err error

	for filterName := range filterNames {
		deleteErr := s.repo.DropFilter(database, schema, table, filterName)
		if deleteErr != nil {
			err = multierror.Append(err, fmt.Errorf("failed to delete filter %s: %w", filterName, deleteErr))
		}
	}

	if err != nil {
		return err
	}

	return nil
}

func filterExpression(ctx context.Context, ap *importer.AccessProvider) (string, []string, error) {
	if ap.FilterCriteria != nil {
		filterQueryBuilder := NewFilterCriteriaBuilder()

		err := ap.FilterCriteria.Accept(ctx, filterQueryBuilder)
		if err != nil {
			return "", nil, fmt.Errorf("failed to generate filter expression for access provider %s: %w", ap.Name, err)
		}

		query, arguments := filterQueryBuilder.GetQueryAndArguments()

		return query, arguments.Slice(), nil
	} else if ap.PolicyRule != nil {
		query, arguments := filterExpressionOfPolicyRule(*ap.PolicyRule)

		return query, arguments, nil
	}

	return "", nil, errors.New("no filter criteria or policy rule")
}

func filterExpressionOfPolicyRule(policyRule string) (string, []string) {
	argumentRegexp := regexp.MustCompile(`\{([a-zA-Z0-9]+)}`)

	argumentsSubMatches := argumentRegexp.FindAllStringSubmatch(policyRule, -1)
	query := argumentRegexp.ReplaceAllString(policyRule, "$1")

	arguments := make([]string, 0, len(argumentsSubMatches))
	for _, match := range argumentsSubMatches {
		arguments = append(arguments, match[1])
	}

	return query, arguments
}

func filterWhoExpression(ap *importer.AccessProvider, roleNameMap map[string]string) (string, bool) {
	whoExpressionParts := make([]string, 0, 2)

	{
		users := make([]string, 0, len(ap.Who.Users))

		for _, user := range ap.Who.Users {
			users = append(users, fmt.Sprintf("'%s'", user))
		}

		if len(users) > 0 {
			whoExpressionParts = append(whoExpressionParts, fmt.Sprintf("current_user() IN (%s)", strings.Join(users, ", ")))
		}
	}

	{
		roles := make([]string, 0, len(ap.Who.InheritFrom))

		for _, role := range ap.Who.InheritFrom {
			if strings.HasPrefix(role, "ID:") {
				if roleName, found := roleNameMap[role[3:]]; found {
					roles = append(roles, fmt.Sprintf("IS_ROLE_IN_SESSION('%s')", roleName))
				}
			} else {
				roles = append(roles, fmt.Sprintf("IS_ROLE_IN_SESSION('%s')", role))
			}
		}

		if len(roles) > 0 {
			whoExpressionParts = append(whoExpressionParts, strings.Join(roles, " OR "))
		}
	}

	if len(whoExpressionParts) == 0 {
		return "FALSE", false
	}

	return strings.Join(whoExpressionParts, " OR "), true
}
