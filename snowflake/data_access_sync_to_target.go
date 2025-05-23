package snowflake

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	importer "github.com/raito-io/cli/base/access_provider/sync_to_target"
	"github.com/raito-io/cli/base/access_provider/sync_to_target/naming_hint"
	"github.com/raito-io/cli/base/access_provider/types"
	ds "github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/util/slice"
	"github.com/raito-io/cli/base/wrappers"
	"github.com/raito-io/golang-set/set"

	"github.com/raito-io/cli-plugin-snowflake/common"
)

type ApMutationAction int

const (
	ApMutationActionCreate ApMutationAction = iota
	ApMutationActionRename
	ApMutationActionUpdate
	ApMutationActionDelete
)

type ApSyncToTargetItem struct {
	mutationAction       ApMutationAction
	calculatedExternalId string
	accessProvider       *importer.AccessProvider
}

type AccessToTargetSyncer struct {
	configMap                     *config.ConfigMap
	namingConstraints             naming_hint.NamingConstraints
	repo                          dataAccessRepository
	accessSyncer                  *AccessSyncer
	accessProviders               *importer.AccessProviderImport
	accessProviderFeedbackHandler wrappers.AccessProviderFeedbackHandler

	ignoreLinksToRole          []string
	databaseRoleSupportEnabled bool

	uniqueRoleNameGeneratorsCache map[*string]naming_hint.UniqueGenerator
	tablesPerSchemaCache          map[string][]TableEntity
	functionsPerSchemaCache       map[string][]FunctionEntity
	proceduresPerSchemaCache      map[string][]ProcedureEntity
	schemasPerDataBaseCache       map[string][]SchemaEntity
	warehousesCache               []DbEntity
	integrationsCache             []DbEntity
}

func NewAccessToTargetSyncer(accessSyncer *AccessSyncer, namingConstraints naming_hint.NamingConstraints, repo dataAccessRepository, accessProviders *importer.AccessProviderImport, accessProviderFeedbackHandler wrappers.AccessProviderFeedbackHandler, configMap *config.ConfigMap) *AccessToTargetSyncer {
	return &AccessToTargetSyncer{
		accessSyncer:                  accessSyncer,
		accessProviders:               accessProviders,
		accessProviderFeedbackHandler: accessProviderFeedbackHandler,
		configMap:                     configMap,
		repo:                          repo,
		tablesPerSchemaCache:          make(map[string][]TableEntity),
		functionsPerSchemaCache:       make(map[string][]FunctionEntity),
		proceduresPerSchemaCache:      make(map[string][]ProcedureEntity),
		schemasPerDataBaseCache:       make(map[string][]SchemaEntity),
		uniqueRoleNameGeneratorsCache: make(map[*string]naming_hint.UniqueGenerator),
		namingConstraints:             namingConstraints,
	}
}

func (s *AccessToTargetSyncer) syncToTarget(ctx context.Context) error {
	s.databaseRoleSupportEnabled = s.configMap.GetBoolWithDefault(SfDatabaseRoles, false)

	existingRoles, err := s.retrieveExistingRoles()
	if err != nil {
		return fmt.Errorf("retrieving existing roles: %w", err)
	}

	ignoreLinksToRoles := s.configMap.GetString(SfIgnoreLinksToRoles)
	if ignoreLinksToRoles != "" {
		s.ignoreLinksToRole = slice.ParseCommaSeparatedList(ignoreLinksToRoles)
	}

	// Expected AP action types that are supported by the SF plugin
	supportedActions := []types.Action{types.Mask, types.Filtered, types.Share, types.Grant}

	var toProcessSortedApIdsByAction map[types.Action][]string
	var toProcessApsById map[string]*ApSyncToTargetItem

	toProcessSortedApIdsByAction, toProcessApsById, err = s.splitItemsByAccessProviderAction(supportedActions, existingRoles)
	if err != nil {
		return err
	}

	mappedGrantExternalIdById := s.mapCalculatedExternalIdByApId(toProcessSortedApIdsByAction, toProcessApsById)

	// Step 1 update/remove all masks
	if toProcessSortedApIdsByAction[types.Mask] != nil {
		err = s.processMasksToTarget(toProcessSortedApIdsByAction[types.Mask], toProcessApsById, mappedGrantExternalIdById)
		if err != nil {
			return fmt.Errorf("processing access provider masks to target: %w", err)
		}
	}

	// Step 2 update/remove all filters
	if toProcessSortedApIdsByAction[types.Filtered] != nil {
		err = s.processFiltersToTarget(ctx, toProcessSortedApIdsByAction[types.Filtered], toProcessApsById, mappedGrantExternalIdById)
		if err != nil {
			return fmt.Errorf("processing access provider filters to target: %w", err)
		}
	}

	// Step 3 update/remove all shares
	if toProcessSortedApIdsByAction[types.Share] != nil {
		err = s.processSharesToTarget(toProcessSortedApIdsByAction[types.Share], toProcessApsById)
		if err != nil {
			return fmt.Errorf("processing access provider shares to target: %w", err)
		}
	}

	// Step 4 update/remove all grants
	if toProcessSortedApIdsByAction[types.Grant] != nil {
		err = s.syncGrantsToTarget(ctx, toProcessSortedApIdsByAction[types.Grant], toProcessApsById)
		if err != nil {
			return fmt.Errorf("processing access provider grants to target: %w", err)
		}
	}

	return nil
}

func (s *AccessToTargetSyncer) mapCalculatedExternalIdByApId(toProcessSortedApIdsByAction map[types.Action][]string, toProcessApsById map[string]*ApSyncToTargetItem) map[string]string {
	grantsById := make(map[string]string)

	if toProcessSortedApIdsByAction[types.Grant] != nil {
		for _, apId := range toProcessSortedApIdsByAction[types.Grant] {
			ap, found := toProcessApsById[apId]
			if !found {
				Logger.Error(fmt.Sprintf("Access provider with ID %q not found in map", apId))
				continue
			}

			grantsById[ap.accessProvider.Id] = ap.calculatedExternalId
		}
	}

	return grantsById
}

func (s *AccessToTargetSyncer) splitItemsByAccessProviderAction(supportedActions []types.Action, existingRoles set.Set[string]) (map[types.Action][]string, map[string]*ApSyncToTargetItem, error) {
	toProcessApsById := make(map[string]*ApSyncToTargetItem)
	toProcessSortedApIdsByAction := make(map[types.Action][]string)

	for _, ap := range s.accessProviders.AccessProviders {
		if !slices.Contains(supportedActions, ap.Action) {
			err := s.accessProviderFeedbackHandler.AddAccessProviderFeedback(importer.AccessProviderSyncFeedback{
				AccessProvider: ap.Id,
				Errors:         []string{fmt.Sprintf("Unsupported action %s", ap.Action.String())},
			})

			if err != nil {
				return nil, nil, fmt.Errorf("handling feedback for a unsupported action AP: %w", err)
			}

			continue
		}

		if _, found := toProcessSortedApIdsByAction[ap.Action]; !found {
			// create a new set for the action
			toProcessSortedApIdsByAction[ap.Action] = make([]string, 0)
		}

		// Generate expected ExternalID
		externalId, err := s.generateUniqueExternalId(ap, "")
		if err != nil {
			return nil, nil, fmt.Errorf("generating unique externalId: %w", err)
		}

		// by default, we will see an item as an update
		mutationAction := ApMutationActionUpdate

		if ap.Delete {
			// We need to delete the role
			mutationAction = ApMutationActionDelete

			if ap.ExternalId == nil {
				Logger.Warn(fmt.Sprintf("No externalId defined for deleted access provider %q. This will be ignored", ap.Id))

				err = s.accessProviderFeedbackHandler.AddAccessProviderFeedback(importer.AccessProviderSyncFeedback{
					AccessProvider: ap.Id,
				})
				if err != nil {
					return nil, nil, fmt.Errorf("handling feedback for a unsupported action AP: %w", err)
				}

				continue
			}

			externalId = *ap.ExternalId
		} else if ap.ExternalId == nil {
			// Will create a new role
			mutationAction = ApMutationActionCreate
		} else if s.cutOffUniquenessSuffixOnExternalId(*ap.ExternalId) != s.cutOffUniquenessSuffixOnExternalId(externalId) {
			// We should rename an existing role.
			// After that, we should make sure the new externalId is unique and does not exist in the existing roles list.
			mutationAction = ApMutationActionRename

			for existingRoles.Contains(externalId) {
				// Generate a new externalId until it doesn't exist
				externalId, err = s.generateUniqueExternalId(ap, "")
				if err != nil {
					return nil, nil, fmt.Errorf("generating unique externalId: %w", err)
				}
			}
		}

		toProcessApsById[ap.Id] = &ApSyncToTargetItem{
			mutationAction:       mutationAction,
			calculatedExternalId: externalId,
			accessProvider:       ap,
		}

		toProcessSortedApIdsByAction[ap.Action] = append(toProcessSortedApIdsByAction[ap.Action], ap.Id)
	}

	return toProcessSortedApIdsByAction, toProcessApsById, nil
}

func (s *AccessToTargetSyncer) cutOffUniquenessSuffixOnExternalId(externalId string) string {
	originalNameSplit := strings.Split(externalId, fmt.Sprintf("%[1]c%[1]c", s.namingConstraints.SplitCharacter()))
	originalName := originalNameSplit[0]

	return originalName
}

func (s *AccessToTargetSyncer) generateUniqueExternalId(ap *importer.AccessProvider, prefix string) (string, error) {
	if isDatabaseRole(ap.Type) {
		return s.generateUniqueExternalIdInNamespace(ap, prefix, parseDatabaseRoleExternalId, databaseRoleExternalIdGenerator)
	} else if isApplicationRole(ap.Type) {
		return s.generateUniqueExternalIdInNamespace(ap, prefix, parseApplicationRoleExternalId, applicationRoleExternalIdGenerator)
	} else {
		uniqueRoleNameGenerator, err := s.uniqueRoleNameGenerator(prefix, nil)
		if err != nil {
			return "", err
		}

		roleName, err := uniqueRoleNameGenerator.Generate(ap)
		if err != nil {
			return "", err
		}

		Logger.Info(fmt.Sprintf("Generated account role name %q", roleName))

		return accountRoleExternalIdGenerator(roleName), nil
	}
}

func (s *AccessToTargetSyncer) generateUniqueExternalIdInNamespace(ap *importer.AccessProvider, prefix string, parseNamespaceRoleExternalId func(string) (string, string, error), externalIdGenerator func(string, string) string) (string, error) {
	sfRoleName := ap.Name
	if ap.NamingHint != "" {
		sfRoleName = ap.NamingHint
	}

	// Finding the database where this db role is linked to
	var database string
	var err error

	if len(ap.What) > 0 {
		// If there is a WHAT, we look for the database of the first element
		parts := strings.Split(ap.What[0].DataObject.FullName, ".")
		database = parts[0]
	} else if ap.ExternalId != nil {
		// Otherwise, we try to parse the externalId
		database, _, err = parseNamespaceRoleExternalId(*ap.ExternalId)

		if err != nil {
			return "", err
		}
	} else {
		return "", errors.New("unable to determine database for database role")
	}

	uniqueRoleNameGenerator, err := s.uniqueRoleNameGenerator(prefix, &database)
	if err != nil {
		return "", err
	}

	// Temp updating namingHint to "resource only without database" as this is the way Generate will create a unique resource name
	oldNamingHint := ap.NamingHint
	ap.NamingHint = sfRoleName

	roleName, err := uniqueRoleNameGenerator.Generate(ap)
	if err != nil {
		return "", err
	}

	ap.NamingHint = oldNamingHint

	Logger.Info(fmt.Sprintf("Generated database role name %q", roleName))

	return externalIdGenerator(database, roleName), nil
}

func (s *AccessToTargetSyncer) uniqueRoleNameGenerator(prefix string, database *string) (naming_hint.UniqueGenerator, error) {
	if generator, found := s.uniqueRoleNameGeneratorsCache[database]; found {
		return generator, nil
	}

	uniqueRoleNameGenerator, err := naming_hint.NewUniqueNameGenerator(Logger, prefix, &s.namingConstraints)
	if err != nil {
		return nil, err
	}

	s.uniqueRoleNameGeneratorsCache[database] = uniqueRoleNameGenerator

	return s.uniqueRoleNameGeneratorsCache[database], nil
}

// retrieveExistingRoles returns the set of existing roles with the given prefix
func (s *AccessToTargetSyncer) retrieveExistingRoles() (set.Set[string], error) {
	existingRoles := set.NewSet[string]()

	defaultPrefix := ""

	roleEntities, err := s.repo.GetAccountRolesWithPrefix(defaultPrefix)
	if err != nil {
		return nil, err
	}

	for _, roleEntity := range roleEntities {
		existingRoles.Add(accountRoleExternalIdGenerator(roleEntity.Name))
	}

	if !s.databaseRoleSupportEnabled {
		return existingRoles, nil
	}

	// Get all database roles for each database and add database roles to existing roles
	databases, err := s.accessSyncer.getAllDatabaseAndShareNames()
	if err != nil {
		return nil, err
	}

	for database := range databases {
		// Get all database roles for this specific database
		roleEntities, err2 := s.repo.GetDatabaseRolesWithPrefix(database, defaultPrefix)
		if err2 != nil {
			return nil, err2
		}

		for _, roleEntity := range roleEntities {
			existingRoles.Add(databaseRoleExternalIdGenerator(database, roleEntity.Name))
		}
	}

	// Get all application roles for each database and add application roles to existing roles
	applications, err := s.repo.GetApplications()
	if err != nil {
		return nil, fmt.Errorf("unable to get applications: %w", err)
	}

	for _, application := range applications {
		// Get all application roles for application
		roleEntities, err := s.repo.GetApplicationRoles(application.Name)
		if err != nil {
			return nil, fmt.Errorf("get application roles: %w", err)
		}

		for _, roleEntity := range roleEntities {
			if strings.HasPrefix(roleEntity.Name, defaultPrefix) {
				existingRoles.Add(applicationRoleExternalIdGenerator(application.Name, roleEntity.Name))
			}
		}
	}

	return existingRoles, nil
}

func (s *AccessToTargetSyncer) accessProvidersForMutationActions(toProcessApIds []string, apsById map[string]*ApSyncToTargetItem, actions []ApMutationAction) []*ApSyncToTargetItem {
	toProcessItems := make([]*ApSyncToTargetItem, 0, len(toProcessApIds))

	// Use the ordered slice instead of ranging directly on the set
	for _, apId := range toProcessApIds {
		ap, found := apsById[apId]

		if !found || ap == nil {
			Logger.Error(fmt.Sprintf("Access provider with ID %q not found in map", apId))
			continue
		}

		if slices.Contains(actions, ap.mutationAction) {
			toProcessItems = append(toProcessItems, ap)
		}
	}

	return toProcessItems
}

func (s *AccessToTargetSyncer) buildMetaDataMap() map[string]map[string]struct{} {
	metaDataMap := make(map[string]map[string]struct{})

	dataObjects := DataObjectTypes()

	for _, dot := range dataObjects {
		dotMap := make(map[string]struct{})
		metaDataMap[dot.Name] = dotMap

		for _, perm := range dot.Permissions {
			dotMap[strings.ToUpper(perm.Permission)] = struct{}{}
		}
	}

	return metaDataMap
}

func (s *AccessToTargetSyncer) createGrantsForWhatObjects(accessProvider *importer.AccessProvider, metaData map[string]map[string]struct{}) (GrantSet, error) {
	expectedGrants := NewGrantSet()

	for _, what := range accessProvider.What {
		permissions := what.Permissions

		if len(permissions) == 0 {
			continue
		}

		if isTableType(what.DataObject.Type) {
			err2 := s.createGrantsForTableOrView(what.DataObject.Type, permissions, what.DataObject.FullName, metaData, &expectedGrants)
			if err2 != nil {
				return expectedGrants, err2
			}
		} else if what.DataObject.Type == ds.Schema {
			err2 := s.createGrantsForSchema(permissions, what.DataObject.FullName, metaData, false, &expectedGrants)
			if err2 != nil {
				return expectedGrants, err2
			}
		} else if what.DataObject.Type == Function || what.DataObject.Type == Procedure {
			s.createGrantsForFunctionOrProcedure(permissions, what.DataObject.FullName, metaData, &expectedGrants, what.DataObject.Type)
		} else if what.DataObject.Type == "shared-schema" {
			err2 := s.createGrantsForSchema(permissions, what.DataObject.FullName, metaData, true, &expectedGrants)
			if err2 != nil {
				return expectedGrants, err2
			}
		} else if what.DataObject.Type == "shared-database" {
			err2 := s.createGrantsForDatabase(permissions, what.DataObject.FullName, metaData, true, &expectedGrants)
			if err2 != nil {
				return expectedGrants, err2
			}
		} else if what.DataObject.Type == ds.Database {
			err2 := s.createGrantsForDatabase(permissions, what.DataObject.FullName, metaData, false, &expectedGrants)
			if err2 != nil {
				return expectedGrants, err2
			}
		} else if what.DataObject.Type == "warehouse" {
			s.createGrantsForWarehouse(permissions, what.DataObject.FullName, metaData, &expectedGrants)
		} else if what.DataObject.Type == Integration {
			s.createGrantsForIntegration(permissions, what.DataObject.FullName, metaData, &expectedGrants)
		} else if what.DataObject.Type == ds.Datasource {
			err2 := s.createGrantsForAccount(permissions, metaData, &expectedGrants)
			if err2 != nil {
				return expectedGrants, err2
			}
		}
	}

	return expectedGrants, nil
}

func (s *AccessToTargetSyncer) createGrantsForSchema(permissions []string, fullName string, metaData map[string]map[string]struct{}, isShared bool, grants *GrantSet) error {
	// TODO: this does not work for Raito full names
	sfObject := common.ParseFullName(fullName)
	if sfObject.Database == nil || sfObject.Schema == nil || sfObject.Table != nil || sfObject.Column != nil {
		return fmt.Errorf("expected fullName %q to have exactly 2 parts (database.schema)", fullName)
	}

	var err error

	for _, p := range permissions {
		permissionMatchFound := false

		permissionMatchFound, err = s.createPermissionGrantsForSchema(*sfObject.Database, *sfObject.Schema, p, metaData, isShared, grants)
		if err != nil {
			return err
		}

		if !permissionMatchFound {
			Logger.Info(fmt.Sprintf("Permission %q does not apply to type SCHEMA or any of its descendants. Skipping", p))
		}
	}

	// Only generate the USAGE grant if any applicable permissions were applied on the schema or any item below
	if grants.Size() > 0 && !isShared {
		grants.Add(Grant{"USAGE", ds.Database, common.FormatQuery(`%s`, *sfObject.Database)}, Grant{"USAGE", ds.Schema, common.FormatQuery(`%s.%s`, *sfObject.Database, *sfObject.Schema)})
	}

	return nil
}

func (s *AccessToTargetSyncer) createPermissionGrantsForSchema(database, schema, p string, metaData map[string]map[string]struct{}, isShared bool, grants *GrantSet) (bool, error) {
	matchFound := false

	schemaType := ds.Schema
	if isShared {
		schemaType = SharedPrefix + schemaType
	}

	// Check if the permission is applicable on the schema itself
	if _, f := metaData[schemaType][strings.ToUpper(p)]; f {
		if strings.EqualFold(p, USAGE_ON_SCHEMA) {
			p = USAGE
		}

		grants.Add(Grant{p, schemaType, common.FormatQuery(`%s.%s`, database, schema)})
		matchFound = true
	} else {
		tables, err := s.getTablesForSchema(database, schema)
		if err != nil {
			return false, err
		}

		// Run through all the tabular things (tables, views, ...) in the schema
		for _, table := range tables {
			tableMatchFound := false
			tableMatchFound = s.createPermissionGrantsForTable(database, schema, table, p, metaData, isShared, grants)
			matchFound = matchFound || tableMatchFound
		}

		functions, err := s.getFunctionsForSchema(database, schema)
		if err != nil {
			return false, err
		}

		// Run through all the tabular things (tables, views, ...) in the schema
		for _, function := range functions {
			functionMatchFound := false
			functionMatchFound = s.createPermissionGrantsForFunctionOrProcedure(database, schema, function.Name, function.ArgumentSignature, p, metaData, grants, Function)
			matchFound = matchFound || functionMatchFound
		}

		procedures, err := s.getProceduresForSchema(database, schema)
		if err != nil {
			return false, err
		}

		// Run through all the tabular things (tables, views, ...) in the schema
		for _, proc := range procedures {
			procedureMatchFound := false
			procedureMatchFound = s.createPermissionGrantsForFunctionOrProcedure(database, schema, proc.Name, proc.ArgumentSignature, p, metaData, grants, Procedure)
			matchFound = matchFound || procedureMatchFound
		}
	}

	return matchFound, nil
}

func (s *AccessToTargetSyncer) createPermissionGrantsForTable(database string, schema string, table TableEntity, p string, metaData map[string]map[string]struct{}, isShared bool, grants *GrantSet) bool {
	// Get the corresponding Raito data object type
	tableType := convertSnowflakeTableTypeToRaito(&table)
	if isShared {
		tableType = SharedPrefix + tableType
	}

	// Check if the permission is applicable on the data object type
	if _, f2 := metaData[tableType][strings.ToUpper(p)]; f2 {
		grants.Add(Grant{p, tableType, common.FormatQuery(`%s.%s.%s`, database, schema, table.Name)})
		return true
	}

	return false
}

func (s *AccessToTargetSyncer) createPermissionGrantsForFunctionOrProcedure(database string, schema string, name, signature, p string, metaData map[string]map[string]struct{}, grants *GrantSet, objType string) bool {
	// Check if the permission is applicable on the data object type
	if _, f2 := metaData[objType][strings.ToUpper(p)]; f2 {
		argumentSignature := convertFunctionArgumentSignature(signature)

		grants.Add(Grant{p, objType, common.FormatQuery(`%s.%s.`, database, schema) + `"` + name + `"` + argumentSignature})

		return true
	}

	return false
}

func (s *AccessToTargetSyncer) createGrantsForDatabase(permissions []string, database string, metaData map[string]map[string]struct{}, isShared bool, grants *GrantSet) error {
	var err error

	for _, p := range permissions {
		databaseMatchFound := false
		databaseMatchFound, err = s.createPermissionGrantsForDatabase(database, p, metaData, isShared, grants)

		if err != nil {
			return err
		}

		if !databaseMatchFound {
			Logger.Info(fmt.Sprintf("Permission %q does not apply to type DATABASE or any of its descendants. Skipping", p))
		}
	}

	// Only generate the USAGE grant if any applicable permissions were applied or any item below
	if grants.Size() > 0 && !isShared {
		sfDBObject := common.SnowflakeObject{Database: &database, Schema: nil, Table: nil, Column: nil}
		grants.Add(Grant{USAGE, ds.Database, sfDBObject.GetFullName(true)})
	}

	return nil
}

func (s *AccessToTargetSyncer) createGrantsForWarehouse(permissions []string, warehouse string, metaData map[string]map[string]struct{}, grants *GrantSet) {
	for _, p := range permissions {
		if _, f := metaData["warehouse"][strings.ToUpper(p)]; !f {
			Logger.Warn(fmt.Sprintf("Permission %q does not apply to type WAREHOUSE. Skipping", p))
			continue
		}

		grants.Add(Grant{p, "warehouse", common.FormatQuery(`%s`, warehouse)})
	}
}

func (s *AccessToTargetSyncer) createGrantsForIntegration(permissions []string, warehouse string, metaData map[string]map[string]struct{}, grants *GrantSet) {
	for _, p := range permissions {
		if _, f := metaData[Integration][strings.ToUpper(p)]; !f {
			Logger.Warn(fmt.Sprintf("Permission %q does not apply to type INTEGRATION. Skipping", p))
			continue
		}

		grants.Add(Grant{p, Integration, common.FormatQuery(`%s`, warehouse)})
	}
}

func (s *AccessToTargetSyncer) createGrantsForAccount(permissions []string, metaData map[string]map[string]struct{}, grants *GrantSet) error {
	for _, p := range permissions {
		matchFound := false

		if _, f := metaData[ds.Datasource][strings.ToUpper(p)]; f {
			grants.Add(Grant{p, "account", ""})
			matchFound = true
		} else {
			if _, f2 := metaData["warehouse"][strings.ToUpper(p)]; f2 {
				matchFound = true

				warehouses, err := s.getWarehouses()
				if err != nil {
					return err
				}

				for _, warehouse := range warehouses {
					grants.Add(Grant{p, "warehouse", common.FormatQuery(`%s`, warehouse.Name)})
				}
			}

			if _, f2 := metaData[Integration][strings.ToUpper(p)]; f2 {
				matchFound = true

				integrations, err := s.getIntegrations()
				if err != nil {
					return err
				}

				for _, integration := range integrations {
					grants.Add(Grant{p, Integration, common.FormatQuery(`%s`, integration.Name)})
				}
			}

			inboundShareNames, err := s.accessSyncer.getInboundShareNames()
			if err != nil {
				return err
			}

			databaseNames, err := s.accessSyncer.getDatabaseNames()
			if err != nil {
				return err
			}

			databaseNames = append(databaseNames, inboundShareNames...)

			for _, database := range databaseNames {
				databaseMatchFound := false

				isShare := slices.Contains(inboundShareNames, database)

				databaseMatchFound, err = s.createPermissionGrantsForDatabase(database, p, metaData, isShare, grants)
				if err != nil {
					return err
				}

				matchFound = matchFound || databaseMatchFound

				// Only generate the USAGE grant if any applicable permissions were applied or any item below
				if databaseMatchFound && !isShare {
					dsName := database
					sfDBObject := common.SnowflakeObject{Database: &dsName, Schema: nil, Table: nil, Column: nil}
					grants.Add(Grant{USAGE, ds.Database, sfDBObject.GetFullName(true)})
				}
			}
		}

		if !matchFound {
			Logger.Info(fmt.Sprintf("Permission %q does not apply to type ACCOUNT (datasource) or any of its descendants. Skipping", p))
			continue
		}
	}

	return nil
}

func verifyGrant(grant Grant, metaData map[string]map[string]struct{}) bool {
	if grant.Permissions == USAGE && (grant.OnType == ds.Database || grant.OnType == ds.Schema) {
		return true
	}

	if tmd, f := metaData[grant.OnType]; f {
		if _, f2 := tmd[grant.Permissions]; f2 {
			return true
		}
	}

	Logger.Warn(fmt.Sprintf("Unknown permission %q for entity type %s. Skipping. %+v", grant.Permissions, grant.OnType, metaData))

	return false
}
