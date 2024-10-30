package snowflake

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"unicode"

	"github.com/aws/smithy-go/ptr"
	"github.com/hashicorp/go-multierror"
	gonanoid "github.com/matoous/go-nanoid/v2"
	importer "github.com/raito-io/cli/base/access_provider/sync_to_target"
	"github.com/raito-io/cli/base/access_provider/sync_to_target/naming_hint"
	ds "github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/util/match"
	"github.com/raito-io/cli/base/util/slice"
	"github.com/raito-io/cli/base/wrappers"
	"github.com/raito-io/golang-set/set"

	"github.com/raito-io/cli-plugin-snowflake/common"
)

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
	schemasPerDataBaseCache       map[string][]SchemaEntity
	warehousesCache               []DbEntity
}

func NewAccessToTargetSyncer(accessSyncer *AccessSyncer, namingConstraints naming_hint.NamingConstraints, repo dataAccessRepository, accessProviders *importer.AccessProviderImport, accessProviderFeedbackHandler wrappers.AccessProviderFeedbackHandler, configMap *config.ConfigMap) *AccessToTargetSyncer {
	return &AccessToTargetSyncer{
		accessSyncer:                  accessSyncer,
		accessProviders:               accessProviders,
		accessProviderFeedbackHandler: accessProviderFeedbackHandler,
		configMap:                     configMap,
		repo:                          repo,
		tablesPerSchemaCache:          make(map[string][]TableEntity),
		schemasPerDataBaseCache:       make(map[string][]SchemaEntity),
		uniqueRoleNameGeneratorsCache: make(map[*string]naming_hint.UniqueGenerator),
		namingConstraints:             namingConstraints,
	}
}

func (s *AccessToTargetSyncer) syncToTarget(ctx context.Context) error {
	s.databaseRoleSupportEnabled = s.configMap.GetBoolWithDefault(SfDatabaseRoles, false)

	ignoreLinksToRoles := s.configMap.GetString(SfIgnoreLinksToRoles)
	if ignoreLinksToRoles != "" {
		s.ignoreLinksToRole = slice.ParseCommaSeparatedList(ignoreLinksToRoles)
	}

	apList := s.accessProviders.AccessProviders
	apIdNameMap := make(map[string]string)

	masksMap := make(map[string]*importer.AccessProvider)
	masksToRemove := make(map[string]*importer.AccessProvider)

	filtersMap := make(map[string]*importer.AccessProvider)
	filtersToRemove := make(map[string]*importer.AccessProvider)

	rolesMap := make(map[string]*importer.AccessProvider)
	rolesToRemove := make(map[string]*importer.AccessProvider)

	for _, ap := range apList {
		var err2 error

		switch ap.Action {
		case importer.Mask:
			_, masksMap, masksToRemove, err2 = s.syncAccessProviderToTargetHandler(ap, masksMap, masksToRemove)
		case importer.Filtered:
			_, filtersMap, filtersToRemove, err2 = s.syncAccessProviderToTargetHandler(ap, filtersMap, filtersToRemove)
		case importer.Grant, importer.Purpose:
			var externalId string
			externalId, rolesMap, rolesToRemove, err2 = s.syncAccessProviderToTargetHandler(ap, rolesMap, rolesToRemove)
			apIdNameMap[ap.Id] = externalId
		case importer.Deny, importer.Promise:
		default:
			err2 = s.accessProviderFeedbackHandler.AddAccessProviderFeedback(importer.AccessProviderSyncFeedback{
				AccessProvider: ap.Id,
				Errors:         []string{fmt.Sprintf("Unsupported action %s", ap.Action.String())},
			})
		}

		if err2 != nil {
			return err2
		}
	}

	// Step 1 first initiate all the masks
	if len(masksMap)+len(masksToRemove) > 0 {
		err := s.SyncAccessProviderMasksToTarget(masksToRemove, masksMap, apIdNameMap)
		if err != nil {
			return fmt.Errorf("sync masks to target: %w", err)
		}
	}

	// Step 2 then initialize all filters
	if len(filtersMap)+len(filtersToRemove) > 0 {
		err := s.SyncAccessProviderFiltersToTarget(ctx, filtersToRemove, filtersMap, apIdNameMap)
		if err != nil {
			return fmt.Errorf("sync filters to target: %w", err)
		}
	}

	// Step 3 then initiate all the roles
	err := s.SyncAccessProviderRolesToTarget(ctx, rolesToRemove, rolesMap)
	if err != nil {
		return fmt.Errorf("sync roles to target: %w", err)
	}

	return nil
}

func (s *AccessToTargetSyncer) syncAccessProviderToTargetHandler(ap *importer.AccessProvider, toProcessAps map[string]*importer.AccessProvider, apToRemoveMap map[string]*importer.AccessProvider) (string, map[string]*importer.AccessProvider, map[string]*importer.AccessProvider, error) {
	var externalId string

	if ap.Delete {
		if ap.ExternalId == nil {
			logger.Warn(fmt.Sprintf("No externalId defined for deleted access provider %q. This will be ignored", ap.Id))

			err := s.accessProviderFeedbackHandler.AddAccessProviderFeedback(importer.AccessProviderSyncFeedback{
				AccessProvider: ap.Id,
			})
			if err != nil {
				return "", nil, nil, err
			}

			return "", toProcessAps, apToRemoveMap, nil
		}

		externalId = *ap.ExternalId

		apToRemoveMap[externalId] = ap
	} else {
		uniqueExternalId, err := s.generateUniqueExternalId(ap, "")
		if err != nil {
			return "", nil, nil, err
		}

		externalId = uniqueExternalId
		if _, f := toProcessAps[externalId]; !f {
			toProcessAps[externalId] = ap
		}
	}

	return externalId, toProcessAps, apToRemoveMap, nil
}

func (s *AccessToTargetSyncer) generateUniqueExternalId(ap *importer.AccessProvider, prefix string) (string, error) {
	if isDatabaseRole(ap.Type) {
		sfRoleName := ap.Name
		if ap.NamingHint != "" {
			sfRoleName = ap.NamingHint
		}

		// Finding the database this db role is linked to
		var database string
		var err error

		if len(ap.What) > 0 {
			// If there is a WHAT, we look for the database of the first element
			parts := strings.Split(ap.What[0].DataObject.FullName, ".")
			database = parts[0]
		} else if ap.ExternalId != nil {
			// Otherwise, we try to parse the externalId
			database, _, err = parseDatabaseRoleExternalId(*ap.ExternalId)

			if err != nil {
				return "", err
			}
		} else {
			return "", errors.New("unable to determine database for database role")
		}

		uniqueRoleNameGenerator, err := s.getUniqueRoleNameGenerator(prefix, &database)
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

		logger.Info(fmt.Sprintf("Generated database role name %q", roleName))

		return databaseRoleExternalIdGenerator(database, roleName), nil
	} else {
		uniqueRoleNameGenerator, err := s.getUniqueRoleNameGenerator(prefix, nil)
		if err != nil {
			return "", err
		}

		roleName, err := uniqueRoleNameGenerator.Generate(ap)
		if err != nil {
			return "", err
		}

		logger.Info(fmt.Sprintf("Generated account role name %q", roleName))

		return accountRoleExternalIdGenerator(roleName), nil
	}
}

func (s *AccessToTargetSyncer) getUniqueRoleNameGenerator(prefix string, database *string) (naming_hint.UniqueGenerator, error) {
	if generator, found := s.uniqueRoleNameGeneratorsCache[database]; found {
		return generator, nil
	}

	uniqueRoleNameGenerator, err := naming_hint.NewUniqueNameGenerator(logger, prefix, &s.namingConstraints)
	if err != nil {
		return nil, err
	}

	s.uniqueRoleNameGeneratorsCache[database] = uniqueRoleNameGenerator

	return s.uniqueRoleNameGeneratorsCache[database], nil
}

func (s *AccessToTargetSyncer) SyncAccessProviderRolesToTarget(ctx context.Context, toRemoveAps map[string]*importer.AccessProvider, toProcessAps map[string]*importer.AccessProvider) error {
	logger.Info("Configuring access providers as roles in Snowflake")

	err := s.removeRolesToRemove(toRemoveAps)
	if err != nil {
		return err
	}

	toRenameAps := make(map[string]string)

	for externalId, ap := range toProcessAps {
		if ap.ExternalId != nil && *ap.ExternalId != externalId {
			toRenameAps[externalId] = *ap.ExternalId
		}
	}

	existingRoles, err := s.findRoles("")
	if err != nil {
		return err
	}

	err = s.generateAccessControls(ctx, toProcessAps, existingRoles, toRenameAps)
	if err != nil {
		return err
	}

	return nil
}

func (s *AccessToTargetSyncer) SyncAccessProviderMasksToTarget(apToRemoveMap map[string]*importer.AccessProvider, apMap map[string]*importer.AccessProvider, roleNameMap map[string]string) error {
	var err error

	if s.configMap.GetBoolWithDefault(SfStandardEdition, false) {
		if len(apToRemoveMap) > 0 || len(apMap) > 0 {
			logger.Error("Skipping masking policies due to Snowflake Standard Edition.")
		}

		return nil
	}

	logger.Info(fmt.Sprintf("Configuring access provider as masks in Snowflake. Update %d masks remove %d masks", len(apMap), len(apToRemoveMap)))

	// Step 1: Update masks and create new masks
	for _, mask := range apMap {
		maskName, err2 := s.updateMask(mask, roleNameMap)
		fi := importer.AccessProviderSyncFeedback{AccessProvider: mask.Id, ActualName: maskName, ExternalId: &maskName}

		if err2 != nil {
			fi.Errors = append(fi.Errors, err2.Error())
		}

		err = s.accessProviderFeedbackHandler.AddAccessProviderFeedback(fi)
		if err != nil {
			return err
		}
	}

	// Step 2: Remove old masks
	for maskToRemove, maskAp := range apToRemoveMap {
		externalId := maskToRemove
		fi := importer.AccessProviderSyncFeedback{AccessProvider: maskAp.Id, ActualName: maskToRemove, ExternalId: &externalId}

		err = s.removeMask(maskToRemove)
		if err != nil {
			fi.Errors = append(fi.Errors, err.Error())
		}

		err = s.accessProviderFeedbackHandler.AddAccessProviderFeedback(fi)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *AccessToTargetSyncer) SyncAccessProviderFiltersToTarget(ctx context.Context, apToRemoveMap map[string]*importer.AccessProvider, apMap map[string]*importer.AccessProvider, roleNameMap map[string]string) error {
	if s.configMap.GetBoolWithDefault(SfStandardEdition, false) {
		if len(apToRemoveMap) > 0 || len(apMap) > 0 {
			logger.Error("Skipping filter policies due to Snowflake Standard Edition.")
		}

		return nil
	}

	logger.Info(fmt.Sprintf("Configuring access provider as filters in Snowflake. Update %d masks remove %d masks", len(apMap), len(apToRemoveMap)))

	//Groups filters by table
	updateGroupedFilters, err := groupFiltersByTable(apMap, s.accessProviderFeedbackHandler)
	if err != nil {
		return err
	}

	removeGroupedFilters, err := groupFiltersByTable(apToRemoveMap, s.accessProviderFeedbackHandler)
	if err != nil {
		return err
	}

	feedbackFn := func(aps []*importer.AccessProvider, actualName *string, externalId *string, err error) error {
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

			ferr := s.accessProviderFeedbackHandler.AddAccessProviderFeedback(importer.AccessProviderSyncFeedback{
				AccessProvider: ap.Id,
				ActualName:     actualNameStr,
				ExternalId:     apExternalId,
				Errors:         errorMessages,
			})
			if ferr != nil {
				feedbackErr = multierror.Append(feedbackErr, ferr)
			}
		}

		return feedbackErr
	}

	//Create or update filters per table
	updatedTables := set.NewSet[string]()

	for table, filters := range updateGroupedFilters {
		filterName, externalId, createErr := s.updateOrCreateFilter(ctx, table, filters, roleNameMap)

		ferr := feedbackFn(filters, &filterName, externalId, createErr)
		if ferr != nil {
			return ferr
		}

		if createErr != nil {
			updatedTables.Add(table)
		}
	}

	// Remove old filters per table
	for table, filters := range removeGroupedFilters {
		if _, found := updateGroupedFilters[table]; found {
			if updatedTables.Contains(table) {
				deleteErr := s.deleteFilter(table, filters)

				ferr := feedbackFn(filters, nil, nil, deleteErr)
				if ferr != nil {
					return ferr
				}
			} else {
				ferr := feedbackFn(filters, nil, nil, fmt.Errorf("prevent deletion of filter because unable to create new filter"))
				if ferr != nil {
					return ferr
				}
			}
		} else {
			deleteErr := s.deleteFilter(table, filters)

			ferr := feedbackFn(filters, nil, nil, deleteErr)
			if ferr != nil {
				return ferr
			}
		}
	}

	return nil
}

func (s *AccessToTargetSyncer) removeRolesToRemove(toRemoveAps map[string]*importer.AccessProvider) error {
	if len(toRemoveAps) > 0 {
		logger.Info(fmt.Sprintf("Removing %d old Raito roles in Snowflake", len(toRemoveAps)))

		for toRemoveExternalId, ap := range toRemoveAps {
			if ap == nil {
				logger.Warn(fmt.Sprintf("no linked access provider found for %q, so just going to remove it from Snowflake", toRemoveExternalId))

				err := s.dropRole(toRemoveExternalId, isDatabaseRoleByExternalId(toRemoveExternalId))
				if err != nil {
					return err
				}

				continue
			}

			fi := importer.AccessProviderSyncFeedback{
				AccessProvider: ap.Id,
				ExternalId:     ptr.String(toRemoveExternalId),
			}

			err := s.dropRole(toRemoveExternalId, isDatabaseRole(ap.Type))
			// If an error occurs (and not already deleted), we send an error back as feedback
			if err != nil && !strings.Contains(err.Error(), "does not exist") {
				logger.Error(fmt.Sprintf("unable to drop role %q: %s", toRemoveExternalId, err.Error()))

				fi.Errors = append(fi.Errors, fmt.Sprintf("unable to drop role %q: %s", toRemoveExternalId, err.Error()))
			}

			err = s.accessProviderFeedbackHandler.AddAccessProviderFeedback(fi)
			if err != nil {
				return err
			}
		}
	} else {
		logger.Info("No old Raito roles to remove in Snowflake")
	}

	return nil
}

// findRoles returns the set of existing roles with the given prefix
func (s *AccessToTargetSyncer) findRoles(prefix string) (set.Set[string], error) {
	existingRoles := set.NewSet[string]()

	roleEntities, err := s.repo.GetAccountRolesWithPrefix(prefix)
	if err != nil {
		return nil, err
	}

	for _, roleEntity := range roleEntities {
		existingRoles.Add(accountRoleExternalIdGenerator(roleEntity.Name))
	}

	if !s.databaseRoleSupportEnabled {
		return existingRoles, nil
	}

	//Get all database roles for each database and add database roles to existing roles
	databases, err := s.accessSyncer.getAllDatabaseAndShareNames()
	if err != nil {
		return nil, err
	}

	for database := range databases {
		// Get all database roles for database
		roleEntities, err := s.repo.GetDatabaseRolesWithPrefix(database, prefix)
		if err != nil {
			return nil, err
		}

		for _, roleEntity := range roleEntities {
			existingRoles.Add(databaseRoleExternalIdGenerator(database, roleEntity.Name))
		}
	}

	return existingRoles, nil
}

func (s *AccessToTargetSyncer) buildMetaDataMap(metaData *ds.MetaData) map[string]map[string]struct{} {
	metaDataMap := make(map[string]map[string]struct{})

	for _, dot := range metaData.DataObjectTypes {
		dotMap := make(map[string]struct{})
		metaDataMap[dot.Name] = dotMap

		for _, perm := range dot.Permissions {
			dotMap[strings.ToUpper(perm.Permission)] = struct{}{}
		}
	}

	return metaDataMap
}

//nolint:gocyclo
func (s *AccessToTargetSyncer) handleAccessProvider(ctx context.Context, externalId string, toProcessAps map[string]*importer.AccessProvider, existingRoles set.Set[string], toRenameAps map[string]string, rolesCreated map[string]interface{}, metaData map[string]map[string]struct{}) (string, error) {
	accessProvider := toProcessAps[externalId]
	logger.Debug(fmt.Sprintf("Handle access provider with key %q - %+v - %+v", externalId, accessProvider, toProcessAps))

	ignoreWho := accessProvider.WhoLocked != nil && *accessProvider.WhoLocked
	ignoreInheritance := accessProvider.InheritanceLocked != nil && *accessProvider.InheritanceLocked
	ignoreWhat := accessProvider.WhatLocked != nil && *accessProvider.WhatLocked
	deleteLocked := accessProvider.DeleteLocked != nil && *accessProvider.DeleteLocked

	logger.Info(fmt.Sprintf("Generating access controls for access provider %q (Ignore who: %t; Ignore inheritance: %t; Ignore what: %t)", accessProvider.Name, ignoreWho, ignoreInheritance, ignoreWhat))

	// Extract RoleNames from Access Providers that are among the whoList of this one
	inheritedRoles := make([]string, 0)

	actualName := externalId
	var err error

	if isDatabaseRole(accessProvider.Type) {
		_, actualName, err = parseDatabaseRoleExternalId(externalId)
		if err != nil {
			return actualName, err
		}
	}

	if !ignoreInheritance {
		for _, apWho := range accessProvider.Who.InheritFrom {
			if strings.HasPrefix(apWho, "ID:") {
				apId := apWho[3:]
				for rn2, accessProvider2 := range toProcessAps {
					if strings.EqualFold(accessProvider2.Id, apId) {
						inheritedRoles = append(inheritedRoles, rn2)
						break
					}
				}
			} else {
				inheritedRoles = append(inheritedRoles, apWho)
			}
		}
	}

	// Build the expected grants
	expectedGrants := set.NewSet[Grant]()

	if !ignoreWhat {
		for _, what := range accessProvider.What {
			permissions := what.Permissions

			if len(permissions) == 0 {
				continue
			}

			if isTableType(what.DataObject.Type) {
				err2 := s.createGrantsForTableOrView(what.DataObject.Type, permissions, what.DataObject.FullName, metaData, expectedGrants)
				if err2 != nil {
					return actualName, err2
				}
			} else if what.DataObject.Type == ds.Schema {
				err2 := s.createGrantsForSchema(permissions, what.DataObject.FullName, metaData, false, expectedGrants)
				if err2 != nil {
					return actualName, err2
				}
			} else if what.DataObject.Type == "shared-schema" {
				err2 := s.createGrantsForSchema(permissions, what.DataObject.FullName, metaData, true, expectedGrants)
				if err2 != nil {
					return actualName, err2
				}
			} else if what.DataObject.Type == "shared-database" {
				err2 := s.createGrantsForDatabase(permissions, what.DataObject.FullName, metaData, true, expectedGrants)
				if err2 != nil {
					return actualName, err2
				}
			} else if what.DataObject.Type == ds.Database {
				err2 := s.createGrantsForDatabase(permissions, what.DataObject.FullName, metaData, false, expectedGrants)
				if err2 != nil {
					return actualName, err2
				}
			} else if what.DataObject.Type == "warehouse" {
				s.createGrantsForWarehouse(permissions, what.DataObject.FullName, metaData, expectedGrants)
			} else if what.DataObject.Type == ds.Datasource {
				err2 := s.createGrantsForAccount(permissions, metaData, expectedGrants)
				if err2 != nil {
					return actualName, err2
				}
			}
		}
	}

	// If we find this role name in the rename map, this means we have to rename it.
	if oldExternalId, f := toRenameAps[externalId]; f {
		if !existingRoles.Contains(externalId) && existingRoles.Contains(oldExternalId) {
			if _, oldFound := toProcessAps[oldExternalId]; oldFound {
				// In this case the old is already taken by another access provider.
				// For example in the case where R2 was renamed to R3 and R1 was then renamed to R2.
				// Therefor, we only log a message for this special case
				logger.Info(fmt.Sprintf("Both the old role name (%s) and the new role name (%s) exist. The old role name is already taken by another (new?) access provider.", externalId, oldExternalId))
			} else {
				// The old name exists and the new one doesn't exist yet, so we have to do the rename
				err = s.renameRole(oldExternalId, externalId, accessProvider.Type)
				if err != nil {
					return actualName, fmt.Errorf("error while renaming role %q to %q: %s", oldExternalId, externalId, err.Error())
				}

				existingRoles.Add(externalId)
			}
		} else if existingRoles.Contains(externalId) && existingRoles.Contains(oldExternalId) {
			if _, oldFound := toProcessAps[oldExternalId]; oldFound {
				// In this case the old is already taken by another access provider.
				// For example in the case where R2 was renamed to R3 and R1 was then renamed to R2.
				// Therefor, we only log a message for this special case
				logger.Info(fmt.Sprintf("Both the old role name (%s) and the new role name (%s) exist. The old role name is already taken by another (new?) access provider.", externalId, oldExternalId))
			} else {
				// The old name exists but also the new one already exists. This is a weird case, but we'll delete the old one in this case and the new one will be updated in the next step of this method.
				err = s.dropRole(oldExternalId, isDatabaseRoleByExternalId(oldExternalId))
				if err != nil {
					return actualName, fmt.Errorf("error while dropping role (%s) which was the old name of access provider %q: %s", oldExternalId, accessProvider.Name, err.Error())
				}

				existingRoles.Remove(oldExternalId)
			}
		}
	}

	var foundGrants []Grant

	// If the role already exists in the system
	if existingRoles.Contains(externalId) {
		logger.Info(fmt.Sprintf("Merging role: %q", externalId))

		// Only update the comment if we have full control over the role (who , inheritance and what not ignored)
		if !ignoreWho && !ignoreWhat && !ignoreInheritance {
			err2 := s.commentOnRoleIfExists(createComment(accessProvider, true), externalId)
			if err2 != nil {
				return actualName, fmt.Errorf("error while updating comment on role %q: %s", externalId, err2.Error())
			}
		}

		if !ignoreWho || !ignoreInheritance {
			grantsOfRole, err3 := s.accessSyncer.retrieveGrantsOfRole(externalId, accessProvider.Type)
			if err3 != nil {
				return actualName, err3
			}

			usersOfRole := make([]string, 0, len(grantsOfRole))
			rolesOfRole := make([]string, 0, len(grantsOfRole))

			for _, gor := range grantsOfRole {
				if strings.EqualFold(gor.GrantedTo, "USER") {
					usersOfRole = append(usersOfRole, gor.GranteeName)
				} else if strings.EqualFold(gor.GrantedTo, "ROLE") {
					rolesOfRole = append(rolesOfRole, accountRoleExternalIdGenerator(gor.GranteeName))
				} else if strings.EqualFold(gor.GrantedTo, "DATABASE_ROLE") {
					database, parsedRoleName, err2 := parseDatabaseRoleRoleName(cleanDoubleQuotes(gor.GranteeName))
					if err2 != nil {
						return actualName, err2
					}

					rolesOfRole = append(rolesOfRole, databaseRoleExternalIdGenerator(database, parsedRoleName))
				}
			}

			if !ignoreWho {
				toAdd := slice.StringSliceDifference(accessProvider.Who.Users, usersOfRole, false)
				toRemove := slice.StringSliceDifference(usersOfRole, accessProvider.Who.Users, false)
				logger.Info(fmt.Sprintf("Identified %d users to add and %d users to remove from role %q", len(toAdd), len(toRemove), externalId))

				if len(toAdd) > 0 {
					if isDatabaseRole(accessProvider.Type) {
						return actualName, fmt.Errorf("error can not assign users from a database role %q", externalId)
					}

					e := s.repo.GrantUsersToAccountRole(ctx, externalId, toAdd...)
					if e != nil {
						return actualName, fmt.Errorf("error while assigning users to role %q: %s", externalId, e.Error())
					}
				}

				if len(toRemove) > 0 {
					if isDatabaseRole(accessProvider.Type) {
						return actualName, fmt.Errorf("error can not unassign users from a database role %q", externalId)
					}

					e := s.repo.RevokeUsersFromAccountRole(ctx, externalId, toRemove...)
					if e != nil {
						return actualName, fmt.Errorf("error while unassigning users from role %q: %s", externalId, e.Error())
					}
				}
			}

			if !ignoreInheritance {
				toAdd := slice.StringSliceDifference(inheritedRoles, rolesOfRole, false)
				toRemove := slice.StringSliceDifference(rolesOfRole, inheritedRoles, false)
				logger.Info(fmt.Sprintf("Identified %d roles to add and %d roles to remove from role %q", len(toAdd), len(toRemove), externalId))

				if len(toAdd) > 0 {
					e := s.grantRolesToRole(ctx, externalId, accessProvider.Type, toAdd...)
					if e != nil {
						return actualName, fmt.Errorf("error while assigning role to role %q: %s", externalId, e.Error())
					}
				}

				if len(toRemove) > 0 {
					e := s.revokeRolesFromRole(ctx, externalId, accessProvider.Type, toRemove...)
					if e != nil {
						return actualName, fmt.Errorf("error while unassigning role from role %q: %s", externalId, e.Error())
					}
				}
			}
		}

		if !ignoreWhat {
			// Remove all future grants on schema and database if applicable.
			// Since these are future grants, it's safe to just remove them and re-add them again (if required).
			// We assume nobody manually added others to this role manually.
			for _, what := range accessProvider.What {
				if what.DataObject.Type == "database" {
					e := s.executeRevokeOnRole("ALL", common.FormatQuery(`FUTURE SCHEMAS IN DATABASE %s`, what.DataObject.FullName), externalId, accessProvider.Type)
					if e != nil {
						return actualName, fmt.Errorf("error while assigning future schema grants in database %q to role %q: %s", what.DataObject.FullName, externalId, e.Error())
					}

					e = s.executeRevokeOnRole("ALL", common.FormatQuery(`FUTURE TABLES IN DATABASE %s`, what.DataObject.FullName), externalId, accessProvider.Type)
					if e != nil {
						return actualName, fmt.Errorf("error while assigning future table grants in database %q to role %q: %s", what.DataObject.FullName, externalId, e.Error())
					}
				} else if what.DataObject.Type == "schema" {
					e := s.executeRevokeOnRole("ALL", fmt.Sprintf("FUTURE TABLES IN SCHEMA %s", what.DataObject.FullName), externalId, accessProvider.Type)
					if e != nil {
						return actualName, fmt.Errorf("error while assigning future table grants in schema %q to role %q: %s", what.DataObject.FullName, externalId, e.Error())
					}
				}
			}

			grantsToRole, err3 := s.accessSyncer.getGrantsToRole(externalId, accessProvider.Type)
			if err3 != nil {
				return actualName, err3
			}

			logger.Debug(fmt.Sprintf("Found grants for role %q: %+v", externalId, grantsToRole))

			foundGrants = make([]Grant, 0, len(grantsToRole))

			for _, grant := range grantsToRole {
				if strings.EqualFold(grant.GrantedOn, "ACCOUNT") {
					foundGrants = append(foundGrants, Grant{grant.Privilege, "account", ""})
				} else if strings.EqualFold(grant.Privilege, "OWNERSHIP") {
					logger.Info(fmt.Sprintf("Ignoring permission %q on %q for Role %q as this will remain untouched", grant.Privilege, grant.Name, externalId))
				} else if strings.EqualFold(grant.Privilege, "USAGE") && (strings.EqualFold(grant.GrantedOn, "ROLE") || strings.EqualFold(grant.GrantedOn, "DATABASE_ROLE")) {
					logger.Debug(fmt.Sprintf("Ignoring USAGE permission on %s %q", grant.GrantedOn, grant.Name))
				} else {
					onType := convertSnowflakeGrantTypeToRaito(grant.GrantedOn)

					foundGrants = append(foundGrants, Grant{grant.Privilege, onType, grant.Name})
				}
			}
		}

		logger.Info(fmt.Sprintf("Done updating users granted to role %q", externalId))
	} else {
		// When delete is locked (so this was originally created/managed in the data source), we don't recreate it again if it is deleted on the data source in the meanwhile.
		if deleteLocked {
			logger.Warn(fmt.Sprintf("Role %q does not exist but is marked as delete locked. Not creating the role as it is probably removed externally.", externalId))
			return actualName, nil
		}

		logger.Info(fmt.Sprintf("Creating role %q", externalId))

		if _, rf := rolesCreated[externalId]; !rf {
			// Create the role if not exists
			err = s.createRole(externalId, accessProvider.Type)
			if err != nil {
				return actualName, fmt.Errorf("error while creating role %q: %s", externalId, err.Error())
			}

			// Updating the comment (independent of creation)
			err = s.commentOnRoleIfExists(createComment(accessProvider, false), externalId)
			if err != nil {
				return actualName, fmt.Errorf("error while updating comment on role %q: %s", externalId, err.Error())
			}
			rolesCreated[externalId] = struct{}{}
		}

		if len(accessProvider.Who.Users) > 0 {
			if isDatabaseRole(accessProvider.Type) {
				return actualName, fmt.Errorf("error can not assign users to a database role %q", externalId)
			}

			err = s.repo.GrantUsersToAccountRole(ctx, externalId, accessProvider.Who.Users...)
			if err != nil {
				return actualName, fmt.Errorf("error while assigning users to role %q: %s", externalId, err.Error())
			}
		}

		err = s.grantRolesToRole(ctx, externalId, accessProvider.Type, inheritedRoles...)
		if err != nil {
			return actualName, fmt.Errorf("error while assigning roles to role %q: %s", externalId, err.Error())
		}
	}

	if !ignoreWhat {
		err = s.mergeGrants(externalId, accessProvider.Type, foundGrants, expectedGrants.Slice(), metaData)
		if err != nil {
			return actualName, err
		}
	}

	return actualName, nil
}

func (s *AccessToTargetSyncer) splitRoles(inheritedRoles []string) ([]string, []string) {
	toAddDatabaseRoles := []string{}

	for _, role := range inheritedRoles {
		if isDatabaseRoleByExternalId(role) {
			toAddDatabaseRoles = append(toAddDatabaseRoles, role)
		}
	}

	toAddAccountRoles := slice.SliceDifference(inheritedRoles, toAddDatabaseRoles)

	return toAddDatabaseRoles, toAddAccountRoles
}

func (s *AccessToTargetSyncer) grantRolesToRole(ctx context.Context, targetExternalId string, targetApType *string, roles ...string) error {
	toAddDatabaseRoles, toAddAccountRoles := s.splitRoles(roles)

	var filteredAccountRoles []string

	for _, accountRole := range toAddAccountRoles {
		shouldIgnore, err2 := s.shouldIgnoreLinkedRole(accountRole)
		if err2 != nil {
			return err2
		}

		if !shouldIgnore {
			filteredAccountRoles = append(filteredAccountRoles, accountRole)
		}
	}

	if isDatabaseRole(targetApType) {
		database, parsedRoleName, err := parseDatabaseRoleExternalId(targetExternalId)
		if err != nil {
			return err
		}

		var filteredDatabaseRoles []string

		for _, dbRole := range toAddDatabaseRoles {
			toDatabase, toParsedRoleName, err2 := parseDatabaseRoleExternalId(dbRole)
			if err2 != nil {
				return err2
			}

			if database != toDatabase {
				return fmt.Errorf("database role %q is from a different database than %q", parsedRoleName, toParsedRoleName)
			}

			shouldIgnore, err2 := s.shouldIgnoreLinkedRole(toParsedRoleName)
			if err2 != nil {
				return err2
			}

			if !shouldIgnore {
				filteredDatabaseRoles = append(filteredDatabaseRoles, toParsedRoleName)
			}
		}

		err = s.repo.GrantDatabaseRolesToDatabaseRole(ctx, database, parsedRoleName, filteredDatabaseRoles...)
		if err != nil {
			return err
		}

		return s.repo.GrantAccountRolesToDatabaseRole(ctx, database, parsedRoleName, filteredAccountRoles...)
	}

	if len(toAddDatabaseRoles) > 0 {
		return fmt.Errorf("error can not assign database roles to an account role %q - %v", targetExternalId, toAddDatabaseRoles)
	}

	return s.repo.GrantAccountRolesToAccountRole(ctx, targetExternalId, filteredAccountRoles...)
}

func (s *AccessToTargetSyncer) shouldIgnoreLinkedRole(roleName string) (bool, error) {
	matched, err := match.MatchesAny(roleName, s.ignoreLinksToRole)
	if err != nil {
		return false, fmt.Errorf("parsing regular expressions in parameter %q: %s", SfIgnoreLinksToRoles, err.Error())
	}

	return matched, nil
}

func (s *AccessToTargetSyncer) revokeRolesFromRole(ctx context.Context, targetExternalId string, targetApType *string, roles ...string) error {
	toAddDatabaseRoles, toAddAccountRoles := s.splitRoles(roles)

	var filteredAccountRoles []string

	for _, accountRole := range toAddAccountRoles {
		shouldIgnore, err2 := s.shouldIgnoreLinkedRole(accountRole)
		if err2 != nil {
			return err2
		}

		if !shouldIgnore {
			filteredAccountRoles = append(filteredAccountRoles, accountRole)
		}
	}

	if isDatabaseRole(targetApType) {
		database, parsedRoleName, err := parseDatabaseRoleExternalId(targetExternalId)
		if err != nil {
			return err
		}

		var filteredDatabaseRoles []string

		for _, dbRole := range toAddDatabaseRoles {
			_, toParsedRoleName, err2 := parseDatabaseRoleExternalId(dbRole)
			if err2 != nil {
				return err2
			}

			shouldIgnore, err2 := s.shouldIgnoreLinkedRole(toParsedRoleName)
			if err2 != nil {
				return err2
			}

			if !shouldIgnore {
				filteredDatabaseRoles = append(filteredDatabaseRoles, toParsedRoleName)
			}
		}

		err = s.repo.RevokeDatabaseRolesFromDatabaseRole(ctx, database, parsedRoleName, filteredDatabaseRoles...)
		if err != nil {
			return err
		}

		return s.repo.RevokeAccountRolesFromDatabaseRole(ctx, database, parsedRoleName, filteredAccountRoles...)
	}

	if len(toAddDatabaseRoles) > 0 {
		return fmt.Errorf("error can not assign database roles to an account role %q - %v", targetExternalId, toAddDatabaseRoles)
	}

	return s.repo.RevokeAccountRolesFromAccountRole(ctx, targetExternalId, filteredAccountRoles...)
}

func (s *AccessToTargetSyncer) createRole(externalId string, apType *string) error {
	if isDatabaseRole(apType) {
		database, cleanedRoleName, err := parseDatabaseRoleExternalId(externalId)
		if err != nil {
			return err
		}

		return s.repo.CreateDatabaseRole(database, cleanedRoleName)
	}

	return s.repo.CreateAccountRole(externalId)
}

func (s *AccessToTargetSyncer) dropRole(externalId string, databaseRole bool) error {
	if databaseRole {
		database, cleanedRoleName, err := parseDatabaseRoleExternalId(externalId)
		if err != nil {
			return err
		}

		return s.repo.DropDatabaseRole(database, cleanedRoleName)
	}

	return s.repo.DropAccountRole(externalId)
}

func (s *AccessToTargetSyncer) renameRole(oldName, newName string, apType *string) error {
	if isDatabaseRole(apType) {
		if !isDatabaseRoleByExternalId(newName) || !isDatabaseRoleByExternalId(oldName) {
			return fmt.Errorf("both roles should be a database role newName:%q - oldName:%q", newName, oldName)
		}

		oldDatabase, oldRoleName, err := parseDatabaseRoleExternalId(oldName)
		if err != nil {
			return err
		}

		newDatabase, newRoleName, err := parseDatabaseRoleExternalId(newName)
		if err != nil {
			return err
		}

		if oldDatabase != newDatabase {
			return fmt.Errorf("expected new roleName %q pointing to the same database as old roleName %q", newName, oldName)
		}

		return s.repo.RenameDatabaseRole(oldDatabase, oldRoleName, newRoleName)
	}

	return s.repo.RenameAccountRole(oldName, newName)
}

func (s *AccessToTargetSyncer) commentOnRoleIfExists(comment, roleName string) error {
	if isDatabaseRoleByExternalId(roleName) {
		database, cleanedRoleName, err := parseDatabaseRoleExternalId(roleName)
		if err != nil {
			return err
		}

		return s.repo.CommentDatabaseRoleIfExists(comment, database, cleanedRoleName)
	}

	return s.repo.CommentAccountRoleIfExists(comment, roleName)
}

func (s *AccessToTargetSyncer) generateAccessControls(ctx context.Context, toProcessAps map[string]*importer.AccessProvider, existingRoles set.Set[string], toRenameAps map[string]string) error {
	// We always need the meta data
	rolesCreated := make(map[string]interface{})
	dsSyncer := DataSourceSyncer{}

	md, err := dsSyncer.GetDataSourceMetaData(ctx, s.configMap)
	if err != nil {
		return err
	}

	metaData := s.buildMetaDataMap(md)

	for externalId, accessProvider := range toProcessAps {
		fi := importer.AccessProviderSyncFeedback{
			AccessProvider: accessProvider.Id,
			ExternalId:     ptr.String(externalId),
			Type:           accessProvider.Type,
		}

		fi.ActualName, err = s.handleAccessProvider(ctx, externalId, toProcessAps, existingRoles, toRenameAps, rolesCreated, metaData)

		err3 := s.handleAccessProviderFeedback(&fi, err)
		if err3 != nil {
			return err3
		}
	}

	return nil
}

func (s *AccessToTargetSyncer) handleAccessProviderFeedback(fi *importer.AccessProviderSyncFeedback, err error) error {
	if err != nil {
		logger.Error(err.Error())
		fi.Errors = append(fi.Errors, err.Error())
	}

	return s.accessProviderFeedbackHandler.AddAccessProviderFeedback(*fi)
}

func (s *AccessToTargetSyncer) updateMask(mask *importer.AccessProvider, roleNameMap map[string]string) (string, error) {
	logger.Info(fmt.Sprintf("Updating mask %q", mask.Name))

	globalMaskName := raitoMaskName(mask.Name)
	uniqueMaskName := raitoMaskUniqueName(mask.Name)

	// Step 0: Load beneficieries
	beneficiaries := MaskingBeneficiaries{
		Users: mask.Who.Users,
	}

	for _, role := range mask.Who.InheritFrom {
		if strings.HasPrefix(role, "ID:") {
			if roleName, found := roleNameMap[role[3:]]; found {
				beneficiaries.Roles = append(beneficiaries.Roles, roleName)
			}
		} else {
			beneficiaries.Roles = append(beneficiaries.Roles, role)
		}
	}

	dosPerSchema := map[string][]string{}

	for _, do := range mask.What {
		fullnameSplit := strings.Split(do.DataObject.FullName, ".")

		if len(fullnameSplit) != 4 {
			logger.Error(fmt.Sprintf("Invalid fullname for column %s in mask %s", do.DataObject.FullName, mask.Name))

			continue
		}

		schemaName := fullnameSplit[1]
		database := fullnameSplit[0]

		schemaFullName := database + "." + schemaName

		dosPerSchema[schemaFullName] = append(dosPerSchema[schemaFullName], do.DataObject.FullName)
	}

	// Step 1: Get existing masking policies with same prefix
	existingPolicies, err := s.repo.GetPoliciesLike("MASKING", fmt.Sprintf("%s%s", globalMaskName, "%"))
	if err != nil {
		return uniqueMaskName, err
	}

	// Step 2: For each schema create a new masking policy and force the DataObjects to use the new policy
	for schema, dos := range dosPerSchema {
		logger.Info(fmt.Sprintf("Updating mask %q for schema %q", mask.Name, schema))
		namesplit := strings.Split(schema, ".")

		database := namesplit[0]
		schemaName := namesplit[1]

		err = s.repo.CreateMaskPolicy(database, schemaName, uniqueMaskName, dos, mask.Type, &beneficiaries)
		if err != nil {
			return uniqueMaskName, err
		}
	}

	// Step 3: Remove old policies that we misted in step 1
	for _, policy := range existingPolicies {
		existingUniqueMaskNameSpit := strings.Split(policy.Name, "_")
		existingUniqueMaskName := strings.Join(existingUniqueMaskNameSpit[:len(existingUniqueMaskNameSpit)-1], "_")

		err = s.repo.DropMaskingPolicy(policy.DatabaseName, policy.SchemaName, existingUniqueMaskName)
		if err != nil {
			return uniqueMaskName, err
		}
	}

	return uniqueMaskName, nil
}

func (s *AccessToTargetSyncer) removeMask(maskName string) error {
	logger.Info(fmt.Sprintf("Remove mask %q", maskName))

	existingPolicies, err := s.repo.GetPoliciesLike("MASKING", fmt.Sprintf("%s%s", maskName, "%"))
	if err != nil {
		return err
	}

	for _, policy := range existingPolicies {
		err = s.repo.DropMaskingPolicy(policy.DatabaseName, policy.SchemaName, maskName)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *AccessToTargetSyncer) createGrantsForTableOrView(doType string, permissions []string, fullName string, metaData map[string]map[string]struct{}, grants set.Set[Grant]) error {
	// TODO: this does not work for Raito full names
	sfObject := common.ParseFullName(fullName)
	if sfObject.Database == nil || sfObject.Schema == nil || sfObject.Table == nil {
		return fmt.Errorf("expected fullName %q to have 3 parts (database.schema.view)", fullName)
	}

	if doType == IcebergTable {
		doType = ds.Table
	}

	for _, p := range permissions {
		if _, f := metaData[doType][strings.ToUpper(p)]; f {
			grants.Add(Grant{p, doType, common.FormatQuery(`%s.%s.%s`, *sfObject.Database, *sfObject.Schema, *sfObject.Table)})
		} else {
			logger.Warn(fmt.Sprintf("Permission %q does not apply to type %s", p, strings.ToUpper(doType)))
		}
	}

	if len(grants) > 0 {
		grants.Add(Grant{"USAGE", ds.Database, common.FormatQuery(`%s`, *sfObject.Database)},
			Grant{"USAGE", ds.Schema, common.FormatQuery(`%s.%s`, *sfObject.Database, *sfObject.Schema)})
	}

	return nil
}

func (s *AccessToTargetSyncer) getTablesForSchema(database, schema string) ([]TableEntity, error) {
	cacheKey := database + "." + schema

	if tables, f := s.tablesPerSchemaCache[cacheKey]; f {
		return tables, nil
	}

	tables := make([]TableEntity, 10)

	err := s.repo.GetTablesInDatabase(database, schema, func(entity interface{}) error {
		table := entity.(*TableEntity)
		tables = append(tables, *table)

		return nil
	})

	if err != nil {
		return nil, err
	}

	s.tablesPerSchemaCache[cacheKey] = tables

	return tables, nil
}

func (s *AccessToTargetSyncer) getSchemasForDatabase(database string) ([]SchemaEntity, error) {
	if schemas, f := s.schemasPerDataBaseCache[database]; f {
		return schemas, nil
	}

	schemas := make([]SchemaEntity, 10)

	err := s.repo.GetSchemasInDatabase(database, func(entity interface{}) error {
		schema := entity.(*SchemaEntity)
		schemas = append(schemas, *schema)

		return nil
	})

	if err != nil {
		return nil, err
	}

	s.schemasPerDataBaseCache[database] = schemas

	return schemas, nil
}

func (s *AccessToTargetSyncer) getWarehouses() ([]DbEntity, error) {
	if s.warehousesCache != nil {
		return s.warehousesCache, nil
	}

	var err error
	s.warehousesCache, err = s.repo.GetWarehouses()

	if err != nil {
		s.warehousesCache = nil
		return nil, err
	}

	return s.warehousesCache, nil
}

func (s *AccessToTargetSyncer) createGrantsForSchema(permissions []string, fullName string, metaData map[string]map[string]struct{}, isShared bool, grants set.Set[Grant]) error {
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
			logger.Info(fmt.Sprintf("Permission %q does not apply to type SCHEMA or any of its descendants. Skipping", p))
		}
	}

	// Only generate the USAGE grant if any applicable permissions were applied on the schema or any item below
	if len(grants) > 0 && !isShared {
		grants.Add(
			Grant{"USAGE", ds.Database, common.FormatQuery(`%s`, *sfObject.Database)},
			Grant{"USAGE", ds.Schema, common.FormatQuery(`%s.%s`, *sfObject.Database, *sfObject.Schema)})
	}

	return nil
}

func (s *AccessToTargetSyncer) createPermissionGrantsForSchema(database, schema, p string, metaData map[string]map[string]struct{}, isShared bool, grants set.Set[Grant]) (bool, error) {
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
	}

	return matchFound, nil
}

func (s *AccessToTargetSyncer) createPermissionGrantsForDatabase(database, p string, metaData map[string]map[string]struct{}, isShared bool, grants set.Set[Grant]) (bool, error) {
	matchFound := false

	dbType := ds.Database
	if isShared {
		dbType = SharedPrefix + dbType
	}

	if _, f := metaData[dbType][strings.ToUpper(p)]; f {
		matchFound = true

		if strings.EqualFold(p, USAGE_ON_DATABASE) {
			p = USAGE
		}

		grants.Add(Grant{p, dbType, database})
	} else {
		schemas, err := s.getSchemasForDatabase(database)
		if err != nil {
			return false, err
		}

		for _, schema := range schemas {
			if schema.Name == "INFORMATION_SCHEMA" || schema.Name == "" {
				continue
			}

			schemaMatchFound := false

			schemaMatchFound, err = s.createPermissionGrantsForSchema(database, schema.Name, p, metaData, isShared, grants)
			if err != nil {
				return matchFound, err
			}

			// Only generate the USAGE grant if any applicable permissions were applied on the schema or any item below
			if schemaMatchFound && !isShared {
				schemaName := schema.Name
				sfSchemaObject := common.SnowflakeObject{Database: &database, Schema: &schemaName, Table: nil, Column: nil}
				grants.Add(Grant{USAGE, ds.Schema, sfSchemaObject.GetFullName(true)})
			}

			matchFound = matchFound || schemaMatchFound
		}
	}

	return matchFound, nil
}

func (s *AccessToTargetSyncer) createPermissionGrantsForTable(database string, schema string, table TableEntity, p string, metaData map[string]map[string]struct{}, isShared bool, grants set.Set[Grant]) bool {
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

func (s *AccessToTargetSyncer) createGrantsForDatabase(permissions []string, database string, metaData map[string]map[string]struct{}, isShared bool, grants set.Set[Grant]) error {
	var err error

	for _, p := range permissions {
		databaseMatchFound := false
		databaseMatchFound, err = s.createPermissionGrantsForDatabase(database, p, metaData, isShared, grants)

		if err != nil {
			return err
		}

		if !databaseMatchFound {
			logger.Info(fmt.Sprintf("Permission %q does not apply to type DATABASE or any of its descendants. Skipping", p))
		}
	}

	// Only generate the USAGE grant if any applicable permissions were applied or any item below
	if len(grants) > 0 && !isShared {
		sfDBObject := common.SnowflakeObject{Database: &database, Schema: nil, Table: nil, Column: nil}
		grants.Add(Grant{USAGE, ds.Database, sfDBObject.GetFullName(true)})
	}

	return nil
}

func (s *AccessToTargetSyncer) createGrantsForWarehouse(permissions []string, warehouse string, metaData map[string]map[string]struct{}, grants set.Set[Grant]) {
	grants.Add(Grant{USAGE, "warehouse", common.FormatQuery(`%s`, warehouse)})

	for _, p := range permissions {
		if _, f := metaData["warehouse"][strings.ToUpper(p)]; !f {
			logger.Warn(fmt.Sprintf("Permission %q does not apply to type WAREHOUSE. Skipping", p))
			continue
		}

		grants.Add(Grant{p, "warehouse", common.FormatQuery(`%s`, warehouse)})
	}
}

func (s *AccessToTargetSyncer) createGrantsForAccount(permissions []string, metaData map[string]map[string]struct{}, grants set.Set[Grant]) error {
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
			logger.Info(fmt.Sprintf("Permission %q does not apply to type ACCOUNT (datasource) or any of its descendants. Skipping", p))
			continue
		}
	}

	return nil
}

func (s *AccessToTargetSyncer) updateOrCreateFilter(ctx context.Context, tableFullName string, aps []*importer.AccessProvider, roleNameMap map[string]string) (string, *string, error) {
	tableFullnameSplit := strings.Split(tableFullName, ".")
	database := tableFullnameSplit[0]
	schema := tableFullnameSplit[1]
	table := tableFullnameSplit[2]

	filterExpressions := make([]string, 0, len(aps))
	arguments := set.NewSet[string]()

	for _, ap := range aps {
		fExpression, apArguments, err := filterExpression(ctx, ap)
		if err != nil {
			return "", nil, fmt.Errorf("failed to generate filter expression for access provider %s: %w", ap.Name, err)
		}

		whoExpressionPart, hasWho := filterWhoExpression(ap, roleNameMap)

		if !hasWho {
			continue
		}

		filterExpressions = append(filterExpressions, fmt.Sprintf("(%s) AND (%s)", whoExpressionPart, fExpression))

		arguments.Add(apArguments...)

		logger.Info(fmt.Sprintf("Filter expression for access provider %s: %s (%+v)", ap.Name, fExpression, apArguments))
	}

	if len(filterExpressions) == 0 {
		// No filter expression for example when no who was defined for the filter
		logger.Info("No filter expressions found for table %s.", tableFullName)

		filterExpressions = append(filterExpressions, "FALSE")
	}

	filterName := fmt.Sprintf("raito_%s_%s_%s_filter", schema, table, gonanoid.MustGenerate(idAlphabet, 8))

	err := s.repo.UpdateFilter(database, schema, table, filterName, arguments.Slice(), strings.Join(filterExpressions, " OR "))
	if err != nil {
		return "", nil, fmt.Errorf("failed to update filter %s: %w", filterName, err)
	}

	return filterName, ptr.String(fmt.Sprintf("%s.%s", tableFullName, filterName)), nil
}

func (s *AccessToTargetSyncer) deleteFilter(tableFullName string, aps []*importer.AccessProvider) error {
	tableFullnameSplit := strings.Split(tableFullName, ".")
	database := tableFullnameSplit[0]
	schema := tableFullnameSplit[1]
	table := tableFullnameSplit[2]

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

func (s *AccessToTargetSyncer) executeGrantOnRole(perm, on, roleName string, apType *string) error {
	if isDatabaseRole(apType) {
		database, parsedRoleName, err := parseDatabaseRoleExternalId(roleName)
		if err != nil {
			return err
		}

		return s.repo.ExecuteGrantOnDatabaseRole(perm, on, database, parsedRoleName)
	}

	return s.repo.ExecuteGrantOnAccountRole(perm, on, roleName, false)
}

func (s *AccessToTargetSyncer) executeRevokeOnRole(perm, on, roleName string, apType *string) error {
	if isDatabaseRole(apType) {
		database, parsedRoleName, err := parseDatabaseRoleExternalId(roleName)
		if err != nil {
			return err
		}

		return s.repo.ExecuteRevokeOnDatabaseRole(perm, on, database, parsedRoleName)
	}

	return s.repo.ExecuteRevokeOnAccountRole(perm, on, roleName, false)
}

func (s *AccessToTargetSyncer) mergeGrants(externalId string, apType *string, found []Grant, expected []Grant, metaData map[string]map[string]struct{}) error {
	toAdd := slice.SliceDifference(expected, found)
	toRemove := slice.SliceDifference(found, expected)

	logger.Info(fmt.Sprintf("Found %d grants to add and %d grants to remove for role %q", len(toAdd), len(toRemove), externalId))

	for _, grant := range toAdd {
		if verifyGrant(grant, metaData) {
			err := s.executeGrantOnRole(grant.Permissions, grant.GetGrantOnType()+" "+grant.On, externalId, apType)
			if err != nil {
				return err
			}
		}
	}

	for _, grant := range toRemove {
		if verifyGrant(grant, metaData) {
			err := s.executeRevokeOnRole(grant.Permissions, grant.GetGrantOnType()+" "+grant.On, externalId, apType)
			if err != nil {
				return err
			}
		}
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
	argumentRegexp := regexp.MustCompile(`\{([a-zA-Z0-9]+)\}`)

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

func groupFiltersByTable(aps map[string]*importer.AccessProvider, feedbackHandler wrappers.AccessProviderFeedbackHandler) (map[string][]*importer.AccessProvider, error) {
	groupedFilters := make(map[string][]*importer.AccessProvider)

	for key, filter := range aps {
		if len(filter.What) != 1 || filter.What[0].DataObject.Type != ds.Table {
			err := feedbackHandler.AddAccessProviderFeedback(importer.AccessProviderSyncFeedback{
				AccessProvider: filter.Id,
				Errors:         []string{"Filters can only be applied to a single table."},
			})

			if err != nil {
				return nil, fmt.Errorf("failed to add access provider feedback: %w", err)
			}

			continue
		}

		table := filter.What[0].DataObject.FullName

		groupedFilters[table] = append(groupedFilters[table], aps[key])
	}

	return groupedFilters, nil
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

	logger.Warn(fmt.Sprintf("Unknown permission %q for entity type %s. Skipping. %+v", grant.Permissions, grant.OnType, metaData))

	return false
}

func createComment(ap *importer.AccessProvider, update bool) string {
	action := "Created"
	if update {
		action = "Updated"
	}

	return fmt.Sprintf("%s by Raito from access provider %s. %s", action, ap.Name, ap.Description)
}

func raitoMaskName(roleName string) string {
	roleNameWithoutPrefix := strings.TrimPrefix(roleName, maskPrefix)

	result := fmt.Sprintf("%s%s", maskPrefix, strings.ReplaceAll(strings.ToUpper(roleNameWithoutPrefix), " ", "_"))

	var validMaskName []rune

	for _, r := range result {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			validMaskName = append(validMaskName, r)
		}
	}

	return string(validMaskName)
}

func raitoMaskUniqueName(name string) string {
	return raitoMaskName(name) + "_" + gonanoid.MustGenerate(idAlphabet, 8)
}
