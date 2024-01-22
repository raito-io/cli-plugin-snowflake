package snowflake

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/aws/smithy-go/ptr"
	"github.com/hashicorp/go-multierror"
	gonanoid "github.com/matoous/go-nanoid/v2"
	exporter "github.com/raito-io/cli/base/access_provider/sync_from_target"
	importer "github.com/raito-io/cli/base/access_provider/sync_to_target"
	"github.com/raito-io/cli/base/access_provider/sync_to_target/naming_hint"
	ds "github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/util/slice"
	"github.com/raito-io/cli/base/wrappers"
	"github.com/raito-io/golang-set/set"

	"github.com/raito-io/cli-plugin-snowflake/common"
)

func (s *AccessSyncer) generateUniqueExternalId(ap *importer.AccessProvider, prefix string) (string, error) {
	if isDatabaseRole(ap.Type) {

		sfRoleName := ap.Name
		if sfRoleName != "" {
			sfRoleName = ap.NamingHint
		}

		database, cleanedRoleName, err := parseDatabaseRoleRoleName(ap.NamingHint)
		if err != nil {
			return "", err
		}

		uniqueRoleNameGenerator, err := s.getUniqueRoleNameGenerator(prefix, &database)
		if err != nil {
			return "", err
		}

		// Temp updating namingHint to "resource only without database" as this is the way Generate will create a unique resource name
		oldNamingHint := ap.NamingHint
		ap.NamingHint = cleanedRoleName

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

func (s *AccessSyncer) getUniqueRoleNameGenerator(prefix string, database *string) (naming_hint.UniqueGenerator, error) {
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

func (s *AccessSyncer) SyncAccessProviderRolesToTarget(ctx context.Context, toRemoveAps map[string]*importer.AccessProvider, toProcessAps map[string]*importer.AccessProvider, feedbackHandler wrappers.AccessProviderFeedbackHandler, configMap *config.ConfigMap, repo dataAccessRepository) error {
	logger.Info("Configuring access providers as roles in Snowflake")

	databaseRoleSupportEnabled := configMap.GetBoolWithDefault(SfDatabaseRoles, false)

	err := s.removeRolesToRemove(toRemoveAps, repo, feedbackHandler)
	if err != nil {
		return err
	}

	toRenameAps := make(map[string]string)

	for externalId, ap := range toProcessAps {
		if ap.ExternalId != nil && *ap.ExternalId != externalId {
			toRenameAps[externalId] = *ap.ExternalId
		}
	}

	existingRoles, err := s.findRoles("", databaseRoleSupportEnabled, repo)
	if err != nil {
		return err
	}

	err = s.generateAccessControls(ctx, toProcessAps, existingRoles, toRenameAps, repo, configMap, feedbackHandler)
	if err != nil {
		return err
	}

	return nil
}

func (s *AccessSyncer) SyncAccessProviderMasksToTarget(ctx context.Context, apToRemoveMap map[string]*importer.AccessProvider, apMap map[string]*importer.AccessProvider, roleNameMap map[string]string, feedbackHandler wrappers.AccessProviderFeedbackHandler, configMap *config.ConfigMap, repo dataAccessRepository) error {
	var err error

	if configMap.GetBoolWithDefault(SfStandardEdition, false) {
		if len(apToRemoveMap) > 0 || len(apMap) > 0 {
			logger.Error("Skipping masking policies due to Snowflake Standard Edition.")
		}

		return nil
	}

	logger.Info(fmt.Sprintf("Configuring access provider as masks in Snowflake. Update %d masks remove %d masks", len(apMap), len(apToRemoveMap)))

	// Step 1: Update masks and create new masks
	for _, mask := range apMap {
		maskName, err2 := s.updateMask(ctx, mask, roleNameMap, repo)
		fi := importer.AccessProviderSyncFeedback{AccessProvider: mask.Id, ActualName: maskName, ExternalId: &maskName}

		if err2 != nil {
			fi.Errors = append(fi.Errors, err2.Error())
		}

		err = feedbackHandler.AddAccessProviderFeedback(fi)
		if err != nil {
			return err
		}
	}

	// Step 2: Remove old masks
	for maskToRemove, maskAp := range apToRemoveMap {
		externalId := maskToRemove
		fi := importer.AccessProviderSyncFeedback{AccessProvider: maskAp.Id, ActualName: maskToRemove, ExternalId: &externalId}

		err = s.removeMask(ctx, maskToRemove, repo)
		if err != nil {
			fi.Errors = append(fi.Errors, err.Error())
		}

		err = feedbackHandler.AddAccessProviderFeedback(fi)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *AccessSyncer) SyncAccessProviderFiltersToTarget(ctx context.Context, apToRemoveMap map[string]*importer.AccessProvider, apMap map[string]*importer.AccessProvider, roleNameMap map[string]string, feedbackHandler wrappers.AccessProviderFeedbackHandler, configMap *config.ConfigMap, repo dataAccessRepository) error {
	if configMap.GetBoolWithDefault(SfStandardEdition, false) {
		if len(apToRemoveMap) > 0 || len(apMap) > 0 {
			logger.Error("Skipping filter policies due to Snowflake Standard Edition.")
		}

		return nil
	}

	logger.Info(fmt.Sprintf("Configuring access provider as filters in Snowflake. Update %d masks remove %d masks", len(apMap), len(apToRemoveMap)))

	//Groups filters by table
	updateGroupedFilters, err := groupFiltersByTable(apMap, feedbackHandler)
	if err != nil {
		return err
	}

	removeGroupedFilters, err := groupFiltersByTable(apToRemoveMap, feedbackHandler)
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
			var actualNameStr string
			if actualName != nil {
				actualNameStr = *actualName
			} else if ap.ActualName != nil {
				actualNameStr = *ap.ActualName
			}

			var apExternalId *string
			if externalId != nil {
				apExternalId = externalId
			} else {
				apExternalId = ap.ExternalId
			}

			ferr := feedbackHandler.AddAccessProviderFeedback(importer.AccessProviderSyncFeedback{
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
		filterName, externalId, createErr := s.updateOrCreateFilter(ctx, repo, table, filters, roleNameMap)

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
				deleteErr := s.deleteFilter(repo, table, filters)

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
			deleteErr := s.deleteFilter(repo, table, filters)

			ferr := feedbackFn(filters, nil, nil, deleteErr)
			if ferr != nil {
				return ferr
			}
		}
	}

	return nil
}

func (d *dummyFeedbackHandler) AddAccessProviderFeedback(accessProviderFeedback importer.AccessProviderSyncFeedback) error {
	if len(accessProviderFeedback.Errors) > 0 {
		for _, err := range accessProviderFeedback.Errors {
			logger.Error(fmt.Sprintf("error during syncing of access provider %q; %s", accessProviderFeedback.AccessProvider, err))
		}
	}

	return nil
}

func (s *AccessSyncer) removeRolesToRemove(toRemoveAps map[string]*importer.AccessProvider, repo dataAccessRepository, feedbackHandler wrappers.AccessProviderFeedbackHandler) error {
	if len(toRemoveAps) > 0 {
		logger.Info(fmt.Sprintf("Removing %d old Raito roles in Snowflake", len(toRemoveAps)))

		for toRemoveExternalId, ap := range toRemoveAps {
			if ap == nil {
				logger.Warn(fmt.Sprintf("no linked access provider found for %q, so just going to remove it from Snowflake", toRemoveExternalId))

				err := s.dropRole(toRemoveExternalId, isDatabaseRoleByExternalId(toRemoveExternalId), repo)
				if err != nil {
					return err
				}

				continue
			}

			fi := importer.AccessProviderSyncFeedback{
				AccessProvider: ap.Id,
				ExternalId:     ptr.String(toRemoveExternalId),
			}

			if ap.ActualName != nil {
				fi.ActualName = *ap.ActualName
			}

			err := s.dropRole(toRemoveExternalId, isDatabaseRole(ap.Type), repo)
			// If an error occurs (and not already deleted), we send an error back as feedback
			if err != nil && !strings.Contains(err.Error(), "does not exist") {
				logger.Error(fmt.Sprintf("unable to drop role %q: %s", toRemoveExternalId, err.Error()))

				fi.Errors = append(fi.Errors, fmt.Sprintf("unable to drop role %q: %s", toRemoveExternalId, err.Error()))
			}

			err = feedbackHandler.AddAccessProviderFeedback(fi)
			if err != nil {
				return err
			}
		}
	} else {
		logger.Info("No old Raito roles to remove in Snowflake")
	}

	return nil
}

func (s *AccessSyncer) getShareNames(repo dataAccessRepository) ([]string, error) {
	dbShares, err := repo.GetShares()
	if err != nil {
		return nil, err
	}

	shareNames := make([]string, len(dbShares))
	for _, e := range dbShares {
		shareNames = append(shareNames, e.Name)
	}

	return shareNames, nil
}

func (s *AccessSyncer) importPoliciesOfType(accessProviderHandler wrappers.AccessProviderHandler, repo dataAccessRepository, policyType string, action exporter.Action) error {
	policyEntities, err := repo.GetPolicies(policyType)
	if err != nil {
		// For Standard edition, row access policies are not supported. Failsafe in case `sf-standard-edition` is overlooked.
		// You can see the Snowflake edition in the UI, or through the 'show organization accounts;' query (ORGADMIN role needed).
		if strings.Contains(err.Error(), "Unsupported feature") {
			logger.Warn(fmt.Sprintf("Could not fetch policies of type %s; unsupported feature.", policyType))
		} else {
			return fmt.Errorf("error fetching all %s policies: %s", policyType, err.Error())
		}
	}

	for _, policy := range policyEntities {
		if !strings.HasPrefix(strings.Replace(policy.Kind, "_", " ", -1), policyType) {
			logger.Warn(fmt.Sprintf("Skipping policy %s of kind %s, expected: %s", policy.Name, policyType, policy.Kind))
			continue
		} else if strings.HasPrefix(policy.Name, maskPrefix) {
			logger.Debug(fmt.Sprintf("Masking policy %s defined by RAITO. Not exporting this", policy.Name))
			continue
		}

		logger.Info(fmt.Sprintf("Reading SnowFlake %s policy %s in Schema %s, Table %s", policyType, policy.Name, policy.SchemaName, policy.DatabaseName))

		fullName := fmt.Sprintf("%s-%s-%s", policy.DatabaseName, policy.SchemaName, policy.Name)

		ap := exporter.AccessProvider{
			ExternalId:        fullName,
			Name:              fullName,
			NamingHint:        policy.Name,
			Action:            action,
			NotInternalizable: true,
			Who:               nil,
			ActualName:        fullName,
			What:              make([]exporter.WhatItem, 0),
		}

		// get policy definition
		desribeMaskingPolicyEntities, err := repo.DescribePolicy(policyType, policy.DatabaseName, policy.SchemaName, policy.Name)
		if err != nil {
			logger.Error(err.Error())

			return err
		}

		if len(desribeMaskingPolicyEntities) != 1 {
			err = fmt.Errorf("found %d definitions for %s policy %s.%s.%s, only expecting one", len(desribeMaskingPolicyEntities), policyType, policy.DatabaseName, policy.SchemaName, policy.Name)
			logger.Error(err.Error())

			return err
		}

		ap.Policy = desribeMaskingPolicyEntities[0].Body

		// get policy references
		policyReferenceEntities, err := repo.GetPolicyReferences(policy.DatabaseName, policy.SchemaName, policy.Name)
		if err != nil {
			logger.Error(err.Error())

			return fmt.Errorf("error fetching %s policy references: %s", policyType, err.Error())
		}

		for ind := range policyReferenceEntities {
			policyReference := policyReferenceEntities[ind]
			if !strings.EqualFold("Active", policyReference.POLICY_STATUS) {
				continue
			}

			var dor ds.DataObjectReference
			if policyReference.REF_COLUMN_NAME.Valid {
				dor = ds.DataObjectReference{
					Type:     "COLUMN",
					FullName: common.FormatQuery(`%s.%s.%s.%s`, policyReference.REF_DATABASE_NAME, policyReference.REF_SCHEMA_NAME, policyReference.REF_ENTITY_NAME, policyReference.REF_COLUMN_NAME.String),
				}
			} else {
				dor = ds.DataObjectReference{
					Type:     "TABLE",
					FullName: common.FormatQuery(`%s.%s.%s`, policyReference.REF_DATABASE_NAME, policyReference.REF_SCHEMA_NAME, policyReference.REF_ENTITY_NAME),
				}
			}

			ap.What = append(ap.What, exporter.WhatItem{
				DataObject:  &dor,
				Permissions: []string{},
			})
		}

		err = accessProviderHandler.AddAccessProviders(&ap)
		if err != nil {
			return fmt.Errorf("error adding access provider to import file: %s", err.Error())
		}
	}

	return nil
}

func (s *AccessSyncer) importMaskingPolicies(accessProviderHandler wrappers.AccessProviderHandler, repo dataAccessRepository) error {
	return s.importPoliciesOfType(accessProviderHandler, repo, "MASKING", exporter.Mask)
}

func (s *AccessSyncer) importRowAccessPolicies(accessProviderHandler wrappers.AccessProviderHandler, repo dataAccessRepository) error {
	return s.importPoliciesOfType(accessProviderHandler, repo, "ROW ACCESS", exporter.Filtered)
}

// findRoles returns the set of existing roles with the given prefix
func (s *AccessSyncer) findRoles(prefix string, databaseRoleSupportEnabled bool, repo dataAccessRepository) (set.Set[string], error) {
	existingRoles := set.NewSet[string]()

	roleEntities, err := repo.GetAccountRolesWithPrefix(prefix)
	if err != nil {
		return nil, err
	}

	for _, roleEntity := range roleEntities {
		existingRoles.Add(accountRoleExternalIdGenerator(roleEntity.Name))
	}

	if !databaseRoleSupportEnabled {
		return existingRoles, nil
	}

	//Get all database roles for each database and add database roles to existing roles
	databases, err := s.getDatabases(repo)
	if err != nil {
		return nil, err
	}

	for _, database := range databases {
		// Get all database roles for database
		roleEntities, err := repo.GetDatabaseRolesWithPrefix(database.Name, prefix)
		if err != nil {
			return nil, err
		}

		for _, roleEntity := range roleEntities {
			existingRoles.Add(databaseRoleExternalIdGenerator(database.Name, roleEntity.Name))
		}
	}

	return existingRoles, nil
}

func (s *AccessSyncer) buildMetaDataMap(metaData *ds.MetaData) map[string]map[string]struct{} {
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
func (s *AccessSyncer) handleAccessProvider(ctx context.Context, externalId string, toProcessAps map[string]*importer.AccessProvider, existingRoles set.Set[string], toRenameAps map[string]string, rolesCreated map[string]interface{}, repo dataAccessRepository, metaData map[string]map[string]struct{}) error {
	accessProvider := toProcessAps[externalId]
	logger.Debug(fmt.Sprintf("Handle access provider with key %q - %+v - %+v", externalId, accessProvider, toProcessAps))

	ignoreWho := accessProvider.WhoLocked != nil && *accessProvider.WhoLocked
	ignoreInheritance := accessProvider.InheritanceLocked != nil && *accessProvider.InheritanceLocked
	ignoreWhat := accessProvider.WhatLocked != nil && *accessProvider.WhatLocked

	logger.Info(fmt.Sprintf("Generating access controls for access provider %q (Ignore who: %t; Ignore inheritance: %t; Ignore what: %t)", accessProvider.Name, ignoreWho, ignoreInheritance, ignoreWhat))

	// Extract RoleNames from Access Providers that are among the whoList of this one
	inheritedRoles := make([]string, 0)

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
	var expectedGrants []Grant

	if !ignoreWhat {
		for _, what := range accessProvider.What {
			permissions := what.Permissions

			if len(permissions) == 0 {
				continue
			}

			if isTableType(what.DataObject.Type) {
				grants, err2 := s.createGrantsForTableOrView(what.DataObject.Type, permissions, what.DataObject.FullName, metaData)
				if err2 != nil {
					return err2
				}

				expectedGrants = append(expectedGrants, grants...)
			} else if what.DataObject.Type == ds.Schema {
				grants, err2 := s.createGrantsForSchema(repo, permissions, what.DataObject.FullName, metaData, false)
				if err2 != nil {
					return err2
				}

				expectedGrants = append(expectedGrants, grants...)
			} else if what.DataObject.Type == "shared-schema" {
				grants, err2 := s.createGrantsForSchema(repo, permissions, what.DataObject.FullName, metaData, true)
				if err2 != nil {
					return err2
				}

				expectedGrants = append(expectedGrants, grants...)
			} else if what.DataObject.Type == "shared-database" {
				grants, err2 := s.createGrantsForDatabase(repo, permissions, what.DataObject.FullName, metaData, true)
				if err2 != nil {
					return err2
				}

				expectedGrants = append(expectedGrants, grants...)
			} else if what.DataObject.Type == ds.Database {
				grants, err2 := s.createGrantsForDatabase(repo, permissions, what.DataObject.FullName, metaData, false)
				if err2 != nil {
					return err2
				}

				expectedGrants = append(expectedGrants, grants...)
			} else if what.DataObject.Type == "warehouse" {
				expectedGrants = append(expectedGrants, s.createGrantsForWarehouse(permissions, what.DataObject.FullName, metaData)...)
			} else if what.DataObject.Type == ds.Datasource {
				grants, err2 := s.createGrantsForAccount(repo, permissions, metaData)
				if err2 != nil {
					return err2
				}

				expectedGrants = append(expectedGrants, grants...)
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
				err := s.renameRole(oldExternalId, externalId, accessProvider.Type, repo)
				if err != nil {
					return fmt.Errorf("error while renaming role %q to %q: %s", oldExternalId, externalId, err.Error())
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
				err := s.dropRole(oldExternalId, isDatabaseRoleByExternalId(oldExternalId), repo)
				if err != nil {
					return fmt.Errorf("error while dropping role (%s) which was the old name of access provider %q: %s", oldExternalId, accessProvider.Name, err.Error())
				}

				existingRoles.Remove(oldExternalId)
			}
		}
	}

	var foundGrants []Grant

	// If the role already exists in the system
	if existingRoles.Contains(externalId) {
		logger.Info(fmt.Sprintf("Merging role: %q", externalId))

		// Only update the comment if we have full control over the role (who and what not ignored)
		if !ignoreWho && !ignoreWhat {
			err2 := s.commentOnRoleIfExists(createComment(accessProvider, true), externalId, repo)
			if err2 != nil {
				return fmt.Errorf("error while updating comment on role %q: %s", externalId, err2.Error())
			}
		}

		if !ignoreWho || !ignoreInheritance {
			grantsOfRole, err3 := s.retrieveGrantsOfRole(externalId, accessProvider.Type, repo)
			if err3 != nil {
				return err3
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
						return err2
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
						return fmt.Errorf("error can not assign users from a database role %q", externalId)
					}

					e := repo.GrantUsersToAccountRole(ctx, externalId, toAdd...)
					if e != nil {
						return fmt.Errorf("error while assigning users to role %q: %s", externalId, e.Error())
					}
				}

				if len(toRemove) > 0 {
					if isDatabaseRole(accessProvider.Type) {
						return fmt.Errorf("error can not unassign users from a database role %q", externalId)
					}

					e := repo.RevokeUsersFromAccountRole(ctx, externalId, toRemove...)
					if e != nil {
						return fmt.Errorf("error while unassigning users from role %q: %s", externalId, e.Error())
					}
				}
			}

			if !ignoreInheritance {
				toAdd := slice.StringSliceDifference(inheritedRoles, rolesOfRole, false)
				toRemove := slice.StringSliceDifference(rolesOfRole, inheritedRoles, false)
				logger.Info(fmt.Sprintf("Identified %d roles to add and %d roles to remove from role %q", len(toAdd), len(toRemove), externalId))

				if len(toAdd) > 0 {
					e := s.grantRolesToRole(ctx, repo, externalId, accessProvider.Type, toAdd...)
					if e != nil {
						return fmt.Errorf("error while assigning role to role %q: %s", externalId, e.Error())
					}
				}

				if len(toRemove) > 0 {
					e := s.revokeRolesFromRole(ctx, repo, externalId, accessProvider.Type, toRemove...)
					if e != nil {
						return fmt.Errorf("error while unassigning role from role %q: %s", externalId, e.Error())
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
					e := s.executeRevokeOnRole("ALL", common.FormatQuery(`FUTURE SCHEMAS IN DATABASE %s`, what.DataObject.FullName), externalId, accessProvider.Type, repo)
					if e != nil {
						return fmt.Errorf("error while assigning future schema grants in database %q to role %q: %s", what.DataObject.FullName, externalId, e.Error())
					}

					e = s.executeRevokeOnRole("ALL", common.FormatQuery(`FUTURE TABLES IN DATABASE %s`, what.DataObject.FullName), externalId, accessProvider.Type, repo)
					if e != nil {
						return fmt.Errorf("error while assigning future table grants in database %q to role %q: %s", what.DataObject.FullName, externalId, e.Error())
					}
				} else if what.DataObject.Type == "schema" {
					e := s.executeRevokeOnRole("ALL", fmt.Sprintf("FUTURE TABLES IN SCHEMA %s", what.DataObject.FullName), externalId, accessProvider.Type, repo)
					if e != nil {
						return fmt.Errorf("error while assigning future table grants in schema %q to role %q: %s", what.DataObject.FullName, externalId, e.Error())
					}
				}
			}

			grantsToRole, err3 := s.getGrantsToRole(externalId, accessProvider.Type, repo)
			if err3 != nil {
				return err3
			}

			logger.Debug(fmt.Sprintf("Found grants for role %q: %+v", externalId, grantsToRole))

			foundGrants = make([]Grant, 0, len(grantsToRole))

			for _, grant := range grantsToRole {
				if strings.EqualFold(grant.GrantedOn, "ACCOUNT") {
					foundGrants = append(foundGrants, Grant{grant.Privilege, "account", ""})
				} else if strings.EqualFold(grant.Privilege, "OWNERSHIP") {
					logger.Warn(fmt.Sprintf("Ignoring permission %q on %q for Role %q as this will remain untouched", grant.Privilege, grant.Name, externalId))
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
		logger.Info(fmt.Sprintf("Creating role %q", externalId))

		if _, rf := rolesCreated[externalId]; !rf {
			// Create the role if not exists
			err := s.createRole(externalId, accessProvider.Type, repo)
			if err != nil {
				return fmt.Errorf("error while creating role %q: %s", externalId, err.Error())
			}

			// Updating the comment (independent of creation)
			err = s.commentOnRoleIfExists(createComment(accessProvider, false), externalId, repo)
			if err != nil {
				return fmt.Errorf("error while updating comment on role %q: %s", externalId, err.Error())
			}
			rolesCreated[externalId] = struct{}{}
		}

		if !ignoreWho && len(accessProvider.Who.Users) > 0 {
			if isDatabaseRole(accessProvider.Type) {
				return fmt.Errorf("error can not assign users to a database role %q", externalId)
			}

			err := repo.GrantUsersToAccountRole(ctx, externalId, accessProvider.Who.Users...)
			if err != nil {
				return fmt.Errorf("error while assigning users to role %q: %s", externalId, err.Error())
			}
		}

		if !ignoreInheritance {
			err := s.grantRolesToRole(ctx, repo, externalId, accessProvider.Type, inheritedRoles...)
			if err != nil {
				return fmt.Errorf("error while assigning roles to role %q: %s", externalId, err.Error())
			}
		}
	}

	if !ignoreWhat {
		err := s.mergeGrants(repo, externalId, accessProvider.Type, foundGrants, expectedGrants, metaData)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *AccessSyncer) getGrantsToRole(externalId string, apType *string, repo dataAccessRepository) ([]GrantToRole, error) {
	if isDatabaseRole(apType) {
		database, parsedRoleName, err := parseDatabaseRoleExternalId(externalId)
		if err != nil {
			return nil, err
		}

		return repo.GetGrantsToDatabaseRole(database, parsedRoleName)
	}

	return repo.GetGrantsToAccountRole(externalId)
}

func (s *AccessSyncer) splitRoles(inheritedRoles []string) ([]string, []string) {
	toAddDatabaseRoles := []string{}

	for _, role := range inheritedRoles {
		if isDatabaseRoleByExternalId(role) {
			toAddDatabaseRoles = append(toAddDatabaseRoles, role)
		}
	}

	toAddAccountRoles := slice.SliceDifference(inheritedRoles, toAddDatabaseRoles)

	return toAddDatabaseRoles, toAddAccountRoles
}

func (s *AccessSyncer) grantRolesToRole(ctx context.Context, repo dataAccessRepository, targetExternalId string, targetApType *string, roles ...string) error {
	toAddDatabaseRoles, toAddAccountRoles := s.splitRoles(roles)

	if isDatabaseRole(targetApType) {
		database, parsedRoleName, err := parseDatabaseRoleExternalId(targetExternalId)
		if err != nil {
			return err
		}

		err = repo.GrantDatabaseRolesToDatabaseRole(ctx, database, parsedRoleName, toAddDatabaseRoles...)
		if err != nil {
			return err
		}

		return repo.GrantAccountRolesToDatabaseRole(ctx, database, parsedRoleName, toAddAccountRoles...)
	}

	if len(toAddDatabaseRoles) > 0 {
		return fmt.Errorf("error can not assign database roles to an account role %q - %v", targetExternalId, toAddAccountRoles)
	}

	return repo.GrantAccountRolesToAccountRole(ctx, targetExternalId, toAddAccountRoles...)
}

func (s *AccessSyncer) revokeRolesFromRole(ctx context.Context, repo dataAccessRepository, targetExternalId string, targetApType *string, roles ...string) error {
	toAddDatabaseRoles, toAddAccountRoles := s.splitRoles(roles)

	if isDatabaseRole(targetApType) {
		database, parsedRoleName, err := parseDatabaseRoleExternalId(targetExternalId)
		if err != nil {
			return err
		}

		err = repo.RevokeDatabaseRolesFromDatabaseRole(ctx, database, parsedRoleName, toAddDatabaseRoles...)
		if err != nil {
			return err
		}

		return repo.RevokeAccountRolesFromDatabaseRole(ctx, database, parsedRoleName, toAddAccountRoles...)
	}

	if len(toAddDatabaseRoles) > 0 {
		return fmt.Errorf("error can not assign database roles to an account role %q - %v", targetExternalId, toAddAccountRoles)
	}

	return repo.RevokeAccountRolesFromAccountRole(ctx, targetExternalId, toAddAccountRoles...)
}

func (s *AccessSyncer) createRole(externalId string, apType *string, repo dataAccessRepository) error {
	if isDatabaseRole(apType) {
		database, cleanedRoleName, err := parseDatabaseRoleExternalId(externalId)
		if err != nil {
			return err
		}

		return repo.CreateDatabaseRole(database, cleanedRoleName)
	}

	return repo.CreateAccountRole(externalId)
}

func (s *AccessSyncer) dropRole(externalId string, databaseRole bool, repo dataAccessRepository) error {
	if databaseRole {
		database, cleanedRoleName, err := parseDatabaseRoleExternalId(externalId)
		if err != nil {
			return err
		}

		return repo.DropDatabaseRole(database, cleanedRoleName)
	}

	return repo.DropAccountRole(externalId)
}

func (s *AccessSyncer) renameRole(oldName, newName string, apType *string, repo dataAccessRepository) error {
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

		return repo.RenameDatabaseRole(oldDatabase, oldRoleName, newRoleName)
	}

	return repo.RenameAccountRole(oldName, newName)
}

func (s *AccessSyncer) commentOnRoleIfExists(comment, roleName string, repo dataAccessRepository) error {
	if isDatabaseRoleByExternalId(roleName) {
		database, cleanedRoleName, err := parseDatabaseRoleExternalId(roleName)
		if err != nil {
			return err
		}

		return repo.CommentDatabaseRoleIfExists(comment, database, cleanedRoleName)
	}

	return repo.CommentAccountRoleIfExists(comment, roleName)
}

func (s *AccessSyncer) generateAccessControls(ctx context.Context, toProcessAps map[string]*importer.AccessProvider, existingRoles set.Set[string], toRenameAps map[string]string, repo dataAccessRepository, configMap *config.ConfigMap, feedbackHandler wrappers.AccessProviderFeedbackHandler) error {
	// We always need the meta data
	rolesCreated := make(map[string]interface{})
	dsSyncer := DataSourceSyncer{}

	md, err := dsSyncer.GetDataSourceMetaData(ctx, configMap)
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

		if accessProvider.ActualName != nil {
			fi.ActualName = *accessProvider.ActualName
		}

		err2 := s.handleAccessProvider(ctx, externalId, toProcessAps, existingRoles, toRenameAps, rolesCreated, repo, metaData)

		err3 := s.handleAccessProviderFeedback(feedbackHandler, &fi, err2)
		if err3 != nil {
			return err3
		}
	}

	return nil
}

func (s *AccessSyncer) handleAccessProviderFeedback(feedbackHandler wrappers.AccessProviderFeedbackHandler, fi *importer.AccessProviderSyncFeedback, err error) error {
	if err != nil {
		logger.Error(err.Error())
		fi.Errors = append(fi.Errors, err.Error())
	}

	return feedbackHandler.AddAccessProviderFeedback(*fi)
}

func (s *AccessSyncer) updateMask(_ context.Context, mask *importer.AccessProvider, roleNameMap map[string]string, repo dataAccessRepository) (string, error) {
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
	existingPolicies, err := repo.GetPoliciesLike("MASKING", fmt.Sprintf("%s%s", globalMaskName, "%"))
	if err != nil {
		return uniqueMaskName, err
	}

	// Step 2: For each schema create a new masking policy and force the DataObjects to use the new policy
	for schema, dos := range dosPerSchema {
		logger.Info(fmt.Sprintf("Updating mask %q for schema %q", mask.Name, schema))
		namesplit := strings.Split(schema, ".")

		database := namesplit[0]
		schemaName := namesplit[1]

		err = repo.CreateMaskPolicy(database, schemaName, uniqueMaskName, dos, mask.Type, &beneficiaries)
		if err != nil {
			return uniqueMaskName, err
		}
	}

	// Step 3: Remove old policies that we misted in step 1
	for _, policy := range existingPolicies {
		existingUniqueMaskNameSpit := strings.Split(policy.Name, "_")
		existingUniqueMaskName := strings.Join(existingUniqueMaskNameSpit[:len(existingUniqueMaskNameSpit)-1], "_")

		err = repo.DropMaskingPolicy(policy.DatabaseName, policy.SchemaName, existingUniqueMaskName)
		if err != nil {
			return uniqueMaskName, err
		}
	}

	return uniqueMaskName, nil
}

func (s *AccessSyncer) removeMask(_ context.Context, maskName string, repo dataAccessRepository) error {
	logger.Info(fmt.Sprintf("Remove mask %q", maskName))

	existingPolicies, err := repo.GetPoliciesLike("MASKING", fmt.Sprintf("%s%s", maskName, "%"))
	if err != nil {
		return err
	}

	for _, policy := range existingPolicies {
		err = repo.DropMaskingPolicy(policy.DatabaseName, policy.SchemaName, maskName)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *AccessSyncer) createGrantsForTableOrView(doType string, permissions []string, fullName string, metaData map[string]map[string]struct{}) ([]Grant, error) {
	// TODO: this does not work for Raito full names
	sfObject := common.ParseFullName(fullName)
	if sfObject.Database == nil || sfObject.Schema == nil || sfObject.Table == nil {
		return nil, fmt.Errorf("expected fullName %q to have 3 parts (database.schema.view)", fullName)
	}

	grants := make([]Grant, 0, len(permissions)+2)

	for _, p := range permissions {
		if _, f := metaData[doType][strings.ToUpper(p)]; f {
			grants = append(grants, Grant{p, doType, common.FormatQuery(`%s.%s.%s`, *sfObject.Database, *sfObject.Schema, *sfObject.Table)})
		} else {
			logger.Warn(fmt.Sprintf("Permission %q does not apply to type %s", p, strings.ToUpper(doType)))
		}
	}

	if len(grants) > 0 {
		grants = append(grants,
			Grant{"USAGE", ds.Database, common.FormatQuery(`%s`, *sfObject.Database)},
			Grant{"USAGE", ds.Schema, common.FormatQuery(`%s.%s`, *sfObject.Database, *sfObject.Schema)})
	}

	return grants, nil
}

func (s *AccessSyncer) getTablesForSchema(repo dataAccessRepository, database, schema string) ([]TableEntity, error) {
	cacheKey := database + "." + schema

	if tables, f := s.tablesPerSchemaCache[cacheKey]; f {
		return tables, nil
	}

	tables := make([]TableEntity, 10)

	err := repo.GetTablesInDatabase(database, schema, func(entity interface{}) error {
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

func (s *AccessSyncer) getSchemasForDatabase(repo dataAccessRepository, database string) ([]SchemaEntity, error) {
	if schemas, f := s.schemasPerDataBaseCache[database]; f {
		return schemas, nil
	}

	schemas := make([]SchemaEntity, 10)

	err := repo.GetSchemasInDatabase(database, func(entity interface{}) error {
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

func (s *AccessSyncer) getDatabases(repo dataAccessRepository) ([]DbEntity, error) {
	if s.databasesCache != nil {
		return s.databasesCache, nil
	}

	var err error
	s.databasesCache, err = repo.GetDatabases()

	if err != nil {
		s.databasesCache = nil
		return nil, err
	}

	return s.databasesCache, nil
}

func (s *AccessSyncer) getWarehouses(repo dataAccessRepository) ([]DbEntity, error) {
	if s.warehousesCache != nil {
		return s.warehousesCache, nil
	}

	var err error
	s.warehousesCache, err = repo.GetWarehouses()

	if err != nil {
		s.warehousesCache = nil
		return nil, err
	}

	return s.warehousesCache, nil
}

func (s *AccessSyncer) createGrantsForSchema(repo dataAccessRepository, permissions []string, fullName string, metaData map[string]map[string]struct{}, isShared bool) ([]Grant, error) {
	// TODO: this does not work for Raito full names
	sfObject := common.ParseFullName(fullName)
	if sfObject.Database == nil || sfObject.Schema == nil || sfObject.Table != nil || sfObject.Column != nil {
		return nil, fmt.Errorf("expected fullName %q to have exactly 2 parts (database.schema)", fullName)
	}

	grants := make([]Grant, 0, len(permissions)+2)

	var err error

	for _, p := range permissions {
		permissionMatchFound := false

		grants, permissionMatchFound, err = s.createPermissionGrantsForSchema(repo, *sfObject.Database, *sfObject.Schema, p, metaData, grants, isShared)
		if err != nil {
			return nil, err
		}

		if !permissionMatchFound {
			logger.Warn(fmt.Sprintf("Permission %q does not apply to type SCHEMA or any of its descendants. Skipping", p))
		}
	}

	// Only generate the USAGE grant if any applicable permissions were applied on the schema or any item below
	if len(grants) > 0 && !isShared {
		grants = append(grants,
			Grant{"USAGE", ds.Database, common.FormatQuery(`%s`, *sfObject.Database)},
			Grant{"USAGE", ds.Schema, common.FormatQuery(`%s.%s`, *sfObject.Database, *sfObject.Schema)})
	}

	return grants, nil
}

func (s *AccessSyncer) createPermissionGrantsForSchema(repo dataAccessRepository, database, schema, p string, metaData map[string]map[string]struct{}, grants []Grant, isShared bool) ([]Grant, bool, error) {
	matchFound := false

	schemaType := ds.Schema
	if isShared {
		schemaType = SharedPrefix + schemaType
	}

	// Check if the permission is applicable on the schema itself
	if _, f := metaData[schemaType][strings.ToUpper(p)]; f {
		grants = append(grants, Grant{p, schemaType, common.FormatQuery(`%s.%s`, database, schema)})
		matchFound = true
	} else {
		tables, err := s.getTablesForSchema(repo, database, schema)
		if err != nil {
			return nil, false, err
		}

		// Run through all the tabular things (tables, views, ...) in the schema
		for _, table := range tables {
			tableMatchFound := false
			grants, tableMatchFound = s.createPermissionGrantsForTable(database, schema, table, p, metaData, grants, isShared)
			matchFound = matchFound || tableMatchFound
		}
	}

	return grants, matchFound, nil
}

func (s *AccessSyncer) createPermissionGrantsForDatabase(repo dataAccessRepository, database, p string, metaData map[string]map[string]struct{}, grants []Grant, isShared bool) ([]Grant, bool, error) {
	matchFound := false

	dbType := ds.Database
	if isShared {
		dbType = SharedPrefix + dbType
	}

	if _, f := metaData[dbType][strings.ToUpper(p)]; f {
		matchFound = true

		grants = append(grants, Grant{p, dbType, database})
	} else {
		schemas, err := s.getSchemasForDatabase(repo, database)
		if err != nil {
			return nil, false, err
		}

		for _, schema := range schemas {
			if schema.Name == "INFORMATION_SCHEMA" || schema.Name == "" {
				continue
			}

			schemaMatchFound := false

			grants, schemaMatchFound, err = s.createPermissionGrantsForSchema(repo, database, schema.Name, p, metaData, grants, isShared)
			if err != nil {
				return nil, matchFound, err
			}

			// Only generate the USAGE grant if any applicable permissions were applied on the schema or any item below
			if schemaMatchFound && !isShared {
				schemaName := schema.Name
				sfSchemaObject := common.SnowflakeObject{Database: &database, Schema: &schemaName, Table: nil, Column: nil}
				grants = append(grants, Grant{"USAGE", ds.Schema, sfSchemaObject.GetFullName(true)})
			}

			matchFound = matchFound || schemaMatchFound
		}
	}

	return grants, matchFound, nil
}

func (s *AccessSyncer) createPermissionGrantsForTable(database string, schema string, table TableEntity, p string, metaData map[string]map[string]struct{}, grants []Grant, isShared bool) ([]Grant, bool) {
	// Get the corresponding Raito data object type
	tableType := convertSnowflakeTableTypeToRaito(table.TableType)
	if isShared {
		tableType = SharedPrefix + tableType
	}

	// Check if the permission is applicable on the data object type
	if _, f2 := metaData[tableType][strings.ToUpper(p)]; f2 {
		grants = append(grants, Grant{p, tableType, common.FormatQuery(`%s.%s.%s`, database, schema, table.Name)})
		return grants, true
	}

	return grants, false
}

func (s *AccessSyncer) createGrantsForDatabase(repo dataAccessRepository, permissions []string, database string, metaData map[string]map[string]struct{}, isShared bool) ([]Grant, error) {
	grants := make([]Grant, 0, len(permissions)+1)

	var err error

	for _, p := range permissions {
		databaseMatchFound := false
		grants, databaseMatchFound, err = s.createPermissionGrantsForDatabase(repo, database, p, metaData, grants, isShared)

		if err != nil {
			return nil, err
		}

		if !databaseMatchFound {
			logger.Warn(fmt.Sprintf("Permission %q does not apply to type DATABASE or any of its descendants. Skipping", p))
		}
	}

	// Only generate the USAGE grant if any applicable permissions were applied or any item below
	if len(grants) > 0 && !isShared {
		sfDBObject := common.SnowflakeObject{Database: &database, Schema: nil, Table: nil, Column: nil}
		grants = append(grants, Grant{"USAGE", ds.Database, sfDBObject.GetFullName(true)})
	}

	return grants, nil
}

func (s *AccessSyncer) createGrantsForWarehouse(permissions []string, warehouse string, metaData map[string]map[string]struct{}) []Grant {
	grants := make([]Grant, 0, len(permissions)+2)
	grants = append(grants, Grant{"USAGE", "warehouse", common.FormatQuery(`%s`, warehouse)})

	for _, p := range permissions {
		if _, f := metaData["warehouse"][strings.ToUpper(p)]; !f {
			logger.Warn(fmt.Sprintf("Permission %q does not apply to type WAREHOUSE. Skipping", p))
			continue
		}

		grants = append(grants, Grant{p, "warehouse", common.FormatQuery(`%s`, warehouse)})
	}

	return grants
}

func (s *AccessSyncer) createGrantsForAccount(repo dataAccessRepository, permissions []string, metaData map[string]map[string]struct{}) ([]Grant, error) {
	grants := make([]Grant, 0, len(permissions))

	for _, p := range permissions {
		matchFound := false

		if _, f := metaData[ds.Datasource][strings.ToUpper(p)]; f {
			grants = append(grants, Grant{p, "account", ""})
			matchFound = true
		} else {
			if _, f2 := metaData["warehouse"][strings.ToUpper(p)]; f2 {
				matchFound = true

				warehouses, err := s.getWarehouses(repo)
				if err != nil {
					return nil, err
				}

				for _, warehouse := range warehouses {
					grants = append(grants, Grant{p, "warehouse", common.FormatQuery(`%s`, warehouse.Name)})
				}
			}

			shareNames, err := s.getShareNames(repo)
			if err != nil {
				return nil, err
			}

			databases, err := s.getDatabases(repo)
			if err != nil {
				return nil, err
			}

			for _, database := range databases {
				databaseMatchFound := false

				isShare := slices.Contains(shareNames, database.Name)

				grants, databaseMatchFound, err = s.createPermissionGrantsForDatabase(repo, database.Name, p, metaData, grants, isShare)
				if err != nil {
					return nil, err
				}

				matchFound = matchFound || databaseMatchFound

				// Only generate the USAGE grant if any applicable permissions were applied or any item below
				if databaseMatchFound && !isShare {
					dsName := database.Name
					sfDBObject := common.SnowflakeObject{Database: &dsName, Schema: nil, Table: nil, Column: nil}
					grants = append(grants, Grant{"USAGE", ds.Database, sfDBObject.GetFullName(true)})
				}
			}
		}

		if !matchFound {
			logger.Warn(fmt.Sprintf("Permission %q does not apply to type ACCOUNT (datasource) or any of its descendants. Skipping", p))
			continue
		}
	}

	return grants, nil
}

func (s *AccessSyncer) updateOrCreateFilter(ctx context.Context, repo dataAccessRepository, tableFullName string, aps []*importer.AccessProvider, roleNameMap map[string]string) (string, *string, error) {
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

	err := repo.UpdateFilter(database, schema, table, filterName, arguments.Slice(), strings.Join(filterExpressions, " OR "))
	if err != nil {
		return "", nil, fmt.Errorf("failed to update filter %s: %w", filterName, err)
	}

	return filterName, ptr.String(fmt.Sprintf("%s.%s", tableFullName, filterName)), nil
}

func (s *AccessSyncer) deleteFilter(repo dataAccessRepository, tableFullName string, aps []*importer.AccessProvider) error {
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
		deleteErr := repo.DropFilter(database, schema, table, filterName)
		if deleteErr != nil {
			err = multierror.Append(err, fmt.Errorf("failed to delete filter %s: %w", filterName, deleteErr))
		}
	}

	if err != nil {
		return err
	}

	return nil
}

func (s *AccessSyncer) executeGrantOnRole(perm, on, roleName string, apType *string, repo dataAccessRepository) error {
	if isDatabaseRole(apType) {
		database, parsedRoleName, err := parseDatabaseRoleExternalId(roleName)
		if err != nil {
			return err
		}

		return repo.ExecuteGrantOnDatabaseRole(perm, on, database, parsedRoleName)
	}

	return repo.ExecuteGrantOnAccountRole(perm, on, roleName)
}

func (s *AccessSyncer) executeRevokeOnRole(perm, on, roleName string, apType *string, repo dataAccessRepository) error {
	if isDatabaseRole(apType) {
		database, parsedRoleName, err := parseDatabaseRoleExternalId(roleName)
		if err != nil {
			return err
		}

		return repo.ExecuteRevokeOnDatabaseRole(perm, on, database, parsedRoleName)
	}

	return repo.ExecuteRevokeOnAccountRole(perm, on, roleName)
}

func (s *AccessSyncer) mergeGrants(repo dataAccessRepository, externalId string, apType *string, found []Grant, expected []Grant, metaData map[string]map[string]struct{}) error {
	toAdd := slice.SliceDifference(expected, found)
	toRemove := slice.SliceDifference(found, expected)

	logger.Info(fmt.Sprintf("Found %d grants to add and %d grants to remove for role %q", len(toAdd), len(toRemove), externalId))

	for _, grant := range toAdd {
		if verifyGrant(grant, metaData) {
			err := s.executeGrantOnRole(grant.Permissions, grant.GetGrantOnType()+" "+grant.On, externalId, apType, repo)
			if err != nil {
				return err
			}
		}
	}

	for _, grant := range toRemove {
		if verifyGrant(grant, metaData) {
			err := s.executeRevokeOnRole(grant.Permissions, grant.GetGrantOnType()+" "+grant.On, externalId, apType, repo)
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
					roles = append(roles, fmt.Sprintf("'%s'", roleName))
				}
			} else {
				roles = append(roles, fmt.Sprintf("'%s'", role))
			}
		}

		if len(roles) > 0 {
			whoExpressionParts = append(whoExpressionParts, fmt.Sprintf("current_role() IN (%s)", strings.Join(roles, ", ")))
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
