package snowflake

import (
	"context"
	"slices"

	"fmt"
	"strings"

	"github.com/aws/smithy-go/ptr"
	"github.com/raito-io/cli/base/access_provider"
	importer "github.com/raito-io/cli/base/access_provider/sync_to_target"
	ds "github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/util/match"
	"github.com/raito-io/cli/base/util/slice"
	"github.com/raito-io/golang-set/set"

	"github.com/raito-io/cli-plugin-snowflake/common"
)

func (s *AccessToTargetSyncer) syncGrantsToTarget(ctx context.Context, toProcessApIds []string, apsById map[string]*ApSyncToTargetItem) error {
	// do combine update action to create, rename and update shares
	toUpdateItems := s.accessProvidersForMutationActions(toProcessApIds, apsById, []ApMutationAction{ApMutationActionCreate, ApMutationActionRename, ApMutationActionUpdate})
	toRemoveItems := s.accessProvidersForMutationActions(toProcessApIds, apsById, []ApMutationAction{ApMutationActionDelete})

	Logger.Info(fmt.Sprintf("Configuring access provider as roles in Snowflake. Updating %d roles and remove %d roles", len(toUpdateItems), len(toRemoveItems)))

	err := s.grantsRemoveAll(toRemoveItems)
	if err != nil {
		return fmt.Errorf("removing grants: %w", err)
	}

	existingRoles, err := s.retrieveExistingRoles()
	if err != nil {
		return fmt.Errorf("retrieving existing roles: %w", err)
	}

	err = s.grantsCreateOrUpdateAll(ctx, toUpdateItems, apsById, existingRoles)
	if err != nil {
		return fmt.Errorf("updating/creating grants: %w", err)
	}

	return nil
}

func (s *AccessToTargetSyncer) grantsRemoveAll(toRemoveAps []*ApSyncToTargetItem) error {
	Logger.Info(fmt.Sprintf("Removing %d old Raito roles in Snowflake", len(toRemoveAps)))

	for _, toRemoveAp := range toRemoveAps {
		apType := s.retrieveAccessProviderType(toRemoveAp.accessProvider)

		fi := importer.AccessProviderSyncFeedback{
			AccessProvider: toRemoveAp.accessProvider.Id,
			ExternalId:     ptr.String(toRemoveAp.calculatedExternalId),
		}

		err := s.dropRole(toRemoveAp.calculatedExternalId, isDatabaseRole(ptr.String(apType)))
		// If an error occurs (and not already deleted), we send an error back as feedback
		if err != nil && !strings.Contains(err.Error(), "does not exist") {
			Logger.Error(fmt.Sprintf("unable to drop role %q: %s", toRemoveAp.calculatedExternalId, err.Error()))

			fi.Errors = append(fi.Errors, fmt.Sprintf("unable to drop role %q: %s", toRemoveAp.calculatedExternalId, err.Error()))
		}

		err = s.accessProviderFeedbackHandler.AddAccessProviderFeedback(fi)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *AccessToTargetSyncer) grantsCreateOrUpdateAll(ctx context.Context, toUpdateOrCreateItems []*ApSyncToTargetItem, apsById map[string]*ApSyncToTargetItem, existingRoles set.Set[string]) error {
	Logger.Info(fmt.Sprintf("Updating or creating %d Raito roles in Snowflake", len(toUpdateOrCreateItems)))

	metaData := s.buildMetaDataMap()

	allCalculatedExternalIds := make([]string, 0)
	for _, ap := range toUpdateOrCreateItems {
		allCalculatedExternalIds = append(allCalculatedExternalIds, ap.calculatedExternalId)
	}

	for _, apItem := range toUpdateOrCreateItems {
		var apHandleError error

		// Making sure we always set a type. If not set by Raito cloud, we take Account Role as default.
		apType := s.retrieveAccessProviderType(apItem.accessProvider)

		fi := importer.AccessProviderSyncFeedback{
			AccessProvider: apItem.accessProvider.Id,
			ExternalId:     ptr.String(apItem.calculatedExternalId),
			Type:           &apType,
		}

		// locks
		isDeleteLocked := apItem.accessProvider.DeleteLocked != nil && *apItem.accessProvider.DeleteLocked

		actualName, _, err := s.grantActualNameAndScopeRetriever(apItem)
		if err != nil {
			return fmt.Errorf("getting actual name and scope: %w", err)
		}

		switch apItem.mutationAction {
		case ApMutationActionCreate:
			// Creating a grant
			// Doing normal stuff same like we should do on update
			apHandleError = s.grantCreateBaseItem(apItem, isDeleteLocked, existingRoles)
			if apHandleError != nil {
				// If we have an error, we need to set the feedback and not perform the normal update actions
				err = s.handleAccessProviderFeedback(&fi, apHandleError)
				if err != nil {
					return fmt.Errorf("handling error feedback when creating access provider %q: %w", apItem.calculatedExternalId, err)
				}

				// stop logic here and go to the next item of the loop
				continue
			}

			//  Normal update actions
			apHandleError = s.grantUpdateItem(ctx, apItem, apsById, existingRoles, metaData)
		case ApMutationActionRename:
			// Renaming grant
			// Doing normal stuff same like we should do on update
			apHandleError = s.grantRenameItem(apItem, existingRoles, allCalculatedExternalIds)
			if apHandleError != nil {
				// If we have an error, we need to set the feedback and not perform the normal update actions
				err = s.handleAccessProviderFeedback(&fi, apHandleError)
				if err != nil {
					return fmt.Errorf("handling error feedback when renaming access provider %q: %w", apItem.calculatedExternalId, err)
				}

				// stop logic here and go to the next item of the loop
				continue
			}

			//  Normal update actions
			apHandleError = s.grantUpdateItem(ctx, apItem, apsById, existingRoles, metaData)
		case ApMutationActionUpdate:
			// Doing normal stuff same like we should do on update
			apHandleError = s.grantUpdateItem(ctx, apItem, apsById, existingRoles, metaData)
		default:
			// Shouldn't happen so will return an error for this ap?
			apHandleError = fmt.Errorf("unknown mutation action %q", apItem.mutationAction)
		}

		if apHandleError == nil {
			// No errors, so we can set the actual name
			fi.ActualName = actualName
		}

		err2 := s.handleAccessProviderFeedback(&fi, apHandleError)
		if err2 != nil {
			return fmt.Errorf("handling feedback for access provider %q: %w", apItem.calculatedExternalId, err2)
		}
	}

	return nil
}

func (s *AccessToTargetSyncer) grantActualNameAndScopeRetriever(accessProviderItem *ApSyncToTargetItem) (string, string, error) {
	apType := ptr.String(s.retrieveAccessProviderType(accessProviderItem.accessProvider))
	actualName := accessProviderItem.calculatedExternalId
	dbName := ""
	var err error

	switch {
	case isDatabaseRole(apType):
		dbName, actualName, err = parseDatabaseRoleExternalId(accessProviderItem.calculatedExternalId)
		if err != nil {
			return actualName, dbName, err
		}
	case isApplicationRole(apType):
		dbName, actualName, err = parseApplicationRoleExternalId(accessProviderItem.calculatedExternalId)
		if err != nil {
			return actualName, dbName, err
		}
	}

	return actualName, dbName, nil
}

func (s *AccessToTargetSyncer) grantRenameItem(toRenameAp *ApSyncToTargetItem, existingRoles set.Set[string], allCalculatedExternalIds []string) error {
	if toRenameAp.accessProvider.ExternalId == nil {
		return fmt.Errorf("access provider %q has no externalId, so a rename is not possible", toRenameAp.accessProvider.Id)
	}

	externalId := toRenameAp.calculatedExternalId
	oldExternalId := *toRenameAp.accessProvider.ExternalId

	apType := ptr.String(s.retrieveAccessProviderType(toRenameAp.accessProvider))

	if !existingRoles.Contains(externalId) && existingRoles.Contains(oldExternalId) {
		// Case 1: New name doesn't exist yet, but the old name does exist
		if slices.Contains(allCalculatedExternalIds, oldExternalId) {
			// Case 1A: Old name is being used by another access provider
			// This happens in cases like: R2 renamed to R3, then R1 renamed to R2
			Logger.Info(fmt.Sprintf("Cannot rename: old role name (%s) is already assigned to another access provider, new name (%s) will be created", oldExternalId, externalId))

			err := s.createRole(externalId, apType)
			if err != nil {
				return fmt.Errorf("error while creating new role %q during the renaming of %q to %q : %w", externalId, oldExternalId, externalId, err)
			}
		} else {
			// Case 1B: Old name exists and not in use, new name doesn't exist yet
			// a standard rename scenario
			err := s.renameRole(oldExternalId, externalId, apType)
			if err != nil {
				return fmt.Errorf("error while renaming role %q to %q: %w", oldExternalId, externalId, err)
			}

			existingRoles.Remove(oldExternalId)
			existingRoles.Add(externalId)
		}
	} else if existingRoles.Contains(externalId) && existingRoles.Contains(oldExternalId) {
		// Case 2: Both old and new names already exist
		if slices.Contains(allCalculatedExternalIds, oldExternalId) {
			// Case 2A: Old name is being used by another access provider
			Logger.Info(fmt.Sprintf("Cannot rename: both names exist and old role name (%s) is already assigned to another access provider", oldExternalId))
		} else {
			// Case 2B: Old name exists but not in use, and new name also exists
			// We'll delete the old one and update the existing new one
			err := s.dropRole(oldExternalId, isDatabaseRoleByExternalId(oldExternalId))
			if err != nil {
				return fmt.Errorf("error while dropping role (%s) which was the old name of access provider %q: %w", oldExternalId, toRenameAp.accessProvider.Name, err)
			}

			existingRoles.Remove(oldExternalId)
		}
	}

	return nil
}

func (s *AccessToTargetSyncer) grantUpdateItem(ctx context.Context, toProcessAp *ApSyncToTargetItem, apsById map[string]*ApSyncToTargetItem, existingRoles set.Set[string], metaData map[string]map[string]struct{}) error {
	accessProvider := toProcessAp.accessProvider
	apType := ptr.String(s.retrieveAccessProviderType(toProcessAp.accessProvider))

	externalId := toProcessAp.calculatedExternalId
	isWhoLocked := accessProvider.WhoLocked != nil && *accessProvider.WhoLocked
	isInheritanceLocked := accessProvider.InheritanceLocked != nil && *accessProvider.InheritanceLocked
	isWhatLocked := accessProvider.WhatLocked != nil && *accessProvider.WhatLocked

	Logger.Info(fmt.Sprintf("Updating access provider %q (Ignore who: %t; Ignore inheritance: %t; Ignore what: %t)", accessProvider.Name, isWhoLocked, isInheritanceLocked, isWhatLocked))

	actualName, dbName, err := s.grantActualNameAndScopeRetriever(toProcessAp)
	if err != nil {
		return fmt.Errorf("determining actual name and scope for role: %w", err)
	}

	// Only update the comment if we have full control over the role (who, inheritance and what not locked)
	if !isWhoLocked && !isInheritanceLocked && !isWhatLocked {
		err = s.commentOnRoleIfExists(createComment(accessProvider, toProcessAp.mutationAction != ApMutationActionCreate), externalId)
		if err != nil {
			return fmt.Errorf("updating comment on role %q: %w", actualName, err)
		}
	}

	// if whoLock or inheritanceLock is not enabled, we will update the beneficiaries for this role
	if !isWhoLocked || !isInheritanceLocked {
		err = s.grantUpdateItemWhoPart(ctx, accessProvider, externalId, isWhoLocked, isInheritanceLocked, apsById, existingRoles)
		if err != nil {
			return fmt.Errorf("updating who part on role %q: %w", actualName, err)
		}
	}

	// if whatLock is set, we will not update the grants for this role
	if !isWhatLocked {
		isNewlyCreatedAp := toProcessAp.mutationAction == ApMutationActionCreate

		err = s.grantUpdateItemWhatPart(accessProvider, externalId, apType, isNewlyCreatedAp, existingRoles, metaData)
		if err != nil {
			return fmt.Errorf("updating what part on role %q: %w", actualName, err)
		}
	}

	Logger.Info(fmt.Sprintf("Done updating users granted to role %q", actualName))

	fullName := actualName
	if dbName != "" {
		fullName = fmt.Sprintf("%s.%s", dbName, actualName)
	}

	err = s.handleOwnerTags(fullName, accessProvider.Owners, isDatabaseRole(apType))
	if err != nil {
		return fmt.Errorf("error while setting owner tags on role %q: %s", actualName, err.Error())
	}

	return nil
}

func (s *AccessToTargetSyncer) grantUpdateItemWhatPart(accessProvider *importer.AccessProvider, externalId string, apType *string, isNewAccessProvider bool, existingRoles set.Set[string], metaData map[string]map[string]struct{}) error {
	// Remove all future grants on schema and database if applicable.
	// Since these are future grants, it's safe to just remove them and re-add them again (if required).
	// We assume nobody manually added others to this role manually.
	if !isNewAccessProvider {
		for _, what := range accessProvider.What {
			switch what.DataObject.Type {
			case "database":
				err := s.executeRevokeOnRole("ALL", common.FormatQuery(`FUTURE SCHEMAS IN DATABASE %s`, what.DataObject.FullName), externalId, apType)
				if err != nil {
					return fmt.Errorf("error while assigning future schema grants in database %q to role %q: %w", what.DataObject.FullName, externalId, err)
				}

				err = s.executeRevokeOnRole("ALL", common.FormatQuery(`FUTURE TABLES IN DATABASE %s`, what.DataObject.FullName), externalId, apType)
				if err != nil {
					return fmt.Errorf("error while assigning future table grants in database %q to role %q: %w", what.DataObject.FullName, externalId, err)
				}
			case "schema":
				err := s.executeRevokeOnRole("ALL", common.FormatQuery("FUTURE TABLES IN SCHEMA %s", what.DataObject.FullName), externalId, apType)
				if err != nil {
					return fmt.Errorf("error while assigning future table grants in schema %q to role %q: %w", what.DataObject.FullName, externalId, err)
				}
			}
		}
	}

	grantsToRole := make([]GrantToRole, 0)
	var err error

	// Only retrieve grants if we are doing an update
	if existingRoles.Contains(externalId) {
		grantsToRole, err = s.accessSyncer.getGrantsToRole(externalId, apType)
		if err != nil {
			return fmt.Errorf("retrieving grants for role: %w", err)
		}
	}

	Logger.Debug(fmt.Sprintf("Found grants for role %q: %+v", externalId, grantsToRole))

	foundGrants := make([]Grant, 0, len(grantsToRole))

	for _, grant := range grantsToRole {
		if strings.EqualFold(grant.GrantedOn, "ACCOUNT") {
			foundGrants = append(foundGrants, Grant{grant.Privilege, "account", ""})
		} else if strings.EqualFold(grant.Privilege, "OWNERSHIP") {
			Logger.Info(fmt.Sprintf("Ignoring permission %q on %q for Role %q as this will remain untouched", grant.Privilege, grant.Name, externalId))
		} else if strings.EqualFold(grant.Privilege, "USAGE") && (strings.EqualFold(grant.GrantedOn, "ROLE") || strings.EqualFold(grant.GrantedOn, GrantTypeDatabaseRole)) {
			Logger.Debug(fmt.Sprintf("Ignoring USAGE permission on %s %q", grant.GrantedOn, grant.Name))
		} else {
			onType := convertSnowflakeGrantTypeToRaito(grant.GrantedOn)
			name := grant.Name

			if onType == Function || onType == Procedure { // For functions and stored procedures we need to do a special conversion
				name = s.accessSyncer.getFullNameFromGrant(name, onType)
			}

			foundGrants = append(foundGrants, Grant{grant.Privilege, onType, name})
		}
	}

	expectedGrants, err2 := s.createGrantsForWhatObjects(accessProvider, metaData)
	if err2 != nil {
		return err2
	}

	err = s.mergeGrants(externalId, apType, foundGrants, expectedGrants.Slice(), metaData)
	if err != nil {
		return err
	}

	return nil
}

func (s *AccessToTargetSyncer) grantUpdateItemWhoPart(ctx context.Context, accessProvider *importer.AccessProvider, externalId string, isWhoLocked bool, isInheritanceLocked bool, apsById map[string]*ApSyncToTargetItem, existingRoles set.Set[string]) error {
	apType := ptr.String(s.retrieveAccessProviderType(accessProvider))

	grantsOfRole := make([]GrantOfRole, 0)
	var err error

	if existingRoles.Contains(externalId) {
		// Only retrieve grants if we are doing an update
		grantsOfRole, err = s.accessSyncer.retrieveGrantsOfRole(externalId, *apType)
		if err != nil {
			return fmt.Errorf("retrieving grants: %w", err)
		}
	}

	usersOfRole := make([]string, 0, len(grantsOfRole))
	rolesOfRole := make([]string, 0, len(grantsOfRole))

	for _, gor := range grantsOfRole {
		switch {
		case strings.EqualFold(gor.GrantedTo, "USER"):
			usersOfRole = append(usersOfRole, gor.GranteeName)
		case strings.EqualFold(gor.GrantedTo, "ROLE"):
			rolesOfRole = append(rolesOfRole, accountRoleExternalIdGenerator(gor.GranteeName))
		case strings.EqualFold(gor.GrantedTo, GrantTypeDatabaseRole):
			database, parsedRoleName, err2 := parseNamespacedRoleRoleName(cleanDoubleQuotes(gor.GranteeName))
			if err2 != nil {
				return fmt.Errorf("parsing role name %q: %w", cleanDoubleQuotes(gor.GranteeName), err2)
			}

			rolesOfRole = append(rolesOfRole, databaseRoleExternalIdGenerator(database, parsedRoleName))
		case strings.EqualFold(gor.GrantedTo, "SHARE"):
			rolesOfRole = append(rolesOfRole, shareExternalIdGenerator(gor.GranteeName))
		case strings.EqualFold(gor.GrantedTo, GrantTypeApplicationRole):
			application, parsedRoleName, err2 := parseNamespacedRoleRoleName(cleanDoubleQuotes(gor.GranteeName))
			if err2 != nil {
				return fmt.Errorf("parsing role name %q: %w", cleanDoubleQuotes(gor.GranteeName), err2)
			}

			rolesOfRole = append(rolesOfRole, applicationRoleExternalIdGenerator(application, parsedRoleName))
		}
	}

	if !isWhoLocked {
		toAdd := slice.StringSliceDifference(accessProvider.Who.Users, usersOfRole, false)
		toRemove := slice.StringSliceDifference(usersOfRole, accessProvider.Who.Users, false)
		Logger.Info(fmt.Sprintf("Identified %d users to add and %d users to remove from role %q", len(toAdd), len(toRemove), externalId))

		if len(toAdd) > 0 {
			if isDatabaseRole(apType) {
				return fmt.Errorf("error can not assign users from a database role %q", externalId)
			}

			err2 := s.repo.GrantUsersToAccountRole(ctx, externalId, toAdd...)
			if err2 != nil {
				return fmt.Errorf("error while assigning users to role %q: %w", externalId, err2)
			}
		}

		if len(toRemove) > 0 {
			if isDatabaseRole(apType) {
				return fmt.Errorf("error can not unassign users from a database role %q", externalId)
			}

			err2 := s.repo.RevokeUsersFromAccountRole(ctx, externalId, toRemove...)
			if err2 != nil {
				return fmt.Errorf("error while unassigning users from role %q: %w", externalId, err2)
			}
		}
	}

	if !isInheritanceLocked {
		// extract RoleNames from Access Providers that are among the whoList of this one
		inheritedRoles := make([]string, 0)

		for _, apWho := range accessProvider.Who.InheritFrom {
			if strings.HasPrefix(apWho, "ID:") {
				inheritedApId := apWho[3:]
				if inheritedAp, found := apsById[inheritedApId]; found {
					inheritedRoles = append(inheritedRoles, inheritedAp.calculatedExternalId)
				}
			} else {
				inheritedRoles = append(inheritedRoles, apWho)
			}
		}

		toAdd := slice.StringSliceDifference(inheritedRoles, rolesOfRole, false)
		toRemove := slice.StringSliceDifference(rolesOfRole, inheritedRoles, false)
		Logger.Info(fmt.Sprintf("Identified %d roles to add and %d roles to remove from role %q", len(toAdd), len(toRemove), externalId))

		if len(toAdd) > 0 {
			err2 := s.grantRolesToRole(ctx, externalId, apType, toAdd...)
			if err2 != nil {
				return fmt.Errorf("error while assigning role to role %q: %w", externalId, err2)
			}
		}

		if len(toRemove) > 0 {
			err2 := s.revokeRolesFromRole(ctx, externalId, apType, toRemove...)
			if err2 != nil {
				return fmt.Errorf("error while unassigning role from role %q: %w", externalId, err2)
			}
		}
	}

	return nil
}

func (s *AccessToTargetSyncer) grantCreateBaseItem(toCreateAp *ApSyncToTargetItem, deleteLocked bool, existingRoles set.Set[string]) error {
	externalId := toCreateAp.calculatedExternalId

	// When delete is locked (so this was originally created/managed in the data source), we don't recreate it again if it is deleted on the data source in the meanwhile.
	if deleteLocked {
		Logger.Warn(fmt.Sprintf("Role %q does not exist but is marked as delete locked. Not creating the role as it is probably removed externally.", externalId))
		return nil
	}

	Logger.Info(fmt.Sprintf("Creating role %q", externalId))

	// If generated externalId does exist, we should stop here as this is a problem
	if existingRoles.Contains(externalId) {
		Logger.Warn(fmt.Sprintf("Role %q already exists in Snowflake. We are going to take over this role.", externalId))
		return nil
	}

	apType := s.retrieveAccessProviderType(toCreateAp.accessProvider)

	err := s.createRole(externalId, ptr.String(apType))
	if err != nil {
		return fmt.Errorf("error while creating role %q: %w", externalId, err)
	}

	return nil
}

func (s *AccessToTargetSyncer) handleOwnerTags(actualName string, owners []importer.Owner, isDatabaseRole bool) error {
	if len(owners) == 0 {
		return nil
	}

	if emailTag, found := s.configMap.Parameters[SfRoleOwnerEmailTag]; found && emailTag != "" {
		tagValues := make([]string, 0, len(owners))

		for _, owner := range owners {
			if owner.Email != nil && *owner.Email != "" {
				tagValues = append(tagValues, fmt.Sprintf("email:%s", *owner.Email))
			}
		}

		err := s.repo.SetTagOnRole(actualName, emailTag, strings.Join(tagValues, ","), isDatabaseRole)
		if err != nil {
			return fmt.Errorf("setting owner email tag on role %q: %s", actualName, err.Error())
		}
	}

	if nameTag, found := s.configMap.Parameters[SfRoleOwnerNameTag]; found && nameTag != "" {
		tagValues := make([]string, 0, len(owners))

		for _, owner := range owners {
			if owner.AccountName != nil && *owner.AccountName != "" {
				tagValues = append(tagValues, *owner.AccountName)
			}
		}

		err := s.repo.SetTagOnRole(actualName, nameTag, strings.Join(tagValues, ","), isDatabaseRole)
		if err != nil {
			return fmt.Errorf("setting owner account name tag on role %q: %s", actualName, err.Error())
		}
	}

	if groupTag, found := s.configMap.Parameters[SfRoleOwnerGroupTag]; found && groupTag != "" {
		tagValues := make([]string, 0, len(owners))

		for _, owner := range owners {
			if owner.GroupName != nil && *owner.GroupName != "" {
				tagValues = append(tagValues, *owner.GroupName)
			}
		}

		err := s.repo.SetTagOnRole(actualName, groupTag, strings.Join(tagValues, ","), isDatabaseRole)
		if err != nil {
			return fmt.Errorf("setting owner group name tag on role %q: %s", actualName, err.Error())
		}
	}

	return nil
}

func (s *AccessToTargetSyncer) splitRolesByRoleType(inheritedRoles []string) ([]string, []string, []string) {
	var databaseRoles []string
	var applicationRoles []string
	var accountRoles []string

	for _, role := range inheritedRoles {
		switch {
		case isDatabaseRoleByExternalId(role):
			databaseRoles = append(databaseRoles, role)
		case isApplicationRoleByExternalId(role):
			applicationRoles = append(applicationRoles, role)
		default:
			accountRoles = append(accountRoles, role)
		}
	}

	return databaseRoles, applicationRoles, accountRoles
}

func (s *AccessToTargetSyncer) grantRolesToRole(ctx context.Context, targetExternalId string, targetApType *string, roles ...string) error {
	toAddDatabaseRoles, toAddApplicationRoles, toAddAccountRoles := s.splitRolesByRoleType(roles)

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

	switch {
	case isDatabaseRole(targetApType):
		err := s.grantRolesToDatabaseRoles(ctx, targetExternalId, toAddDatabaseRoles, filteredAccountRoles)
		if err != nil {
			return fmt.Errorf("grant roles to database roles: %w", err)
		}

		return nil
	case isApplicationRole(targetApType):
		err := s.grantRolesToApplicationRoles(ctx, targetExternalId, toAddApplicationRoles, filteredAccountRoles)
		if err != nil {
			return fmt.Errorf("grant roles to application roles: %w", err)
		}

		return nil
	case len(toAddDatabaseRoles) > 0:
		return fmt.Errorf("error can not assign database roles to an account role %q - %v", targetExternalId, toAddDatabaseRoles)
	}

	err := s.repo.GrantAccountRolesToAccountRole(ctx, targetExternalId, filteredAccountRoles...)
	if err != nil {
		return fmt.Errorf("granting account roles to account role: %w", err)
	}

	return nil
}

func (s *AccessToTargetSyncer) grantRolesToDatabaseRoles(ctx context.Context, targetExternalId string, toAddDatabaseRoles []string, filteredAccountRoles []string) error {
	return s.handleRolesToNamespacedRoles(ctx, targetExternalId, toAddDatabaseRoles, filteredAccountRoles, parseDatabaseRoleExternalId, s.repo.GrantDatabaseRolesToDatabaseRole, s.repo.GrantAccountRolesToDatabaseRole)
}

func (s *AccessToTargetSyncer) grantRolesToApplicationRoles(ctx context.Context, targetExternalId string, toAddApplicationRoles []string, filteredAccountRole []string) error {
	return s.handleRolesToNamespacedRoles(ctx, targetExternalId, toAddApplicationRoles, filteredAccountRole, parseApplicationRoleExternalId, s.repo.GrantApplicationRolesToApplicationRole, s.repo.GrantAccountRolesToApplicationRole)
}

func (s *AccessToTargetSyncer) shouldIgnoreLinkedRole(roleName string) (bool, error) {
	matched, err := match.MatchesAny(roleName, s.ignoreLinksToRole)
	if err != nil {
		return false, fmt.Errorf("parsing regular expressions in parameter %q: %s", SfIgnoreLinksToRoles, err.Error())
	}

	return matched, nil
}

func (s *AccessToTargetSyncer) revokeRolesFromRole(ctx context.Context, targetExternalId string, targetApType *string, roles ...string) error {
	toRemoveDatabaseRoles, toRemoveApplicationRoles, toRemoveAccountRoles := s.splitRolesByRoleType(roles)

	var filteredAccountRoles []string

	for _, accountRole := range toRemoveAccountRoles {
		shouldIgnore, err2 := s.shouldIgnoreLinkedRole(accountRole)
		if err2 != nil {
			return err2
		}

		if !shouldIgnore {
			filteredAccountRoles = append(filteredAccountRoles, accountRole)
		}
	}

	switch {
	case isDatabaseRole(targetApType):
		err := s.revokeRolesFromDatabaseRole(ctx, targetExternalId, toRemoveDatabaseRoles, filteredAccountRoles)
		if err != nil {
			return fmt.Errorf("revoke roles from database role: %w", err)
		}

		return nil
	case isApplicationRole(targetApType):
		err := s.revokeRolesFromApplicationRole(ctx, targetExternalId, toRemoveApplicationRoles, filteredAccountRoles)
		if err != nil {
			return fmt.Errorf("revoke roles from application role: %w", err)
		}

		return nil
	case len(toRemoveDatabaseRoles) > 0:
		return fmt.Errorf("error can not assign database roles to an account role %q - %v", targetExternalId, toRemoveDatabaseRoles)
	}

	return s.repo.RevokeAccountRolesFromAccountRole(ctx, targetExternalId, filteredAccountRoles...)
}

func (s *AccessToTargetSyncer) revokeRolesFromDatabaseRole(ctx context.Context, targetExternalId string, toRemoveDatabaseRoles []string, filteredAccountRoles []string) error {
	return s.handleRolesToNamespacedRoles(ctx, targetExternalId, toRemoveDatabaseRoles, filteredAccountRoles, parseDatabaseRoleExternalId, s.repo.RevokeDatabaseRolesFromDatabaseRole, s.repo.RevokeAccountRolesFromDatabaseRole)
}

func (s *AccessToTargetSyncer) revokeRolesFromApplicationRole(ctx context.Context, targetExternalId string, toRemoveApplicationRoles []string, filteredAccountRoles []string) error {
	return s.handleRolesToNamespacedRoles(ctx, targetExternalId, toRemoveApplicationRoles, filteredAccountRoles, parseApplicationRoleExternalId, s.repo.RevokeApplicationRolesFromApplicationRole, s.repo.RevokeAccountRolesFromApplicationRole)
}

func (s *AccessToTargetSyncer) handleRolesToNamespacedRoles(ctx context.Context, targetExternalId string, toAddNamespacedRoles []string, filteredAccountRoles []string, parseNamespacedRoleExternalId func(string) (string, string, error), handleNamespaceRole func(context.Context, string, string, ...string) error, handleAccountRoles func(context.Context, string, string, ...string) error) error {
	namespace, parsedRoleName, err := parseNamespacedRoleExternalId(targetExternalId)
	if err != nil {
		return fmt.Errorf("parsing namespaced role external id %q: %w", targetExternalId, err)
	}

	filteredNamespacedRoles := make([]string, 0, len(toAddNamespacedRoles))

	for _, namespacedRole := range toAddNamespacedRoles {
		toNamespace, toParsedRoleName, err2 := parseNamespacedRoleExternalId(namespacedRole)
		if err2 != nil {
			return fmt.Errorf("parsing namespace role %q, %w", namespacedRole, err2)
		}

		if namespace != toNamespace {
			return fmt.Errorf("namespaced role %q is from a different namespace than %q", parsedRoleName, toParsedRoleName)
		}

		shouldIgnore, err2 := s.shouldIgnoreLinkedRole(toParsedRoleName)
		if err2 != nil {
			return err2
		}

		if !shouldIgnore {
			filteredNamespacedRoles = append(filteredNamespacedRoles, toParsedRoleName)
		}
	}

	err = handleNamespaceRole(ctx, namespace, parsedRoleName, filteredNamespacedRoles...)
	if err != nil {
		return fmt.Errorf("granting namespace role: %w", err)
	}

	err = handleAccountRoles(ctx, namespace, parsedRoleName, filteredAccountRoles...)
	if err != nil {
		return fmt.Errorf("granting account roles: %w", err)
	}

	return nil
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

func (s *AccessToTargetSyncer) retrieveAccessProviderType(ap *importer.AccessProvider) string {
	apType := access_provider.Role
	if ap != nil && ap.Type != nil && *ap.Type != "" {
		apType = *ap.Type
	}

	return apType
}

func (s *AccessToTargetSyncer) handleAccessProviderFeedback(fi *importer.AccessProviderSyncFeedback, err error) error {
	if err != nil {
		Logger.Error(err.Error())
		fi.Errors = append(fi.Errors, err.Error())
	}

	return s.accessProviderFeedbackHandler.AddAccessProviderFeedback(*fi)
}

func (s *AccessToTargetSyncer) createGrantsForTableOrView(doType string, permissions []string, fullName string, metaData map[string]map[string]struct{}, grants *GrantSet) error {
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
			Logger.Warn(fmt.Sprintf("Permission %q does not apply to type %s", p, strings.ToUpper(doType)))
		}
	}

	if grants.Size() > 0 {
		grants.Add(Grant{"USAGE", ds.Database, common.FormatQuery(`%s`, *sfObject.Database)}, Grant{"USAGE", ds.Schema, common.FormatQuery(`%s.%s`, *sfObject.Database, *sfObject.Schema)})
	}

	return nil
}

func (s *AccessToTargetSyncer) createGrantsForFunctionOrProcedure(permissions []string, fullName string, metaData map[string]map[string]struct{}, grants *GrantSet, objType string) {
	for _, p := range permissions {
		if _, f := metaData[objType][strings.ToUpper(p)]; f {
			grants.Add(Grant{p, objType, fullName}) // fullName should already be in the right format
		} else {
			Logger.Warn(fmt.Sprintf("Permission %q does not apply to type %s", p, strings.ToUpper(objType)))
		}
	}

	if grants.Size() > 0 {
		split := strings.Split(fullName, ".")

		if len(split) >= 3 {
			grants.Add(Grant{"USAGE", ds.Database, common.FormatQuery(`%s`, split[0])}, Grant{"USAGE", ds.Schema, common.FormatQuery(`%s.%s`, split[0], split[1])})
		}
	}
}

func (s *AccessToTargetSyncer) getTablesForSchema(database, schema string) ([]TableEntity, error) {
	cacheKey := database + "." + schema

	if tables, f := s.tablesPerSchemaCache[cacheKey]; f {
		return tables, nil
	}

	tables := make([]TableEntity, 0, 10)

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

func (s *AccessToTargetSyncer) getFunctionsForSchema(database, schema string) ([]FunctionEntity, error) {
	cacheKey := database + "." + schema

	if functions, f := s.functionsPerSchemaCache[cacheKey]; f {
		return functions, nil
	}

	functions := make([]FunctionEntity, 0, 10)

	err := s.repo.GetFunctionsInDatabase(database, func(entity interface{}) error {
		function := entity.(*FunctionEntity)
		if function.Schema == schema {
			functions = append(functions, *function)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	s.functionsPerSchemaCache[cacheKey] = functions

	return functions, nil
}

func (s *AccessToTargetSyncer) getProceduresForSchema(database, schema string) ([]ProcedureEntity, error) {
	cacheKey := database + "." + schema

	if procedures, f := s.proceduresPerSchemaCache[cacheKey]; f {
		return procedures, nil
	}

	procedures := make([]ProcedureEntity, 0, 10)

	err := s.repo.GetProceduresInDatabase(database, func(entity interface{}) error {
		proc := entity.(*ProcedureEntity)
		if proc.Schema == schema {
			procedures = append(procedures, *proc)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	s.proceduresPerSchemaCache[cacheKey] = procedures

	return procedures, nil
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

func (s *AccessToTargetSyncer) getIntegrations() ([]DbEntity, error) {
	if s.integrationsCache != nil {
		return s.integrationsCache, nil
	}

	var err error
	s.integrationsCache, err = s.repo.GetIntegrations()

	if err != nil {
		s.integrationsCache = nil
		return nil, err
	}

	return s.integrationsCache, nil
}

func (s *AccessToTargetSyncer) createPermissionGrantsForDatabase(database, p string, metaData map[string]map[string]struct{}, isShared bool, grants *GrantSet) (bool, error) {
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

	Logger.Info(fmt.Sprintf("Found %d grants to add and %d grants to remove for role %q", len(toAdd), len(toRemove), externalId))

	for _, grant := range toAdd {
		if verifyGrant(grant, metaData) {
			err := s.executeGrantOnRole(grant.Permissions, grant.OnWithType(), externalId, apType)
			if err != nil {
				return err
			}
		}
	}

	for _, grant := range toRemove {
		if verifyGrant(grant, metaData) {
			err := s.executeRevokeOnRole(grant.Permissions, grant.OnWithType(), externalId, apType)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func createComment(ap *importer.AccessProvider, update bool) string {
	action := "Created"
	if update {
		action = "Updated"
	}

	return fmt.Sprintf("%s by Raito from access provider %s. %s", action, ap.Name, ap.Description)
}
