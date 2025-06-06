package snowflake

import (
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/aws/smithy-go/ptr"
	"github.com/gammazero/workerpool"
	"github.com/raito-io/cli/base/access_provider"
	exporter "github.com/raito-io/cli/base/access_provider/sync_from_target"
	"github.com/raito-io/cli/base/access_provider/types"
	ds "github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/tag"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers"
	"github.com/raito-io/golang-set/set"

	"github.com/raito-io/cli-plugin-snowflake/common"
)

type AccessFromTargetSyncer struct {
	configMap             *config.ConfigMap
	repo                  dataAccessRepository
	accessSyncer          *AccessSyncer
	accessProviderHandler wrappers.AccessProviderHandler

	inboundShares                     []string
	linkToExternalIdentityStoreGroups bool
	externalGroupOwners               string
	excludedRoles                     map[string]struct{}
	lock                              sync.Mutex
}

func NewAccessFromTargetSyncer(accessSyncer *AccessSyncer, repo dataAccessRepository, accessProviderHandler wrappers.AccessProviderHandler, configMap *config.ConfigMap) *AccessFromTargetSyncer {
	return &AccessFromTargetSyncer{
		accessSyncer:          accessSyncer,
		configMap:             configMap,
		repo:                  repo,
		accessProviderHandler: accessProviderHandler,
	}
}

func (s *AccessFromTargetSyncer) syncFromTarget() error {
	s.externalGroupOwners = s.configMap.GetStringWithDefault(SfExternalIdentityStoreOwners, "")
	s.excludedRoles = s.extractExcludeRoleList()
	s.linkToExternalIdentityStoreGroups = s.configMap.GetBoolWithDefault(SfLinkToExternalIdentityStoreGroups, false)

	Logger.Info("Reading account and database roles from Snowflake")

	inboundShares, err := s.accessSyncer.getInboundShareNames()
	if err != nil {
		return err
	}

	s.inboundShares = inboundShares
	s.externalGroupOwners = s.configMap.GetStringWithDefault(SfExternalIdentityStoreOwners, "")
	s.linkToExternalIdentityStoreGroups = s.configMap.GetBoolWithDefault(SfLinkToExternalIdentityStoreGroups, false)

	Logger.Info("Reading account roles from Snowflake")

	err = s.importAllRolesOnAccountLevel(s.accessProviderHandler)
	if err != nil {
		return fmt.Errorf("importing account roles: %w", err)
	}

	err = s.importOutboundShares(s.accessProviderHandler)
	if err != nil {
		return fmt.Errorf("importing shares: %w", err)
	}

	excludedDatabases := s.extractExcludeDatabases()

	databaseRoleSupportEnabled := s.configMap.GetBoolWithDefault(SfDatabaseRoles, false)
	if databaseRoleSupportEnabled {
		Logger.Info("Reading database roles from Snowflake")

		err = s.importAllRolesOnDatabaseLevel(s.accessProviderHandler, excludedDatabases)
		if err != nil {
			return err
		}
	}

	applicationSupportEnabled := s.configMap.GetBoolWithDefault(SfApplications, false)
	if applicationSupportEnabled {
		Logger.Info("Reading application roles from Snowflake")

		err = s.importAllRolesOnApplicationLevel(s.accessProviderHandler, excludedDatabases)
		if err != nil {
			return fmt.Errorf("application roles: %w", err)
		}
	}

	skipColumns := s.configMap.GetBoolWithDefault(SfSkipColumns, false)
	standardEdition := s.configMap.GetBoolWithDefault(SfStandardEdition, false)

	if !standardEdition {
		if !skipColumns {
			Logger.Info("Reading masking policies from Snowflake")

			err = s.importMaskingPolicies()
			if err != nil {
				return err
			}
		} else {
			Logger.Info("Skipping masking policies")
		}

		Logger.Info("Reading row access policies from Snowflake")

		err = s.importRowAccessPolicies()
		if err != nil {
			return err
		}
	} else {
		Logger.Info("Skipping masking policies and row access policies due to Snowflake Standard Edition.")
	}

	return nil
}

func (s *AccessFromTargetSyncer) importOutboundShares(accessProviderHandler wrappers.AccessProviderHandler) error {
	// Get all output shares and import them
	shareEntities, err := s.repo.GetOutboundShares()
	if err != nil {
		return err
	}

	shareMap := make(map[string][]ShareEntity)

	for _, shareEntity := range shareEntities {
		shareMap[shareEntity.Name] = append(shareMap[shareEntity.Name], shareEntity)
	}

	wp := workerpool.New(getWorkerPoolSize(s.configMap))

	processedAps := make(map[string]*exporter.AccessProvider)

	for shareName, shareEntityItems := range shareMap {
		if _, exclude := s.excludedRoles[shareName]; exclude {
			Logger.Info("Skipping SnowFlake SHARE " + shareName)
			continue
		}

		wp.Submit(func() {
			err2 := s.transformShareToAccessProvider(shareName, shareEntityItems, processedAps)
			if err2 != nil {
				Logger.Warn(fmt.Sprintf("Error importing SnowFlake share %q: %s", shareName, err2.Error()))
				return
			}
		})
	}

	wp.StopWait()

	err = accessProviderHandler.AddAccessProviders(values(processedAps)...)
	if err != nil {
		return fmt.Errorf("error adding shares to import file: %s", err.Error())
	}

	return nil
}

func (s *AccessFromTargetSyncer) importAllRolesOnAccountLevel(accessProviderHandler wrappers.AccessProviderHandler) error {
	availableTags := make(map[string][]*tag.Tag)

	if s.shouldRetrieveTags() {
		var err error

		availableTags, err = s.repo.GetTagsByDomain("ROLE")
		if err != nil {
			Logger.Error(fmt.Sprintf("Error retrieving tags for account roles: %s", err.Error()))
		}
	}

	processedAps := make(map[string]*exporter.AccessProvider)

	// Get all account roles and import them
	roleEntities, err := s.repo.GetAccountRoles()
	if err != nil {
		return err
	}

	wp := workerpool.New(getWorkerPoolSize(s.configMap))

	for _, roleEntity := range roleEntities {
		if _, exclude := s.excludedRoles[roleEntity.Name]; exclude {
			Logger.Info("Skipping SnowFlake ROLE " + roleEntity.Name)
			continue
		}

		wp.Submit(func() {
			err2 := s.transformAccountRoleToAccessProvider(roleEntity, processedAps, availableTags)
			if err2 != nil {
				Logger.Warn(fmt.Sprintf("Error importing SnowFlake role %q: %s", roleEntity.Name, err2.Error()))
			}
		})
	}

	wp.StopWait()

	err = accessProviderHandler.AddAccessProviders(values(processedAps)...)
	if err != nil {
		return fmt.Errorf("error adding account roles to import file: %s", err.Error())
	}

	return nil
}

func (s *AccessFromTargetSyncer) shouldRetrieveTags() bool {
	standard := s.configMap.GetBoolWithDefault(SfStandardEdition, false)
	skipTags := s.configMap.GetBoolWithDefault(SfSkipTags, false)

	tagSupportEnabled := !standard && !skipTags

	return tagSupportEnabled
}

func (s *AccessFromTargetSyncer) transformShareToAccessProvider(shareName string, shareEntity []ShareEntity, processedAps map[string]*exporter.AccessProvider) error {
	Logger.Info(fmt.Sprintf("Reading SnowFlake SHARE %s (%d items)", shareName, len(shareEntity)))

	externalId := apTypeSharePrefix + shareName

	// Locking to make sure only one goroutine can read & write to the processedAps map at a time
	s.lock.Lock()

	recipients := make([]string, 0, len(shareEntity))

	var commonDatabase *string

	for _, share := range shareEntity {
		trimmedRecipient := strings.TrimSpace(share.To)

		if trimmedRecipient == "" {
			continue
		}

		recipients = append(recipients, trimmedRecipient)

		if commonDatabase == nil {
			commonDatabase = &share.DatabaseName
		} else if *commonDatabase != share.DatabaseName {
			return fmt.Errorf("share %s has multiple databases: %s and %s", shareName, *commonDatabase, share.DatabaseName)
		}
	}

	ap, f := processedAps[externalId]
	if !f {
		processedAps[externalId] = &exporter.AccessProvider{
			ExternalId: externalId,
			ActualName: shareName,
			Name:       shareName,
			NamingHint: shareName,
			Action:     types.Share,
			What:       make([]exporter.WhatItem, 0),
			Who: &exporter.WhoItem{
				Recipients: recipients,
			},
			CommonWhatDataObject: commonDatabase,
		}

		ap = processedAps[externalId]
	}

	s.lock.Unlock()

	// get objects granted TO share
	grantToEntities, err := s.repo.GetGrantsToShare(shareName)
	if err != nil {
		return fmt.Errorf("retrieving grants for share: %s", err.Error())
	}

	ap.What = append(ap.What, s.mapGrantToRoleToWhatItems(grantToEntities)...)

	return nil
}

func (s *AccessFromTargetSyncer) transformAccountRoleToAccessProvider(roleEntity RoleEntity, processedAps map[string]*exporter.AccessProvider, availableTags map[string][]*tag.Tag) error {
	Logger.Info(fmt.Sprintf("Reading SnowFlake ROLE %s", roleEntity.Name))

	roleName := roleEntity.Name
	externalId := roleName
	currentApType := access_provider.Role
	fromExternalIS := s.comesFromExternalIdentityStore(roleEntity, s.externalGroupOwners)

	users, groups, accessProviders, incomplete, err := s.retrieveWhoEntitiesForRole(roleEntity, externalId, currentApType, fromExternalIS)
	if err != nil {
		return err
	}

	// Locking to make sure only one goroutine can read & write to the processedAps map at a time
	s.lock.Lock()

	ap, f := processedAps[externalId]
	if !f {
		processedAps[externalId] = &exporter.AccessProvider{
			Type:       &currentApType,
			ExternalId: externalId,
			ActualName: roleName,
			Name:       roleName,
			NamingHint: roleName,
			Action:     types.Grant,
			Incomplete: &incomplete,
			Who: &exporter.WhoItem{
				Users:           users,
				AccessProviders: accessProviders,
				Groups:          groups,
			},
			What: make([]exporter.WhatItem, 0),
		}
		ap = processedAps[externalId]

		if fromExternalIS {
			if s.linkToExternalIdentityStoreGroups {
				// If we link to groups in the external identity store, we can just partially lock
				ap.NameLocked = ptr.Bool(true)
				ap.NameLockedReason = ptr.String(nameLockedReason)
				ap.DeleteLocked = ptr.Bool(true)
				ap.DeleteLockedReason = ptr.String(deleteLockedReason)
				ap.WhoLocked = ptr.Bool(true)
				ap.WhoLockedReason = ptr.String(whoLockedReason)
				ap.InheritanceLocked = ptr.Bool(true)
				ap.InheritanceLockedReason = ptr.String(inheritanceLockedReason)
			} else {
				// Otherwise we have to do a full lock
				ap.NotInternalizable = true
			}
		}
	} else {
		ap.Who.Users = users
		ap.Who.AccessProviders = accessProviders
		ap.Who.Groups = groups
	}

	s.lock.Unlock()

	// get objects granted TO role
	grantToEntities, err := s.accessSyncer.getGrantsToRole(ap.ExternalId, ap.Type)
	if err != nil {
		return fmt.Errorf("error retrieving grants for role: %s", err.Error())
	}

	ap.What = append(ap.What, s.mapGrantToRoleToWhatItems(grantToEntities)...)

	if isNotInternalizableRole(ap.ExternalId, ap.Type) {
		Logger.Info(fmt.Sprintf("Marking role %s as read-only (notInternalizable)", ap.ExternalId))
		ap.NotInternalizable = true
	}

	if len(availableTags) > 0 && availableTags[ap.Name] != nil {
		ap.Tags = availableTags[ap.Name]
		Logger.Debug(fmt.Sprintf("Going to add tags to AP %s", ap.ExternalId))
	}

	return nil
}

func (s *AccessFromTargetSyncer) extractExcludeRoleList() map[string]struct{} {
	excludedRoles := make(map[string]struct{})

	if excludedRoleList, ok := s.configMap.Parameters[SfExcludedRoles]; ok {
		if excludedRoleList != "" {
			for _, e := range strings.Split(excludedRoleList, ",") {
				e = strings.TrimSpace(e)
				excludedRoles[e] = struct{}{}
			}
		}
	}

	return excludedRoles
}

func (s *AccessFromTargetSyncer) importAllRolesOnDatabaseLevel(accessProviderHandler wrappers.AccessProviderHandler, excludedDatabases set.Set[string]) error {
	// Get all database roles for each database and import them
	databases, err := s.getApplicableDatabases(excludedDatabases)
	if err != nil {
		return err
	}

	wp := workerpool.New(getWorkerPoolSize(s.configMap))

	processedAps := make(map[string]*exporter.AccessProvider)

	for database := range databases {
		Logger.Info(fmt.Sprintf("Reading roles from Snowflake inside database %s", database))

		// Get all database roles for database
		roleEntities, err2 := s.repo.GetDatabaseRoles(database)
		if err2 != nil {
			return err2
		}

		for _, roleEntity := range roleEntities {
			fullRoleName := fmt.Sprintf("%s.%s", database, roleEntity.Name)
			if _, exclude := s.excludedRoles[fullRoleName]; exclude {
				Logger.Info("Skipping SnowFlake DATABASE ROLE " + fullRoleName)
				continue
			}

			wp.Submit(func() {
				availableTags := make(map[string][]*tag.Tag)

				if s.shouldRetrieveTags() {
					var err3 error

					availableTags, err3 = s.repo.GetDatabaseRoleTags(database, roleEntity.Name)
					if err3 != nil {
						Logger.Error(fmt.Sprintf("Error retrieving tags for database role: %q - %s", fullRoleName, err3.Error()))
					}
				}

				err2 := s.importAccessForDatabaseRole(database, roleEntity, availableTags, processedAps)
				if err2 != nil {
					Logger.Warn(fmt.Sprintf("Error importing SnowFlake Database role %q: %s", fullRoleName, err2.Error()))
				}
			})
		}
	}

	wp.StopWait()

	err = accessProviderHandler.AddAccessProviders(values(processedAps)...)
	if err != nil {
		return fmt.Errorf("error adding access provider to import file: %s", err.Error())
	}

	return nil
}

func (s *AccessFromTargetSyncer) importAllRolesOnApplicationLevel(accessProviderHandler wrappers.AccessProviderHandler, excludeDatabases set.Set[string]) error {
	applications, err := s.getApplicableApplications(excludeDatabases)
	if err != nil {
		return fmt.Errorf("retrieving applications: %w", err)
	}

	wp := workerpool.New(getWorkerPoolSize(s.configMap))

	processedAps := make(map[string]*exporter.AccessProvider)

	for application := range applications {
		Logger.Info(fmt.Sprintf("Reading roles from Snowflake inside application %s", application))

		wp.Submit(func() {
			roleEntitites, err2 := s.repo.GetApplicationRoles(application)
			if err2 != nil {
				Logger.Error(fmt.Sprintf("Error retrieving roles for application %q: %s", application, err2.Error()))

				return
			}

			for _, roleEntity := range roleEntitites {
				fullRoleName := fmt.Sprintf("%s.%s", application, roleEntity.Name)
				if _, exclude := s.excludedRoles[fullRoleName]; exclude {
					Logger.Info("Skipping SnowFlake APPLICATION ROLE " + fullRoleName)

					continue
				}

				err2 = s.importAccessForApplicationRole(application, RoleEntity{
					Name: roleEntity.Name,
				}, processedAps)
				if err2 != nil {
					Logger.Error(fmt.Sprintf("Error importing SnowFlake Application role %q: %s", fullRoleName, err2.Error()))

					return
				}
			}
		})
	}

	wp.StopWait()

	err = accessProviderHandler.AddAccessProviders(values(processedAps)...)
	if err != nil {
		return fmt.Errorf("adding access provider to import file: %w", err)
	}

	return nil
}

func (s *AccessFromTargetSyncer) comesFromExternalIdentityStore(roleEntity RoleEntity, externalGroupOwners string) bool {
	fromExternalIS := false

	// check if Role Owner is part of the ones that should be (partially) locked
	for _, i := range strings.Split(externalGroupOwners, ",") {
		if strings.EqualFold(i, roleEntity.Owner) {
			fromExternalIS = true
		}
	}

	return fromExternalIS
}

func (s *AccessFromTargetSyncer) importAccessForDatabaseRole(database string, roleEntity RoleEntity, availableTags map[string][]*tag.Tag, processedAps map[string]*exporter.AccessProvider) error {
	Logger.Info(fmt.Sprintf("Reading SnowFlake DATABASE ROLE %s inside %s", roleEntity.Name, database))

	roleName := roleEntity.Name
	externalId := databaseRoleExternalIdGenerator(database, roleName)
	currentApType := apTypeDatabaseRole
	fromExternalIS := s.comesFromExternalIdentityStore(roleEntity, s.externalGroupOwners)

	users, groups, accessProviders, incomplete, err := s.retrieveWhoEntitiesForRole(roleEntity, externalId, currentApType, fromExternalIS)
	if err != nil {
		return err
	}

	s.lock.Lock()

	ap, f := processedAps[externalId]
	if !f {
		processedAps[externalId] = &exporter.AccessProvider{
			Type:       &currentApType,
			ExternalId: externalId,
			// Updated this because of https://github.com/raito-io/appserver/blob/587484940a2e356a486dd8779166852761885353/lambda/appserver/services/access_provider/importer/importer.go#L523
			ActualName: roleName,

			Name:       fmt.Sprintf("%s.%s", database, roleName),
			NamingHint: roleName,
			Action:     types.Grant,
			Who: &exporter.WhoItem{
				Users:           users,
				AccessProviders: accessProviders,
				Groups:          groups,
			},
			What: make([]exporter.WhatItem, 0),
			// In a first implementation, we lock the who and what side for a database role
			// Who side will always be locked as you can't directly grant access to a database role from a user
			WhoLocked:        ptr.Bool(true),
			WhoLockedReason:  ptr.String(databaseRoleWhoLockedReason),
			WhatLocked:       ptr.Bool(true),
			WhatLockedReason: ptr.String(databaseRoleWhatLockedReason),

			Incomplete:           &incomplete,
			CommonWhatDataObject: &database,
		}
		ap = processedAps[externalId]
	} else {
		ap.Who.Users = users
		ap.Who.AccessProviders = accessProviders
		ap.Who.Groups = groups
	}

	s.lock.Unlock()

	// get objects granted TO role
	grantToEntities, err := s.accessSyncer.getGrantsToRole(ap.ExternalId, ap.Type)
	if err != nil {
		return fmt.Errorf("error retrieving grants for role: %s", err.Error())
	}

	ap.What = append(ap.What, s.mapGrantToRoleToWhatItems(grantToEntities)...)

	if isNotInternalizableRole(ap.ExternalId, ap.Type) {
		Logger.Info(fmt.Sprintf("Marking role %s as read-only (notInternalizable)", ap.ExternalId))
		ap.NotInternalizable = true
	}

	if len(availableTags) > 0 && availableTags[ap.Name] != nil {
		ap.Tags = availableTags[ap.Name]
		Logger.Debug(fmt.Sprintf("Going to add tags to AP %s", ap.ExternalId))
	}

	return nil
}

func (s *AccessFromTargetSyncer) importAccessForApplicationRole(application string, roleEntity RoleEntity, processedAps map[string]*exporter.AccessProvider) error {
	Logger.Info(fmt.Sprintf("Reading SnowFlake APPLICATION ROLE %s inside %s", roleEntity.Name, application))

	roleName := roleEntity.Name
	externalId := applicationRoleExternalIdGenerator(application, roleName)
	currentApType := apTypeApplicationRole
	fromExternalIS := s.comesFromExternalIdentityStore(roleEntity, s.externalGroupOwners)

	users, groups, accessProviders, incomplete, err := s.retrieveWhoEntitiesForRole(roleEntity, externalId, currentApType, fromExternalIS)
	if err != nil {
		return err
	}

	s.lock.Lock()

	ap, f := processedAps[externalId]
	if !f {
		processedAps[externalId] = &exporter.AccessProvider{
			Type:       &currentApType,
			ExternalId: externalId,
			ActualName: roleName,

			Name:       roleName,
			NamingHint: fmt.Sprintf("%s.%s", application, roleName),
			Action:     types.Grant,
			Who: &exporter.WhoItem{
				Users:           users,
				AccessProviders: accessProviders,
				Groups:          groups,
			},
			What: make([]exporter.WhatItem, 0),
			// In a first implementation, we lock the who and what side for a database role
			// Who side will always be locked as you can't directly grant access to a database role from a user
			WhoLocked:          ptr.Bool(true),
			WhoLockedReason:    ptr.String(databaseRoleWhoLockedReason),
			WhatLocked:         ptr.Bool(true),
			WhatLockedReason:   ptr.String(databaseRoleWhatLockedReason),
			DeleteLocked:       ptr.Bool(true),
			DeleteLockedReason: ptr.String(deleteLockReasonApp),

			Incomplete:           &incomplete,
			CommonWhatDataObject: &application,
		}
	} else {
		ap.Who.Users = users
		ap.Who.AccessProviders = accessProviders
		ap.Who.Groups = groups
	}

	s.lock.Unlock()

	return nil
}

func (s *AccessFromTargetSyncer) mapGrantToRoleToWhatItems(grantToEntities []GrantToRole) []exporter.WhatItem {
	var do *ds.DataObjectReference

	whatItems := make([]exporter.WhatItem, 0)
	permissions := make([]string, 0)
	sharesApplied := make([]string, 0)

	first := true

	for _, grant := range grantToEntities {
		if grant.GrantedOn == GrantTypeDatabaseRole { // It looks like database roles assigned to a SHARE are also included here, ignoring that
			continue
		}

		if first {
			// We set type to empty string because that's not needed by the importer to match the data object
			// + we cannot make the mapping to the correct Raito data object types here.
			do = &ds.DataObjectReference{FullName: s.accessSyncer.getFullNameFromGrant(grant.Name, grant.GrantedOn), Type: ""}
			first = false
		} else if do.FullName != grant.Name {
			if len(permissions) > 0 {
				whatItems = append(whatItems, exporter.WhatItem{
					DataObject:  do,
					Permissions: permissions,
				})
			}

			// We set type to empty string because that's not needed by the importer to match the data object
			// + we cannot make the mapping to the correct Raito data object types here.
			do = &ds.DataObjectReference{FullName: s.accessSyncer.getFullNameFromGrant(grant.Name, grant.GrantedOn), Type: ""}
			permissions = make([]string, 0)
		}

		if do.Type == "ACCOUNT" {
			do.Type = "datasource"
		}

		if _, f := AcceptedTypes[strings.ToUpper(grant.GrantedOn)]; f {
			permissions = append(permissions, mapPrivilege(grant.Privilege, grant.GrantedOn))
		}

		databaseName := strings.Split(grant.Name, ".")[0]
		if slices.Contains(s.inboundShares, databaseName) {
			// TODO do we need to do this for all tabular types?
			if strings.EqualFold(grant.GrantedOn, "TABLE") && !slices.Contains(sharesApplied, databaseName) {
				whatItems = append(whatItems, exporter.WhatItem{
					DataObject:  &ds.DataObjectReference{FullName: databaseName, Type: SharedPrefix + ds.Database},
					Permissions: []string{"IMPORTED PRIVILEGES"},
				})

				sharesApplied = append(sharesApplied, databaseName)
			}
		}
	}

	if len(permissions) > 0 {
		whatItems = append(whatItems, exporter.WhatItem{
			DataObject:  do,
			Permissions: permissions,
		})
	}

	return whatItems
}

// mapPrivilege maps the USAGE privilege to the corresponding one on database or schema.
// We do this to separate USAGE between database and schema because this is a special case that does not inherit from database to schema.
func mapPrivilege(privilege string, grantedOn string) string {
	if strings.EqualFold(privilege, "USAGE") {
		doType := strings.ToUpper(grantedOn)
		if strings.Contains(doType, "DATABASE") {
			return "USAGE on DATABASE"
		} else if strings.Contains(doType, "SCHEMA") {
			return "USAGE on SCHEMA"
		}
	}

	return privilege
}

func (s *AccessFromTargetSyncer) retrieveWhoEntitiesForRole(roleEntity RoleEntity, externalId string, apType string, fromExternalIS bool) (users []string, groups []string, accessProviders []string, incomplete bool, err error) {
	roleName := roleEntity.Name

	users = make([]string, 0)
	groups = make([]string, 0)
	accessProviders = make([]string, 0)

	if fromExternalIS && s.linkToExternalIdentityStoreGroups {
		groups = append(groups, roleName)
	} else {
		grantOfEntities, err := s.accessSyncer.retrieveGrantsOfRole(externalId, apType)
		if err != nil {
			return nil, nil, nil, false, err
		}

		for _, grantee := range grantOfEntities {
			if grantee.GrantedTo == "USER" {
				users = append(users, cleanDoubleQuotes(grantee.GranteeName))
			} else if grantee.GrantedTo == "ROLE" { //nolint:goconst
				if _, exclude := s.excludedRoles[grantee.GranteeName]; exclude {
					Logger.Warn(fmt.Sprintf("Skipping Snowflake ROLE %q may break the hierarchy for role %q", grantee.GranteeName, roleName))

					incomplete = true

					continue
				}

				accessProviders = append(accessProviders, accountRoleExternalIdGenerator(cleanDoubleQuotes(grantee.GranteeName)))
			} else if grantee.GrantedTo == "SHARE" {
				accessProviders = append(accessProviders, shareExternalIdGenerator(cleanDoubleQuotes(grantee.GranteeName)))
			} else if grantee.GrantedTo == GrantTypeDatabaseRole {
				if _, exclude := s.excludedRoles[grantee.GranteeName]; exclude {
					Logger.Warn(fmt.Sprintf("Skipping Snowflake DATABASE ROLE %q may break the hierarchy for role %q", grantee.GranteeName, roleName))

					incomplete = true

					continue
				}

				database, parsedRoleName, err2 := parseNamespacedRoleRoleName(cleanDoubleQuotes(grantee.GranteeName))
				if err2 != nil {
					return nil, nil, nil, false, err2
				}

				accessProviders = append(accessProviders, databaseRoleExternalIdGenerator(database, parsedRoleName))
			}
		}
	}

	return users, groups, accessProviders, incomplete, nil
}

func (s *AccessFromTargetSyncer) importPoliciesOfType(policyType string, action types.Action) error {
	policyEntities, err := s.repo.GetPolicies(policyType)
	if err != nil {
		// For Standard edition, row access policies are not supported. Failsafe in case `sf-standard-edition` is overlooked.
		// You can see the Snowflake edition in the UI, or through the 'show organization accounts;' query (ORGADMIN role needed).
		if strings.Contains(err.Error(), "Unsupported feature") {
			Logger.Warn(fmt.Sprintf("Could not fetch policies of type %s; unsupported feature.", policyType))
		} else {
			return fmt.Errorf("error fetching all %s policies: %s", policyType, err.Error())
		}
	}

	for _, policy := range policyEntities {
		if !strings.HasPrefix(strings.Replace(policy.Kind, "_", " ", -1), policyType) {
			Logger.Warn(fmt.Sprintf("Skipping policy %s of kind %s, expected: %s", policy.Name, policyType, policy.Kind))
			continue
		} else if strings.HasPrefix(policy.Name, maskPrefix) {
			Logger.Debug(fmt.Sprintf("Masking policy %s defined by RAITO. Not exporting this", policy.Name))
			continue
		}

		Logger.Info(fmt.Sprintf("Reading SnowFlake %s policy %s in Schema %s, Table %s", policyType, policy.Name, policy.SchemaName, policy.DatabaseName))

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
		describeMaskingPolicyEntities, err2 := s.repo.DescribePolicy(policyType, policy.DatabaseName, policy.SchemaName, policy.Name)
		if err2 != nil {
			Logger.Warn(fmt.Sprintf("Error fetching description for policy %s.%s.%s: %s", policy.DatabaseName, policy.SchemaName, policy.Name, err2.Error()))

			continue
		}

		if len(describeMaskingPolicyEntities) != 1 {
			Logger.Warn(fmt.Sprintf("Found %d definitions for %s policy %s.%s.%s, only expecting one", len(describeMaskingPolicyEntities), policyType, policy.DatabaseName, policy.SchemaName, policy.Name))

			continue
		}

		ap.Policy = describeMaskingPolicyEntities[0].Body

		// get policy references
		policyReferenceEntities, err2 := s.repo.GetPolicyReferences(policy.DatabaseName, policy.SchemaName, policy.Name)
		if err2 != nil {
			Logger.Warn(fmt.Sprintf("Error fetching policy references for %s.%s.%s: %s", policy.DatabaseName, policy.SchemaName, policy.Name, err2.Error()))

			continue
		}

		for ind := range policyReferenceEntities {
			policyReference := policyReferenceEntities[ind]
			if !strings.EqualFold("Active", policyReference.POLICY_STATUS) {
				continue
			}

			var dor *ds.DataObjectReference

			if policyReference.POLICY_KIND == "MASKING_POLICY" {
				if policyReference.REF_COLUMN_NAME.Valid {
					dor = &ds.DataObjectReference{
						Type:     "COLUMN",
						FullName: common.FormatQuery(`%s.%s.%s.%s`, policyReference.REF_DATABASE_NAME, policyReference.REF_SCHEMA_NAME, policyReference.REF_ENTITY_NAME, policyReference.REF_COLUMN_NAME.String),
					}
				} else {
					Logger.Info(fmt.Sprintf("Masking policy %s.%s.%s refers to something that isn't a column. Skipping", policyReference.REF_DATABASE_NAME, policyReference.REF_SCHEMA_NAME, policyReference.POLICY_NAME))
				}
			} else if policyReference.POLICY_KIND == "ROW_ACCESS_POLICY" {
				dor = &ds.DataObjectReference{
					Type:     "TABLE",
					FullName: common.FormatQuery(`%s.%s.%s`, policyReference.REF_DATABASE_NAME, policyReference.REF_SCHEMA_NAME, policyReference.REF_ENTITY_NAME),
				}
			}

			if dor != nil {
				ap.What = append(ap.What, exporter.WhatItem{
					DataObject:  dor,
					Permissions: []string{},
				})
			}
		}

		err2 = s.accessProviderHandler.AddAccessProviders(&ap)
		if err2 != nil {
			return fmt.Errorf("error adding access provider to import file: %s", err2.Error())
		}
	}

	return nil
}

func (s *AccessFromTargetSyncer) importMaskingPolicies() error {
	return s.importPoliciesOfType("MASKING", types.Mask)
}

func (s *AccessFromTargetSyncer) importRowAccessPolicies() error {
	return s.importPoliciesOfType("ROW ACCESS", types.Filtered)
}

func (s *AccessFromTargetSyncer) getApplicableDatabases(dbExcludes set.Set[string]) (set.Set[string], error) {
	allDatabases, err := s.accessSyncer.getAllDatabaseAndShareNames()
	if err != nil {
		return nil, err
	}

	filteredDatabases := set.NewSet[string]()

	for db := range allDatabases {
		if !dbExcludes.Contains(db) {
			filteredDatabases.Add(db)
		}
	}

	return filteredDatabases, nil
}

func (s *AccessFromTargetSyncer) getApplicableApplications(dbExcludes set.Set[string]) (set.Set[string], error) {
	allApplications, err := s.repo.GetApplications()
	if err != nil {
		return nil, err
	}

	filteredApplications := set.NewSet[string]()

	for _, app := range allApplications {
		if !dbExcludes.Contains(app.Name) {
			filteredApplications.Add(app.Name)
		}
	}

	return filteredApplications, nil
}

func (s *AccessFromTargetSyncer) extractExcludeDatabases() set.Set[string] {
	excludedDatabases := "SNOWFLAKE"
	if v, ok := s.configMap.Parameters[SfExcludedDatabases]; ok {
		excludedDatabases = v
	}

	return parseCommaSeparatedList(excludedDatabases)
}

func isNotInternalizableRole(externalId string, roleType *string) bool {
	searchForRole := externalId

	if isDatabaseRole(roleType) {
		database, parsedRoleName, err := parseDatabaseRoleExternalId(externalId)
		if err != nil {
			return true
		}

		searchForRole = fmt.Sprintf("%s.%s", database, parsedRoleName)
	} else if isApplicationRole(roleType) {
		return true
	}

	for _, r := range RolesNotInternalizable {
		if strings.EqualFold(r, searchForRole) {
			return true
		}
	}

	return false
}

func values[I comparable, A any](m map[I]A) []A {
	values := make([]A, 0, len(m))

	for _, value := range m {
		values = append(values, value)
	}

	return values
}
