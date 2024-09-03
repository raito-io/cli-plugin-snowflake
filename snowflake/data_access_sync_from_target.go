package snowflake

import (
	"fmt"
	"slices"
	"strings"

	"github.com/aws/smithy-go/ptr"
	"github.com/gammazero/workerpool"
	"github.com/raito-io/cli/base/access_provider"
	exporter "github.com/raito-io/cli/base/access_provider/sync_from_target"
	ds "github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/tag"
	"github.com/raito-io/cli/base/wrappers"
	"github.com/raito-io/golang-set/set"

	"github.com/raito-io/cli-plugin-snowflake/common"
)

func (s *AccessSyncer) importAllRolesOnAccountLevel(accessProviderHandler wrappers.AccessProviderHandler) error {
	s.externalGroupOwners = s.configMap.GetStringWithDefault(SfExternalIdentityStoreOwners, "")
	s.extractExcludeRoleList()
	s.linkToExternalIdentityStoreGroups = s.configMap.GetBoolWithDefault(SfLinkToExternalIdentityStoreGroups, false)
	s.availableTags = make(map[string][]*tag.Tag)

	if s.shouldRetrieveTags() {
		var err error

		s.availableTags, err = s.repo.GetTagsByDomain("ROLE")
		if err != nil {
			logger.Error(fmt.Sprintf("Error retrieving tags for account roles: %s", err.Error()))
		}
	}

	s.processedAps = make(map[string]*exporter.AccessProvider)

	// Get all account roles and import them
	roleEntities, err := s.repo.GetAccountRoles()
	if err != nil {
		return err
	}

	wp := workerpool.New(getWorkerPoolSize(s.configMap))

	for _, roleEntity := range roleEntities {
		if _, exclude := s.excludedRoles[roleEntity.Name]; exclude {
			logger.Info("Skipping SnowFlake ROLE " + roleEntity.Name)
			continue
		}

		wp.Submit(func() {
			s.handleRole(roleEntity)
		})
	}

	wp.StopWait()

	err = accessProviderHandler.AddAccessProviders(values(s.processedAps)...)
	if err != nil {
		return fmt.Errorf("error adding access provider to import file: %s", err.Error())
	}

	return nil
}

func (s *AccessSyncer) shouldRetrieveTags() bool {
	standard := s.configMap.GetBoolWithDefault(SfStandardEdition, false)
	skipTags := s.configMap.GetBoolWithDefault(SfSkipTags, false)

	tagSupportEnabled := !standard && !skipTags

	return tagSupportEnabled
}

func (s *AccessSyncer) handleRole(role RoleEntity) {
	err := s.transformAccountRoleToAccessProvider(role)
	if err != nil {
		logger.Warn(fmt.Sprintf("Error importing SnowFlake role %q: %s"+role.Name, err.Error()))
	}
}

func (s *AccessSyncer) transformAccountRoleToAccessProvider(roleEntity RoleEntity) error {
	logger.Info(fmt.Sprintf("Reading SnowFlake ROLE %s", roleEntity.Name))

	roleName := roleEntity.Name
	externalId := roleName
	currentApType := ptr.String(access_provider.Role)
	fromExternalIS := s.comesFromExternalIdentityStore(roleEntity, s.externalGroupOwners)

	users, groups, accessProviders, err := s.retrieveWhoEntitiesForRole(roleEntity, externalId, currentApType, fromExternalIS)
	if err != nil {
		return err
	}

	// Locking to make sure only one goroutine can read & write to the processedAps map at a time
	s.lock.Lock()

	ap, f := s.processedAps[externalId]
	if !f {
		s.processedAps[externalId] = &exporter.AccessProvider{
			Type:       currentApType,
			ExternalId: externalId,
			ActualName: roleName,
			Name:       roleName,
			NamingHint: roleName,
			Action:     exporter.Grant,
			Who: &exporter.WhoItem{
				Users:           users,
				AccessProviders: accessProviders,
				Groups:          groups,
			},
			What: make([]exporter.WhatItem, 0),
		}
		ap = s.processedAps[externalId]

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
	grantToEntities, err := s.getGrantsToRole(ap.ExternalId, ap.Type)
	if err != nil {
		return fmt.Errorf("error retrieving grants for role: %s", err.Error())
	}

	ap.What = append(ap.What, s.mapGrantToRoleToWhatItems(grantToEntities)...)

	if isNotInternalizableRole(ap.ExternalId, ap.Type) {
		logger.Info(fmt.Sprintf("Marking role %s as read-only (notInternalizable)", ap.ExternalId))
		ap.NotInternalizable = true
	}

	if len(s.availableTags) > 0 && s.availableTags[ap.Name] != nil {
		ap.Tags = s.availableTags[ap.Name]
		logger.Debug(fmt.Sprintf("Going to add tags to AP %s", ap.ExternalId))
	}

	return nil
}

func (s *AccessSyncer) extractExcludeRoleList() {
	excludedRoles := make(map[string]struct{})

	if excludedRoleList, ok := s.configMap.Parameters[SfExcludedRoles]; ok {
		if excludedRoleList != "" {
			for _, e := range strings.Split(excludedRoleList, ",") {
				e = strings.TrimSpace(e)
				excludedRoles[e] = struct{}{}
			}
		}
	}

	s.excludedRoles = excludedRoles
}

func (s *AccessSyncer) importAllRolesOnDatabaseLevel(accessProviderHandler wrappers.AccessProviderHandler, excludedDatabases set.Set[string]) error {
	//Get all database roles for each database and import them
	databases, err := s.getApplicableDatabases(excludedDatabases)
	if err != nil {
		return err
	}

	wp := workerpool.New(getWorkerPoolSize(s.configMap))

	processedAps := make(map[string]*exporter.AccessProvider)

	for _, database := range databases {
		logger.Info(fmt.Sprintf("Reading roles from Snowflake inside database %s", database.Name))

		// Get all database roles for database
		roleEntities, err2 := s.repo.GetDatabaseRoles(database.Name)
		if err2 != nil {
			return err2
		}

		for _, roleEntity := range roleEntities {
			fullRoleName := fmt.Sprintf("%s.%s", database.Name, roleEntity.Name)
			if _, exclude := s.excludedRoles[fullRoleName]; exclude {
				logger.Info("Skipping SnowFlake DATABASE ROLE " + fullRoleName)
				continue
			}

			wp.Submit(func() {
				availableTags := make(map[string][]*tag.Tag)

				if s.shouldRetrieveTags() {
					var err3 error

					availableTags, err3 = s.repo.GetDatabaseRoleTags(database.Name, roleEntity.Name)
					if err3 != nil {
						logger.Error(fmt.Sprintf("Error retrieving tags for database role: %q - %s", fullRoleName, err3.Error()))
					}
				}

				err2 := s.importAccessForDatabaseRole(database.Name, roleEntity, availableTags, processedAps)
				if err2 != nil {
					logger.Warn(fmt.Sprintf("Error importing SnowFlake Database role %q: %s", fullRoleName, err2.Error()))
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

func (s *AccessSyncer) comesFromExternalIdentityStore(roleEntity RoleEntity, externalGroupOwners string) bool {
	fromExternalIS := false

	// check if Role Owner is part of the ones that should be (partially) locked
	for _, i := range strings.Split(externalGroupOwners, ",") {
		if strings.EqualFold(i, roleEntity.Owner) {
			fromExternalIS = true
		}
	}

	return fromExternalIS
}

func (s *AccessSyncer) importAccessForDatabaseRole(database string, roleEntity RoleEntity, availableTags map[string][]*tag.Tag, processedAps map[string]*exporter.AccessProvider) error {
	logger.Info(fmt.Sprintf("Reading SnowFlake DATABASE ROLE %s inside %s", roleEntity.Name, database))

	roleName := roleEntity.Name
	externalId := databaseRoleExternalIdGenerator(database, roleName)
	currentApType := ptr.String(apTypeDatabaseRole)
	fromExternalIS := s.comesFromExternalIdentityStore(roleEntity, s.externalGroupOwners)

	users, groups, accessProviders, err := s.retrieveWhoEntitiesForRole(roleEntity, externalId, currentApType, fromExternalIS)
	if err != nil {
		return err
	}

	s.lock.Lock()

	ap, f := processedAps[externalId]
	if !f {
		processedAps[externalId] = &exporter.AccessProvider{
			Type:       currentApType,
			ExternalId: externalId,
			// Updated this because of https://github.com/raito-io/appserver/blob/587484940a2e356a486dd8779166852761885353/lambda/appserver/services/access_provider/importer/importer.go#L523
			ActualName: roleName,

			Name:       fmt.Sprintf("%s.%s", database, roleName),
			NamingHint: roleName,
			Action:     exporter.Grant,
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
		}
		ap = processedAps[externalId]
	} else {
		ap.Who.Users = users
		ap.Who.AccessProviders = accessProviders
		ap.Who.Groups = groups
	}

	s.lock.Unlock()

	// get objects granted TO role
	grantToEntities, err := s.getGrantsToRole(ap.ExternalId, ap.Type)
	if err != nil {
		return fmt.Errorf("error retrieving grants for role: %s", err.Error())
	}

	ap.What = append(ap.What, s.mapGrantToRoleToWhatItems(grantToEntities)...)

	if isNotInternalizableRole(ap.ExternalId, ap.Type) {
		logger.Info(fmt.Sprintf("Marking role %s as read-only (notInternalizable)", ap.ExternalId))
		ap.NotInternalizable = true
	}

	if len(availableTags) > 0 && availableTags[ap.Name] != nil {
		ap.Tags = availableTags[ap.Name]
		logger.Debug(fmt.Sprintf("Going to add tags to AP %s", ap.ExternalId))
	}

	return nil
}

func (s *AccessSyncer) mapGrantToRoleToWhatItems(grantToEntities []GrantToRole) []exporter.WhatItem {
	var do *ds.DataObjectReference

	whatItems := make([]exporter.WhatItem, 0)
	permissions := make([]string, 0)
	sharesApplied := make([]string, 0)

	for k, grant := range grantToEntities {
		if k == 0 {
			sfObject := common.ParseFullName(grant.Name)
			// We set type to empty string because that's not needed by the importer to match the data object
			// + we cannot make the mapping to the correct Raito data object types here.
			do = &ds.DataObjectReference{FullName: sfObject.GetFullName(false), Type: ""}
		} else if do.FullName != grant.Name {
			if len(permissions) > 0 {
				whatItems = append(whatItems, exporter.WhatItem{
					DataObject:  do,
					Permissions: permissions,
				})
			}
			sfObject := common.ParseFullName(grant.Name)
			// We set type to empty string because that's not needed by the importer to match the data object
			// + we cannot make the mapping to the correct Raito data object types here.
			do = &ds.DataObjectReference{FullName: sfObject.GetFullName(false), Type: ""}
			permissions = make([]string, 0)
		}

		if do.Type == "ACCOUNT" {
			do.Type = "datasource"
		}

		if _, f := AcceptedTypes[strings.ToUpper(grant.GrantedOn)]; f {
			permissions = append(permissions, mapPrivilege(grant.Privilege, grant.GrantedOn))
		}

		databaseName := strings.Split(grant.Name, ".")[0]
		if slices.Contains(s.shares, databaseName) {
			// TODO do we need to do this for all tabular types?
			if strings.EqualFold(grant.GrantedOn, "TABLE") && !slices.Contains(sharesApplied, databaseName) {
				whatItems = append(whatItems, exporter.WhatItem{
					DataObject:  &ds.DataObjectReference{FullName: databaseName, Type: SharedPrefix + ds.Database},
					Permissions: []string{"IMPORTED PRIVILEGES"},
				})

				sharesApplied = append(sharesApplied, databaseName)
			}
		}

		if k == len(grantToEntities)-1 && len(permissions) > 0 {
			whatItems = append(whatItems, exporter.WhatItem{
				DataObject:  do,
				Permissions: permissions,
			})
		}
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

func (s *AccessSyncer) retrieveWhoEntitiesForRole(roleEntity RoleEntity, externalId string, apType *string, fromExternalIS bool) (users []string, groups []string, accessProviders []string, err error) {
	roleName := roleEntity.Name

	users = make([]string, 0)
	groups = make([]string, 0)
	accessProviders = make([]string, 0)

	if fromExternalIS && s.linkToExternalIdentityStoreGroups {
		groups = append(groups, roleName)
	} else {
		grantOfEntities, err := s.retrieveGrantsOfRole(externalId, apType)
		if err != nil {
			return nil, nil, nil, err
		}

		for _, grantee := range grantOfEntities {
			if grantee.GrantedTo == "USER" {
				users = append(users, cleanDoubleQuotes(grantee.GranteeName))
			} else if grantee.GrantedTo == "ROLE" {
				accessProviders = append(accessProviders, accountRoleExternalIdGenerator(cleanDoubleQuotes(grantee.GranteeName)))
			} else if grantee.GrantedTo == "DATABASE_ROLE" {
				database, parsedRoleName, err2 := parseDatabaseRoleRoleName(cleanDoubleQuotes(grantee.GranteeName))
				if err2 != nil {
					return nil, nil, nil, err2
				}

				accessProviders = append(accessProviders, databaseRoleExternalIdGenerator(database, parsedRoleName))
			}
		}
	}

	return users, groups, accessProviders, nil
}

func (s *AccessSyncer) retrieveGrantsOfRole(externalId string, apType *string) (grantOfEntities []GrantOfRole, err error) {
	if isDatabaseRole(apType) {
		database, parsedRoleName, err2 := parseDatabaseRoleExternalId(externalId)
		if err2 != nil {
			return nil, err2
		}

		grantOfEntities, err = s.repo.GetGrantsOfDatabaseRole(database, parsedRoleName)
	} else {
		grantOfEntities, err = s.repo.GetGrantsOfAccountRole(externalId)
	}

	return grantOfEntities, err
}

func isNotInternalizableRole(externalId string, roleType *string) bool {
	searchForRole := externalId

	if isDatabaseRole(roleType) {
		database, parsedRoleName, err := parseDatabaseRoleExternalId(externalId)
		if err != nil {
			return true
		}

		searchForRole = fmt.Sprintf("%s.%s", database, parsedRoleName)
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
