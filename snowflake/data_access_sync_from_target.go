package snowflake

import (
	"fmt"
	"slices"
	"strings"

	"github.com/aws/smithy-go/ptr"
	"github.com/raito-io/cli/base/access_provider"
	exporter "github.com/raito-io/cli/base/access_provider/sync_from_target"
	ds "github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/tag"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers"
	"github.com/raito-io/golang-set/set"

	"github.com/raito-io/cli-plugin-snowflake/common"
)

type tagApRetrievalConfig struct {
	enabled       bool
	availableTags map[string][]*tag.Tag
}

func (s *AccessSyncer) importAllRolesOnAccountLevel(accessProviderHandler wrappers.AccessProviderHandler, repo dataAccessRepository, shares []string, configMap *config.ConfigMap) error {
	externalGroupOwners := configMap.GetStringWithDefault(SfExternalIdentityStoreOwners, "")
	excludedRoles := s.extractExcludeRoleList(configMap)
	linkToExternalIdentityStoreGroups := configMap.GetBoolWithDefault(SfLinkToExternalIdentityStoreGroups, false)

	tagRetrieval, err := s.shouldRetrieveTags(configMap, repo, "ROLE")
	if err != nil {
		return err
	}

	processedAps := make(map[string]*exporter.AccessProvider)

	// Get all account roles and import them
	roleEntities, err := repo.GetAccountRoles()
	if err != nil {
		return err
	}

	for _, roleEntity := range roleEntities {
		if _, exclude := excludedRoles[roleEntity.Name]; exclude {
			logger.Info("Skipping SnowFlake ROLE " + roleEntity.Name)
			continue
		}

		err = s.transformAccountRoleToAccessProvider(roleEntity, processedAps, linkToExternalIdentityStoreGroups, *tagRetrieval, externalGroupOwners, shares, repo)
		if err != nil {
			return err
		}
	}

	err = accessProviderHandler.AddAccessProviders(values(processedAps)...)
	if err != nil {
		return fmt.Errorf("error adding access provider to import file: %s", err.Error())
	}

	return nil
}
func (s *AccessSyncer) shouldRetrieveTags(configMap *config.ConfigMap, repo dataAccessRepository, tagDomain string) (*tagApRetrievalConfig, error) {
	standard := configMap.GetBoolWithDefault(SfStandardEdition, false)
	skipTags := configMap.GetBoolWithDefault(SfSkipTags, false)
	availableTags := make(map[string][]*tag.Tag)

	tagSupportEnabled := !standard && !skipTags
	if tagSupportEnabled {
		var err error

		availableTags, err = repo.GetTagsByDomain(tagDomain)
		if err != nil {
			return nil, err
		}
	}

	return &tagApRetrievalConfig{
		enabled:       tagSupportEnabled,
		availableTags: availableTags,
	}, nil
}

func (s *AccessSyncer) transformAccountRoleToAccessProvider(roleEntity RoleEntity, processedAps map[string]*exporter.AccessProvider, linkToExternalIdentityStoreGroups bool, tagRetrieval tagApRetrievalConfig, externalGroupOwners string, shares []string, repo dataAccessRepository) error {
	logger.Info(fmt.Sprintf("Reading SnowFlake ROLE %s", roleEntity.Name))

	roleName := roleEntity.Name
	externalId := roleName
	currentApType := ptr.String(access_provider.Role)
	fromExternalIS := s.comesFromExternalIdentityStore(roleEntity, externalGroupOwners)

	users, groups, accessProviders, err := s.retrieveWhoEntitiesForRole(roleEntity, externalId, currentApType, fromExternalIS, linkToExternalIdentityStoreGroups, repo)
	if err != nil {
		return err
	}

	ap, f := processedAps[externalId]
	if !f {
		processedAps[externalId] = &exporter.AccessProvider{
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
		ap = processedAps[externalId]

		if fromExternalIS {
			if linkToExternalIdentityStoreGroups {
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

	// get objects granted TO role
	grantToEntities, err := s.getGrantsToRole(ap.ExternalId, ap.Type, repo)
	if err != nil {
		return fmt.Errorf("error retrieving grants for role: %s", err.Error())
	}

	ap.What = append(ap.What, s.mapGrantToRoleToWhatItems(grantToEntities, shares)...)

	if isNotInternalizableRole(ap.ExternalId, ap.Type) {
		logger.Info(fmt.Sprintf("Marking role %s as read-only (notInternalizable)", ap.ExternalId))
		ap.NotInternalizable = true
	}

	if tagRetrieval.enabled && len(tagRetrieval.availableTags) > 0 && tagRetrieval.availableTags[ap.Name] != nil {
		ap.Tags = tagRetrieval.availableTags[ap.Name]
		logger.Debug(fmt.Sprintf("Going to add tags to AP %s", ap.ExternalId))
	}

	return nil
}

func (s *AccessSyncer) extractExcludeRoleList(configMap *config.ConfigMap) map[string]struct{} {
	excludedRoles := make(map[string]struct{})

	if excludedRoleList, ok := configMap.Parameters[SfExcludedRoles]; ok {
		if excludedRoleList != "" {
			for _, e := range strings.Split(excludedRoleList, ",") {
				e = strings.TrimSpace(e)
				excludedRoles[e] = struct{}{}
			}
		}
	}

	return excludedRoles
}

func (s *AccessSyncer) importAllRolesOnDatabaseLevel(accessProviderHandler wrappers.AccessProviderHandler, repo dataAccessRepository, excludedDatabases set.Set[string], shares []string, configMap *config.ConfigMap) error {
	externalGroupOwners := configMap.GetStringWithDefault(SfExternalIdentityStoreOwners, "")
	excludedRoles := s.extractExcludeRoleList(configMap)
	linkToExternalIdentityStoreGroups := configMap.GetBoolWithDefault(SfLinkToExternalIdentityStoreGroups, false)

	tagRetrieval, err := s.shouldRetrieveTags(configMap, repo, "DATABASE ROLE")
	if err != nil {
		return err
	}

	//Get all database roles for each database and import them
	databases, err := s.getApplicableDatabases(repo, excludedDatabases)
	if err != nil {
		return err
	}

	for _, database := range databases {
		logger.Info(fmt.Sprintf("Reading roles from Snowflake inside database %s", database.Name))
		processedAps := make(map[string]*exporter.AccessProvider)

		// Get all database roles for database
		roleEntities, err := repo.GetDatabaseRoles(database.Name)
		if err != nil {
			return err
		}

		for _, roleEntity := range roleEntities {
			fullRoleName := fmt.Sprintf("%s.%s", database.Name, roleEntity.Name)
			if _, exclude := excludedRoles[fullRoleName]; exclude {
				logger.Info("Skipping SnowFlake DATABASE ROLE " + fullRoleName)
				continue
			}

			err = s.importAccessForDatabaseRole(database.Name, roleEntity, externalGroupOwners, linkToExternalIdentityStoreGroups, *tagRetrieval, repo, processedAps, shares)
			if err != nil {
				return err
			}
		}

		err = accessProviderHandler.AddAccessProviders(values(processedAps)...)
		if err != nil {
			return fmt.Errorf("error adding access provider to import file: %s", err.Error())
		}
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

func (s *AccessSyncer) importAccessForDatabaseRole(database string, roleEntity RoleEntity, externalGroupOwners string, linkToExternalIdentityStoreGroups bool, tagRetrieval tagApRetrievalConfig, repo dataAccessRepository, processedAps map[string]*exporter.AccessProvider, shares []string) error {
	logger.Info(fmt.Sprintf("Reading SnowFlake DATABASE ROLE %s inside %s", roleEntity.Name, database))

	roleName := roleEntity.Name
	externalId := databaseRoleExternalIdGenerator(database, roleName)
	currentApType := ptr.String(apTypeDatabaseRole)
	fromExternalIS := s.comesFromExternalIdentityStore(roleEntity, externalGroupOwners)

	users, groups, accessProviders, err := s.retrieveWhoEntitiesForRole(roleEntity, externalId, currentApType, fromExternalIS, linkToExternalIdentityStoreGroups, repo)
	if err != nil {
		return err
	}

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

	// get objects granted TO role
	grantToEntities, err := s.getGrantsToRole(ap.ExternalId, ap.Type, repo)
	if err != nil {
		return fmt.Errorf("error retrieving grants for role: %s", err.Error())
	}

	ap.What = append(ap.What, s.mapGrantToRoleToWhatItems(grantToEntities, shares)...)

	if isNotInternalizableRole(ap.ExternalId, ap.Type) {
		logger.Info(fmt.Sprintf("Marking role %s as read-only (notInternalizable)", ap.ExternalId))
		ap.NotInternalizable = true
	}

	if tagRetrieval.enabled && len(tagRetrieval.availableTags) > 0 && tagRetrieval.availableTags[ap.Name] != nil {
		ap.Tags = tagRetrieval.availableTags[ap.Name]
		logger.Debug(fmt.Sprintf("Going to add tags to AP %s", ap.ExternalId))
	}

	return nil
}

func (s *AccessSyncer) mapGrantToRoleToWhatItems(grantToEntities []GrantToRole, shares []string) []exporter.WhatItem {
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
			do.Type = "DATASOURCE"
		}

		// We do not import USAGE as this is handled separately in the data access export
		if !strings.EqualFold("USAGE", grant.Privilege) {
			if _, f := AcceptedTypes[strings.ToUpper(grant.GrantedOn)]; f {
				permissions = append(permissions, grant.Privilege)
			}

			databaseName := strings.Split(grant.Name, ".")[0]
			if slices.Contains(shares, databaseName) {
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

		if k == len(grantToEntities)-1 && len(permissions) > 0 {
			whatItems = append(whatItems, exporter.WhatItem{
				DataObject:  do,
				Permissions: permissions,
			})
		}
	}

	return whatItems
}

func (s *AccessSyncer) retrieveWhoEntitiesForRole(roleEntity RoleEntity, externalId string, apType *string, fromExternalIS bool, linkToExternalIdentityStoreGroups bool, repo dataAccessRepository) (users []string, groups []string, accessProviders []string, err error) {
	roleName := roleEntity.Name

	users = make([]string, 0)
	groups = make([]string, 0)
	accessProviders = make([]string, 0)

	if fromExternalIS && linkToExternalIdentityStoreGroups {
		groups = append(groups, roleName)
	} else {
		grantOfEntities, err := s.retrieveGrantsOfRole(externalId, apType, repo)
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

func (s *AccessSyncer) retrieveGrantsOfRole(externalId string, apType *string, repo dataAccessRepository) (grantOfEntities []GrantOfRole, err error) {
	if isDatabaseRole(apType) {
		database, parsedRoleName, err2 := parseDatabaseRoleExternalId(externalId)
		if err2 != nil {
			return nil, err2
		}

		grantOfEntities, err = repo.GetGrantsOfDatabaseRole(database, parsedRoleName)
	} else {
		grantOfEntities, err = repo.GetGrantsOfAccountRole(externalId)
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
