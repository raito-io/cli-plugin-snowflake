package snowflake

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/smithy-go/ptr"

	exporter "github.com/raito-io/cli/base/access_provider/sync_from_target"
	importer "github.com/raito-io/cli/base/access_provider/sync_to_target"
	ds "github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/util/slice"
	"github.com/raito-io/cli/base/wrappers"

	"github.com/raito-io/cli-plugin-snowflake/common"
)

var RolesNotinternalizable = []string{"ORGADMIN", "ACCOUNTADMIN", "SECURITYADMIN", "USERADMIN", "SYSADMIN", "PUBLIC"}
var AcceptedTypes = map[string]struct{}{"ACCOUNT": {}, "WAREHOUSE": {}, "DATABASE": {}, "SCHEMA": {}, "TABLE": {}, "VIEW": {}, "COLUMN": {}, "SHARED-DATABASE": {}, "EXTERNAL_TABLE": {}, "MATERIALIZED_VIEW": {}}

const (
	whoLockedReason    = "The 'who' for this Snowflake role cannot be changed because it was imported from an external identity store"
	nameLockedReason   = "This Snowflake role cannot be renamed because it was imported from an external identity store"
	deleteLockedReason = "This Snowflake role cannot be deleted because it was imported from an external identity store"
)

//go:generate go run github.com/vektra/mockery/v2 --name=dataAccessRepository --with-expecter --inpackage
type dataAccessRepository interface {
	Close() error
	TotalQueryTime() time.Duration
	GetShares() ([]DbEntity, error)
	GetRoles() ([]RoleEntity, error)
	GetRolesWithPrefix(prefix string) ([]RoleEntity, error)
	GetGrantsOfRole(roleName string) ([]GrantOfRole, error)
	GetGrantsToRole(roleName string) ([]GrantToRole, error)
	GetPolicies(policy string) ([]policyEntity, error)
	DescribePolicy(policyType, dbName, schema, policyName string) ([]describePolicyEntity, error)
	GetPolicyReferences(dbName, schema, policyName string) ([]policyReferenceEntity, error)
	DropRole(roleName string) error
	ExecuteGrant(perm, on, role string) error
	ExecuteRevoke(perm, on, role string) error
	GetTablesInDatabase(databaseName string, schemaName string, handleEntity EntityHandler) error
	GetSchemasInDatabase(databaseName string, handleEntity EntityHandler) error
	GetDataBases() ([]DbEntity, error)
	GetWarehouses() ([]DbEntity, error)
	CommentRoleIfExists(comment, objectName string) error
	GrantUsersToRole(ctx context.Context, role string, users ...string) error
	RevokeUsersFromRole(ctx context.Context, role string, users ...string) error
	GrantRolesToRole(ctx context.Context, role string, roles ...string) error
	RevokeRolesFromRole(ctx context.Context, role string, roles ...string) error
	CreateRole(roleName string) error
}

type AccessSyncer struct {
	repoProvider            func(params map[string]string, role string) (dataAccessRepository, error)
	tablesPerSchemaCache    map[string][]TableEntity
	schemasPerDataBaseCache map[string][]SchemaEntity
	databasesCache          []DbEntity
	warehousesCache         []DbEntity
}

func NewDataAccessSyncer() *AccessSyncer {
	return &AccessSyncer{
		repoProvider:            newDataAccessSnowflakeRepo,
		tablesPerSchemaCache:    make(map[string][]TableEntity),
		schemasPerDataBaseCache: make(map[string][]SchemaEntity),
	}
}

func newDataAccessSnowflakeRepo(params map[string]string, role string) (dataAccessRepository, error) {
	return NewSnowflakeRepository(params, role)
}

func (s *AccessSyncer) SyncAccessProvidersFromTarget(ctx context.Context, accessProviderHandler wrappers.AccessProviderHandler, configMap *config.ConfigMap) error {
	repo, err := s.repoProvider(configMap.Parameters, "")
	if err != nil {
		return err
	}

	defer func() {
		logger.Info(fmt.Sprintf("Total snowflake query time:  %s", repo.TotalQueryTime()))
		repo.Close()
	}()

	logger.Info("Reading roles from Snowflake")

	err = s.importAccess(accessProviderHandler, configMap, repo)
	if err != nil {
		return err
	}

	skipColumns := configMap.GetBoolWithDefault(SfSkipColumns, false)
	standardEdition := configMap.GetBoolWithDefault(SfStandardEdition, false)

	if !standardEdition {
		if !skipColumns {
			logger.Info("Reading masking policies from Snowflake")

			err = s.importMaskingPolicies(accessProviderHandler, repo)
			if err != nil {
				return err
			}
		} else {
			logger.Info("Skipping masking policies")
		}

		logger.Info("Reading row access policies from Snowflake")

		err = s.importRowAccessPolicies(accessProviderHandler, repo)
		if err != nil {
			return err
		}
	} else {
		logger.Info("Skipping masking policies and row access policies due to Snowflake Standard Edition.")
	}

	return nil
}

func (s *AccessSyncer) SyncAccessProviderRolesToTarget(ctx context.Context, rolesToRemove []string, accessProviders map[string]*importer.AccessProvider, feedbackHandler wrappers.AccessProviderFeedbackHandler, configMap *config.ConfigMap) error {
	logger.Info("Configuring access providers as roles in Snowflake")

	repo, err := s.repoProvider(configMap.Parameters, "")
	if err != nil {
		return err
	}

	defer func() {
		logger.Info(fmt.Sprintf("Total snowflake query time:  %s", repo.TotalQueryTime()))
		repo.Close()
	}()

	err = s.removeRolesToRemove(rolesToRemove, repo)
	if err != nil {
		return err
	}

	existingRoles, err := s.findRoles("", accessProviders, repo)
	if err != nil {
		return err
	}

	err = s.generateAccessControls(ctx, accessProviders, existingRoles, repo, configMap)
	if err != nil {
		return err
	}

	feedbackMap := make(map[string][]importer.AccessSyncFeedbackInformation)

	for roleName, accessProvider := range accessProviders {
		feedbackElement := importer.AccessSyncFeedbackInformation{AccessId: accessProvider.Id, ActualName: roleName}
		if feedbackObjects, found := feedbackMap[accessProvider.Id]; found {
			feedbackMap[accessProvider.Id] = append(feedbackObjects, feedbackElement)
		} else {
			feedbackMap[accessProvider.Id] = []importer.AccessSyncFeedbackInformation{feedbackElement}
		}
	}

	for apId, feedbackObjects := range feedbackMap {
		err = feedbackHandler.AddAccessProviderFeedback(apId, feedbackObjects...)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *AccessSyncer) SyncAccessProviderMasksToTarget(ctx context.Context, masksToRemove []string, access []*importer.AccessProvider, feedbackHandler wrappers.AccessProviderFeedbackHandler, configMap *config.ConfigMap) error {
	//TODO implement me
	panic("implement me")
}

func (s *AccessSyncer) SyncAccessAsCodeToTarget(ctx context.Context, access map[string]*importer.AccessProvider, prefix string, configMap *config.ConfigMap) error {
	logger.Info("Configuring access providers as roles in Snowflake")

	repo, err := s.repoProvider(configMap.Parameters, "")
	if err != nil {
		return err
	}

	defer func() {
		logger.Info(fmt.Sprintf("Total snowflake query time:  %s", repo.TotalQueryTime()))
		repo.Close()
	}()

	existingRoles, err := s.findRoles(prefix, access, repo)
	if err != nil {
		return err
	}

	rolesToRemove := make([]string, 0)

	for role, toKeep := range existingRoles {
		if !toKeep {
			rolesToRemove = append(rolesToRemove, role)
		}
	}

	err = s.removeRolesToRemove(rolesToRemove, repo)
	if err != nil {
		return err
	}

	err = s.generateAccessControls(ctx, access, existingRoles, repo, configMap)
	if err != nil {
		return err
	}

	return nil
}

func (s *AccessSyncer) removeRolesToRemove(rolesToRemove []string, repo dataAccessRepository) error {
	if len(rolesToRemove) > 0 {
		logger.Info(fmt.Sprintf("Removing old Raito roles in Snowflake: %s", rolesToRemove))

		for _, roleToRemove := range rolesToRemove {
			err := repo.DropRole(roleToRemove)
			if err != nil && !strings.Contains(err.Error(), "does not exist") {
				return fmt.Errorf("unable to drop role %q: %s", roleToRemove, err.Error())
			}
		}
	} else {
		logger.Info("No old Raito roles to remove in Snowflake")
	}

	return nil
}

func getShareNames(repo dataAccessRepository) (map[string]struct{}, error) {
	dbShares, err := repo.GetShares()
	if err != nil {
		return nil, err
	}

	shares := make(map[string]struct{}, len(dbShares))
	for _, e := range dbShares {
		shares[e.Name] = struct{}{}
	}

	return shares, nil
}

func (s *AccessSyncer) importAccess(accessProviderHandler wrappers.AccessProviderHandler, configMap *config.ConfigMap, repo dataAccessRepository) error {
	externalGroupOwners := configMap.GetStringWithDefault(SfExternalIdentityStoreOwners, "")

	linkToExternalIdentityStoreGroups := configMap.GetBoolWithDefault(SfLinkToExternalIdentityStoreGroups, false)

	excludedRoleList := ""
	if v, ok := configMap.Parameters[SfExcludedRoles]; ok {
		excludedRoleList = v
	}

	excludedRoles := make(map[string]struct{})

	if excludedRoleList != "" {
		for _, e := range strings.Split(excludedRoleList, ",") {
			e = strings.TrimSpace(e)
			excludedRoles[e] = struct{}{}
		}
	}

	shares, err := getShareNames(repo)
	if err != nil {
		return err
	}

	roleEntities, err := repo.GetRoles()
	if err != nil {
		return err
	}

	accessProviderMap := make(map[string]*exporter.AccessProvider)

	for _, roleEntity := range roleEntities {
		if _, exclude := excludedRoles[roleEntity.Name]; exclude {
			logger.Info("Skipping SnowFlake ROLE " + roleEntity.Name)
			continue
		}

		err = s.importAccessForRole(roleEntity, externalGroupOwners, linkToExternalIdentityStoreGroups, repo, accessProviderMap, shares, accessProviderHandler)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *AccessSyncer) importAccessForRole(roleEntity RoleEntity, externalGroupOwners string, linkToExternalIdentityStoreGroups bool, repo dataAccessRepository, accessProviderMap map[string]*exporter.AccessProvider, shares map[string]struct{}, accessProviderHandler wrappers.AccessProviderHandler) error {
	logger.Info("Reading SnowFlake ROLE " + roleEntity.Name)

	fromExternalIS := false

	// check if Role Owner is part of the ones that should be (partially) locked
	for _, i := range strings.Split(externalGroupOwners, ",") {
		if strings.EqualFold(i, roleEntity.Owner) {
			fromExternalIS = true
		}
	}

	users := make([]string, 0)
	accessProviders := make([]string, 0)
	groups := make([]string, 0)

	if fromExternalIS && linkToExternalIdentityStoreGroups {
		groups = append(groups, roleEntity.Name)
	} else {
		grantOfEntities, err := repo.GetGrantsOfRole(roleEntity.Name)
		if err != nil {
			return err
		}

		for _, grantee := range grantOfEntities {
			if grantee.GrantedTo == "USER" {
				users = append(users, cleanDoubleQuotes(grantee.GranteeName))
			} else if grantee.GrantedTo == "ROLE" {
				accessProviders = append(accessProviders, cleanDoubleQuotes(grantee.GranteeName))
			}
		}
	}

	ap, f := accessProviderMap[roleEntity.Name]
	if !f {
		accessProviderMap[roleEntity.Name] = &exporter.AccessProvider{
			ExternalId: roleEntity.Name,
			Name:       roleEntity.Name,
			NamingHint: roleEntity.Name,
			Action:     exporter.Grant,
			Who: &exporter.WhoItem{
				Users:           users,
				AccessProviders: accessProviders,
				Groups:          groups,
			},
			ActualName: roleEntity.Name,
			What:       make([]exporter.WhatItem, 0),
		}
		ap = accessProviderMap[roleEntity.Name]

		if fromExternalIS {
			if linkToExternalIdentityStoreGroups {
				// If we link to groups in the external identity store, we can just partially lock
				ap.NameLocked = ptr.Bool(true)
				ap.NameLockedReason = ptr.String(nameLockedReason)
				ap.DeleteLocked = ptr.Bool(true)
				ap.DeleteLockedReason = ptr.String(deleteLockedReason)
				ap.WhoLocked = ptr.Bool(true)
				ap.WhoLockedReason = ptr.String(whoLockedReason)
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

	var do *ds.DataObjectReference
	permissions := make([]string, 0)

	sharesApplied := make(map[string]struct{}, 0)

	// get objects granted TO role
	grantToEntities, err := repo.GetGrantsToRole(roleEntity.Name)
	if err != nil {
		return err
	}

	for k, grant := range grantToEntities {
		if k == 0 {
			sfObject := common.ParseFullName(grant.Name)
			// We set type to empty string because that's not needed by the importer to match the data object
			// + we cannot make the mapping to the correct Raito data object types here.
			do = &ds.DataObjectReference{FullName: sfObject.GetFullName(false), Type: ""}
		} else if do.FullName != grant.Name {
			if len(permissions) > 0 {
				ap.What = append(ap.What, exporter.WhatItem{
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
			if _, f := shares[databaseName]; f {
				// TODO do we need to do this for all tabular types?
				if _, f := sharesApplied[databaseName]; strings.EqualFold(grant.GrantedOn, "TABLE") && !f {
					ap.What = append(ap.What, exporter.WhatItem{
						DataObject:  &ds.DataObjectReference{FullName: databaseName, Type: "shared-" + ds.Database},
						Permissions: []string{"IMPORTED PRIVILEGES"},
					})
					sharesApplied[databaseName] = struct{}{}
				}
			}
		}

		if k == len(grantToEntities)-1 && len(permissions) > 0 {
			ap.What = append(ap.What, exporter.WhatItem{
				DataObject:  do,
				Permissions: permissions,
			})
		}
	}

	if isNotInternizableRole(ap.Name) {
		logger.Info(fmt.Sprintf("Marking role %s as read-only (notInternalizable)", ap.Name))
		ap.NotInternalizable = true
	}

	err = accessProviderHandler.AddAccessProviders(ap)
	if err != nil {
		return fmt.Errorf("error adding access provider to import file: %s", err.Error())
	}

	return nil
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

func isNotInternizableRole(role string) bool {
	for _, r := range RolesNotinternalizable {
		if strings.EqualFold(r, role) {
			return true
		}
	}

	return false
}

// findRoles returns a map where the keys are all the roles that exist in Snowflake right now and the value indicates if it was found in apMap or not.
func (s *AccessSyncer) findRoles(prefix string, apMap map[string]*importer.AccessProvider, repo dataAccessRepository) (map[string]bool, error) {
	foundRoles := make(map[string]bool)

	roleEntities, err := repo.GetRolesWithPrefix(prefix)
	if err != nil {
		return nil, err
	}

	for _, roleEntity := range roleEntities {
		_, f := apMap[roleEntity.Name]
		foundRoles[roleEntity.Name] = f
	}

	return foundRoles, nil
}

func buildMetaDataMap(metaData *ds.MetaData) map[string]map[string]struct{} {
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
func (s *AccessSyncer) generateAccessControls(ctx context.Context, apMap map[string]*importer.AccessProvider, existingRoles map[string]bool, repo dataAccessRepository, configMap *config.ConfigMap) error {
	// We always need the meta data
	syncer := DataSourceSyncer{}
	md, err := syncer.GetDataSourceMetaData(ctx, configMap)

	if err != nil {
		return err
	}

	metaData := buildMetaDataMap(md)

	roleCreated := make(map[string]interface{})

	for rn, accessProvider := range apMap {
		ignoreWho := accessProvider.WhoLocked != nil && *accessProvider.WhoLocked
		ignoreWhat := accessProvider.WhatLocked != nil && *accessProvider.WhatLocked

		logger.Info(fmt.Sprintf("Generating access controls for access provider %q (Ignore who: %t; Ignore what: %t)", accessProvider.Name, ignoreWho, ignoreWhat))

		// Extract RoleNames from Access Providers that are among the whoList of this one
		inheritedRoles := make([]string, 0)

		if !ignoreWho {
			for _, apWho := range accessProvider.Who.InheritFrom {
				if strings.HasPrefix(apWho, "ID:") {
					apId := apWho[3:]
					for rn2, accessProvider2 := range apMap {
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
					grants, err := s.createGrantsForTableOrView(what.DataObject.Type, permissions, what.DataObject.FullName, metaData)
					if err != nil {
						return err
					}

					expectedGrants = append(expectedGrants, grants...)
				} else if what.DataObject.Type == ds.Schema {
					grants, err := s.createGrantsForSchema(repo, permissions, what.DataObject.FullName, metaData, false)
					if err != nil {
						return err
					}

					expectedGrants = append(expectedGrants, grants...)
				} else if what.DataObject.Type == "shared-schema" {
					grants, err := s.createGrantsForSchema(repo, permissions, what.DataObject.FullName, metaData, true)
					if err != nil {
						return err
					}

					expectedGrants = append(expectedGrants, grants...)
				} else if what.DataObject.Type == "shared-database" {
					grants, err := s.createGrantsForDatabase(repo, permissions, what.DataObject.FullName, metaData, true)
					if err != nil {
						return err
					}

					expectedGrants = append(expectedGrants, grants...)
				} else if what.DataObject.Type == ds.Database {
					grants, err := s.createGrantsForDatabase(repo, permissions, what.DataObject.FullName, metaData, false)
					if err != nil {
						return err
					}

					expectedGrants = append(expectedGrants, grants...)
				} else if what.DataObject.Type == "warehouse" {
					expectedGrants = append(expectedGrants, s.createGrantsForWarehouse(permissions, what.DataObject.FullName, metaData)...)
				} else if what.DataObject.Type == ds.Datasource {
					grants, err := s.createGrantsForAccount(repo, permissions, metaData)
					if err != nil {
						return err
					}

					expectedGrants = append(expectedGrants, grants...)
				}
			}
		}

		var foundGrants []Grant

		// If the role already exists in the system
		if _, f := existingRoles[rn]; f {
			logger.Info(fmt.Sprintf("Merging role %q", rn))

			err := repo.CommentRoleIfExists(createComment(accessProvider, true), rn)
			if err != nil {
				return fmt.Errorf("error while updating comment on role %q: %s", rn, err.Error())
			}

			if !ignoreWho {
				grantsOfRole, err := repo.GetGrantsOfRole(rn)
				if err != nil {
					return err
				}

				usersOfRole := make([]string, 0, len(grantsOfRole))
				rolesOfRole := make([]string, 0, len(grantsOfRole))

				for _, gor := range grantsOfRole {
					if strings.EqualFold(gor.GrantedTo, "USER") {
						usersOfRole = append(usersOfRole, gor.GranteeName)
					} else if strings.EqualFold(gor.GrantedTo, "ROLE") {
						rolesOfRole = append(rolesOfRole, gor.GranteeName)
					}
				}

				toAdd := slice.StringSliceDifference(accessProvider.Who.Users, usersOfRole, false)
				toRemove := slice.StringSliceDifference(usersOfRole, accessProvider.Who.Users, false)
				logger.Info(fmt.Sprintf("Identified %d users to add and %d users to remove from role %q", len(toAdd), len(toRemove), rn))

				if len(toAdd) > 0 {
					e := repo.GrantUsersToRole(ctx, rn, toAdd...)
					if e != nil {
						return fmt.Errorf("error while assigning users to role %q: %s", rn, e.Error())
					}
				}

				if len(toRemove) > 0 {
					e := repo.RevokeUsersFromRole(ctx, rn, toRemove...)
					if e != nil {
						return fmt.Errorf("error while unassigning users from role %q: %s", rn, e.Error())
					}
				}

				toAdd = slice.StringSliceDifference(inheritedRoles, rolesOfRole, false)
				toRemove = slice.StringSliceDifference(rolesOfRole, inheritedRoles, false)
				logger.Info(fmt.Sprintf("Identified %d roles to add and %d roles to remove from role %q", len(toAdd), len(toRemove), rn))

				if len(toAdd) > 0 {
					e := repo.GrantRolesToRole(ctx, rn, toAdd...)
					if e != nil {
						return fmt.Errorf("error while assigning role to role %q: %s", rn, e.Error())
					}
				}

				if len(toRemove) > 0 {
					e := repo.RevokeRolesFromRole(ctx, rn, toRemove...)
					if e != nil {
						return fmt.Errorf("error while unassigning role from role %q: %s", rn, e.Error())
					}
				}
			}

			if !ignoreWhat {
				// Remove all future grants on schema and database if applicable.
				// Since these are future grants, it's safe to just remove them and re-add them again (if required).
				// We assume nobody manually added others to this role manually.
				for _, what := range accessProvider.What {
					if what.DataObject.Type == "database" {
						e := repo.ExecuteRevoke("ALL", common.FormatQuery(`FUTURE SCHEMAS IN DATABASE %s`, what.DataObject.FullName), rn)
						if e != nil {
							return fmt.Errorf("error while assigning future schema grants in database %q to role %q: %s", what.DataObject.FullName, rn, e.Error())
						}

						e = repo.ExecuteRevoke("ALL", common.FormatQuery(`FUTURE TABLES IN DATABASE %s`, what.DataObject.FullName), rn)
						if e != nil {
							return fmt.Errorf("error while assigning future table grants in database %q to role %q: %s", what.DataObject.FullName, rn, e.Error())
						}
					} else if what.DataObject.Type == "schema" {
						e := repo.ExecuteRevoke("ALL", fmt.Sprintf("FUTURE TABLES IN SCHEMA %s", what.DataObject.FullName), rn)
						if e != nil {
							return fmt.Errorf("error while assigning future table grants in schema %q to role %q: %s", what.DataObject.FullName, rn, e.Error())
						}
					}
				}

				grantsToRole, err := repo.GetGrantsToRole(rn)
				if err != nil {
					return err
				}

				logger.Debug(fmt.Sprintf("Found grants for role %q: %+v", rn, grantsToRole))

				foundGrants = make([]Grant, 0, len(grantsToRole))

				for _, grant := range grantsToRole {
					if strings.EqualFold(grant.GrantedOn, "ACCOUNT") {
						foundGrants = append(foundGrants, Grant{grant.Privilege, "account", ""})
					} else if strings.EqualFold(grant.Privilege, "OWNERSHIP") {
						logger.Warn(fmt.Sprintf("Ignoring permission %q on %q for Role %q as this will remain untouched", grant.Privilege, grant.Name, rn))
					} else if strings.EqualFold(grant.Privilege, "USAGE") && strings.EqualFold(grant.GrantedOn, "ROLE") {
						logger.Debug(fmt.Sprintf("Ignoring USAGE permission on ROLE %q", grant.Name))
					} else {
						onType := convertSnowflakeGrantTypeToRaito(grant.GrantedOn)

						foundGrants = append(foundGrants, Grant{grant.Privilege, onType, grant.Name})
					}
				}
			}

			logger.Info(fmt.Sprintf("Done updating users granted to role %q", rn))
		} else {
			logger.Info(fmt.Sprintf("Creating role %q", rn))

			if _, f := roleCreated[rn]; !f {
				// Create the role if not exists
				err := repo.CreateRole(rn)
				if err != nil {
					return fmt.Errorf("error while creating role %q: %s", rn, err.Error())
				}

				// Updating the comment (independent of creation)
				err = repo.CommentRoleIfExists(createComment(accessProvider, false), rn)
				if err != nil {
					return fmt.Errorf("error while updating comment on role %q: %s", rn, err.Error())
				}
				roleCreated[rn] = struct{}{}
			}

			if !ignoreWho {
				err := repo.GrantUsersToRole(ctx, rn, accessProvider.Who.Users...)
				if err != nil {
					logger.Error("Encountered error :" + err.Error())

					return fmt.Errorf("error while assigning users to role %q: %s", rn, err.Error())
				}

				err = repo.GrantRolesToRole(ctx, rn, inheritedRoles...)
				if err != nil {
					logger.Error("Encountered error :" + err.Error())

					return fmt.Errorf("error while assigning roles to role %q: %s", rn, err.Error())
				}
				// TODO assign role to SYSADMIN if requested (add as input parameter)
			}
		}

		if !ignoreWhat {
			err := mergeGrants(repo, rn, foundGrants, expectedGrants, metaData)
			if err != nil {
				logger.Error("Encountered error :" + err.Error())
				return err
			}
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
			logger.Warn("Permission %q does not apply to type %s", p, strings.ToUpper(doType))
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
	s.databasesCache, err = repo.GetDataBases()

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
			logger.Warn("Permission %q does not apply to type SCHEMA or any of its descendants. Skipping", p)
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
		schemaType = "shared-" + schemaType
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
		dbType = "shared-" + dbType
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
		tableType = "shared-" + tableType
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
			logger.Warn("Permission %q does not apply to type DATABASE or any of its descendants. Skipping", p)
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
			logger.Warn("Permission %q does not apply to type WAREHOUSE. Skipping", p)
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

			shareNames, err := getShareNames(repo)
			if err != nil {
				return nil, err
			}

			databases, err := s.getDatabases(repo)
			if err != nil {
				return nil, err
			}

			for _, database := range databases {
				databaseMatchFound := false

				_, isShare := shareNames[database.Name]

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
			logger.Warn("Permission %q does not apply to type ACCOUNT (datasource) or any of its descendants. Skipping", p)
			continue
		}
	}

	return grants, nil
}

func mergeGrants(repo dataAccessRepository, role string, found []Grant, expected []Grant, metaData map[string]map[string]struct{}) error {
	toAdd := slice.SliceDifference(expected, found)
	toRemove := slice.SliceDifference(found, expected)

	logger.Info(fmt.Sprintf("Found %d grants to add and %d grants to remove for role %q", len(toAdd), len(toRemove), role))

	for _, grant := range toAdd {
		if verifyGrant(grant, metaData) {
			err := repo.ExecuteGrant(grant.Permissions, grant.GetGrantOnType()+" "+grant.On, role)
			if err != nil {
				return err
			}
		}
	}

	for _, grant := range toRemove {
		if verifyGrant(grant, metaData) {
			err := repo.ExecuteRevoke(grant.Permissions, grant.GetGrantOnType()+" "+grant.On, role)
			if err != nil {
				return err
			}
		}
	}

	return nil
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
