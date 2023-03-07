package snowflake

import (
	"context"
	"fmt"
	"sort"
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

// PermissionTarget is used as value for the PermissionMap to map a Raito permission to a list of snowflake permissions
// and a string to use in the role name to represent the permission
type PermissionTarget struct {
	snowflakePermissions []string
	// The name (typically just 1 or 2 letters) to use in the generated role name
	roleName string
}

var PermissionMap = map[string]PermissionTarget{
	"READ":  {snowflakePermissions: []string{"SELECT"}, roleName: "R"},
	"WRITE": {snowflakePermissions: []string{"UPDATE", "INSERT", "DELETE"}, roleName: "W"},
}

var RolesNotinternalizable = []string{"ORGADMIN", "ACCOUNTADMIN", "SECURITYADMIN", "USERADMIN", "SYSADMIN", "PUBLIC"}
var AcceptedTypes = map[string]struct{}{"ACCOUNT": {}, "WAREHOUSE": {}, "DATABASE": {}, "SCHEMA": {}, "TABLE": {}, "COLUMN": {}, "SHARED-DATABASE": {}}

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
	GetTablesInSchema(sfObject *common.SnowflakeObject) ([]DbEntity, error)
	GetViewsInSchema(sfObject *common.SnowflakeObject) ([]DbEntity, error)
	GetSchemasInDatabase(databaseName string) ([]DbEntity, error)
	CommentIfExists(comment, objectType, objectName string) error
	GrantUsersToRole(ctx context.Context, role string, users ...string) error
	RevokeUsersFromRole(ctx context.Context, role string, users ...string) error
	GrantRolesToRole(ctx context.Context, role string, roles ...string) error
	RevokeRolesFromRole(ctx context.Context, role string, roles ...string) error
	CreateRole(roleName string) error
}

type AccessSyncer struct {
	repoProvider func(params map[string]string, role string) (dataAccessRepository, error)
}

func NewDataAccessSyncer() *AccessSyncer {
	return &AccessSyncer{
		repoProvider: newDataAccessSnowflakeRepo,
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

	if !configMap.GetBoolWithDefault(SfStandardEdition, false) {
		logger.Info("Reading masking policies from Snowflake")

		err = s.importMaskingPolicies(accessProviderHandler, repo)
		if err != nil {
			return err
		}

		logger.Info("Reading row access policies from Snowflake")

		err = s.importRowAccessPolicies(accessProviderHandler, repo)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *AccessSyncer) SyncAccessProvidersToTarget(ctx context.Context, rolesToRemove []string, accessProviders map[string]*importer.AccessProvider, feedbackHandler wrappers.AccessProviderFeedbackHandler, configMap *config.ConfigMap) error {
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

	err = s.generateAccessControls(ctx, accessProviders, existingRoles, repo, false)
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

	err = s.generateAccessControls(ctx, access, existingRoles, repo, true)
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
				users = append(users, grantee.GranteeName)
			} else if grantee.GrantedTo == "ROLE" {
				accessProviders = append(accessProviders, grantee.GranteeName)
			}
		}
	}

	// get objects granted TO role
	grantToEntities, err := repo.GetGrantsToRole(roleEntity.Name)
	if err != nil {
		return err
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
			Access: []*exporter.Access{
				{
					ActualName: roleEntity.Name,
					What:       make([]exporter.WhatItem, 0),
				},
			},
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

	for k, object := range grantToEntities {
		if k == 0 {
			sfObject := common.ParseFullName(object.Name)
			do = &ds.DataObjectReference{FullName: sfObject.GetFullName(false), Type: object.GrantedOn}
		} else if do.FullName != object.Name {
			if len(permissions) > 0 {
				ap.What = append(ap.What, exporter.WhatItem{
					DataObject:  do,
					Permissions: permissions,
				})
			}
			sfObject := common.ParseFullName(object.Name)
			do = &ds.DataObjectReference{FullName: sfObject.GetFullName(false), Type: object.GrantedOn}
			permissions = make([]string, 0)
		}

		if do.Type == "ACCOUNT" {
			do.Type = "DATASOURCE"
		}

		// We do not import USAGE as this is handled separately in the data access export
		if !strings.EqualFold("USAGE", object.Privilege) {
			if _, f := AcceptedTypes[strings.ToUpper(object.GrantedOn)]; f {
				permissions = append(permissions, object.Privilege)
			}

			databaseName := strings.Split(object.Name, ".")[0]
			if _, f := shares[databaseName]; f {
				if _, f := sharesApplied[databaseName]; strings.EqualFold(object.GrantedOn, "TABLE") && !f {
					ap.What = append(ap.What, exporter.WhatItem{
						DataObject:  &ds.DataObjectReference{FullName: databaseName, Type: "shared-" + ds.Database},
						Permissions: []string{"IMPORTED PRIVILEGES"},
					})
					sharesApplied[databaseName] = struct{}{}
				}

				if !strings.HasPrefix(do.Type, "SHARED") {
					do.Type = "SHARED-" + do.Type
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

		ap := exporter.AccessProvider{
			ExternalId:        fmt.Sprintf("%s-%s-%s", policy.DatabaseName, policy.SchemaName, policy.Name),
			Name:              fmt.Sprintf("%s-%s-%s", policy.DatabaseName, policy.SchemaName, policy.Name),
			NamingHint:        policy.Name,
			Action:            action,
			NotInternalizable: true,
			Who:               nil,
			Access: []*exporter.Access{
				{
					ActualName: policy.Name,
					What:       make([]exporter.WhatItem, 0),
				},
			},
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
func (s *AccessSyncer) generateAccessControls(ctx context.Context, apMap map[string]*importer.AccessProvider, existingRoles map[string]bool, repo dataAccessRepository, verifyAndPropagate bool) error {
	// Initializes empty map
	metaData := make(map[string]map[string]struct{})

	if verifyAndPropagate {
		syncer := DataSourceSyncer{}

		md, err := syncer.GetDataSourceMetaData(ctx)
		if err != nil {
			return err
		}

		metaData = buildMetaDataMap(md)
	}

	roleCreated := make(map[string]interface{})

	for rn, accessProvider := range apMap {
		ignoreWho := accessProvider.WhoLocked != nil && *accessProvider.WhoLocked
		ignoreWhat := accessProvider.WhatLocked != nil && *accessProvider.WhatLocked

		logger.Info(fmt.Sprintf("Generating access controls for access provider %q (Ignore who: %t; Ignore what: %t)", accessProvider.Name, ignoreWho, ignoreWhat))

		// Merge the users that are specified separately and from the expanded groups.
		// Note: we don't expand groups ourselves here, because Snowflake doesn't have the concept of groups.
		users := slice.StringSliceMerge(accessProvider.Who.Users, accessProvider.Who.UsersInGroups)

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

		// Build the expected expectedGrants
		var expectedGrants []Grant

		if !ignoreWhat {
			for whatIndex, what := range accessProvider.What {
				permissions := getAllSnowflakePermissions(&accessProvider.What[whatIndex])

				if len(permissions) == 0 {
					continue
				}

				if what.DataObject.Type == ds.Table {
					grants, err := createGrantsForTable(permissions, what.DataObject.FullName, metaData)
					if err != nil {
						return err
					}

					expectedGrants = append(expectedGrants, grants...)
				} else if what.DataObject.Type == ds.View {
					grants, err := createGrantsForView(permissions, what.DataObject.FullName, metaData)
					if err != nil {
						return err
					}

					expectedGrants = append(expectedGrants, grants...)
				} else if what.DataObject.Type == ds.Schema {
					grants, err := createGrantsForSchema(repo, permissions, what.DataObject.FullName, metaData)
					if err != nil {
						return err
					}

					expectedGrants = append(expectedGrants, grants...)
				} else if what.DataObject.Type == "shared-database" {
					for _, p := range permissions {
						expectedGrants = append(expectedGrants, Grant{p, fmt.Sprintf("DATABASE %s", what.DataObject.FullName)})
					}
				} else if what.DataObject.Type == ds.Database {
					grants, err := createGrantsForDatabase(repo, permissions, what.DataObject.FullName, metaData)
					if err != nil {
						return err
					}

					expectedGrants = append(expectedGrants, grants...)
				} else if what.DataObject.Type == "warehouse" {
					expectedGrants = append(expectedGrants, createGrantsForWarehouse(permissions, what.DataObject.FullName, metaData)...)
				} else if what.DataObject.Type == ds.Datasource {
					expectedGrants = append(expectedGrants, createGrantsForAccount(permissions, metaData)...)
				}
			}
		}

		var foundGrants []Grant

		// If the role already exists in the system
		if _, f := existingRoles[rn]; f {
			logger.Info(fmt.Sprintf("Merging role %q", rn))

			err := repo.CommentIfExists(createComment(accessProvider, true), "ROLE", rn)
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

				toAdd := slice.StringSliceDifference(users, usersOfRole, false)
				toRemove := slice.StringSliceDifference(usersOfRole, users, false)
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

				foundGrants = make([]Grant, 0, len(grantsToRole))

				for _, grant := range grantsToRole {
					if strings.EqualFold(grant.GrantedOn, "ACCOUNT") {
						foundGrants = append(foundGrants, Grant{grant.Privilege, grant.GrantedOn})
					} else if strings.EqualFold(grant.Privilege, "OWNERSHIP") {
						logger.Warn(fmt.Sprintf("Ignoring permission %q on %q for Role %q as this will remain untouched", grant.Privilege, grant.Name, rn))
					} else if strings.EqualFold(grant.Privilege, "USAGE") && strings.EqualFold(grant.GrantedOn, "ROLE") {
						logger.Debug(fmt.Sprintf("Ignoring USAGE permission on ROLE %q", grant.Name))
					} else {
						foundGrants = append(foundGrants, Grant{grant.Privilege, grant.GrantedOn + " " + grant.Name})
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
				err = repo.CommentIfExists(createComment(accessProvider, false), "ROLE", rn)
				if err != nil {
					return fmt.Errorf("error while updating comment on role %q: %s", rn, err.Error())
				}
				roleCreated[rn] = struct{}{}
			}

			if !ignoreWho {
				err := repo.GrantUsersToRole(ctx, rn, users...)
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
			err := mergeGrants(repo, rn, foundGrants, expectedGrants)
			if err != nil {
				logger.Error("Encountered error :" + err.Error())
				return err
			}
		}
	}

	return nil
}

func createGrantsForTable(permissions []string, fullName string, metaData map[string]map[string]struct{}) ([]Grant, error) {
	return createGrantsForTableOrView(ds.Table, permissions, fullName, metaData)
}

func createGrantsForView(permissions []string, fullName string, metaData map[string]map[string]struct{}) ([]Grant, error) {
	return createGrantsForTableOrView(ds.View, permissions, fullName, metaData)
}

func createGrantsForTableOrView(doType string, permissions []string, fullName string, metaData map[string]map[string]struct{}) ([]Grant, error) {
	// TODO: this does not work for Raito full names
	sfObject := common.ParseFullName(fullName)
	if sfObject.Database == nil || sfObject.Schema == nil || sfObject.Table == nil {
		return nil, fmt.Errorf("expected fullName %q to have 3 parts (database.schema.view)", fullName)
	}

	grants := make([]Grant, 0, len(permissions)+2)
	grants = append(grants,
		Grant{"USAGE", common.FormatQuery(`DATABASE %s`, *sfObject.Database)},
		Grant{"USAGE", common.FormatQuery(`SCHEMA %s.%s`, *sfObject.Database, *sfObject.Schema)})

	for _, p := range permissions {
		if _, f := metaData[doType][strings.ToUpper(p)]; len(metaData) == 0 || f {
			grants = append(grants, Grant{p, common.FormatQuery(`%s %s.%s.%s`, strings.ToUpper(doType), *sfObject.Database, *sfObject.Schema, *sfObject.Table)})
		} else {
			logger.Warn("Permission %q does not apply to type %s", p, strings.ToUpper(doType))
		}
	}

	return grants, nil
}

func createGrantsForSchema(repo dataAccessRepository, permissions []string, fullName string, metaData map[string]map[string]struct{}) ([]Grant, error) {
	// TODO: this does not work for Raito full names
	sfObject := common.ParseFullName(fullName)
	if sfObject.Database == nil || sfObject.Schema == nil || sfObject.Table != nil || sfObject.Column != nil {
		return nil, fmt.Errorf("expected fullName %q to have exactly 2 parts (database.schema)", fullName)
	}

	grants := make([]Grant, 0, len(permissions)+2)
	grants = append(grants,
		Grant{"USAGE", common.FormatQuery(`DATABASE %s`, *sfObject.Database)},
		Grant{"USAGE", common.FormatQuery(`SCHEMA %s.%s`, *sfObject.Database, *sfObject.Schema)})

	var tables []DbEntity
	var views []DbEntity
	var err error

	for _, p := range permissions {
		// Check if the permission is applicable on the schema itself
		if _, f := metaData[ds.Schema][strings.ToUpper(p)]; len(metaData) == 0 || f {
			grants = append(grants, Grant{p, common.FormatQuery(`SCHEMA %s.%s`, *sfObject.Database, *sfObject.Schema)})
		} else {
			matchFound := false

			// Check if the permission is applicable on the tables in the schema
			if _, f := metaData[ds.Table][strings.ToUpper(p)]; f {
				if tables == nil {
					tables, err = repo.GetTablesInSchema(&sfObject)
					if err != nil {
						return nil, err
					}
				}

				matchFound = true

				for _, table := range tables {
					grants = append(grants, Grant{p, common.FormatQuery(`TABLE %s.%s.%s`, *sfObject.Database, *sfObject.Schema, table.Name)})
				}
			}

			// Check if the permission is applicable on the views in the schema
			if _, f := metaData[ds.View][strings.ToUpper(p)]; f {
				if views == nil {
					views, err = repo.GetViewsInSchema(&sfObject)
					if err != nil {
						return nil, err
					}
				}

				matchFound = true

				for _, view := range views {
					grants = append(grants, Grant{p, common.FormatQuery(`VIEW %s.%s.%s`, *sfObject.Database, *sfObject.Schema, view.Name)})
				}
			}

			if !matchFound {
				logger.Warn("Permission %q does not apply to type VIEW or any of its descendants. Skipping", p)
			}
		}
	}

	return grants, nil
}

func createGrantsForDatabase(repo dataAccessRepository, permissions []string, database string, metaData map[string]map[string]struct{}) ([]Grant, error) {
	grants := make([]Grant, 0, len(permissions)+1)

	sfObject := common.SnowflakeObject{Database: &database, Schema: nil, Table: nil, Column: nil}

	grants = append(grants, Grant{"USAGE", fmt.Sprintf(`DATABASE %s`, sfObject.GetFullName(true))})

	var schemas []DbEntity
	tablesPerSchema := make(map[string][]DbEntity)
	viewsPerSchema := make(map[string][]DbEntity)
	var err error

	for _, p := range permissions {
		matchFound := false

		if _, f := metaData[ds.Database][strings.ToUpper(p)]; len(metaData) == 0 || f {
			matchFound = true
			grants = append(grants, Grant{p, fmt.Sprintf(`DATABASE %s`, sfObject.GetFullName(true))})
		} else {
			if schemas == nil {
				schemas, err = repo.GetSchemasInDatabase(database)
				if err != nil {
					return nil, err
				}
			}

			for i, schema := range schemas {
				if schema.Name == "INFORMATION_SCHEMA" {
					continue
				}

				sfObject.Schema = &schemas[i].Name
				grants = append(grants, Grant{"USAGE", fmt.Sprintf("SCHEMA %s", sfObject.GetFullName(true))})

				// Check if the permission is applicable on schemas
				if _, f := metaData[ds.Schema][strings.ToUpper(p)]; f {
					matchFound = true
					grants = append(grants, Grant{p, common.FormatQuery(`SCHEMA %s.%s`, *sfObject.Database, *sfObject.Schema)})
				} else {
					// Check if the permission is applicable on the tables in the schema
					if _, f := metaData[ds.Table][strings.ToUpper(p)]; f {
						tables, f := tablesPerSchema[schema.Name]
						if !f {
							tables, err = repo.GetTablesInSchema(&sfObject)
							if err != nil {
								return nil, err
							}
							tablesPerSchema[schema.Name] = tables
						}
						matchFound = true
						for _, table := range tables {
							grants = append(grants, Grant{p, common.FormatQuery(`TABLE %s.%s.%s`, *sfObject.Database, *sfObject.Schema, table.Name)})
						}
					}

					// Check if the permission is applicable on the views in the schema
					if _, f := metaData[ds.View][strings.ToUpper(p)]; f {
						views, f := viewsPerSchema[schema.Name]
						if !f {
							views, err = repo.GetViewsInSchema(&sfObject)
							if err != nil {
								return nil, err
							}
							viewsPerSchema[schema.Name] = views
						}

						matchFound = true
						for _, view := range views {
							grants = append(grants, Grant{p, common.FormatQuery(`VIEW %s.%s.%s`, *sfObject.Database, *sfObject.Schema, view.Name)})
						}
					}
				}
			}
		}

		if !matchFound {
			logger.Warn("Permission %q does not apply to type DATABASE or any of its descendants. Skipping", p)
		}
	}

	return grants, nil
}

func createGrantsForWarehouse(permissions []string, warehouse string, metaData map[string]map[string]struct{}) []Grant {
	grants := make([]Grant, 0, len(permissions)+2)
	grants = append(grants, Grant{"USAGE", common.FormatQuery(`WAREHOUSE %s`, warehouse)})

	for _, p := range permissions {
		if _, f := metaData["warehouse"][strings.ToUpper(p)]; len(metaData) != 0 && !f {
			logger.Warn("Permission %q does not apply to type WAREHOUSE. Skipping", p)
			continue
		}

		grants = append(grants, Grant{p, common.FormatQuery(`WAREHOUSE %s`, warehouse)})
	}

	return grants
}

func createGrantsForAccount(permissions []string, metaData map[string]map[string]struct{}) []Grant {
	grants := make([]Grant, 0, len(permissions))

	for _, p := range permissions {
		if _, f := metaData[ds.Datasource][strings.ToUpper(p)]; len(metaData) != 0 && !f {
			logger.Warn("Permission %q does not apply to type ACCOUNT (datasource). Skipping", p)
			continue
		}

		grants = append(grants, Grant{p, "ACCOUNT"})
	}

	return grants
}

func mergeGrants(repo dataAccessRepository, role string, found []Grant, expected []Grant) error {
	toAdd := slice.SliceDifference(expected, found)
	toRemove := slice.SliceDifference(found, expected)

	logger.Info(fmt.Sprintf("Found %d grants to add and %d grants to remove for role %q", len(toAdd), len(toRemove), role))

	for _, grant := range toAdd {
		err := repo.ExecuteGrant(grant.Permissions, grant.On, role)
		if err != nil {
			return err
		}
	}

	for _, grant := range toRemove {
		err := repo.ExecuteRevoke(grant.Permissions, grant.On, role)
		if err != nil {
			return err
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

// getAllSnowflakePermissions maps a Raito permission from the data access element to the list of permissions it corresponds to in Snowflake
// The result will be sorted alphabetically
func getAllSnowflakePermissions(what *importer.WhatItem) []string {
	allPerms := make([]string, 0, len(what.Permissions))

	for _, perm := range what.Permissions {
		perm = strings.ToUpper(perm)
		if strings.EqualFold(perm, "USAGE") {
			logger.Debug("Skipping explicit USAGE permission as Raito handles this automatically")
			continue
		} else if strings.EqualFold(perm, "OWNERSHIP") {
			logger.Debug("Skipping explicit OWNERSHIP permission as Raito does not manage this permission")
			continue
		}

		allPerms = append(allPerms, getSnowflakePermissions(perm)...)
	}

	sort.Strings(allPerms)

	return allPerms
}

// mapPermission maps a Raito permission to the list of permissions it corresponds to in Snowflake
func getSnowflakePermissions(permission string) []string {
	pt, f := PermissionMap[permission]
	if f {
		return pt.snowflakePermissions
	}

	logger.Warn(fmt.Sprintf("Unknown raito permission %q found. Mapping as is", permission))

	return []string{permission}
}
