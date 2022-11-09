package snowflake

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

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

var ROLES_NOTINTERNALIZABLE = []string{"ORGADMIN", "ACCOUNTADMIN", "SECURITYADMIN", "USERADMIN", "SYSADMIN", "PUBLIC"}
var ACCEPTED_TYPES = map[string]struct{}{"ACCOUNT": {}, "WAREHOUSE": {}, "DATABASE": {}, "SCHEMA": {}, "TABLE": {}, "COLUMN": {}, "SHARED-DATABASE": {}}

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
	GetSchemaInDatabase(databaseName string) ([]DbEntity, error)
	CommentIfExists(comment, objectType, objectName string) error
	GrantUsersToRole(ctx context.Context, role string, users ...string) error
	RevokeUsersFromRole(ctx context.Context, role string, users ...string) error
	GrantRolesToRole(ctx context.Context, role string, roles ...string) error
	RevokeRolesFromRole(ctx context.Context, role string, roles ...string) error
	CreateRole(roleName string, comment string) error
}

type AccessSyncer struct {
	repoProvider func(params map[string]interface{}, role string) (dataAccessRepository, error)
}

func NewDataAccessSyncer() *AccessSyncer {
	return &AccessSyncer{
		repoProvider: newDataAccessSnowflakeRepo,
	}
}

func newDataAccessSnowflakeRepo(params map[string]interface{}, role string) (dataAccessRepository, error) {
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

	if v, f := configMap.Parameters[SfStandardEdition]; !f || !(v.(bool)) {
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

func (s *AccessSyncer) SyncAccessProvidersToTarget(ctx context.Context, rolesToRemove []string, access map[string]importer.EnrichedAccess, feedbackHandler wrappers.AccessProviderFeedbackHandler, configMap *config.ConfigMap) error {
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

	existingRoles, err := s.findRoles("", access, repo)
	if err != nil {
		return err
	}

	err = s.generateAccessControls(ctx, access, existingRoles, repo)
	if err != nil {
		return err
	}

	feedbackMap := make(map[string][]importer.AccessSyncFeedbackInformation)

	for roleName, access := range access {
		feedbackElement := importer.AccessSyncFeedbackInformation{AccessId: access.Access.Id, ActualName: roleName}
		if feedbackObjects, found := feedbackMap[access.AccessProvider.Id]; found {
			feedbackMap[access.AccessProvider.Id] = append(feedbackObjects, feedbackElement)
		} else {
			feedbackMap[access.AccessProvider.Id] = []importer.AccessSyncFeedbackInformation{feedbackElement}
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

func (s *AccessSyncer) SyncAccessAsCodeToTarget(ctx context.Context, access map[string]importer.EnrichedAccess, prefix string, configMap *config.ConfigMap) error {
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

	err = s.generateAccessControls(ctx, access, existingRoles, repo)
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
	ownersToExclude := ""
	if v, ok := configMap.Parameters[SfExcludedOwners]; ok && v != nil {
		ownersToExclude = v.(string)
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
		err = s.importAccessForRole(roleEntity, ownersToExclude, repo, accessProviderMap, shares, accessProviderHandler)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *AccessSyncer) importAccessForRole(roleEntity RoleEntity, ownersToExclude string, repo dataAccessRepository, accessProviderMap map[string]*exporter.AccessProvider, shares map[string]struct{}, accessProviderHandler wrappers.AccessProviderHandler) error {
	logger.Info("Reading SnowFlake ROLE " + roleEntity.Name)
	// get users granted OF role

	// check if Role Owner is part of the ones that should be notInternalizable
	for _, i := range strings.Split(ownersToExclude, ",") {
		if strings.EqualFold(i, roleEntity.Owner) {
			ROLES_NOTINTERNALIZABLE = append(ROLES_NOTINTERNALIZABLE, roleEntity.Name)
		}
	}

	grantOfEntities, err := repo.GetGrantsOfRole(roleEntity.Name)
	if err != nil {
		return err
	}

	// get objects granted TO role
	grantToEntities, err := repo.GetGrantsToRole(roleEntity.Name)
	if err != nil {
		return err
	}

	users := make([]string, 0)
	accessProviders := make([]string, 0)

	for _, grantee := range grantOfEntities {
		if grantee.GrantedTo == "USER" {
			users = append(users, grantee.GranteeName)
		} else if grantee.GrantedTo == "ROLE" {
			accessProviders = append(accessProviders, grantee.GranteeName)
		}
	}

	da, f := accessProviderMap[roleEntity.Name]
	if !f {
		accessProviderMap[roleEntity.Name] = &exporter.AccessProvider{
			ExternalId: roleEntity.Name,
			Name:       roleEntity.Name,
			NamingHint: roleEntity.Name,
			Action:     exporter.Grant,
			Access: []*exporter.Access{
				{
					ActualName: roleEntity.Name,
					Who: &exporter.WhoItem{
						Users:           users,
						AccessProviders: accessProviders,
						Groups:          []string{},
					},
					What: make([]exporter.WhatItem, 0),
				},
			},
		}
		da = accessProviderMap[roleEntity.Name]
	} else {
		da.Access[0].Who.Users = users
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
				da.Access[0].What = append(da.Access[0].What, exporter.WhatItem{
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
			if _, f := ACCEPTED_TYPES[strings.ToUpper(object.GrantedOn)]; f {
				permissions = append(permissions, object.Privilege)
			}

			database_name := strings.Split(object.Name, ".")[0]
			if _, f := shares[database_name]; f {
				if _, f := sharesApplied[database_name]; strings.EqualFold(object.GrantedOn, "TABLE") && !f {
					da.Access[0].What = append(da.Access[0].What, exporter.WhatItem{
						DataObject:  &ds.DataObjectReference{FullName: database_name, Type: "shared-" + ds.Database},
						Permissions: []string{"IMPORTED PRIVILEGES"},
					})
					sharesApplied[database_name] = struct{}{}
				}

				if !strings.HasPrefix(do.Type, "SHARED") {
					do.Type = "SHARED-" + do.Type
				}
			}
		}

		if k == len(grantToEntities)-1 && len(permissions) > 0 {
			da.Access[0].What = append(da.Access[0].What, exporter.WhatItem{
				DataObject:  do,
				Permissions: permissions,
			})
		}
	}

	if isNotInternizableRole(da.Name) {
		logger.Info(fmt.Sprintf("Marking role %s as read-only (notInternalizable)", da.Name))
		da.NotInternalizable = true
	}

	err = accessProviderHandler.AddAccessProviders(da)
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
			Access: []*exporter.Access{
				{
					ActualName: policy.Name,
					Who:        nil,
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

			ap.Access[0].What = append(ap.Access[0].What, exporter.WhatItem{
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
	for _, r := range ROLES_NOTINTERNALIZABLE {
		if strings.EqualFold(r, role) {
			return true
		}
	}

	return false
}

// findRoles returns a map where the keys are all the roles that exist in Snowflake right now and the key indicates if it was found in apMap or not.
func (s *AccessSyncer) findRoles(prefix string, apMap map[string]importer.EnrichedAccess, repo dataAccessRepository) (map[string]bool, error) {
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

func (s *AccessSyncer) generateAccessControls(ctx context.Context, apMap map[string]importer.EnrichedAccess, existingRoles map[string]bool, repo dataAccessRepository) error {
	roleCreated := make(map[string]interface{})

	for rn, ea := range apMap {
		da := ea.Access

		// Merge the users that are specified separately and from the expanded groups.
		// Note: we don't expand groups ourselves here, because Snowflake doesn't have the concept of groups.
		users := slice.StringSliceMerge(da.Who.Users, da.Who.UsersInGroups)

		// Extract RoleNames from Access Providers that are among the whoList of this one
		roles := make([]string, 0)

		for _, apWho := range da.Who.InheritFrom {
			if strings.HasPrefix(apWho, "ID:") {
				apId := apWho[3:]
				for rn2, ea2 := range apMap {
					if strings.EqualFold(ea2.AccessProvider.Id, apId) {
						roles = append(roles, rn2)
						break
					}
				}
			} else {
				roles = append(roles, apWho)
			}
		}

		// TODO for now we suppose the permissions on the database and schema level are only USAGE.
		//      Later we should support to have specific permissions on these levels as well.

		// Build the expected expectedGrants
		var expectedGrants []Grant

		for whatIndex, what := range da.What {
			permissions := getAllSnowflakePermissions(&da.What[whatIndex])

			if len(permissions) == 0 {
				continue
			}

			if what.DataObject.Type == ds.Table {
				grants, err := createGrantsForTable(permissions, what.DataObject.FullName)
				if err != nil {
					return err
				}

				expectedGrants = append(expectedGrants, grants...)
			} else if what.DataObject.Type == ds.View {
				grants, err := createGrantsForView(permissions, what.DataObject.FullName)
				if err != nil {
					return err
				}

				expectedGrants = append(expectedGrants, grants...)
			} else if what.DataObject.Type == ds.Schema {
				grants, err := createGrantsForSchema(repo, permissions, what.DataObject.FullName)
				if err != nil {
					return err
				}

				expectedGrants = append(expectedGrants, grants...)
			} else if what.DataObject.Type == "shared-database" {
				for _, p := range permissions {
					expectedGrants = append(expectedGrants, Grant{p, fmt.Sprintf("DATABASE %s", what.DataObject.FullName)})
				}
			} else if what.DataObject.Type == ds.Database {
				grants, err := createGrantsForDatabase(repo, permissions, what.DataObject.FullName)
				if err != nil {
					return err
				}

				expectedGrants = append(expectedGrants, grants...)
			} else if what.DataObject.Type == "warehouse" {
				expectedGrants = append(expectedGrants, createGrantsForWarehouse(permissions, what.DataObject.FullName)...)
			} else if what.DataObject.Type == ds.Datasource {
				expectedGrants = append(expectedGrants, createGrantsForAccount(permissions)...)
			}
		}

		var foundGrants []Grant

		if keep, f := existingRoles[rn]; f && keep {
			logger.Info(fmt.Sprintf("Merging role %q", rn))

			err := repo.CommentIfExists(createComment(ea.AccessProvider, true), "ROLE", rn)
			if err != nil {
				return fmt.Errorf("error while updating comment on role %q: %s", rn, err.Error())
			}

			grantsOfRole, err := repo.GetGrantsOfRole(rn)
			if err != nil {
				return err
			}

			usersOfRole := make([]string, 0, len(grantsOfRole))
			rolesOfRole := make([]string, 0, len(grantsOfRole))

			for _, gor := range grantsOfRole {
				// TODO we ignore other roles that have been granted this role. What should we do with it?
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

			toAdd = slice.StringSliceDifference(roles, rolesOfRole, false)
			toRemove = slice.StringSliceDifference(rolesOfRole, roles, false)
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

			// Remove all future grants on schema and database if applicable.
			// Since these are future grants, it's safe to just remove them and re-add them again (if required).
			// We assume nobody manually added others to this role manually.
			for _, what := range da.What {
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
				} else {
					foundGrants = append(foundGrants, Grant{grant.Privilege, grant.GrantedOn + " " + grant.Name})
				}
			}

			logger.Info(fmt.Sprintf("Done updating users granted to role %q", rn))
		} else {
			logger.Info(fmt.Sprintf("Creating role %q", rn))

			if _, f := roleCreated[rn]; !f {
				err := repo.CreateRole(rn, createComment(ea.AccessProvider, false))
				if err != nil {
					return fmt.Errorf("error while creating role %q: %s", rn, err.Error())
				}
				roleCreated[rn] = struct{}{}
			}
			err := repo.GrantUsersToRole(ctx, rn, users...)
			if err != nil {
				logger.Error("Encountered error :" + err.Error())

				return fmt.Errorf("error while assigning users to role %q: %s", rn, err.Error())
			}

			err = repo.GrantRolesToRole(ctx, rn, roles...)
			if err != nil {
				logger.Error("Encountered error :" + err.Error())

				return fmt.Errorf("error while assigning roles to role %q: %s", rn, err.Error())
			}
			// TODO assign role to SYSADMIN if requested (add as input parameter)
		}

		err := mergeGrants(repo, rn, foundGrants, expectedGrants)
		if err != nil {
			logger.Error("Encountered error :" + err.Error())
			return err
		}
	}

	return nil
}

func createGrantsForTable(permissions []string, fullName string) ([]Grant, error) {
	// TODO: this does not work for Raito full names
	sfObject := common.ParseFullName(fullName)
	if sfObject.Database == nil || sfObject.Schema == nil || sfObject.Table == nil {
		return nil, fmt.Errorf("expected fullName %q to have 3 parts (database.schema.table)", fullName)
	}

	grants := make([]Grant, 0, len(permissions)+2)
	grants = append(grants,
		Grant{"USAGE", common.FormatQuery(`DATABASE %s`, *sfObject.Database)},
		Grant{"USAGE", common.FormatQuery(`SCHEMA %s.%s`, *sfObject.Database, *sfObject.Schema)})

	for _, p := range permissions {
		grants = append(grants, Grant{p, common.FormatQuery(`TABLE %s.%s.%s`, *sfObject.Database, *sfObject.Schema, *sfObject.Table)})
	}

	return grants, nil
}

func createGrantsForView(permissions []string, fullName string) ([]Grant, error) {
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
		grants = append(grants, Grant{p, common.FormatQuery(`VIEW %s.%s.%s`, *sfObject.Database, *sfObject.Schema, *sfObject.Table)})
	}

	return grants, nil
}

func createGrantsForSchema(repo dataAccessRepository, permissions []string, fullName string) ([]Grant, error) {
	// TODO: this does not work for Raito full names
	sfObject := common.ParseFullName(fullName)
	if sfObject.Database == nil || sfObject.Schema == nil || sfObject.Table != nil || sfObject.Column != nil {
		return nil, fmt.Errorf("expected fullName %q to have exactly 2 parts (database.schema)", fullName)
	}

	tables, err := repo.GetTablesInSchema(&sfObject)
	if err != nil {
		return nil, err
	}

	grants := make([]Grant, 0, (len(permissions)*len(tables))+2)
	grants = append(grants,
		Grant{"USAGE", common.FormatQuery(`DATABASE %s`, *sfObject.Database)},
		Grant{"USAGE", common.FormatQuery(`SCHEMA %s.%s`, *sfObject.Database, *sfObject.Schema)})

	for _, table := range tables {
		for _, p := range permissions {
			grants = append(grants, Grant{p, common.FormatQuery(`TABLE %s.%s.%s`, *sfObject.Database, *sfObject.Schema, table.Name)})
		}
	}

	return grants, nil
}

func createGrantsForDatabase(repo dataAccessRepository, permissions []string, database string) ([]Grant, error) {
	schemas, err := repo.GetSchemaInDatabase(database)
	if err != nil {
		return nil, err
	}

	grants := make([]Grant, 0, (len(permissions)*len(schemas)*11)+1)

	sfObject := common.SnowflakeObject{Database: &database, Schema: nil, Table: nil, Column: nil}

	grants = append(grants, Grant{"USAGE", fmt.Sprintf(`DATABASE %s`, sfObject.GetFullName(true))})

	for _, p := range permissions {
		grants = append(grants, Grant{p, fmt.Sprintf(`DATABASE %s`, sfObject.GetFullName(true))})
	}

	for i, schema := range schemas {
		if schema.Name == "INFORMATION_SCHEMA" {
			continue
		}

		sfObject.Schema = &schemas[i].Name
		grants = append(grants, Grant{"USAGE", fmt.Sprintf("SCHEMA %s", sfObject.GetFullName(true))})

		tables, err := repo.GetTablesInSchema(&sfObject)
		if err != nil {
			return nil, err
		}

		for j := range tables {
			for _, p := range permissions {
				sfObject.Table = &tables[j].Name
				grants = append(grants, Grant{p, common.FormatQuery(`TABLE %s`, sfObject.GetFullName(true))})
			}
		}
	}

	return grants, nil
}

func createGrantsForWarehouse(permissions []string, warehouse string) []Grant {
	grants := make([]Grant, 0, len(permissions)+2)
	grants = append(grants, Grant{"USAGE", common.FormatQuery(`WAREHOUSE %s`, warehouse)})

	for _, p := range permissions {
		grants = append(grants, Grant{p, common.FormatQuery(`WAREHOUSE %s`, warehouse)})
	}

	return grants
}

func createGrantsForAccount(permissions []string) []Grant {
	grants := make([]Grant, 0, len(permissions))

	for _, p := range permissions {
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
