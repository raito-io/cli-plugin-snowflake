package main

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/blockloop/scan"
	dap "github.com/raito-io/cli/base/access_provider"
	dsb "github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/util/slice"
	"github.com/raito-io/cli/common/api"
	"github.com/raito-io/cli/common/api/data_access"
	sf "github.com/snowflakedb/gosnowflake"
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
var ACCEPTED_TYPES = map[string]struct{}{"ACCOUNT": {}, "WAREHOUSE": {}, "DATASET": {}, "SCHEMA": {}, "TABLE": {}, "COLUMN": {}}

const ROLE_SEPARATOR = "_"

type DataAccessSyncer struct {
	importAccessProviderList []dap.AccessProvider
	revokedRolesList         []string
}

func (s *DataAccessSyncer) SyncDataAccess(config *data_access.DataAccessSyncConfig) data_access.DataAccessSyncResult {
	if config.RunImport {
		logger.Info("Importing Snowflake Roles into Raito")
		res := s.importDataAccess(config)

		if res.Error != nil {
			return res
		}

		logger.Info("Importing Snowflake Masking Policies into Raito")

		res = s.importMaskingPolicies(config)
		if res.Error != nil {
			return res
		}

		logger.Info("Importing Snowflake Row Access Policies into Raito")

		res = s.importRowAccessPolicies(config)
		if res.Error != nil {
			return res
		}
	}

	logger.Info("Pushing Data Access to Snowflake")
	err := s.exportDataAccess(config)

	// write import file and filter roles removed during export
	if config.RunImport {
		exportList := []dap.AccessProvider{}

		for i := range s.importAccessProviderList {
			match := false

			for _, r := range s.revokedRolesList {
				if strings.EqualFold(r, s.importAccessProviderList[i].Name) {
					match = true
					continue
				}
			}

			if !match {
				exportList = append(exportList, s.importAccessProviderList[i])
			} else {
				logger.Info(fmt.Sprintf("Dropping role %s from import as it got removed during export", s.importAccessProviderList[i].Name))
			}
		}

		fileCreator, err := dap.NewAccessProviderFileCreator(config)
		if err != nil {
			return data_access.DataAccessSyncResult{
				Error: api.ToErrorResult(err),
			}
		}
		defer fileCreator.Close()
		err = fileCreator.AddAccessProvider(exportList)

		if err != nil {
			return data_access.DataAccessSyncResult{
				Error: api.ToErrorResult(fmt.Errorf("error adding access provider to import file: %s", err.Error())),
			}
		}
	}

	return err
}

func (s *DataAccessSyncer) importDataAccess(config *data_access.DataAccessSyncConfig) data_access.DataAccessSyncResult {
	ownersToExclude := ""
	if v, ok := config.Parameters[SfExcludedOwners]; ok && v != nil {
		ownersToExclude = v.(string)
	}

	conn, err := ConnectToSnowflake(config.Parameters, "")
	if err != nil {
		return data_access.DataAccessSyncResult{
			Error: api.ToErrorResult(err),
		}
	}
	defer conn.Close()

	q := "SHOW ROLES"

	rows, err := QuerySnowflake(conn, q)
	if err != nil {
		return data_access.DataAccessSyncResult{
			Error: api.ToErrorResult(fmt.Errorf("error fetching all roles: %s", err.Error())),
		}
	}

	var roleEntities []roleEntity

	err = scan.Rows(&roleEntities, rows)
	if err != nil {
		return data_access.DataAccessSyncResult{
			Error: api.ToErrorResult(fmt.Errorf("error fetching all roles: %s", err.Error())),
		}
	}

	accessProviderMap := make(map[string]*dap.AccessProvider)

	for _, roleEntity := range roleEntities {
		logger.Info("Reading SnowFlake ROLE " + roleEntity.Name)
		// get users granted OF role
		q := fmt.Sprintf("SHOW GRANTS OF ROLE %s", roleEntity.Name)
		rows, err := QuerySnowflake(conn, q)

		if err != nil {
			logger.Error(err.Error())

			return data_access.DataAccessSyncResult{
				Error: api.ToErrorResult(fmt.Errorf("error fetching grants of role: %s", err.Error())),
			}
		}

		// check if Role Owner is part of the ones that should be notInternalizable
		for _, i := range strings.Split(ownersToExclude, ",") {
			if strings.EqualFold(i, roleEntity.Owner) {
				ROLES_NOTINTERNALIZABLE = append(ROLES_NOTINTERNALIZABLE, roleEntity.Name)
			}
		}

		grantOfEntities := make([]grantOfRole, 0)

		err = scan.Rows(&grantOfEntities, rows)
		if err != nil {
			logger.Error(err.Error())

			return data_access.DataAccessSyncResult{
				Error: api.ToErrorResult(fmt.Errorf("error fetching grants of role: %s", err.Error())),
			}
		}

		// get objects granted TO role
		q = fmt.Sprintf("SHOW GRANTS TO ROLE %s", roleEntity.Name)

		rows, err = QuerySnowflake(conn, q)
		if err != nil {
			logger.Error(err.Error())

			return data_access.DataAccessSyncResult{
				Error: api.ToErrorResult(fmt.Errorf("error fetching grants TO role: %s", err.Error())),
			}
		}

		grantToEntities := make([]grantToRole, 0)

		err = scan.Rows(&grantToEntities, rows)
		if err != nil {
			logger.Error(err.Error())

			return data_access.DataAccessSyncResult{
				Error: api.ToErrorResult(fmt.Errorf("error fetching grants TO role: %s", err.Error())),
			}
		}

		var users []string = nil

		for _, grantee := range grantOfEntities {
			if grantee.GrantedTo == "USER" {
				users = append(users, grantee.GranteeName)
			}
		}

		da, f := accessProviderMap[roleEntity.Name]
		if !f {
			accessProviderMap[roleEntity.Name] = &dap.AccessProvider{
				ExternalId: roleEntity.Name,
				Name:       roleEntity.Name,
				NamingHint: roleEntity.Name,
				Users:      users,
				Action:     dap.Grant,
			}
			da = accessProviderMap[roleEntity.Name]
		} else {
			da.Users = users
		}

		var do *dsb.DataObjectReference
		permissions := make([]string, 0)

		for k, object := range grantToEntities {
			if k == 0 {
				do = &dsb.DataObjectReference{FullName: object.Name, Type: object.GrantedOn}
			} else if do.FullName != object.Name {
				if len(permissions) > 0 {
					da.AccessObjects = append(da.AccessObjects, dap.Access{
						DataObjectReference: do,
						Permissions:         permissions,
					})
				}
				do = &dsb.DataObjectReference{FullName: object.Name, Type: object.GrantedOn}
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
			}

			if k == len(grantToEntities)-1 && len(permissions) > 0 {
				da.AccessObjects = append(da.AccessObjects, dap.Access{
					DataObjectReference: do,
					Permissions:         permissions,
				})
			}
		}

		// copy AccessObjects from this role to all roles that have a GRANT on this one
		for _, grantee := range grantOfEntities {
			if grantee.GrantedTo == "ROLE" {
				if _, f := accessProviderMap[grantee.GranteeName]; !f {
					accessProviderMap[grantee.GranteeName] = &dap.AccessProvider{
						ExternalId: grantee.GranteeName,
						Name:       grantee.GranteeName,
					}
				}

				granteeDa := accessProviderMap[grantee.GranteeName]
				granteeDa.AccessObjects = append(granteeDa.AccessObjects, da.AccessObjects...)
				logger.Info(fmt.Sprintf("Adding AccessObjects for role %s to grantee %s", roleEntity.Name, granteeDa.Name))
			}
		}
	}

	for _, da := range accessProviderMap {
		if isNotInternizableRole(da.Name) {
			logger.Info(fmt.Sprintf("Marking role %s as read-only (notInternalizable)", da.Name))
			da.NotInternalizable = true
		}

		s.importAccessProviderList = append(s.importAccessProviderList, *da)
	}

	return data_access.DataAccessSyncResult{
		Error: nil,
	}
}

func (s *DataAccessSyncer) importPoliciesOfType(config *data_access.DataAccessSyncConfig, policyType string, action dap.Action) data_access.DataAccessSyncResult {
	conn, err := ConnectToSnowflake(config.Parameters, "")
	if err != nil {
		return data_access.DataAccessSyncResult{
			Error: api.ToErrorResult(err),
		}
	}
	defer conn.Close()

	policyTypePlural := strings.Replace(policyType, "POLICY", "POLICIES", 1)
	q := fmt.Sprintf("SHOW %s", policyTypePlural)

	rows, err := QuerySnowflake(conn, q)
	if err != nil {
		return data_access.DataAccessSyncResult{
			Error: api.ToErrorResult(fmt.Errorf("error fetching all %s policies: %s", policyType, err.Error())),
		}
	}

	var policyEntities []policyEntity

	err = scan.Rows(&policyEntities, rows)
	if err != nil {
		return data_access.DataAccessSyncResult{
			Error: api.ToErrorResult(fmt.Errorf("error fetching all %s policies: %s", policyType, err.Error())),
		}
	}

	for _, policy := range policyEntities {
		if !strings.EqualFold(strings.Replace(policyType, " ", "_", -1), policy.Kind) {
			continue
		}

		logger.Info(fmt.Sprintf("Reading SnowFlake %s %s in Schema %s, Table %s", policyType, policy.Name, policy.SchemaName, policy.DatabaseName))

		ap := dap.AccessProvider{
			ExternalId:        fmt.Sprintf("%s-%s-%s", policy.DatabaseName, policy.SchemaName, policy.Name),
			Name:              fmt.Sprintf("%s-%s-%s", policy.DatabaseName, policy.SchemaName, policy.Name),
			NamingHint:        policy.Name,
			Users:             nil,
			Action:            action,
			NotInternalizable: true,
		}

		// get policy definition
		q := fmt.Sprintf("DESCRIBE %s %s.%s.%s", policyType, policy.DatabaseName, policy.SchemaName, policy.Name)

		rows, err := QuerySnowflake(conn, q)
		if err != nil {
			logger.Error(err.Error())

			return data_access.DataAccessSyncResult{
				Error: api.ToErrorResult(fmt.Errorf("error fetching all %s policies: %s", policyType, err.Error())),
			}
		}

		var desribeMaskingPolicyEntities []desribePolicyEntity

		err = scan.Rows(&desribeMaskingPolicyEntities, rows)
		if err != nil {
			logger.Error(err.Error())

			return data_access.DataAccessSyncResult{
				Error: api.ToErrorResult(fmt.Errorf("error fetching all %s policies: %s", policyType, err.Error())),
			}
		}

		if len(desribeMaskingPolicyEntities) != 1 {
			logger.Error(fmt.Sprintf("Found %d definitions for Masking policy %s.%s.%s, only expecting one", len(desribeMaskingPolicyEntities), policy.DatabaseName, policy.SchemaName, policy.Name))
		} else {
			ap.Policy = desribeMaskingPolicyEntities[0].Body
		}

		// get policy references
		q = fmt.Sprintf(`select * from table(information_schema.policy_references(policy_name => '%s.%s.%s'))`, policy.DatabaseName, policy.SchemaName, policy.Name)

		rows, err = QuerySnowflake(conn, q)
		if err != nil {
			logger.Error(err.Error())

			return data_access.DataAccessSyncResult{
				Error: api.ToErrorResult(fmt.Errorf("error fetching all %s policies: %s", policyType, err.Error())),
			}
		}

		var policyReferenceEntities []policyReferenceEntity

		err = scan.Rows(&policyReferenceEntities, rows)
		if err != nil {
			logger.Error(err.Error())

			return data_access.DataAccessSyncResult{
				Error: api.ToErrorResult(fmt.Errorf("error fetching %s policy references: %s", policyType, err.Error())),
			}
		}

		for ind := range policyReferenceEntities {
			policyReference := policyReferenceEntities[ind]
			if !strings.EqualFold("Active", policyReference.POLICY_STATUS) {
				continue
			}

			if policyReference.REF_COLUMN_NAME.Valid {
				dor := dsb.DataObjectReference{
					Type:     "COLUMN",
					FullName: fmt.Sprintf("%s.%s.%s.%s", policyReference.REF_DATABASE_NAME, policyReference.REF_SCHEMA_NAME, policyReference.REF_ENTITY_NAME, policyReference.REF_COLUMN_NAME.String),
				}

				ap.AccessObjects = append(ap.AccessObjects, dap.Access{
					DataObjectReference: &dor,
					Permissions:         []string{},
				})
			} else {
				dor := dsb.DataObjectReference{
					Type:     "TABLE",
					FullName: fmt.Sprintf("%s.%s.%s", policyReference.REF_DATABASE_NAME, policyReference.REF_SCHEMA_NAME, policyReference.REF_ENTITY_NAME),
				}

				ap.AccessObjects = append(ap.AccessObjects, dap.Access{
					DataObjectReference: &dor,
					Permissions:         []string{},
				})
			}
		}

		s.importAccessProviderList = append(s.importAccessProviderList, ap)
	}

	return data_access.DataAccessSyncResult{}
}

func (s *DataAccessSyncer) importMaskingPolicies(config *data_access.DataAccessSyncConfig) data_access.DataAccessSyncResult {
	return s.importPoliciesOfType(config, "MASKING POLICY", dap.Mask)
}

func (s *DataAccessSyncer) importRowAccessPolicies(config *data_access.DataAccessSyncConfig) data_access.DataAccessSyncResult {
	return s.importPoliciesOfType(config, "ROW ACCESS POLICY", dap.Filtered)
}

func isNotInternizableRole(role string) bool {
	for _, r := range ROLES_NOTINTERNALIZABLE {
		if strings.EqualFold(r, role) {
			return true
		}
	}

	return false
}

func find(s []string, q string) bool {
	for _, r := range s {
		if strings.EqualFold(r, q) {
			return true
		}
	}

	return false
}

//nolint:gocyclo
func (s *DataAccessSyncer) exportDataAccess(config *data_access.DataAccessSyncConfig) data_access.DataAccessSyncResult {
	dar := config.DataAccess
	if dar == nil {
		logger.Info("No changes in the data access rights recorded since previous sync. Skipping")
		return data_access.DataAccessSyncResult{}
	}

	daList := dar.AccessRights
	daMap := make(map[string]*data_access.DataAccess)

	// Removing old roles
	conn, err := ConnectToSnowflake(config.Parameters, "")
	if err != nil {
		return data_access.DataAccessSyncResult{
			Error: api.ToErrorResult(err),
		}
	}
	defer conn.Close()

	rolesToRemove := make([]string, 0, 20)
	rolesToMerge := make(map[string]struct{})

	// When exporting Access from Raito Cloud, prefix will be empty as the delete instructions are passed explicitly during export. For access-as-code the prefix should not be empty as it is used to detect Raito CLI managed roles
	prefix := config.Prefix
	if prefix != "" {
		prefix = strings.ToUpper(strings.TrimSpace(prefix))
		if !strings.HasSuffix(prefix, ROLE_SEPARATOR) {
			prefix += ROLE_SEPARATOR
		}

		logger.Info(fmt.Sprintf("Using prefix %q", prefix))

		for _, da := range daList {
			logger.Info(fmt.Sprintf("%+v", da))
			roleName := generateUniqueRoleName(prefix, da)
			logger.Info(fmt.Sprintf("Generated rolename %q", roleName))

			daMap[roleName] = da
		}

		q := "SHOW ROLES LIKE '" + prefix + "%'"

		rows, e := QuerySnowflake(conn, q)
		if e != nil {
			return data_access.DataAccessSyncResult{
				Error: api.ToErrorResult(fmt.Errorf("error while cleaning up old roles: %s", e.Error())),
			}
		}
		var roleEntities []roleEntity

		e = scan.Rows(&roleEntities, rows)
		if e != nil {
			return data_access.DataAccessSyncResult{
				Error: api.ToErrorResult(fmt.Errorf("error while cleaning up old roles: %s", e.Error())),
			}
		}

		e = CheckSFLimitExceeded(q, len(roleEntities))
		if e != nil {
			return data_access.DataAccessSyncResult{
				Error: api.ToErrorResult(fmt.Errorf("error while cleaning up old roles: %s", e.Error())),
			}
		}

		for _, roleEntity := range roleEntities {
			if _, f := daMap[roleEntity.Name]; !f {
				if !find(rolesToRemove, roleEntity.Name) {
					rolesToRemove = append(rolesToRemove, roleEntity.Name)
				}
			} else {
				rolesToMerge[roleEntity.Name] = struct{}{}
			}
		}
	} else {
		for _, da := range daList {
			if da.Delete {
				roleName := generateUniqueRoleName(prefix, da)
				if !find(rolesToRemove, roleName) {
					rolesToRemove = append(rolesToRemove, roleName)
				}
			} else {
				key := da.NamingHint
				if key == "" {
					key = da.Provider.Name
				}
				key += da.DataObject.Name
				if _, f := daMap[key]; !f {
					daMap[key] = da
				}
			}
		}
	}

	if len(rolesToRemove) > 0 {
		logger.Info(fmt.Sprintf("Removing old Raito roles in Snowflake: %s", rolesToRemove))

		for _, roleToRemove := range rolesToRemove {
			_, err = QuerySnowflake(conn, "DROP ROLE "+roleToRemove)
			if err != nil && !strings.Contains(err.Error(), "does not exist") {
				return data_access.DataAccessSyncResult{
					Error: api.ToErrorResult(fmt.Errorf("unable to drop role %q: %s", roleToRemove, err.Error())),
				}
			}

			s.revokedRolesList = append(s.revokedRolesList, roleToRemove)
		}
	} else {
		logger.Info("No old Raito roles to remove in Snowflake")
	}

	createFutureGrants := config.GetBool(SfCreateFutureGrants)
	roleCreated := make(map[string]interface{})

	for _, da := range daMap {
		if da.Delete {
			continue
		}

		rn := generateUniqueRoleName(prefix, da)
		permissions := getAllSnowflakePermissions(da)
		permissionString := strings.ToUpper(strings.Join(permissions, ","))

		if len(permissions) == 0 {
			continue
		}

		// TODO for now we suppose the permissions on the database and schema level are only USAGE.
		//      Later we should support to have specific permissions on these levels as well.

		// Build the expected expectedGrants
		var expectedGrants []interface{}
		if da.DataObject.Type == "table" {
			expectedGrants = append(expectedGrants, createGrantsForTable(permissions, da.DataObject.Parent.Parent.Name, da.DataObject.Parent.Name, da.DataObject.Name)...)
		} else if da.DataObject.Type == "view" {
			expectedGrants = append(expectedGrants, createGrantsForView(permissions, da.DataObject.Parent.Parent.Name, da.DataObject.Parent.Name, da.DataObject.Name)...)
		} else if da.DataObject.Type == "schema" {
			expectedGrants = append(expectedGrants, createGrantsForSchema(conn, permissions, da.DataObject.Parent.Name, da.DataObject.Name)...)

			if createFutureGrants {
				expectedGrants = append(expectedGrants, Grant{permissionString, "FUTURE TABLES IN SCHEMA " + da.DataObject.BuildPath(".")})
			}
		} else if da.DataObject.Type == "database" {
			expectedGrants = append(expectedGrants, createGrantsForDatabase(conn, permissions, da.DataObject.Name)...)

			if createFutureGrants {
				expectedGrants = append(expectedGrants,
					Grant{"USAGE", "FUTURE SCHEMAS IN DATABASE " + da.DataObject.Name},
					Grant{permissionString, "FUTURE TABLES IN DATABASE " + da.DataObject.Name})
			}
		} else if da.DataObject.Type == "warehouse" {
			expectedGrants = append(expectedGrants, createGrantsForWarehouse(permissions, da.DataObject.Name)...)
		} else if da.DataObject.Type == "datasource" {
			expectedGrants = append(expectedGrants, createGrantsForAccount(permissions)...)
		}

		var foundGrants []interface{}

		if _, f := rolesToMerge[rn]; f {
			logger.Info(fmt.Sprintf("Merging role %q from data access %q", rn, da.Id))

			// Merge the users for the role (= add the new and remove the old)
			q := "SHOW GRANTS OF ROLE " + rn

			rows, e := QuerySnowflake(conn, q)
			if e != nil {
				return data_access.DataAccessSyncResult{
					Error: api.ToErrorResult(fmt.Errorf("error while fetching expectedGrants of existing role %q: %s", rn, e.Error())),
				}
			}
			var grantsOfRole []grantOfRole
			e = scan.Rows(&grantsOfRole, rows)

			if e != nil {
				return data_access.DataAccessSyncResult{
					Error: api.ToErrorResult(fmt.Errorf("error while fetching expectedGrants of existing role %q: %s", rn, e.Error())),
				}
			}

			e = CheckSFLimitExceeded(q, len(grantsOfRole))
			if e != nil {
				return data_access.DataAccessSyncResult{
					Error: api.ToErrorResult(fmt.Errorf("error while fetching expectedGrants of existing role %q: %s", rn, e.Error())),
				}
			}

			usersOfRole := make([]string, 0, len(grantsOfRole))

			for _, gor := range grantsOfRole {
				// TODO we ignore other roles that have been granted this role. What should we do with it?
				if strings.EqualFold(gor.GrantedTo, "USER") {
					usersOfRole = append(usersOfRole, gor.GranteeName)
				}
			}

			toAdd := slice.StringSliceDifference(da.Users, usersOfRole, false)
			toRemove := slice.StringSliceDifference(usersOfRole, da.Users, false)
			logger.Info(fmt.Sprintf("Identified %d users to add and %d users to remove from role %q", len(toAdd), len(toRemove), rn))

			if len(toAdd) > 0 {
				e = grantUsersToRole(conn, rn, toAdd)
				if e != nil {
					return data_access.DataAccessSyncResult{
						Error: api.ToErrorResult(fmt.Errorf("error while assigning users to role %q: %s", rn, e.Error())),
					}
				}
			}

			if len(toRemove) > 0 {
				e = revokeUsersFromRole(conn, rn, toRemove)
				if e != nil {
					return data_access.DataAccessSyncResult{
						Error: api.ToErrorResult(fmt.Errorf("error while unassigning users from role %q: %s", rn, e.Error())),
					}
				}
			}

			// Remove all future grants on schema and database if applicable.
			// Since these are future grants, it's safe to just remove them and re-add them again (if required).
			// We assume nobody manually added others to this role manually.
			if da.DataObject.Type == "database" {
				e = executeRevoke(conn, "ALL", "FUTURE SCHEMAS IN DATABASE "+da.DataObject.Name, rn)
				if e != nil {
					return data_access.DataAccessSyncResult{
						Error: api.ToErrorResult(fmt.Errorf("error while assigning future schema grants in database %q to role %q: %s", da.DataObject.Name, rn, e.Error())),
					}
				}

				e = executeRevoke(conn, "ALL", "FUTURE TABLES IN DATABASE "+da.DataObject.Name, rn)
				if e != nil {
					return data_access.DataAccessSyncResult{
						Error: api.ToErrorResult(fmt.Errorf("error while assigning future table grants in database %q to role %q: %s", da.DataObject.Name, rn, e.Error())),
					}
				}
			} else if da.DataObject.Type == "schema" {
				e = executeRevoke(conn, "ALL", "FUTURE TABLES IN SCHEMA "+da.DataObject.BuildPath("."), rn)
				if e != nil {
					return data_access.DataAccessSyncResult{
						Error: api.ToErrorResult(fmt.Errorf("error while assigning future table grants in schema %q to role %q: %s", da.DataObject.BuildPath("."), rn, e.Error())),
					}
				}
			}

			q = "SHOW GRANTS TO ROLE " + rn

			rows, e = QuerySnowflake(conn, q)
			if e != nil {
				return data_access.DataAccessSyncResult{
					Error: api.ToErrorResult(fmt.Errorf("error while fetching permissions on role %q: %s", rn, e.Error())),
				}
			}
			var grantsToRole []grantToRole

			e = scan.Rows(&grantsToRole, rows)
			if e != nil {
				return data_access.DataAccessSyncResult{
					Error: api.ToErrorResult(fmt.Errorf("error while fetching permissions on role %q: %s", rn, e.Error())),
				}
			}

			e = CheckSFLimitExceeded(q, len(grantsToRole))
			if e != nil {
				return data_access.DataAccessSyncResult{
					Error: api.ToErrorResult(fmt.Errorf("error while fetching permissions on role %q: %s", rn, e.Error())),
				}
			}

			foundGrants = make([]interface{}, 0, len(grantsToRole))
			for _, grant := range grantsToRole {
				foundGrants = append(foundGrants, Grant{grant.Privilege, grant.GrantedOn + " " + grant.Name})
			}

			logger.Info(fmt.Sprintf("Done updating users granted to role %q", rn))
		} else {
			logger.Info(fmt.Sprintf("Creating role %q from data access %q", rn, da.Id))

			if _, f := roleCreated[rn]; !f {
				_, err = QuerySnowflake(conn, fmt.Sprintf("CREATE OR REPLACE ROLE %s COMMENT='%s'", rn, createComment(da)))
				if err != nil {
					return data_access.DataAccessSyncResult{
						Error: api.ToErrorResult(fmt.Errorf("error while creating role %q: %s", rn, err.Error())),
					}
				}
				roleCreated[rn] = struct{}{}
			}
			err = grantUsersToRole(conn, rn, da.Users)
			if err != nil {
				logger.Error("Encountered error :" + err.Error())

				return data_access.DataAccessSyncResult{
					Error: api.ToErrorResult(fmt.Errorf("error while assigning users to role %q: %s", rn, err.Error())),
				}
			}
			// TODO assign role to SYSADMIN if requested (add as input parameter)
		}

		err = mergeGrants(conn, rn, foundGrants, expectedGrants)
		if err != nil {
			logger.Error("Encountered error :" + err.Error())

			return data_access.DataAccessSyncResult{
				Error: api.ToErrorResult(err),
			}
		}
	}

	return data_access.DataAccessSyncResult{}
}

func createGrantsForTable(permissions []string, database string, schema string, table string) []interface{} {
	grants := make([]interface{}, 0, len(permissions)+2)
	grants = append(grants,
		Grant{"USAGE", "DATABASE " + database},
		Grant{"USAGE", fmt.Sprintf("SCHEMA %s.%s", database, schema)})

	for _, p := range permissions {
		grants = append(grants, Grant{p, fmt.Sprintf("TABLE %s.%s.%s", database, schema, table)})
	}

	return grants
}

func createGrantsForView(permissions []string, database string, schema string, view string) []interface{} {
	grants := make([]interface{}, 0, len(permissions)+2)
	grants = append(grants,
		Grant{"USAGE", "DATABASE " + database},
		Grant{"USAGE", fmt.Sprintf("SCHEMA %s.%s", database, schema)})

	for _, p := range permissions {
		grants = append(grants, Grant{p, fmt.Sprintf("VIEW %s.%s.%s", database, schema, view)})
	}

	return grants
}

func createGrantsForSchema(conn *sql.DB, permissions []string, database string, schema string) []interface{} {
	q := fmt.Sprintf("SHOW TABLES IN SCHEMA %s.%s", database, schema)
	tables, _ := readDbEntities(conn, q)
	grants := make([]interface{}, 0, (len(permissions)*len(tables))+2)
	grants = append(grants,
		Grant{"USAGE", "DATABASE " + database},
		Grant{"USAGE", fmt.Sprintf("SCHEMA %s.%s", database, schema)})

	for _, table := range tables {
		for _, p := range permissions {
			grants = append(grants, Grant{p, fmt.Sprintf("TABLE %s.%s.%s", database, schema, table.Name)})
		}
	}

	return grants
}

func createGrantsForDatabase(conn *sql.DB, permissions []string, database string) []interface{} {
	schemas, _ := readDbEntities(conn, fmt.Sprintf("SHOW SCHEMAS IN DATABASE %s", database))
	grants := make([]interface{}, 0, (len(permissions)*len(schemas)*11)+1)

	grants = append(grants, Grant{"USAGE", "DATABASE " + database})

	for _, p := range permissions {
		grants = append(grants, Grant{p, fmt.Sprintf("DATABASE %s", database)})
	}

	for _, schema := range schemas {
		if schema.Name == "INFORMATION_SCHEMA" {
			continue
		}

		grants = append(grants, Grant{"USAGE", fmt.Sprintf("SCHEMA %s.%s", database, schema.Name)})

		tables, _ := readDbEntities(conn, fmt.Sprintf("SHOW TABLES IN SCHEMA %s.%s", database, schema.Name))
		for _, table := range tables {
			for _, p := range permissions {
				grants = append(grants, Grant{p, fmt.Sprintf("TABLE %s.%s.%s", database, schema.Name, table.Name)})
			}
		}
	}

	return grants
}

func createGrantsForWarehouse(permissions []string, warehouse string) []interface{} {
	grants := make([]interface{}, 0, len(permissions)+2)
	grants = append(grants, Grant{"USAGE", "WAREHOUSE " + warehouse})

	for _, p := range permissions {
		grants = append(grants, Grant{p, fmt.Sprintf("WAREHOUSE %s", warehouse)})
	}

	return grants
}

func createGrantsForAccount(permissions []string) []interface{} {
	grants := make([]interface{}, 0, len(permissions))

	for _, p := range permissions {
		grants = append(grants, Grant{p, "ACCOUNT"})
		logger.Error(fmt.Sprintf("%+v", Grant{p, "ACCOUNT"}))
	}

	return grants
}

func mergeGrants(conn *sql.DB, role string, found []interface{}, expected []interface{}) error {
	toAdd := slice.SliceDifference(expected, found)
	toRemove := slice.SliceDifference(found, expected)

	logger.Info(fmt.Sprintf("Found %d grants to add and %d grants to remove for role %q", len(toAdd), len(toRemove), role))

	for _, g := range toAdd {
		grant := g.(Grant)

		err := executeGrant(conn, grant.Permissions, grant.On, role)
		if err != nil {
			return err
		}
	}

	for _, g := range toRemove {
		grant := g.(Grant)

		err := executeRevoke(conn, grant.Permissions, grant.On, role)
		if err != nil {
			return err
		}
	}

	return nil
}

func revokeUsersFromRole(conn *sql.DB, role string, users []string) error {
	statements := make([]string, 0, 200)
	userCount := len(users)

	for i, user := range users {
		q := fmt.Sprintf("REVOKE ROLE %s FROM USER %q", role, strings.ToUpper(user))
		statements = append(statements, q)

		if len(statements) == 200 || i == userCount-1 {
			logger.Info(fmt.Sprintf("Executing statements to revoke role %q from %d users", role, len(statements)))

			err := executeStatements(conn, statements)
			if err != nil {
				return fmt.Errorf("error while revoking users from role %q: %s", role, err.Error())
			}

			logger.Info(fmt.Sprintf("Done revoking role from %d users", len(statements)))
			statements = make([]string, 0, 200)
		}
	}

	return nil
}

func grantUsersToRole(conn *sql.DB, role string, users []string) error {
	statements := make([]string, 0, 200)
	userCount := len(users)

	for i, user := range users {
		q := fmt.Sprintf("GRANT ROLE %s TO USER %q", role, strings.ToUpper(user))
		statements = append(statements, q)

		if len(statements) == 200 || i == userCount-1 {
			logger.Info(fmt.Sprintf("Executing statements to grant role %q to %d users", role, len(statements)))

			err := executeStatements(conn, statements)
			if err != nil {
				return fmt.Errorf("error while granting users to role %q: %s", role, err.Error())
			}

			logger.Info(fmt.Sprintf("Done granting role to %d users", len(statements)))
			statements = make([]string, 0, 200)
		}
	}

	return nil
}

func executeStatements(conn *sql.DB, statements []string) error {
	blank := context.Background()
	multiContext, _ := sf.WithMultiStatement(blank, len(statements))

	_, err := conn.ExecContext(multiContext, strings.Join(statements, "; "))
	if err != nil {
		return err
	}

	return nil
}

func executeGrant(conn *sql.DB, perm, on, role string) error {
	q := fmt.Sprintf("GRANT %s ON %s TO ROLE %s", perm, on, role)
	logger.Debug("Executing grant query", "query", q)

	_, err := QuerySnowflake(conn, q)
	if err != nil {
		return fmt.Errorf("error while executing grant query on Snowflake for role %q: %s", role, err.Error())
	}

	return nil
}

func executeRevoke(conn *sql.DB, perm, on, role string) error {
	q := fmt.Sprintf("REVOKE %s ON %s FROM ROLE %s", perm, on, role)
	logger.Debug("Executing revoke query: %s", q)

	_, err := QuerySnowflake(conn, q)
	if err != nil {
		return fmt.Errorf("error while executing revoke query on Snowflake for role %q: %s", role, err.Error())
	}

	return nil
}

func createComment(da *data_access.DataAccess) string {
	if da.Provider != nil {
		return fmt.Sprintf("Created by Raito from access provider %q", da.Provider.Name)
	}

	return "Created by Raito"
}

func generateUniqueRoleName(prefix string, da *data_access.DataAccess) string {
	if da.NamingHint != "" {
		return prefix + da.NamingHint
	} else if da.Provider.Name != "" {
		return prefix + da.Provider.Name
	}

	perm := generatePermissionsName(da.Permissions)

	return prefix + strings.ToUpper(da.DataObject.BuildPath(ROLE_SEPARATOR)) + ROLE_SEPARATOR + perm
}

// getAllSnowflakePermissions maps a Raito permissions from the data access element to the list of permissions it corresponds to in Snowflake
// The result will be sorted alphabetically
func getAllSnowflakePermissions(da *data_access.DataAccess) []string {
	allPerms := make([]string, 0, len(da.Permissions))

	for _, perm := range da.Permissions {
		if perm == "USAGE" {
			logger.Debug("Skipping explicit USAGE permission as Raito handles this automatically")
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

// generatePermissionsName generates a user-friendly name for the set of permissions for a data access.
// This is done by renaming sets to a fixed name where possible.
func generatePermissionsName(permissions []string) string {
	parts := make([]string, 0, len(permissions))

	for _, p := range permissions {
		pt, f := PermissionMap[p]
		if f {
			parts = append(parts, strings.ToUpper(pt.roleName))
		} else {
			// TODO In theory this can still cause conflicts if there are multiple permissions with the same starting letter
			parts = append(parts, strings.ToUpper(p[0:1]))
		}
	}

	sort.Strings(parts)

	return strings.Join(parts, "")
}

type roleEntity struct {
	Name            string `db:"name"`
	AssignedToUsers int    `db:"assigned_to_users"`
	GrantedToRoles  int    `db:"granted_to_roles"`
	GrantedRoles    int    `db:"granted_roles"`
	Owner           string `db:"owner"`
}

type grantOfRole struct {
	GrantedTo   string `db:"granted_to"`
	GranteeName string `db:"grantee_name"`
}

type grantToRole struct {
	Privilege string `db:"privilege"`
	GrantedOn string `db:"granted_on"`
	Name      string `db:"name"`
}

type Grant struct {
	Permissions string
	On          string
}

type policyEntity struct {
	Name         string `db:"name"`
	DatabaseName string `db:"database_name"`
	SchemaName   string `db:"schema_name"`
	Kind         string `db:"kind"`
	Owner        string `db:"owner"`
}

type desribePolicyEntity struct {
	Name string `db:"name"`
	Body string `db:"body"`
}

type policyReferenceEntity struct {
	POLICY_DB            string         `db:"POLICY_DB"`
	POLICY_SCHEMA        string         `db:"POLICY_SCHEMA"`
	POLICY_NAME          string         `db:"POLICY_NAME"`
	POLICY_KIND          string         `db:"POLICY_KIND"`
	REF_DATABASE_NAME    string         `db:"REF_DATABASE_NAME"`
	REF_SCHEMA_NAME      string         `db:"REF_SCHEMA_NAME"`
	REF_ENTITY_NAME      string         `db:"REF_ENTITY_NAME"`
	REF_ENTITY_DOMAIN    string         `db:"REF_ENTITY_DOMAIN"`
	REF_COLUMN_NAME      sql.NullString `db:"REF_COLUMN_NAME"`
	REF_ARG_COLUMN_NAMES sql.NullString `db:"REF_ARG_COLUMN_NAMES"`
	TAG_DATABASE         sql.NullString `db:"TAG_DATABASE"`
	TAG_SCHEMA           sql.NullString `db:"TAG_SCHEMA"`
	TAG_NAME             sql.NullString `db:"TAG_NAME"`
	POLICY_STATUS        string         `db:"POLICY_STATUS"`
}
