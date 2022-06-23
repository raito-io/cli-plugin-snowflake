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

const ROLE_SEPARATOR = "_"

type DataAccessSyncer struct {
}

func (s *DataAccessSyncer) SyncDataAccess(config *data_access.DataAccessSyncConfig) data_access.DataAccessSyncResult {
	if config.ConfigMap.GetBool("runImport") {
		fileCreator, err := dap.NewAccessProviderFileCreator(config)
		if err != nil {
			return data_access.DataAccessSyncResult{
				Error: api.ToErrorResult(err),
			}
		}
		defer fileCreator.Close()

		logger.Info("Importing Snowflake Roles into Raito")
		res := s.importDataAccess(config, fileCreator)

		if res.Error != nil {
			return res
		}

		logger.Info("Importing Snowflake Masking Policies into Raito")
		res = s.importMaskingPolicies(config, fileCreator)

		if res.Error != nil {
			return res
		}

		logger.Info("Importing Snowflake Row Access Policies into Raito")
		res = s.importRowAccessPolicies(config, fileCreator)

		if res.Error != nil {
			return res
		}
	}

	logger.Info("Pushing Data Access to Snowflake")

	return s.exportDataAccess(config)
}

func (s *DataAccessSyncer) importDataAccess(config *data_access.DataAccessSyncConfig, fileCreator dap.AccessProviderFileCreator) data_access.DataAccessSyncResult {
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

		rows, err = QuerySnowflake(conn, q)
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

		users := make([]string, 0)

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
				da.AccessObjects = append(da.AccessObjects, dap.Access{
					DataObjectReference: do,
					Permissions:         permissions,
				})
				do = &dsb.DataObjectReference{FullName: object.Name, Type: object.GrantedOn}
				permissions = make([]string, 0)
			}

			permissions = append(permissions, object.Privilege)

			if k == len(grantToEntities)-1 {
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

		err = fileCreator.AddAccessProvider([]dap.AccessProvider{*da})
		if err != nil {
			return data_access.DataAccessSyncResult{
				Error: api.ToErrorResult(fmt.Errorf("error adding access provider to import file: %s", err.Error())),
			}
		}
	}

	return data_access.DataAccessSyncResult{
		Error: nil,
	}
}

func (s *DataAccessSyncer) importPoliciesOfType(config data_access.DataAccessSyncConfig, fileCreator dap.AccessProviderFileCreator, policyType string, action dap.Action) data_access.DataAccessSyncResult {
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
			ExternalId: fmt.Sprintf("%s-%s-%s", policy.DatabaseName, policy.SchemaName, policy.Name),
			Name:       fmt.Sprintf("%s-%s-%s", policy.DatabaseName, policy.SchemaName, policy.Name),
			NamingHint: policy.Name,
			Users:      make([]string, 0),
			Action:     action,
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

		var describeMaskingPolicyEntities []desribePolicyEntity

		err = scan.Rows(&describeMaskingPolicyEntities, rows)
		if err != nil {
			logger.Error(err.Error())

			return data_access.DataAccessSyncResult{
				Error: api.ToErrorResult(fmt.Errorf("error fetching all %s policies: %s", policyType, err.Error())),
			}
		}

		if len(describeMaskingPolicyEntities) != 1 {
			logger.Error(fmt.Sprintf("Found %d definitions for Masking policy %s.%s.%s, only expecting one", len(describeMaskingPolicyEntities), policy.DatabaseName, policy.SchemaName, policy.Name))
		} else {
			ap.Policy = describeMaskingPolicyEntities[0].Body
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
		var policyReferenceEntities []*policyReferenceEntity

		err = scan.Rows(&policyReferenceEntities, rows)
		if err != nil {
			logger.Error(err.Error())

			return data_access.DataAccessSyncResult{
				Error: api.ToErrorResult(fmt.Errorf("error fetching %s policy references: %s", policyType, err.Error())),
			}
		}

		for _, policyReference := range policyReferenceEntities {
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

		err = fileCreator.AddAccessProvider([]dap.AccessProvider{ap})
		if err != nil {
			return data_access.DataAccessSyncResult{
				Error: api.ToErrorResult(fmt.Errorf("error adding access provider to import file: %s", err.Error())),
			}
		}
	}

	return data_access.DataAccessSyncResult{}
}

func (s *DataAccessSyncer) importMaskingPolicies(config *data_access.DataAccessSyncConfig, fileCreator dap.AccessProviderFileCreator) data_access.DataAccessSyncResult {
	return s.importPoliciesOfType(*config, fileCreator, "MASKING POLICY", dap.Mask)
}

func (s *DataAccessSyncer) importRowAccessPolicies(config *data_access.DataAccessSyncConfig, fileCreator dap.AccessProviderFileCreator) data_access.DataAccessSyncResult {
	return s.importPoliciesOfType(*config, fileCreator, "ROW ACCESS POLICY", dap.Filtered)
}

func isNotInternizableRole(role string) bool {
	for _, r := range ROLES_NOTINTERNALIZABLE {
		if strings.EqualFold(r, role) {
			return true
		}
	}

	return false
}

//nolint:gocyclo
func (s *DataAccessSyncer) exportDataAccess(config *data_access.DataAccessSyncConfig) data_access.DataAccessSyncResult {
	prefix := config.Prefix
	if prefix == "" {
		return data_access.DataAccessSyncResult{
			Error: api.CreateMissingInputParameterError("prefix"),
		}
	}

	prefix = strings.ToUpper(strings.TrimSpace(prefix))
	if !strings.HasSuffix(prefix, ROLE_SEPARATOR) {
		prefix += ROLE_SEPARATOR
	}

	logger.Info(fmt.Sprintf("Using prefix %q", prefix))

	dar := config.DataAccess
	if dar == nil {
		logger.Info("No changes in the data access rights recorded since previous sync. Skipping")
		return data_access.DataAccessSyncResult{}
	}

	daList := dar.AccessRights
	daMap := make(map[string]*data_access.DataAccess)

	for _, da := range daList {
		roleName := generateUniqueRoleName(prefix, da)
		logger.Info(fmt.Sprintf("Generated rolename %q", roleName))

		daMap[roleName] = da
	}

	// Removing old roles
	conn, err := ConnectToSnowflake(config.Parameters, "")
	if err != nil {
		return data_access.DataAccessSyncResult{
			Error: api.ToErrorResult(err),
		}
	}
	defer conn.Close()

	q := "SHOW ROLES LIKE '" + prefix + "%'"

	rows, err := QuerySnowflake(conn, q)
	if err != nil {
		return data_access.DataAccessSyncResult{
			Error: api.ToErrorResult(fmt.Errorf("error while cleaning up old roles: %s", err.Error())),
		}
	}

	var roleEntities []roleEntity

	err = scan.Rows(&roleEntities, rows)
	if err != nil {
		return data_access.DataAccessSyncResult{
			Error: api.ToErrorResult(fmt.Errorf("error while cleaning up old roles: %s", err.Error())),
		}
	}

	err = CheckSFLimitExceeded(q, len(roleEntities))
	if err != nil {
		return data_access.DataAccessSyncResult{
			Error: api.ToErrorResult(fmt.Errorf("error while cleaning up old roles: %s", err.Error())),
		}
	}

	rolesToRemove := make([]string, 0, 20)
	rolesToMerge := make(map[string]struct{})

	for _, roleEntity := range roleEntities {
		if _, f := daMap[roleEntity.Name]; !f {
			rolesToRemove = append(rolesToRemove, roleEntity.Name)
		} else {
			rolesToMerge[roleEntity.Name] = struct{}{}
		}
	}

	if len(rolesToRemove) > 0 {
		logger.Info(fmt.Sprintf("Removing old Raito roles in Snowflake: %s", rolesToRemove))

		for _, roleToRemove := range rolesToRemove {
			_, err = QuerySnowflake(conn, "DROP ROLE "+roleToRemove)
			if err != nil {
				return data_access.DataAccessSyncResult{
					Error: api.ToErrorResult(fmt.Errorf("unable to drop role %q: %s", roleToRemove, err.Error())),
				}
			}
		}
	} else {
		logger.Info("No old Raito roles to remove in Snowflake")
	}

	createFutureGrants := config.GetBool(SfCreateFutureGrants)

	for rn, da := range daMap {
		permissions := getAllSnowflakePermissions(da)
		permissionString := strings.ToUpper(strings.Join(permissions, ","))

		// TODO for now we suppose the permissions on the database and schema level are only USAGE.
		//      Later we should support to have specific permissions on these levels as well.

		// Build the expected expectedGrants
		var expectedGrants []interface{}
		if da.DataObject.Type == "table" {
			expectedGrants = append(expectedGrants, createGrantsForTable(permissions, da.DataObject.Parent.Parent.Name, da.DataObject.Parent.Name, da.DataObject.Name)...)
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
		}

		var foundGrants []interface{}

		if _, f := rolesToMerge[rn]; f {
			logger.Info(fmt.Sprintf("Merging role %q from data access %q", rn, da.Id))

			// Merge the users for the role (= add the new and remove the old)
			q := "SHOW GRANTS OF ROLE " + rn

			rows, err = QuerySnowflake(conn, q)
			if err != nil {
				return data_access.DataAccessSyncResult{
					Error: api.ToErrorResult(fmt.Errorf("error while fetching expectedGrants of existing role %q: %s", rn, err.Error())),
				}
			}
			var grantsOfRole []grantOfRole

			err = scan.Rows(&grantsOfRole, rows)
			if err != nil {
				return data_access.DataAccessSyncResult{
					Error: api.ToErrorResult(fmt.Errorf("error while fetching expectedGrants of existing role %q: %s", rn, err.Error())),
				}
			}

			err = CheckSFLimitExceeded(q, len(grantsOfRole))
			if err != nil {
				return data_access.DataAccessSyncResult{
					Error: api.ToErrorResult(fmt.Errorf("error while fetching expectedGrants of existing role %q: %s", rn, err.Error())),
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
				err = grantUsersToRole(conn, rn, toAdd)
				if err != nil {
					return data_access.DataAccessSyncResult{
						Error: api.ToErrorResult(fmt.Errorf("error while assigning users to role %q: %s", rn, err.Error())),
					}
				}
			}

			if len(toRemove) > 0 {
				err = revokeUsersFromRole(conn, rn, toRemove)
				if err != nil {
					return data_access.DataAccessSyncResult{
						Error: api.ToErrorResult(fmt.Errorf("error while unassigning users from role %q: %s", rn, err.Error())),
					}
				}
			}

			// Remove all future grants on schema and database if applicable.
			// Since these are future grants, it's safe to just remove them and re-add them again (if required).
			// We assume nobody manually added others to this role manually.
			if da.DataObject.Type == "database" {
				err = executeRevoke(conn, "ALL", "FUTURE SCHEMAS IN DATABASE "+da.DataObject.Name, rn)
				if err != nil {
					return data_access.DataAccessSyncResult{
						Error: api.ToErrorResult(fmt.Errorf("error while assigning future schema grants in database %q to role %q: %s", da.DataObject.Name, rn, err.Error())),
					}
				}

				err = executeRevoke(conn, "ALL", "FUTURE TABLES IN DATABASE "+da.DataObject.Name, rn)
				if err != nil {
					return data_access.DataAccessSyncResult{
						Error: api.ToErrorResult(fmt.Errorf("error while assigning future table grants in database %q to role %q: %s", da.DataObject.Name, rn, err.Error())),
					}
				}
			} else if da.DataObject.Type == "schema" {
				err = executeRevoke(conn, "ALL", "FUTURE TABLES IN SCHEMA "+da.DataObject.BuildPath("."), rn)
				if err != nil {
					return data_access.DataAccessSyncResult{
						Error: api.ToErrorResult(fmt.Errorf("error while assigning future table grants in schema %q to role %q: %s", da.DataObject.BuildPath("."), rn, err.Error())),
					}
				}
			}

			q = "SHOW GRANTS TO ROLE " + rn

			rows, err = QuerySnowflake(conn, q)
			if err != nil {
				return data_access.DataAccessSyncResult{
					Error: api.ToErrorResult(fmt.Errorf("error while fetching permissions on role %q: %s", rn, err.Error())),
				}
			}
			var grantsToRole []grantToRole

			err = scan.Rows(&grantsToRole, rows)
			if err != nil {
				return data_access.DataAccessSyncResult{
					Error: api.ToErrorResult(fmt.Errorf("error while fetching permissions on role %q: %s", rn, err.Error())),
				}
			}

			err = CheckSFLimitExceeded(q, len(grantsToRole))
			if err != nil {
				return data_access.DataAccessSyncResult{
					Error: api.ToErrorResult(fmt.Errorf("error while fetching permissions on role %q: %s", rn, err.Error())),
				}
			}

			foundGrants = make([]interface{}, 0, len(grantsToRole))
			for _, grant := range grantsToRole {
				foundGrants = append(foundGrants, Grant{grant.Privilege, grant.GrantedOn + " " + grant.Name})
			}

			logger.Info(fmt.Sprintf("Done updating users granted to role %q", rn))
		} else {
			logger.Info(fmt.Sprintf("Creating role %q from data access %q", rn, da.Id))

			_, err = QuerySnowflake(conn, fmt.Sprintf("CREATE ROLE %s COMMENT='%s'", rn, createComment(da)))
			if err != nil {
				return data_access.DataAccessSyncResult{
					Error: api.ToErrorResult(fmt.Errorf("error while creating role %q: %s", rn, err.Error())),
				}
			}

			err = grantUsersToRole(conn, rn, da.Users)
			if err != nil {
				return data_access.DataAccessSyncResult{
					Error: api.ToErrorResult(fmt.Errorf("error while assigning users to role %q: %s", rn, err.Error())),
				}
			}
			// TODO assign role to SYSADMIN if requested (add as input parameter)
		}

		err = mergeGrants(conn, rn, foundGrants, expectedGrants)
		if err != nil {
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

	for _, schema := range schemas {
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
	if da.Rule != nil {
		return fmt.Sprintf("Created by Raito from data policy rule %q", da.Rule.Name)
	}

	return "Created by Raito"
}

func generateUniqueRoleName(prefix string, da *data_access.DataAccess) string {
	perm := generatePermissionsName(da.Permissions)

	return prefix + strings.ToUpper(da.DataObject.BuildPath(ROLE_SEPARATOR)) + ROLE_SEPARATOR + perm
}

// getAllSnowflakePermissions maps a Raito permissions from the data access element to the list of permissions it corresponds to in Snowflake
// The result will be sorted alphabetically
func getAllSnowflakePermissions(da *data_access.DataAccess) []string {
	allPerms := make([]string, 0, len(da.Permissions))
	for _, perm := range da.Permissions {
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
