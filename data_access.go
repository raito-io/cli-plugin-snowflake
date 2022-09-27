package main

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/blockloop/scan"
	"github.com/raito-io/cli/base/access_provider"
	exporter "github.com/raito-io/cli/base/access_provider/sync_from_target"
	importer "github.com/raito-io/cli/base/access_provider/sync_to_target"
	ds "github.com/raito-io/cli/base/data_source"
	e "github.com/raito-io/cli/base/util/error"
	"github.com/raito-io/cli/base/util/slice"
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
var ACCEPTED_TYPES = map[string]struct{}{"ACCOUNT": {}, "WAREHOUSE": {}, "DATABASE": {}, "SCHEMA": {}, "TABLE": {}, "COLUMN": {}, "SHARED-DATABASE": {}}

const ROLE_SEPARATOR = "_"

type AccessSyncer struct {
}

func (s *AccessSyncer) SyncFromTarget(config *access_provider.AccessSyncFromTarget) access_provider.AccessSyncResult {
	logger.Info("Reading roles from Snowflake")

	fileCreator, err := exporter.NewAccessProviderFileCreator(config)
	if err != nil {
		return access_provider.AccessSyncResult{
			Error: e.ToErrorResult(err),
		}
	}
	defer fileCreator.Close()

	err = s.importAccess(config, fileCreator)

	if err != nil {
		return access_provider.AccessSyncResult{
			Error: e.ToErrorResult(err),
		}
	}

	logger.Info("Reading masking policies from")

	err = s.importMaskingPolicies(config, fileCreator)
	if err != nil {
		return access_provider.AccessSyncResult{
			Error: e.ToErrorResult(err),
		}
	}

	logger.Info("Reading row access policies from Snowflake")

	err = s.importRowAccessPolicies(config, fileCreator)
	if err != nil {
		return access_provider.AccessSyncResult{
			Error: e.ToErrorResult(err),
		}
	}

	return access_provider.AccessSyncResult{
		Error: nil,
	}
}

func (s *AccessSyncer) SyncToTarget(config *access_provider.AccessSyncToTarget) access_provider.AccessSyncResult {
	logger.Info("Configuring access providers as roles in Snowflake")

	err := s.exportAccess(config)
	if err != nil {
		return access_provider.AccessSyncResult{
			Error: e.ToErrorResult(err),
		}
	}

	return access_provider.AccessSyncResult{
		Error: nil,
	}
}

func getShareNames(conn *sql.DB) (map[string]struct{}, error) {
	_, err := readDbEntities(conn, "SHOW SHARES")
	if err != nil {
		return nil, err
	}

	entities, err := readDbEntities(conn, "select \"database_name\" as \"name\" from table(result_scan(LAST_QUERY_ID())) WHERE \"kind\" = 'INBOUND'")

	if err != nil {
		return nil, err
	}

	shares := make(map[string]struct{}, len(entities))
	for _, e := range entities {
		shares[e.Name] = struct{}{}
	}

	return shares, nil
}

func (s *AccessSyncer) importAccess(config *access_provider.AccessSyncFromTarget, fileCreator exporter.AccessProviderFileCreator) error {
	ownersToExclude := ""
	if v, ok := config.Parameters[SfExcludedOwners]; ok && v != nil {
		ownersToExclude = v.(string)
	}

	conn, err := ConnectToSnowflake(config.Parameters, "")
	if err != nil {
		return err
	}
	defer conn.Close()

	shares, err := getShareNames(conn)
	if err != nil {
		return err
	}

	q := "SHOW ROLES"

	rows, err := QuerySnowflake(conn, q)
	if err != nil {
		return fmt.Errorf("error fetching all roles: %s", err.Error())
	}

	var roleEntities []roleEntity

	err = scan.Rows(&roleEntities, rows)
	if err != nil {
		return fmt.Errorf("error fetching all roles: %s", err.Error())
	}

	accessProviderMap := make(map[string]*exporter.AccessProvider)

	for _, roleEntity := range roleEntities {
		logger.Info("Reading SnowFlake ROLE " + roleEntity.Name)
		// get users granted OF role
		q := FormatQuery(`SHOW GRANTS OF ROLE %s`, roleEntity.Name)
		rows, err := QuerySnowflake(conn, q)

		if err != nil {
			logger.Error(err.Error())

			return fmt.Errorf("error fetching grants of role: %s", err.Error())
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

			return fmt.Errorf("error fetching grants of role: %s", err.Error())
		}

		// get objects granted TO role
		q = FormatQuery(`SHOW GRANTS TO ROLE %s`, roleEntity.Name)

		rows, err = QuerySnowflake(conn, q)
		if err != nil {
			logger.Error(err.Error())

			return fmt.Errorf("error fetching grants TO role: %s", err.Error())
		}

		grantToEntities := make([]grantToRole, 0)

		err = scan.Rows(&grantToEntities, rows)
		if err != nil {
			logger.Error(err.Error())

			return fmt.Errorf("error fetching grants TO role: %s", err.Error())
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
						NamingHint: roleEntity.Name,
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
				// TODO: make parser still more robust
				sfObject := ParseFullName(object.Name)
				if !strings.EqualFold(sfObject.getFullName(false), strings.ReplaceAll(object.Name, `"`, ``)) {
					logger.Info(object.Name)
					fmt.Println(fmt.Sprintf("Difference between %s and %s", sfObject.getFullName(false), strings.ReplaceAll(object.Name, `"`, ``)))
					logger.Info(fmt.Sprintf("Difference between %s and %s", sfObject.getFullName(false), strings.ReplaceAll(object.Name, `"`, ``)))
				}
				do = &ds.DataObjectReference{FullName: sfObject.getFullName(false), Type: object.GrantedOn}
				logger.Info(object.Name)
			} else if do.FullName != object.Name {
				if len(permissions) > 0 {
					da.Access[0].What = append(da.Access[0].What, exporter.WhatItem{
						DataObject:  do,
						Permissions: permissions,
					})
				}
				// TODO: make parser still more robust
				sfObject := ParseFullName(object.Name)
				if !strings.EqualFold(sfObject.getFullName(false), strings.ReplaceAll(object.Name, `"`, ``)) {
					logger.Info(object.Name)
					fmt.Println(fmt.Sprintf("Difference between %s and %s", sfObject.getFullName(false), strings.ReplaceAll(object.Name, `"`, ``)))
					logger.Info(fmt.Sprintf("Difference between %s and %s", sfObject.getFullName(false), strings.ReplaceAll(object.Name, `"`, ``)))
				}
				do = &ds.DataObjectReference{FullName: sfObject.getFullName(false), Type: object.GrantedOn}
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
	}

	for _, da := range accessProviderMap {
		if isNotInternizableRole(da.Name) {
			logger.Info(fmt.Sprintf("Marking role %s as read-only (notInternalizable)", da.Name))
			da.NotInternalizable = true
		}

		err := fileCreator.AddAccessProviders([]exporter.AccessProvider{*da})
		if err != nil {
			return fmt.Errorf("error adding access provider to import file: %s", err.Error())
		}
	}

	return nil
}

func (s *AccessSyncer) importPoliciesOfType(config *access_provider.AccessSyncFromTarget, fileCreator exporter.AccessProviderFileCreator, policyType string, action exporter.Action) error {
	conn, err := ConnectToSnowflake(config.Parameters, "")
	if err != nil {
		return err
	}
	defer conn.Close()

	policyTypePlural := strings.Replace(policyType, "POLICY", "POLICIES", 1)
	q := fmt.Sprintf(`SHOW %s`, policyTypePlural)

	rows, err := QuerySnowflake(conn, q)
	if err != nil {
		return fmt.Errorf("error fetching all %s policies: %s", policyType, err.Error())
	}

	var policyEntities []policyEntity

	err = scan.Rows(&policyEntities, rows)
	if err != nil {
		return fmt.Errorf("error fetching all %s policies: %s", policyType, err.Error())
	}

	for _, policy := range policyEntities {
		if !strings.EqualFold(strings.Replace(policyType, " ", "_", -1), policy.Kind) {
			continue
		}

		logger.Info(fmt.Sprintf("Reading SnowFlake %s %s in Schema %s, Table %s", policyType, policy.Name, policy.SchemaName, policy.DatabaseName))

		ap := exporter.AccessProvider{
			ExternalId:        fmt.Sprintf("%s-%s-%s", policy.DatabaseName, policy.SchemaName, policy.Name),
			Name:              fmt.Sprintf("%s-%s-%s", policy.DatabaseName, policy.SchemaName, policy.Name),
			NamingHint:        policy.Name,
			Action:            action,
			NotInternalizable: true,
			Access: []*exporter.Access{
				{
					NamingHint: policy.Name,
					Who:        nil,
					What:       make([]exporter.WhatItem, 0),
				},
			},
		}

		// get policy definition
		q := FormatQuery("DESCRIBE "+policyType+" %s.%s.%s", policy.DatabaseName, policy.SchemaName, policy.Name)

		rows, err := QuerySnowflake(conn, q)
		if err != nil {
			logger.Error(err.Error())

			return fmt.Errorf("error fetching all %s policies: %s", policyType, err.Error())
		}

		var desribeMaskingPolicyEntities []desribePolicyEntity

		err = scan.Rows(&desribeMaskingPolicyEntities, rows)
		if err != nil {
			logger.Error(err.Error())

			return fmt.Errorf("error fetching all %s policies: %s", policyType, err.Error())
		}

		if len(desribeMaskingPolicyEntities) != 1 {
			logger.Error(fmt.Sprintf("Found %d definitions for Masking policy %s.%s.%s, only expecting one", len(desribeMaskingPolicyEntities), policy.DatabaseName, policy.SchemaName, policy.Name))
		} else {
			ap.Policy = desribeMaskingPolicyEntities[0].Body
		}

		// get policy references
		q = fmt.Sprintf(`select * from table(%s.information_schema.policy_references(policy_name => '%s'))`, policy.DatabaseName, FormatQuery(`%s.%s.%s`, policy.DatabaseName, policy.SchemaName, policy.Name))

		rows, err = QuerySnowflake(conn, q)
		if err != nil {
			logger.Error(err.Error())

			return fmt.Errorf("error fetching all %s policies: %s", policyType, err.Error())
		}

		var policyReferenceEntities []policyReferenceEntity

		err = scan.Rows(&policyReferenceEntities, rows)
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
					FullName: FormatQuery(`%s.%s.%s.%s`, policyReference.REF_DATABASE_NAME, policyReference.REF_SCHEMA_NAME, policyReference.REF_ENTITY_NAME, policyReference.REF_COLUMN_NAME.String),
				}
			} else {
				dor = ds.DataObjectReference{
					Type:     "TABLE",
					FullName: FormatQuery(`%s.%s.%s`, policyReference.REF_DATABASE_NAME, policyReference.REF_SCHEMA_NAME, policyReference.REF_ENTITY_NAME),
				}
			}

			ap.Access[0].What = append(ap.Access[0].What, exporter.WhatItem{
				DataObject:  &dor,
				Permissions: []string{},
			})
		}

		err = fileCreator.AddAccessProviders([]exporter.AccessProvider{ap})
		if err != nil {
			return fmt.Errorf("error adding access provider to import file: %s", err.Error())
		}
	}

	return nil
}

func (s *AccessSyncer) importMaskingPolicies(config *access_provider.AccessSyncFromTarget, fileCreator exporter.AccessProviderFileCreator) error {
	return s.importPoliciesOfType(config, fileCreator, "MASKING POLICY", exporter.Mask)
}

func (s *AccessSyncer) importRowAccessPolicies(config *access_provider.AccessSyncFromTarget, fileCreator exporter.AccessProviderFileCreator) error {
	return s.importPoliciesOfType(config, fileCreator, "ROW ACCESS POLICY", exporter.Filtered)
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

func (s *AccessSyncer) exportAccess(config *access_provider.AccessSyncToTarget) error {
	dar, err := importer.ParseAccessProviderImportFile(config)
	if err != nil {
		return fmt.Errorf("error parsing acccess providers from %q: %s", config.SourceFile, err.Error())
	}

	apList := dar.AccessProviders
	apMap := make(map[string]EnrichedAccess)

	conn, err := ConnectToSnowflake(config.Parameters, "")
	if err != nil {
		return err
	}
	defer conn.Close()

	rolesToRemove := make([]string, 0)

	// When exporting Access from Raito Cloud, prefix will be empty as the delete instructions are passed explicitly during export. For access-as-code the prefix should not be empty as it is used to detect Raito CLI managed roles
	prefix := config.Prefix
	if prefix != "" {
		prefix = strings.ToUpper(strings.TrimSpace(prefix))
		if !strings.HasSuffix(prefix, ROLE_SEPARATOR) {
			prefix += ROLE_SEPARATOR
		}

		logger.Info(fmt.Sprintf("Using prefix %q", prefix))

		for apIndex, ap := range apList {
			for accessIndex, access := range ap.Access {
				roleName, err2 := generateUniqueRoleName(prefix, &apList[apIndex], accessIndex)
				if err2 != nil {
					return err2
				}

				logger.Info(fmt.Sprintf("Generated rolename %q", roleName))
				apMap[roleName] = EnrichedAccess{Access: access, AccessProvider: &apList[apIndex]}
			}
		}
	} else {
		for apIndex, ap := range apList {
			if ap.Delete {
				for accessIndex := range ap.Access {
					roleName, err2 := generateUniqueRoleName(prefix, &apList[apIndex], accessIndex)
					if err2 != nil {
						return err2
					}
					if !find(rolesToRemove, roleName) {
						rolesToRemove = append(rolesToRemove, roleName)
					}
				}
			} else {
				for accessIndex, access := range ap.Access {
					roleName, err2 := generateUniqueRoleName(prefix, &apList[apIndex], accessIndex)
					if err2 != nil {
						return err2
					}
					if _, f := apMap[roleName]; !f {
						apMap[roleName] = EnrichedAccess{Access: access, AccessProvider: &apList[apIndex]}
					}
				}
			}
		}
	}

	existingRoles, err := s.findRoles(prefix, apMap, conn)
	if err != nil {
		return err
	}

	// If there is a prefix (= scope) set, we remove the roles that are not defined anymore.
	// In case of no prefix, we only work with explicit deletes. This case is already covered above.
	if prefix != "" {
		for role, toKeep := range existingRoles {
			if !toKeep {
				rolesToRemove = append(rolesToRemove, role)
			}
		}
	}

	if len(rolesToRemove) > 0 {
		logger.Info(fmt.Sprintf("Removing old Raito roles in Snowflake: %s", rolesToRemove))

		for _, roleToRemove := range rolesToRemove {
			_, err = QuerySnowflake(conn, FormatQuery(`DROP ROLE %s`, roleToRemove))
			if err != nil && !strings.Contains(err.Error(), "does not exist") {
				return fmt.Errorf("unable to drop role %q: %s", roleToRemove, err.Error())
			}
		}
	} else {
		logger.Info("No old Raito roles to remove in Snowflake")
	}

	err = s.generateAccessControls(apMap, existingRoles, conn)
	if err != nil {
		return err
	}

	fileCreator, err := importer.NewFeedbackFileCreator(config)
	if err != nil {
		return err
	}
	defer fileCreator.Close()

	feedbackMap := make(map[string][]importer.AccessSyncFeedbackInformation)

	for roleName, access := range apMap {
		feedbackElement := importer.AccessSyncFeedbackInformation{AccessId: access.Access.Id, ActualName: roleName}
		if feedbackObjects, found := feedbackMap[access.AccessProvider.Id]; found {
			feedbackMap[access.AccessProvider.Id] = append(feedbackObjects, feedbackElement)
		} else {
			feedbackMap[access.AccessProvider.Id] = []importer.AccessSyncFeedbackInformation{feedbackElement}
		}
	}

	for apId, feedbackObjects := range feedbackMap {
		err = fileCreator.AddAccessProviderFeedback(apId, feedbackObjects...)
		if err != nil {
			return err
		}
	}

	return err
}

// findRoles returns a map where the keys are all the roles that exist in Snowflake right now and the key indicates if it was found in apMap or not.
func (s *AccessSyncer) findRoles(prefix string, apMap map[string]EnrichedAccess, conn *sql.DB) (map[string]bool, error) {
	foundRoles := make(map[string]bool)

	q := "SHOW ROLES"
	if prefix != "" {
		q += " LIKE '" + prefix + "%'"
	}

	rows, e := QuerySnowflake(conn, q)
	if e != nil {
		return nil, fmt.Errorf("error while finding existing roles: %s", e.Error())
	}
	var roleEntities []roleEntity

	e = scan.Rows(&roleEntities, rows)
	if e != nil {
		return nil, fmt.Errorf("error while finding existing roles: %s", e.Error())
	}

	e = CheckSFLimitExceeded(q, len(roleEntities))
	if e != nil {
		return nil, fmt.Errorf("error while finding existing roles: %s", e.Error())
	}

	for _, roleEntity := range roleEntities {
		_, f := apMap[roleEntity.Name]
		foundRoles[roleEntity.Name] = f
	}

	return foundRoles, nil
}

type EnrichedAccess struct {
	Access         *importer.Access
	AccessProvider *importer.AccessProvider
}

func (s *AccessSyncer) generateAccessControls(apMap map[string]EnrichedAccess, existingRoles map[string]bool, conn *sql.DB) error {
	roleCreated := make(map[string]interface{})

	for rn, ea := range apMap {
		da := ea.Access

		// Merge the users that are specified separately and from the expanded groups.
		// Note: we don't expand groups ourselves here, because Snowflake doesn't have the concept of groups.
		users := slice.StringSliceMerge(da.Who.Users, da.Who.UsersInGroups)

		// Extract RoleNames from Access Providers that are among the whoList of this one
		roles := make([]string, 0)

		for _, apWho := range da.Who.InheritFrom {
			for rn2, ea2 := range apMap {
				if strings.EqualFold(ea2.AccessProvider.Id, apWho) {
					roles = append(roles, rn2)
					break
				}
			}
		}

		// TODO for now we suppose the permissions on the database and schema level are only USAGE.
		//      Later we should support to have specific permissions on these levels as well.

		// Build the expected expectedGrants
		var expectedGrants []interface{}

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
				grants, err := createGrantsForSchema(conn, permissions, what.DataObject.FullName)
				if err != nil {
					return err
				}

				expectedGrants = append(expectedGrants, grants...)
			} else if what.DataObject.Type == "shared-database" {
				for _, p := range permissions {
					expectedGrants = append(expectedGrants, Grant{p, fmt.Sprintf("DATABASE %s", what.DataObject.FullName)})
				}
			} else if what.DataObject.Type == ds.Database {
				expectedGrants = append(expectedGrants, createGrantsForDatabase(conn, permissions, what.DataObject.FullName)...)
			} else if what.DataObject.Type == "warehouse" {
				expectedGrants = append(expectedGrants, createGrantsForWarehouse(permissions, what.DataObject.FullName)...)
			} else if what.DataObject.Type == ds.Datasource {
				expectedGrants = append(expectedGrants, createGrantsForAccount(permissions)...)
			}
		}

		var foundGrants []interface{}

		if keep, f := existingRoles[rn]; f && keep {
			logger.Info(fmt.Sprintf("Merging role %q", rn))

			grantsOfRole, err := s.getGrantsOfRole(rn, conn)
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
				e := grantUsersToRole(conn, rn, toAdd)
				if e != nil {
					return fmt.Errorf("error while assigning users to role %q: %s", rn, e.Error())
				}
			}

			if len(toRemove) > 0 {
				e := revokeUsersFromRole(conn, rn, toRemove)
				if e != nil {
					return fmt.Errorf("error while unassigning users from role %q: %s", rn, e.Error())
				}
			}

			toAdd = slice.StringSliceDifference(roles, rolesOfRole, false)
			toRemove = slice.StringSliceDifference(rolesOfRole, roles, false)
			logger.Info(fmt.Sprintf("Identified %d roles to add and %d roles to remove from role %q", len(toAdd), len(toRemove), rn))

			if len(toAdd) > 0 {
				e := grantRolesToRole(conn, rn, toAdd)
				if e != nil {
					return fmt.Errorf("error while assigning role to role %q: %s", rn, e.Error())
				}
			}

			if len(toRemove) > 0 {
				e := revokeRolesFromRole(conn, rn, toRemove)
				if e != nil {
					return fmt.Errorf("error while unassigning role from role %q: %s", rn, e.Error())
				}
			}

			// Remove all future grants on schema and database if applicable.
			// Since these are future grants, it's safe to just remove them and re-add them again (if required).
			// We assume nobody manually added others to this role manually.
			for _, what := range da.What {
				if what.DataObject.Type == "database" {
					// TODO
					e := executeRevoke(conn, "ALL", FormatQuery(`FUTURE SCHEMAS IN DATABASE %s`, what.DataObject.FullName), rn)
					if e != nil {
						return fmt.Errorf("error while assigning future schema grants in database %q to role %q: %s", what.DataObject.FullName, rn, e.Error())
					}

					// TODO
					e = executeRevoke(conn, "ALL", FormatQuery(`FUTURE TABLES IN DATABASE %s`, what.DataObject.FullName), rn)
					if e != nil {
						return fmt.Errorf("error while assigning future table grants in database %q to role %q: %s", what.DataObject.FullName, rn, e.Error())
					}
				} else if what.DataObject.Type == "schema" {
					// TODO
					e := executeRevoke(conn, "ALL", fmt.Sprintf("FUTURE TABLES IN SCHEMA %s", what.DataObject.FullName), rn)
					if e != nil {
						return fmt.Errorf("error while assigning future table grants in schema %q to role %q: %s", what.DataObject.FullName, rn, e.Error())
					}
				}
			}

			grantsToRole, err := s.getGrantsToRole(rn, conn)
			if err != nil {
				return err
			}

			foundGrants = make([]interface{}, 0, len(grantsToRole))

			for _, grant := range grantsToRole {
				if strings.EqualFold(grant.GrantedOn, "ACCOUNT") {
					foundGrants = append(foundGrants, Grant{grant.Privilege, grant.GrantedOn})
				} else {
					foundGrants = append(foundGrants, Grant{grant.Privilege, grant.GrantedOn + " " + grant.Name})
				}
			}

			logger.Info(fmt.Sprintf("Done updating users granted to role %q", rn))
		} else {
			logger.Info(fmt.Sprintf("Creating role %q", rn))

			if _, f := roleCreated[rn]; !f {
				_, err := QuerySnowflake(conn, FormatQuery(`CREATE OR REPLACE ROLE %s COMMENT=%s`, rn, createComment(ea.AccessProvider)))
				if err != nil {
					return fmt.Errorf("error while creating role %q: %s", rn, err.Error())
				}
				roleCreated[rn] = struct{}{}
			}
			err := grantUsersToRole(conn, rn, users)
			if err != nil {
				logger.Error("Encountered error :" + err.Error())

				return fmt.Errorf("error while assigning users to role %q: %s", rn, err.Error())
			}

			err = grantRolesToRole(conn, rn, roles)
			if err != nil {
				logger.Error("Encountered error :" + err.Error())

				return fmt.Errorf("error while assigning roles to role %q: %s", rn, err.Error())
			}
			// TODO assign role to SYSADMIN if requested (add as input parameter)
		}

		err := mergeGrants(conn, rn, foundGrants, expectedGrants)
		if err != nil {
			logger.Error("Encountered error :" + err.Error())
			return err
		}
	}

	return nil
}

func (s *AccessSyncer) getGrantsToRole(rn string, conn *sql.DB) ([]grantToRole, error) {
	shares, e := getShareNames(conn)
	if e != nil {
		return nil, fmt.Errorf("error while fetching permissions on role %q: %s", rn, e.Error())
	}

	q := FormatQuery(`SHOW GRANTS TO ROLE %s`, rn)

	rows, e := QuerySnowflake(conn, q)
	if e != nil {
		return nil, fmt.Errorf("error while fetching permissions on role %q: %s", rn, e.Error())
	}
	var grantsToRole []grantToRole
	var res []grantToRole

	e = scan.Rows(&res, rows)
	if e != nil {
		return nil, fmt.Errorf("error while fetching permissions on role %q: %s", rn, e.Error())
	}

	e = CheckSFLimitExceeded(q, len(res))
	if e != nil {
		return nil, fmt.Errorf("error while fetching permissions on role %q: %s", rn, e.Error())
	}

	sharedDbsHandled := make(map[string]struct{})

	for _, r := range res {
		if strings.EqualFold(r.GrantedOn, "ROLE") { // ROLE USAGE permissions are handled separately
			continue
		}
		db := strings.Split(r.Name, ".")[0]

		if _, f := shares[db]; !f {
			grantsToRole = append(grantsToRole, r)
		} else if _, f := sharedDbsHandled[db]; !f {
			grantsToRole = append(grantsToRole, grantToRole{Privilege: "IMPORTED PRIVILEGES", GrantedOn: "DATABASE", Name: db})
			sharedDbsHandled[db] = struct{}{}
		}
	}

	return grantsToRole, nil
}

func (s *AccessSyncer) getGrantsOfRole(rn string, conn *sql.DB) ([]grantOfRole, error) {
	// Merge the users for the role (= add the new and remove the old)
	q := FormatQuery(`SHOW GRANTS OF ROLE %s`, rn)

	rows, e := QuerySnowflake(conn, q)
	if e != nil {
		return nil, fmt.Errorf("error while fetching expectedGrants of existing role %q: %s", rn, e.Error())
	}
	var grantsOfRole []grantOfRole
	e = scan.Rows(&grantsOfRole, rows)

	if e != nil {
		return nil, fmt.Errorf("error while fetching expectedGrants of existing role %q: %s", rn, e.Error())
	}

	e = CheckSFLimitExceeded(q, len(grantsOfRole))
	if e != nil {
		return nil, fmt.Errorf("error while fetching expectedGrants of existing role %q: %s", rn, e.Error())
	}

	return grantsOfRole, nil
}

func createGrantsForTable(permissions []string, fullName string) ([]interface{}, error) {
	// TODO. What if there's a dot in one of the data objects' names?
	parts := strings.Split(fullName, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("expected fullName %q to have 3 parts (database.schema.table)", fullName)
	}

	grants := make([]interface{}, 0, len(permissions)+2)
	grants = append(grants,
		Grant{"USAGE", FormatQuery(`DATABASE %s`, parts[0])},
		Grant{"USAGE", FormatQuery(`SCHEMA %s.%s`, parts[0], parts[1])})

	for _, p := range permissions {
		grants = append(grants, Grant{p, FormatQuery(`TABLE %s.%s.%s`, parts[0], parts[1], parts[2])})
	}

	return grants, nil
}

func createGrantsForView(permissions []string, fullName string) ([]interface{}, error) {
	// TODO. What if there's a dot in one of the data objects' names?
	parts := strings.Split(fullName, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("expected fullName %q to have 3 parts (database.schema.view)", fullName)
	}

	grants := make([]interface{}, 0, len(permissions)+2)
	grants = append(grants,
		Grant{"USAGE", FormatQuery(`DATABASE %s`, parts[0])},
		Grant{"USAGE", FormatQuery(`SCHEMA %s.%s`, parts[0], parts[1])})

	for _, p := range permissions {
		grants = append(grants, Grant{p, FormatQuery(`VIEW %s.%s.%s`, parts[0], parts[1], parts[2])})
	}

	return grants, nil
}

func createGrantsForSchema(conn *sql.DB, permissions []string, fullName string) ([]interface{}, error) {
	parts := strings.Split(fullName, ".")
	if len(parts) != 2 {
		return nil, fmt.Errorf("expected fullName %q to have 2 parts (database.schema)", fullName)
	}

	q := FormatQuery(`SHOW TABLES IN SCHEMA %s.%s`, parts[0], parts[1])
	tables, _ := readDbEntities(conn, q)
	grants := make([]interface{}, 0, (len(permissions)*len(tables))+2)
	grants = append(grants,
		Grant{"USAGE", FormatQuery(`DATABASE %s`, parts[0])},
		Grant{"USAGE", FormatQuery(`SCHEMA %s.%s`, parts[0], parts[1])})

	for _, table := range tables {
		for _, p := range permissions {
			grants = append(grants, Grant{p, FormatQuery(`TABLE %s.%s.%s`, parts[0], parts[1], table.Name)})
		}
	}

	return grants, nil
}

func createGrantsForDatabase(conn *sql.DB, permissions []string, database string) []interface{} {
	schemas, _ := readDbEntities(conn, getSchemasInDatabaseQuery(database))
	grants := make([]interface{}, 0, (len(permissions)*len(schemas)*11)+1)

	sfObject := SnowflakeObject{&database, nil, nil, nil}

	grants = append(grants, Grant{"USAGE", fmt.Sprintf(`DATABASE %s`, sfObject.getFullName(true))})

	for _, p := range permissions {
		grants = append(grants, Grant{p, fmt.Sprintf(`DATABASE %s`, sfObject.getFullName(true))})
	}

	for _, schema := range schemas {
		if schema.Name == "INFORMATION_SCHEMA" {
			continue
		}

		sfObject.Schema = &schema.Name
		grants = append(grants, Grant{"USAGE", fmt.Sprintf("SCHEMA %s", sfObject.getFullName(true))})

		tables, _ := readDbEntities(conn, getTablesInSchemaQuery(sfObject, "TABLES"))
		for _, table := range tables {
			for _, p := range permissions {
				sfObject.Table = &table.Name
				grants = append(grants, Grant{p, FormatQuery(`TABLE %s`, sfObject.getFullName(true))})
			}
		}
	}

	return grants
}

func createGrantsForWarehouse(permissions []string, warehouse string) []interface{} {
	grants := make([]interface{}, 0, len(permissions)+2)
	grants = append(grants, Grant{"USAGE", FormatQuery(`WAREHOUSE %s`, warehouse)})

	for _, p := range permissions {
		grants = append(grants, Grant{p, FormatQuery(`WAREHOUSE %s`, warehouse)})
	}

	return grants
}

func createGrantsForAccount(permissions []string) []interface{} {
	grants := make([]interface{}, 0, len(permissions))

	for _, p := range permissions {
		grants = append(grants, Grant{p, "ACCOUNT"})
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
		q := FormatQuery(`REVOKE ROLE %s FROM USER %s`, role, user)
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

func revokeRolesFromRole(conn *sql.DB, role string, roles []string) error {
	statements := make([]string, 0, 200)
	roleCount := len(roles)

	for i, otherRole := range roles {
		q := FormatQuery(`REVOKE ROLE %s FROM ROLE %s`, role, otherRole)
		statements = append(statements, q)

		if len(statements) == 200 || i == roleCount-1 {
			logger.Info(fmt.Sprintf("Executing statements to revoke role %q from %d roles", role, len(statements)))

			err := executeStatements(conn, statements)
			if err != nil {
				return fmt.Errorf("error while revoking roles from role %q: %s", role, err.Error())
			}

			logger.Info(fmt.Sprintf("Done revoking role from %d roles", len(statements)))
			statements = make([]string, 0, 200)
		}
	}

	return nil
}

func grantUsersToRole(conn *sql.DB, role string, users []string) error {
	statements := make([]string, 0, 200)
	userCount := len(users)

	for i, user := range users {
		q := FormatQuery(`GRANT ROLE %s TO USER %s`, role, user)
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

func grantRolesToRole(conn *sql.DB, role string, roles []string) error {
	statements := make([]string, 0, 200)
	roleCount := len(roles)

	for i, otherRole := range roles {
		// execute a CREATE IF NOT EXISTS for the other Role as it could be that it does not exist and will be created after this one
		q := FormatQuery(`CREATE ROLE IF NOT EXISTS %s`, otherRole)
		statements = append(statements, q)

		q = FormatQuery(`GRANT ROLE %s TO ROLE %s`, role, otherRole)
		statements = append(statements, q)

		if len(statements) == 200 || i == roleCount-1 {
			logger.Info(fmt.Sprintf("Executing statements to grant role %q to %d roles", role, len(statements)))

			err := executeStatements(conn, statements)
			if err != nil {
				return fmt.Errorf("error while granting roles to role %q: %s", role, err.Error())
			}

			logger.Info(fmt.Sprintf("Done granting role to %d roles", len(statements)))
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
	q := fmt.Sprintf(`GRANT %s %s`, perm, FormatQuery(`ON %s TO ROLE %s`, on, role))
	logger.Debug("Executing grant query", "query", q)

	_, err := QuerySnowflake(conn, q)
	if err != nil {
		return fmt.Errorf("error while executing grant query on Snowflake for role %q: %s", role, err.Error())
	}

	return nil
}

func executeRevoke(conn *sql.DB, perm, on, role string) error {
	q := fmt.Sprintf(`REVOKE %s %s`, perm, FormatQuery(`ON %s FROM ROLE %s`, on, role))
	logger.Debug("Executing revoke query: %s", q)

	_, err := QuerySnowflake(conn, q)
	if err != nil {
		return fmt.Errorf("error while executing revoke query on Snowflake for role %q: %s", role, err.Error())
	}

	return nil
}

func createComment(ap *importer.AccessProvider) string {
	return fmt.Sprintf("Created by Raito from access provider %q. %s", ap.Name, ap.Description)
}

func generateUniqueRoleName(prefix string, ap *importer.AccessProvider, accessIndex int) (string, error) {
	access := ap.Access[accessIndex]
	if access.NamingHint != "" {
		return fmt.Sprintf("%s%s", prefix, access.NamingHint), nil
	} else if ap.NamingHint != "" {
		return fmt.Sprintf("%s%s%s%d", prefix, ap.NamingHint, ROLE_SEPARATOR, accessIndex), nil
	} else if ap.Name != "" {
		name := generateRoleNameFromAPName(ap.Name)
		if len(name) < 10 {
			return "", fmt.Errorf("generated role name %q needs to be at least 10 characters", name)
		}
		return fmt.Sprintf("%s%s%s%d", prefix, name, ROLE_SEPARATOR, accessIndex), nil
	}

	return "", fmt.Errorf("no naming hint provided for access provider %q", ap.NamingHint)
}

func generateRoleNameFromAPName(name string) string {
	generated := ""

	for _, c := range name {
		if c == '-' || c == '_' || c == ' ' {
			generated += "_"
		} else if checkAlphaNum(c) {
			generated += strings.ToUpper(string(c))
		}
	}

	return generated
}

func checkAlphaNum(charVariable rune) bool {
	return (charVariable >= 'a' && charVariable <= 'z') || (charVariable >= 'A' && charVariable <= 'Z') || (charVariable >= '0' && charVariable <= '9')
}

// getAllSnowflakePermissions maps a Raito permission from the data access element to the list of permissions it corresponds to in Snowflake
// The result will be sorted alphabetically
func getAllSnowflakePermissions(what *importer.WhatItem) []string {
	allPerms := make([]string, 0, len(what.Permissions))

	for _, perm := range what.Permissions {
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
