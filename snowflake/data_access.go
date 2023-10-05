package snowflake

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/smithy-go/ptr"
	gonanoid "github.com/matoous/go-nanoid/v2"
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

	idAlphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
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
	GetPolicies(policy string) ([]PolicyEntity, error)
	GetPoliciesLike(policy string, like string) ([]PolicyEntity, error)
	DescribePolicy(policyType, dbName, schema, policyName string) ([]describePolicyEntity, error)
	GetPolicyReferences(dbName, schema, policyName string) ([]policyReferenceEntity, error)
	DropRole(roleName string) error
	ExecuteGrant(perm, on, role string) error
	ExecuteRevoke(perm, on, role string) error
	GetTablesInDatabase(databaseName string, schemaName string, handleEntity EntityHandler) error
	GetSchemasInDatabase(databaseName string, handleEntity EntityHandler) error
	CommentRoleIfExists(comment, objectName string) error
	GrantUsersToRole(ctx context.Context, role string, users ...string) error
	RevokeUsersFromRole(ctx context.Context, role string, users ...string) error
	GrantRolesToRole(ctx context.Context, role string, roles ...string) error
	RevokeRolesFromRole(ctx context.Context, role string, roles ...string) error
	CreateRole(roleName string) error
	CreateMaskPolicy(databaseName string, schema string, maskName string, columnsFullName []string, maskType *string, beneficiaries *MaskingBeneficiaries) error
	DropMaskingPolicy(databaseName string, schema string, maskName string) (err error)
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

func (s *AccessSyncer) SyncAccessProvidersFromTarget(_ context.Context, accessProviderHandler wrappers.AccessProviderHandler, configMap *config.ConfigMap) error {
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

func (s *AccessSyncer) SyncAccessProviderMasksToTarget(ctx context.Context, masksToRemove []string, access []*importer.AccessProvider, feedbackHandler wrappers.AccessProviderFeedbackHandler, configMap *config.ConfigMap) error {
	logger.Info(fmt.Sprintf("Configuring access provider as masks in Snowflake. Update %d masks remove %d masks", len(access), len(masksToRemove)))

	repo, err := s.repoProvider(configMap.Parameters, "")
	if err != nil {
		return err
	}

	// Step 1: Update masks en create new masks
	for _, mask := range access {
		maskName, err2 := s.updateMask(ctx, mask, repo)
		if err2 != nil {
			return err2
		}

		err = feedbackHandler.AddAccessProviderFeedback(mask.Id, importer.AccessSyncFeedbackInformation{AccessId: mask.Id, ActualName: maskName})
		if err != nil {
			return err
		}
	}

	// Step 2: Remove old masks
	for _, maskToRemove := range masksToRemove {
		err = s.removeMask(ctx, maskToRemove, repo)
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
func (s *AccessSyncer) generateAccessControls(ctx context.Context, apMap map[string]*importer.AccessProvider, existingRoles map[string]bool, repo dataAccessRepository, verifyAndPropagate bool) error {
	// We always need the meta data
	syncer := DataSourceSyncer{}
	md, err := syncer.GetDataSourceMetaData(ctx)

	if err != nil {
		return err
	}

	metaData := buildMetaDataMap(md)

	// Initializes empty map
	propagateMetaData := make(map[string]map[string]struct{})

	// Keep the propagateMetaData empty if not needing to do the propagation. Otherwise, use the metadata map.
	if verifyAndPropagate {
		propagateMetaData = metaData
	}

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
					grants, err := createGrantsForTableOrView(what.DataObject.Type, permissions, what.DataObject.FullName, propagateMetaData)
					if err != nil {
						return err
					}

					expectedGrants = append(expectedGrants, grants...)
				} else if what.DataObject.Type == ds.Schema {
					grants, err := createGrantsForSchema(repo, permissions, what.DataObject.FullName, propagateMetaData)
					if err != nil {
						return err
					}

					expectedGrants = append(expectedGrants, grants...)
				} else if what.DataObject.Type == "shared-database" {
					for _, p := range permissions {
						expectedGrants = append(expectedGrants, Grant{p, "shared-database", what.DataObject.FullName})
					}
				} else if what.DataObject.Type == ds.Database {
					grants, err := createGrantsForDatabase(repo, permissions, what.DataObject.FullName, propagateMetaData)
					if err != nil {
						return err
					}

					expectedGrants = append(expectedGrants, grants...)
				} else if what.DataObject.Type == "warehouse" {
					expectedGrants = append(expectedGrants, createGrantsForWarehouse(permissions, what.DataObject.FullName, propagateMetaData)...)
				} else if what.DataObject.Type == ds.Datasource {
					expectedGrants = append(expectedGrants, createGrantsForAccount(permissions, propagateMetaData)...)
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

func (s *AccessSyncer) updateMask(_ context.Context, mask *importer.AccessProvider, repo dataAccessRepository) (string, error) {
	logger.Info(fmt.Sprintf("Updating mask %q", mask.Name))

	globalMaskName := raitoMaskName(mask.Name)
	uniqueMaskName := raitoMaskUniqueName(mask.Name)

	// Load beneficieries
	beneficiaries := MaskingBeneficiaries{
		Users: mask.Who.Users,
		Roles: mask.Who.InheritFrom,
	}

	dosPerSchema := map[string][]string{}

	for _, do := range mask.What {
		fullnameSplit := strings.Split(do.DataObject.FullName, ".")

		if len(fullnameSplit) != 4 {
			logger.Error(fmt.Sprintf("Invalid fullname for column %s in mask %s", do.DataObject.FullName, mask.Name))

			continue
		}

		schemaName := fullnameSplit[1]
		database := fullnameSplit[0]

		schemaFullName := strings.Join([]string{database, schemaName}, ".")

		dosPerSchema[schemaFullName] = append(dosPerSchema[schemaFullName], do.DataObject.FullName)
	}

	existingPolicies, err := repo.GetPoliciesLike("MASKING", fmt.Sprintf("%s%s", globalMaskName, "%"))
	if err != nil {
		return uniqueMaskName, err
	}

	for schema, dos := range dosPerSchema {
		logger.Info(fmt.Sprintf("Updating mask %q for schema %q", mask.Name, schema))
		namesplit := strings.Split(schema, ".")

		database := namesplit[0]
		schemaName := namesplit[1]

		err = repo.CreateMaskPolicy(database, schemaName, uniqueMaskName, dos, nil, &beneficiaries)
		if err != nil {
			return uniqueMaskName, err
		}
	}

	for _, policy := range existingPolicies {
		existingUniqueMaskNameSpit := strings.Split(policy.Name, "_")
		existingUniqueMaskName := strings.Join(existingUniqueMaskNameSpit[:len(existingUniqueMaskNameSpit)-1], "_")

		err = repo.DropMaskingPolicy(policy.DatabaseName, policy.SchemaName, existingUniqueMaskName)
		if err != nil {
			return uniqueMaskName, err
		}
	}

	return uniqueMaskName, nil
}

func (s *AccessSyncer) removeMask(_ context.Context, mask string, repo dataAccessRepository) error {
	logger.Info(fmt.Sprintf("Remove mask %q", mask))

	maskName := raitoMaskName(mask)

	existingPolicies, err := repo.GetPoliciesLike("MASKING", fmt.Sprintf("%s%s", maskName, "%"))
	if err != nil {
		return err
	}

	for _, policy := range existingPolicies {
		err = repo.DropMaskingPolicy(policy.DatabaseName, policy.SchemaName, maskName)
		if err != nil {
			return err
		}
	}

	return nil
}

func createGrantsForTableOrView(doType string, permissions []string, fullName string, metaData map[string]map[string]struct{}) ([]Grant, error) {
	// TODO: this does not work for Raito full names
	sfObject := common.ParseFullName(fullName)
	if sfObject.Database == nil || sfObject.Schema == nil || sfObject.Table == nil {
		return nil, fmt.Errorf("expected fullName %q to have 3 parts (database.schema.view)", fullName)
	}

	grants := make([]Grant, 0, len(permissions)+2)
	grants = append(grants,
		Grant{"USAGE", ds.Database, common.FormatQuery(`%s`, *sfObject.Database)},
		Grant{"USAGE", ds.Schema, common.FormatQuery(`%s.%s`, *sfObject.Database, *sfObject.Schema)})

	for _, p := range permissions {
		if _, f := metaData[doType][strings.ToUpper(p)]; len(metaData) == 0 || f {
			grants = append(grants, Grant{p, doType, common.FormatQuery(`%s.%s.%s`, *sfObject.Database, *sfObject.Schema, *sfObject.Table)})
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
		Grant{"USAGE", ds.Database, common.FormatQuery(`%s`, *sfObject.Database)},
		Grant{"USAGE", ds.Schema, common.FormatQuery(`%s.%s`, *sfObject.Database, *sfObject.Schema)})

	var tables []TableEntity
	var err error

	for _, p := range permissions {
		// Check if the permission is applicable on the schema itself
		if _, f := metaData[ds.Schema][strings.ToUpper(p)]; len(metaData) == 0 || f {
			grants = append(grants, Grant{p, ds.Schema, common.FormatQuery(`%s.%s`, *sfObject.Database, *sfObject.Schema)})
		} else {
			if tables == nil {
				err = repo.GetTablesInDatabase(*sfObject.Database, *sfObject.Schema, func(entity interface{}) error {
					table := entity.(*TableEntity)
					tables = append(tables, *table)
					return nil
				})
				if err != nil {
					return nil, err
				}
			}

			matchFound := false

			// Run through all the tabular things (tables, views, ...) in the schema
			for _, table := range tables {
				// Get the corresponding Raito data object type
				raitoType := convertSnowflakeTableTypeToRaito(table.TableType)

				// Check if the permission is applicable on the data object type
				if _, f2 := metaData[raitoType][strings.ToUpper(p)]; f2 {
					matchFound = true
					grants = append(grants, Grant{p, raitoType, common.FormatQuery(`%s.%s.%s`, *sfObject.Database, *sfObject.Schema, table.Name)})
				}
			}

			if !matchFound {
				logger.Warn("Permission %q does not apply to type SCHEMA or any of its descendants. Skipping", p)
			}
		}
	}

	return grants, nil
}

func createGrantsForDatabase(repo dataAccessRepository, permissions []string, database string, metaData map[string]map[string]struct{}) ([]Grant, error) {
	grants := make([]Grant, 0, len(permissions)+1)

	sfObject := common.SnowflakeObject{Database: &database, Schema: nil, Table: nil, Column: nil}

	grants = append(grants, Grant{"USAGE", ds.Database, sfObject.GetFullName(true)})

	var schemas []SchemaEntity
	tablesPerSchema := make(map[string][]TableEntity)
	var err error

	for _, p := range permissions {
		matchFound := false

		if _, f := metaData[ds.Database][strings.ToUpper(p)]; len(metaData) == 0 || f {
			matchFound = true
			grants = append(grants, Grant{p, ds.Database, sfObject.GetFullName(true)})
		} else if schemas == nil {
			err = repo.GetSchemasInDatabase(database, func(entity interface{}) error {
				schema := entity.(*SchemaEntity)

				if schema.Name == "INFORMATION_SCHEMA" {
					return nil
				}

				sfObject.Schema = &schema.Name
				grants = append(grants, Grant{"USAGE", ds.Schema, sfObject.GetFullName(true)})

				// Check if the permission is applicable on schemas
				if _, f := metaData[ds.Schema][strings.ToUpper(p)]; f {
					matchFound = true
					grants = append(grants, Grant{p, ds.Schema, common.FormatQuery(`%s.%s`, *sfObject.Database, *sfObject.Schema)})
				} else {
					tables, f := tablesPerSchema[schema.Name]
					if !f {
						tables = make([]TableEntity, 0)
						err = repo.GetTablesInDatabase(*sfObject.Database, *sfObject.Schema, func(entity interface{}) error {
							table := entity.(*TableEntity)
							tables = append(tables, *table)
							return nil
						})
						if err != nil {
							return err
						}

						tablesPerSchema[schema.Name] = tables
					}

					// Run through all the tabular things (tables, views, ...) in the schema
					for _, table := range tables {
						// Get the corresponding Raito data object type
						raitoType := convertSnowflakeTableTypeToRaito(table.TableType)

						// Check if the permission is applicable on the data object type
						if _, f := metaData[raitoType][strings.ToUpper(p)]; f {
							matchFound = true
							grants = append(grants, Grant{p, raitoType, common.FormatQuery(`%s.%s.%s`, *sfObject.Database, *sfObject.Schema, table.Name)})
						}
					}
				}

				return nil
			})

			if err != nil {
				return nil, err
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
	grants = append(grants, Grant{"USAGE", "warehouse", common.FormatQuery(`%s`, warehouse)})

	for _, p := range permissions {
		if _, f := metaData["warehouse"][strings.ToUpper(p)]; len(metaData) != 0 && !f {
			logger.Warn("Permission %q does not apply to type WAREHOUSE. Skipping", p)
			continue
		}

		grants = append(grants, Grant{p, "warehouse", common.FormatQuery(`%s`, warehouse)})
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

		grants = append(grants, Grant{p, "account", ""})
	}

	return grants
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

func raitoMaskName(name string) string {
	return fmt.Sprintf("RAITO_%s", strings.ToUpper(name))
}

func raitoMaskUniqueName(name string) string {
	return strings.Join([]string{raitoMaskName(name), gonanoid.MustGenerate(idAlphabet, 8)}, "_")
}
