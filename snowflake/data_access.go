package snowflake

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/aws/smithy-go/ptr"
	"github.com/hashicorp/go-multierror"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/raito-io/cli/base/access_provider"
	exporter "github.com/raito-io/cli/base/access_provider/sync_from_target"
	importer "github.com/raito-io/cli/base/access_provider/sync_to_target"
	ds "github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/util/slice"
	"github.com/raito-io/cli/base/wrappers"
	"github.com/raito-io/cli/base/wrappers/role_based"
	"github.com/raito-io/golang-set/set"

	"github.com/raito-io/cli-plugin-snowflake/common"
)

var RolesNotInternalizable = []string{"ORGADMIN", "ACCOUNTADMIN", "SECURITYADMIN", "USERADMIN", "SYSADMIN", "PUBLIC"}
var AcceptedTypes = map[string]struct{}{"ACCOUNT": {}, "WAREHOUSE": {}, "DATABASE": {}, "SCHEMA": {}, "TABLE": {}, "VIEW": {}, "COLUMN": {}, "SHARED-DATABASE": {}, "EXTERNAL_TABLE": {}, "MATERIALIZED_VIEW": {}}

const (
	whoLockedReason         = "The 'who' for this Snowflake role cannot be changed because it was imported from an external identity store"
	inheritanceLockedReason = "The inheritance for this Snowflake role cannot be changed because it was imported from an external identity store"
	nameLockedReason        = "This Snowflake role cannot be renamed because it was imported from an external identity store"
	deleteLockedReason      = "This Snowflake role cannot be deleted because it was imported from an external identity store"
	maskPrefix              = "RAITO_"
	idAlphabet              = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	databaseRoleWhoLockedReason  = "The 'who' for this Snowflake role cannot be changed because we currently do not support database role changes"
	databaseRoleWhatLockedReason = "The 'what' for this Snowflake role cannot be changed because we currently do not support database role changes"

	databaseRolePrefix = "DATABASEROLE###"
)

//go:generate go run github.com/vektra/mockery/v2 --name=dataAccessRepository --with-expecter --inpackage
type dataAccessRepository interface {
	Close() error
	CommentAccountRoleIfExists(comment, objectName string) error
	CommentDatabaseRoleIfExists(comment, database, roleName string) error
	CreateAccountRole(roleName string) error
	CreateDatabaseRole(database, roleName string) error
	CreateMaskPolicy(databaseName string, schema string, maskName string, columnsFullName []string, maskType *string, beneficiaries *MaskingBeneficiaries) error
	DescribePolicy(policyType, dbName, schema, policyName string) ([]describePolicyEntity, error)
	DropAccountRole(roleName string) error
	DropDatabaseRole(database string, roleName string) error
	DropFilter(databaseName string, schema string, tableName string, filterName string) error
	DropMaskingPolicy(databaseName string, schema string, maskName string) (err error)
	ExecuteGrantOnAccountRole(perm, on, role string) error
	ExecuteGrantOnDatabaseRole(perm, on, database, databaseRole string) error
	ExecuteRevokeOnAccountRole(perm, on, role string) error
	ExecuteRevokeOnDatabaseRole(perm, on, database, databaseRole string) error
	GetAccountRoles() ([]RoleEntity, error)
	GetAccountRolesWithPrefix(prefix string) ([]RoleEntity, error)
	GetDatabaseRoles(database string) ([]RoleEntity, error)
	GetDatabaseRolesWithPrefix(database string, prefix string) ([]RoleEntity, error)
	GetDatabases() ([]DbEntity, error)
	GetGrantsOfAccountRole(roleName string) ([]GrantOfRole, error)
	GetGrantsOfDatabaseRole(database, roleName string) ([]GrantOfRole, error)
	GetGrantsToAccountRole(roleName string) ([]GrantToRole, error)
	GetGrantsToDatabaseRole(database, roleName string) ([]GrantToRole, error)
	GetPolicies(policy string) ([]PolicyEntity, error)
	GetPoliciesLike(policy string, like string) ([]PolicyEntity, error)
	GetPolicyReferences(dbName, schema, policyName string) ([]policyReferenceEntity, error)
	GetSchemasInDatabase(databaseName string, handleEntity EntityHandler) error
	GetShares() ([]DbEntity, error)
	GetTablesInDatabase(databaseName string, schemaName string, handleEntity EntityHandler) error
	GetWarehouses() ([]DbEntity, error)
	GrantAccountRolesToAccountRole(ctx context.Context, role string, roles ...string) error
	GrantAccountRolesToDatabaseRole(ctx context.Context, database string, databaseRole string, accountRoles ...string) error
	GrantDatabaseRolesToDatabaseRole(ctx context.Context, database string, databaseRole string, databaseRoles ...string) error
	GrantUsersToAccountRole(ctx context.Context, role string, users ...string) error
	RenameAccountRole(oldName, newName string) error
	RenameDatabaseRole(database, oldName, newName string) error
	RevokeAccountRolesFromAccountRole(ctx context.Context, role string, roles ...string) error
	RevokeAccountRolesFromDatabaseRole(ctx context.Context, database string, databaseRole string, accountRoles ...string) error
	RevokeDatabaseRolesFromDatabaseRole(ctx context.Context, database string, databaseRole string, databaseRoles ...string) error
	RevokeUsersFromAccountRole(ctx context.Context, role string, users ...string) error
	TotalQueryTime() time.Duration
	UpdateFilter(databaseName string, schema string, tableName string, filterName string, argumentNames []string, expression string) error
}

var _ role_based.AccessProviderRoleSyncer = (*AccessSyncer)(nil)

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

func (s *AccessSyncer) SyncAccessProviderRolesToTarget(ctx context.Context, apToRemoveMap map[string]*importer.AccessProvider, apMap map[string]*importer.AccessProvider, feedbackHandler wrappers.AccessProviderFeedbackHandler, configMap *config.ConfigMap) error {
	logger.Info("Configuring access providers as roles in Snowflake")

	repo, err := s.repoProvider(configMap.Parameters, "")
	if err != nil {
		return err
	}

	defer func() {
		logger.Info(fmt.Sprintf("Total snowflake query time:  %s", repo.TotalQueryTime()))
		repo.Close()
	}()

	err = s.removeRolesToRemove(apToRemoveMap, repo, feedbackHandler)
	if err != nil {
		return err
	}

	renameMap := make(map[string]string)

	for roleName, ap := range apMap {
		if ap.ActualName != nil && *ap.ActualName != roleName {
			renameMap[roleName] = *ap.ActualName
		}
	}

	existingRoles, err := s.findRoles("", repo)
	if err != nil {
		return err
	}

	err = s.generateAccessControls(ctx, apMap, existingRoles, renameMap, repo, configMap, feedbackHandler)
	if err != nil {
		return err
	}

	return nil
}

func (s *AccessSyncer) SyncAccessProviderMasksToTarget(ctx context.Context, apToRemoveMap map[string]*importer.AccessProvider, apMap map[string]*importer.AccessProvider, roleNameMap map[string]string, feedbackHandler wrappers.AccessProviderFeedbackHandler, configMap *config.ConfigMap) error {
	if configMap.GetBoolWithDefault(SfStandardEdition, false) {
		if len(apToRemoveMap) > 0 || len(apMap) > 0 {
			logger.Error("Skipping masking policies due to Snowflake Standard Edition.")
		}

		return nil
	}

	logger.Info(fmt.Sprintf("Configuring access provider as masks in Snowflake. Update %d masks remove %d masks", len(apMap), len(apToRemoveMap)))

	repo, err := s.repoProvider(configMap.Parameters, "")
	if err != nil {
		return err
	}

	// Step 1: Update masks and create new masks
	for _, mask := range apMap {
		maskName, err2 := s.updateMask(ctx, mask, roleNameMap, repo)
		fi := importer.AccessProviderSyncFeedback{AccessProvider: mask.Id, ActualName: maskName, ExternalId: &maskName}

		if err2 != nil {
			fi.Errors = append(fi.Errors, err2.Error())
		}

		err = feedbackHandler.AddAccessProviderFeedback(fi)
		if err != nil {
			return err
		}
	}

	// Step 2: Remove old masks
	for maskToRemove, maskAp := range apToRemoveMap {
		externalId := maskToRemove
		fi := importer.AccessProviderSyncFeedback{AccessProvider: maskAp.Id, ActualName: maskToRemove, ExternalId: &externalId}

		err = s.removeMask(ctx, maskToRemove, repo)
		if err != nil {
			fi.Errors = append(fi.Errors, err.Error())
		}

		err = feedbackHandler.AddAccessProviderFeedback(fi)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *AccessSyncer) SyncAccessProviderFiltersToTarget(ctx context.Context, apToRemoveMap map[string]*importer.AccessProvider, apMap map[string]*importer.AccessProvider, roleNameMap map[string]string, feedbackHandler wrappers.AccessProviderFeedbackHandler, configMap *config.ConfigMap) error {
	if configMap.GetBoolWithDefault(SfStandardEdition, false) {
		if len(apToRemoveMap) > 0 || len(apMap) > 0 {
			logger.Error("Skipping filter policies due to Snowflake Standard Edition.")
		}

		return nil
	}

	logger.Info(fmt.Sprintf("Configuring access provider as filters in Snowflake. Update %d masks remove %d masks", len(apMap), len(apToRemoveMap)))

	repo, err := s.repoProvider(configMap.Parameters, "")
	if err != nil {
		return err
	}

	//Groups filters by table
	updateGroupedFilters, err := groupFiltersByTable(apMap, feedbackHandler)
	if err != nil {
		return err
	}

	removeGroupedFilters, err := groupFiltersByTable(apToRemoveMap, feedbackHandler)
	if err != nil {
		return err
	}

	feedbackFn := func(aps []*importer.AccessProvider, actualName *string, externalId *string, err error) error {
		var feedbackErr error

		var errorMessages []string

		if err != nil {
			errorMessages = []string{err.Error()}
		}

		for _, ap := range aps {
			var actualNameStr string
			if actualName != nil {
				actualNameStr = *actualName
			} else if ap.ActualName != nil {
				actualNameStr = *ap.ActualName
			}

			var apExternalId *string
			if externalId != nil {
				apExternalId = externalId
			} else {
				apExternalId = ap.ExternalId
			}

			ferr := feedbackHandler.AddAccessProviderFeedback(importer.AccessProviderSyncFeedback{
				AccessProvider: ap.Id,
				ActualName:     actualNameStr,
				ExternalId:     apExternalId,
				Errors:         errorMessages,
			})
			if ferr != nil {
				feedbackErr = multierror.Append(feedbackErr, ferr)
			}
		}

		return feedbackErr
	}

	//Create or update filters per table
	updatedTables := set.NewSet[string]()

	for table, filters := range updateGroupedFilters {
		filterName, externalId, createErr := s.updateOrCreateFilter(ctx, repo, table, filters, roleNameMap)

		ferr := feedbackFn(filters, &filterName, externalId, createErr)
		if ferr != nil {
			return ferr
		}

		if createErr != nil {
			updatedTables.Add(table)
		}
	}

	// Remove old filters per table
	for table, filters := range removeGroupedFilters {
		if _, found := updateGroupedFilters[table]; found {
			if updatedTables.Contains(table) {
				deleteErr := s.deleteFilter(repo, table, filters)

				ferr := feedbackFn(filters, nil, nil, deleteErr)
				if ferr != nil {
					return ferr
				}
			} else {
				ferr := feedbackFn(filters, nil, nil, fmt.Errorf("prevent deletion of filter because unable to create new filter"))
				if ferr != nil {
					return ferr
				}
			}
		} else {
			deleteErr := s.deleteFilter(repo, table, filters)

			ferr := feedbackFn(filters, nil, nil, deleteErr)
			if ferr != nil {
				return ferr
			}
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

	existingRoles, err := s.findRoles(prefix, repo)
	if err != nil {
		return err
	}

	rolesToRemove := make(map[string]*importer.AccessProvider, 0)

	for _, role := range existingRoles.Slice() {
		// If the existing role is not found in the roles to handle, we need to remove it.
		if _, f := access[role]; !f {
			rolesToRemove[role] = &importer.AccessProvider{Id: role, ActualName: ptr.String(role)}
		}
	}

	err = s.removeRolesToRemove(rolesToRemove, repo, &dummyFeedbackHandler{})
	if err != nil {
		return err
	}

	err = s.generateAccessControls(ctx, access, existingRoles, map[string]string{}, repo, configMap, &dummyFeedbackHandler{})
	if err != nil {
		return err
	}

	return nil
}

type dummyFeedbackHandler struct {
}

func (d *dummyFeedbackHandler) AddAccessProviderFeedback(accessProviderFeedback importer.AccessProviderSyncFeedback) error {
	if len(accessProviderFeedback.Errors) > 0 {
		for _, err := range accessProviderFeedback.Errors {
			logger.Error(fmt.Sprintf("error during syncing of access provider %q; %s", accessProviderFeedback.AccessProvider, err))
		}
	}

	return nil
}

func (s *AccessSyncer) removeRolesToRemove(rolesToRemove map[string]*importer.AccessProvider, repo dataAccessRepository, feedbackHandler wrappers.AccessProviderFeedbackHandler) error {
	if len(rolesToRemove) > 0 {
		logger.Info(fmt.Sprintf("Removing %d old Raito roles in Snowflake", len(rolesToRemove)))

		for roleToRemove, roleAp := range rolesToRemove {
			externalId := roleToRemove

			fi := importer.AccessProviderSyncFeedback{
				AccessProvider: roleAp.Id,
				ActualName:     roleToRemove,
				ExternalId:     &externalId,
			}

			err := s.dropRole(roleToRemove, repo)
			// If an error occurs (and not already deleted), we send an error back as feedback
			if err != nil && !strings.Contains(err.Error(), "does not exist") {
				logger.Error(fmt.Sprintf("unable to drop role %q: %s", roleToRemove, err.Error()))

				fi.Errors = append(fi.Errors, fmt.Sprintf("unable to drop role %q: %s", roleToRemove, err.Error()))
			}

			err = feedbackHandler.AddAccessProviderFeedback(fi)
			if err != nil {
				return err
			}
		}
	} else {
		logger.Info("No old Raito roles to remove in Snowflake")
	}

	return nil
}

func (s *AccessSyncer) getShareNames(repo dataAccessRepository) (map[string]struct{}, error) {
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

	shares, err := s.getShareNames(repo)
	if err != nil {
		return err
	}

	accessProviderMap := make(map[string]*exporter.AccessProvider)

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

		err = s.importAccessForAccountRole(roleEntity, externalGroupOwners, linkToExternalIdentityStoreGroups, repo, accessProviderMap, shares, accessProviderHandler)
		if err != nil {
			return err
		}
	}

	//Get all database roles for each database and import them
	databases, err := s.getDatabases(repo)
	if err != nil {
		return err
	}

	for i := range databases {
		// Get all database roles for database
		roleEntities, err := repo.GetDatabaseRoles(databases[i].Name)
		if err != nil {
			return err
		}

		for _, roleEntity := range roleEntities {
			if _, exclude := excludedRoles[roleEntity.Name]; exclude {
				logger.Info("Skipping SnowFlake DATABASE ROLE " + roleEntity.Name)
				continue
			}

			err = s.importAccessForDatabaseRole(databases[i].Name, roleEntity, externalGroupOwners, linkToExternalIdentityStoreGroups, repo, accessProviderMap, shares, accessProviderHandler)
			if err != nil {
				return err
			}
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

func (s *AccessSyncer) importAccessForAccountRole(roleEntity RoleEntity, externalGroupOwners string, linkToExternalIdentityStoreGroups bool, repo dataAccessRepository, accessProviderMap map[string]*exporter.AccessProvider, shares map[string]struct{}, accessProviderHandler wrappers.AccessProviderHandler) error {
	logger.Info(fmt.Sprintf("Reading SnowFlake ROLE %s", roleEntity.Name))

	actualName := roleEntity.Name
	fromExternalIS := s.comesFromExternalIdentityStore(roleEntity, externalGroupOwners)

	users, groups, accessProviders, err := s.retrieveWhoEntitiesForRole(actualName, fromExternalIS, linkToExternalIdentityStoreGroups, repo)
	if err != nil {
		return err
	}

	ap, f := accessProviderMap[actualName]
	if !f {
		accessProviderMap[actualName] = &exporter.AccessProvider{
			ExternalId: roleEntity.Name,
			Name:       roleEntity.Name,
			NamingHint: roleEntity.Name,
			Action:     exporter.Grant,
			Who: &exporter.WhoItem{
				Users:           users,
				AccessProviders: accessProviders,
				Groups:          groups,
			},
			ActualName: actualName,
			What:       make([]exporter.WhatItem, 0),
		}
		ap = accessProviderMap[actualName]

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
	grantToEntities, err := s.getGrantsToRole(actualName, repo)
	if err != nil {
		return fmt.Errorf("error retrieving grants for role: %s", err.Error())
	}

	ap.What = append(ap.What, s.mapGrantToRoleToWhatItems(grantToEntities, shares)...)

	if isNotInternalizableRole(ap.Name) {
		logger.Info(fmt.Sprintf("Marking role %s as read-only (notInternalizable)", ap.Name))
		ap.NotInternalizable = true
	}

	err = accessProviderHandler.AddAccessProviders(ap)
	if err != nil {
		return fmt.Errorf("error adding access provider to import file: %s", err.Error())
	}

	return nil
}

func (s *AccessSyncer) importAccessForDatabaseRole(database string, roleEntity RoleEntity, externalGroupOwners string, linkToExternalIdentityStoreGroups bool, repo dataAccessRepository, accessProviderMap map[string]*exporter.AccessProvider, shares map[string]struct{}, accessProviderHandler wrappers.AccessProviderHandler) error {
	logger.Info(fmt.Sprintf("Reading SnowFlake DATABASE ROLE %s inside %s", roleEntity.Name, database))

	databaseRoleName := DatabaseRoleNameGenerator(database, roleEntity.Name)
	actualName := DatabaseRoleActualNameGenerator(databaseRoleName)
	fromExternalIS := s.comesFromExternalIdentityStore(roleEntity, externalGroupOwners)

	users, groups, accessProviders, err := s.retrieveWhoEntitiesForRole(actualName, fromExternalIS, linkToExternalIdentityStoreGroups, repo)
	if err != nil {
		return err
	}

	ap, f := accessProviderMap[actualName]
	if !f {
		accessProviderMap[actualName] = &exporter.AccessProvider{
			Type:       ptr.String(DatabaseRole),
			ExternalId: databaseRoleName,
			Name:       databaseRoleName,
			NamingHint: databaseRoleName,
			ActualName: actualName,
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
		ap = accessProviderMap[actualName]
	} else {
		ap.Who.Users = users
		ap.Who.AccessProviders = accessProviders
		ap.Who.Groups = groups
	}

	// get objects granted TO role
	grantToEntities, err := s.getGrantsToRole(actualName, repo)
	if err != nil {
		return fmt.Errorf("error retrieving grants for role: %s", err.Error())
	}

	ap.What = append(ap.What, s.mapGrantToRoleToWhatItems(grantToEntities, shares)...)

	if isNotInternalizableRole(ap.Name) {
		logger.Info(fmt.Sprintf("Marking role %s as read-only (notInternalizable)", ap.Name))
		ap.NotInternalizable = true
	}

	err = accessProviderHandler.AddAccessProviders(ap)
	if err != nil {
		return fmt.Errorf("error adding access provider to import file: %s", err.Error())
	}

	return nil
}

func (s *AccessSyncer) mapGrantToRoleToWhatItems(grantToEntities []GrantToRole, shares map[string]struct{}) []exporter.WhatItem {
	var do *ds.DataObjectReference

	whatItems := make([]exporter.WhatItem, 0)
	permissions := make([]string, 0)
	sharesApplied := make(map[string]struct{}, 0)

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
			if _, f := shares[databaseName]; f {
				// TODO do we need to do this for all tabular types?
				if _, f := sharesApplied[databaseName]; strings.EqualFold(grant.GrantedOn, "TABLE") && !f {
					whatItems = append(whatItems, exporter.WhatItem{
						DataObject:  &ds.DataObjectReference{FullName: databaseName, Type: SharedPrefix + ds.Database},
						Permissions: []string{"IMPORTED PRIVILEGES"},
					})
					sharesApplied[databaseName] = struct{}{}
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

func (s *AccessSyncer) retrieveWhoEntitiesForRole(roleName string, fromExternalIS bool, linkToExternalIdentityStoreGroups bool, repo dataAccessRepository) (users []string, groups []string, accessProviders []string, err error) {
	users = make([]string, 0)
	accessProviders = make([]string, 0)
	groups = make([]string, 0)

	if fromExternalIS && linkToExternalIdentityStoreGroups {
		groups = append(groups, roleName)
	} else {
		grantOfEntities, err := s.retrieveGrantsOfRole(roleName, repo)
		if err != nil {
			return nil, nil, nil, err
		}

		for _, grantee := range grantOfEntities {
			if grantee.GrantedTo == "USER" {
				users = append(users, cleanDoubleQuotes(grantee.GranteeName))
			} else if grantee.GrantedTo == "ROLE" {
				accessProviders = append(accessProviders, cleanDoubleQuotes(grantee.GranteeName))
			} else if grantee.GrantedTo == "DATABASE_ROLE" {
				accessProviders = append(accessProviders, DatabaseRoleActualNameGenerator(cleanDoubleQuotes(grantee.GranteeName)))
			}
		}
	}

	return users, groups, accessProviders, nil
}

func (s *AccessSyncer) retrieveGrantsOfRole(roleName string, repo dataAccessRepository) (grantOfEntities []GrantOfRole, err error) {
	if isDatabaseRole(roleName) {
		database, parsedRoleName, err2 := parseDatabaseRoleName(roleName)
		if err2 != nil {
			return nil, err2
		}

		grantOfEntities, err = repo.GetGrantsOfDatabaseRole(database, parsedRoleName)
	} else {
		grantOfEntities, err = repo.GetGrantsOfAccountRole(roleName)
	}

	return grantOfEntities, err
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
		} else if strings.HasPrefix(policy.Name, maskPrefix) {
			logger.Debug(fmt.Sprintf("Masking policy %s defined by RAITO. Not exporting this", policy.Name))
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

func isNotInternalizableRole(role string) bool {
	searchForRole := role

	if isDatabaseRole(role) {
		_, parsedRoleName, err := parseDatabaseRoleName(role)
		if err != nil {
			return true
		}

		searchForRole = parsedRoleName
	}

	for _, r := range RolesNotInternalizable {
		if strings.EqualFold(r, searchForRole) {
			return true
		}
	}

	return false
}

// findRoles returns the set of existing roles with the given prefix
func (s *AccessSyncer) findRoles(prefix string, repo dataAccessRepository) (set.Set[string], error) {
	existingRoles := set.NewSet[string]()

	roleEntities, err := repo.GetAccountRolesWithPrefix(prefix)
	if err != nil {
		return nil, err
	}

	for _, roleEntity := range roleEntities {
		existingRoles.Add(roleEntity.Name)
	}

	//Get all database roles for each database and add database roles to existing roles
	databases, err := s.getDatabases(repo)
	if err != nil {
		return nil, err
	}

	for i := range databases {
		// Get all database roles for database
		roleEntities, err := repo.GetDatabaseRolesWithPrefix(databases[i].Name, prefix)
		if err != nil {
			return nil, err
		}

		for _, roleEntity := range roleEntities {
			existingRoles.Add(DatabaseRoleActualNameGenerator(DatabaseRoleNameGenerator(databases[i].Name, roleEntity.Name)))
		}
	}

	return existingRoles, nil
}

func (s *AccessSyncer) buildMetaDataMap(metaData *ds.MetaData) map[string]map[string]struct{} {
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
func (s *AccessSyncer) handleAccessProvider(ctx context.Context, rn string, apMap map[string]*importer.AccessProvider, existingRoles set.Set[string], renameMap map[string]string, rolesCreated map[string]interface{}, repo dataAccessRepository, metaData map[string]map[string]struct{}) error {
	accessProvider := apMap[rn]

	ignoreWho := accessProvider.WhoLocked != nil && *accessProvider.WhoLocked
	ignoreInheritance := accessProvider.InheritanceLocked != nil && *accessProvider.InheritanceLocked
	ignoreWhat := accessProvider.WhatLocked != nil && *accessProvider.WhatLocked

	logger.Info(fmt.Sprintf("Generating access controls for access provider %q (Ignore who: %t; Ignore inheritance: %t; Ignore what: %t)", accessProvider.Name, ignoreWho, ignoreInheritance, ignoreWhat))

	// Extract RoleNames from Access Providers that are among the whoList of this one
	inheritedRoles := make([]string, 0)

	if !ignoreInheritance {
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
				grants, err2 := s.createGrantsForTableOrView(what.DataObject.Type, permissions, what.DataObject.FullName, metaData)
				if err2 != nil {
					return err2
				}

				expectedGrants = append(expectedGrants, grants...)
			} else if what.DataObject.Type == ds.Schema {
				grants, err2 := s.createGrantsForSchema(repo, permissions, what.DataObject.FullName, metaData, false)
				if err2 != nil {
					return err2
				}

				expectedGrants = append(expectedGrants, grants...)
			} else if what.DataObject.Type == "shared-schema" {
				grants, err2 := s.createGrantsForSchema(repo, permissions, what.DataObject.FullName, metaData, true)
				if err2 != nil {
					return err2
				}

				expectedGrants = append(expectedGrants, grants...)
			} else if what.DataObject.Type == "shared-database" {
				grants, err2 := s.createGrantsForDatabase(repo, permissions, what.DataObject.FullName, metaData, true)
				if err2 != nil {
					return err2
				}

				expectedGrants = append(expectedGrants, grants...)
			} else if what.DataObject.Type == ds.Database {
				grants, err2 := s.createGrantsForDatabase(repo, permissions, what.DataObject.FullName, metaData, false)
				if err2 != nil {
					return err2
				}

				expectedGrants = append(expectedGrants, grants...)
			} else if what.DataObject.Type == "warehouse" {
				expectedGrants = append(expectedGrants, s.createGrantsForWarehouse(permissions, what.DataObject.FullName, metaData)...)
			} else if what.DataObject.Type == ds.Datasource {
				grants, err2 := s.createGrantsForAccount(repo, permissions, metaData)
				if err2 != nil {
					return err2
				}

				expectedGrants = append(expectedGrants, grants...)
			}
		}
	}

	var foundGrants []Grant

	// If we find this role name in the rename map, this means we have to rename it.
	if oldName, f := renameMap[rn]; f {
		if !existingRoles.Contains(rn) && existingRoles.Contains(oldName) {
			if _, oldFound := apMap[oldName]; oldFound {
				// In this case the old is already taken by another access provider.
				// For example in the case where R2 was renamed to R3 and R1 was then renamed to R2.
				// Therefor, we only log a message for this special case
				logger.Info(fmt.Sprintf("Both the old role name (%s) and the new role name (%s) exist. The old role name is already taken by another (new?) access provider.", rn, oldName))
			} else {
				// The old name exists and the new one doesn't exist yet, so we have to do the rename
				err := s.renameRole(oldName, rn, repo)
				if err != nil {
					return fmt.Errorf("error while renaming role %q to %q: %s", oldName, rn, err.Error())
				}

				existingRoles.Add(rn)
			}
		} else if existingRoles.Contains(rn) && existingRoles.Contains(oldName) {
			if _, oldFound := apMap[oldName]; oldFound {
				// In this case the old is already taken by another access provider.
				// For example in the case where R2 was renamed to R3 and R1 was then renamed to R2.
				// Therefor, we only log a message for this special case
				logger.Info(fmt.Sprintf("Both the old role name (%s) and the new role name (%s) exist. The old role name is already taken by another (new?) access provider.", rn, oldName))
			} else {
				// The old name exists but also the new one already exists. This is a weird case, but we'll delete the old one in this case and the new one will be updated in the next step of this method.
				err := s.dropRole(oldName, repo)
				if err != nil {
					return fmt.Errorf("error while dropping role (%s) which was the old name of access provider %q: %s", oldName, accessProvider.Name, err.Error())
				}

				existingRoles.Remove(oldName)
			}
		}
	}

	// If the role already exists in the system
	if existingRoles.Contains(rn) {
		logger.Info(fmt.Sprintf("Merging role %q", rn))

		// Only update the comment if we have full control over the role (who and what not ignored)
		if !ignoreWho && !ignoreWhat {
			err2 := s.commentOnRoleIfExists(createComment(accessProvider, true), rn, repo)
			if err2 != nil {
				return fmt.Errorf("error while updating comment on role %q: %s", rn, err2.Error())
			}
		}

		if !ignoreWho || !ignoreInheritance {
			grantsOfRole, err3 := s.retrieveGrantsOfRole(rn, repo)
			if err3 != nil {
				return err3
			}

			usersOfRole := make([]string, 0, len(grantsOfRole))
			rolesOfRole := make([]string, 0, len(grantsOfRole))

			for _, gor := range grantsOfRole {
				if strings.EqualFold(gor.GrantedTo, "USER") {
					usersOfRole = append(usersOfRole, gor.GranteeName)
				} else if strings.EqualFold(gor.GrantedTo, "ROLE") {
					rolesOfRole = append(rolesOfRole, gor.GranteeName)
				} else if strings.EqualFold(gor.GrantedTo, "DATABASE_ROLE") {
					rolesOfRole = append(rolesOfRole, DatabaseRoleActualNameGenerator(cleanDoubleQuotes(gor.GranteeName)))
				}
			}

			if !ignoreWho {
				toAdd := slice.StringSliceDifference(accessProvider.Who.Users, usersOfRole, false)
				toRemove := slice.StringSliceDifference(usersOfRole, accessProvider.Who.Users, false)
				logger.Info(fmt.Sprintf("Identified %d users to add and %d users to remove from role %q", len(toAdd), len(toRemove), rn))

				if len(toAdd) > 0 {
					if isDatabaseRole(rn) {
						return fmt.Errorf("error can not assign users to a database role %q", rn)
					}

					e := repo.GrantUsersToAccountRole(ctx, rn, toAdd...)
					if e != nil {
						return fmt.Errorf("error while assigning users to role %q: %s", rn, e.Error())
					}
				}

				if len(toRemove) > 0 {
					if isDatabaseRole(rn) {
						return fmt.Errorf("error can not unassign users from a database role %q", rn)
					}

					e := repo.RevokeUsersFromAccountRole(ctx, rn, toRemove...)
					if e != nil {
						return fmt.Errorf("error while unassigning users from role %q: %s", rn, e.Error())
					}
				}
			}

			if !ignoreInheritance {
				toAdd := slice.StringSliceDifference(inheritedRoles, rolesOfRole, false)
				toRemove := slice.StringSliceDifference(rolesOfRole, inheritedRoles, false)
				logger.Info(fmt.Sprintf("Identified %d roles to add and %d roles to remove from role %q", len(toAdd), len(toRemove), rn))

				if len(toAdd) > 0 {
					e := s.grantRolesToRole(ctx, repo, rn, toAdd...)
					if e != nil {
						return fmt.Errorf("error while assigning role to role %q: %s", rn, e.Error())
					}
				}

				if len(toRemove) > 0 {
					e := s.revokeRolesFromRole(ctx, repo, rn, toRemove...)
					if e != nil {
						return fmt.Errorf("error while unassigning role from role %q: %s", rn, e.Error())
					}
				}
			}
		}

		if !ignoreWhat {
			// Remove all future grants on schema and database if applicable.
			// Since these are future grants, it's safe to just remove them and re-add them again (if required).
			// We assume nobody manually added others to this role manually.
			for _, what := range accessProvider.What {
				if what.DataObject.Type == "database" {
					e := s.executeRevokeOnRole("ALL", common.FormatQuery(`FUTURE SCHEMAS IN DATABASE %s`, what.DataObject.FullName), rn, repo)
					if e != nil {
						return fmt.Errorf("error while assigning future schema grants in database %q to role %q: %s", what.DataObject.FullName, rn, e.Error())
					}

					e = s.executeRevokeOnRole("ALL", common.FormatQuery(`FUTURE TABLES IN DATABASE %s`, what.DataObject.FullName), rn, repo)
					if e != nil {
						return fmt.Errorf("error while assigning future table grants in database %q to role %q: %s", what.DataObject.FullName, rn, e.Error())
					}
				} else if what.DataObject.Type == "schema" {
					e := s.executeRevokeOnRole("ALL", fmt.Sprintf("FUTURE TABLES IN SCHEMA %s", what.DataObject.FullName), rn, repo)
					if e != nil {
						return fmt.Errorf("error while assigning future table grants in schema %q to role %q: %s", what.DataObject.FullName, rn, e.Error())
					}
				}
			}

			grantsToRole, err3 := s.getGrantsToRole(rn, repo)
			if err3 != nil {
				return err3
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

		if _, rf := rolesCreated[rn]; !rf {
			// Create the role if not exists
			err := s.createRole(rn, repo)
			if err != nil {
				return fmt.Errorf("error while creating role %q: %s", rn, err.Error())
			}

			// Updating the comment (independent of creation)
			err = s.commentOnRoleIfExists(createComment(accessProvider, false), rn, repo)
			if err != nil {
				return fmt.Errorf("error while updating comment on role %q: %s", rn, err.Error())
			}
			rolesCreated[rn] = struct{}{}
		}

		if !ignoreWho && len(accessProvider.Who.Users) > 0 {
			if isDatabaseRole(rn) {
				return fmt.Errorf("error can not assign users to a database role %q", rn)
			}

			err := repo.GrantUsersToAccountRole(ctx, rn, accessProvider.Who.Users...)
			if err != nil {
				return fmt.Errorf("error while assigning users to role %q: %s", rn, err.Error())
			}
		}

		if !ignoreInheritance {
			err := s.grantRolesToRole(ctx, repo, rn, inheritedRoles...)
			if err != nil {
				return fmt.Errorf("error while assigning roles to role %q: %s", rn, err.Error())
			}
		}
	}

	if !ignoreWhat {
		err := s.mergeGrants(repo, rn, foundGrants, expectedGrants, metaData)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *AccessSyncer) getGrantsToRole(roleName string, repo dataAccessRepository) ([]GrantToRole, error) {
	if isDatabaseRole(roleName) {
		database, parsedRoleName, err := parseDatabaseRoleName(roleName)
		if err != nil {
			return nil, err
		}

		return repo.GetGrantsToDatabaseRole(database, parsedRoleName)
	}

	return repo.GetGrantsToAccountRole(roleName)
}

func (s *AccessSyncer) grantRolesToRole(ctx context.Context, repo dataAccessRepository, targetRoleName string, roles ...string) error {
	toAddDatabaseRoles := []string{}

	for _, role := range roles {
		if isDatabaseRole(role) {
			toAddDatabaseRoles = append(toAddDatabaseRoles, role)
		}
	}

	toAddAccountRoles := slice.SliceDifference(roles, toAddDatabaseRoles)

	if isDatabaseRole(targetRoleName) {
		database, parsedRoleName, err := parseDatabaseRoleName(targetRoleName)
		if err != nil {
			return err
		}

		err = repo.GrantDatabaseRolesToDatabaseRole(ctx, database, parsedRoleName, toAddDatabaseRoles...)
		if err != nil {
			return err
		}

		return repo.GrantAccountRolesToDatabaseRole(ctx, database, parsedRoleName, toAddAccountRoles...)
	}

	if len(toAddDatabaseRoles) > 0 {
		return fmt.Errorf("error can not assign database roles to an account role %q - %v", targetRoleName, toAddAccountRoles)
	}

	return repo.GrantAccountRolesToAccountRole(ctx, targetRoleName, toAddAccountRoles...)
}

func (s *AccessSyncer) revokeRolesFromRole(ctx context.Context, repo dataAccessRepository, targetRoleName string, roles ...string) error {
	toAddDatabaseRoles := []string{}

	for _, role := range roles {
		if isDatabaseRole(role) {
			toAddDatabaseRoles = append(toAddDatabaseRoles, role)
		}
	}

	toAddAccountRoles := slice.SliceDifference(roles, toAddDatabaseRoles)

	if isDatabaseRole(targetRoleName) {
		database, parsedRoleName, err := parseDatabaseRoleName(targetRoleName)
		if err != nil {
			return err
		}

		err = repo.RevokeDatabaseRolesFromDatabaseRole(ctx, database, parsedRoleName, toAddDatabaseRoles...)
		if err != nil {
			return err
		}

		return repo.RevokeAccountRolesFromDatabaseRole(ctx, database, parsedRoleName, toAddAccountRoles...)
	}

	if len(toAddDatabaseRoles) > 0 {
		return fmt.Errorf("error can not assign database roles to an account role %q - %v", targetRoleName, toAddAccountRoles)
	}

	return repo.RevokeAccountRolesFromAccountRole(ctx, targetRoleName, toAddAccountRoles...)
}

func (s *AccessSyncer) createRole(roleName string, repo dataAccessRepository) error {
	if isDatabaseRole(roleName) {
		database, cleanedRoleName, err := parseDatabaseRoleName(roleName)
		if err != nil {
			return err
		}

		return repo.CreateDatabaseRole(database, cleanedRoleName)
	}

	return repo.CreateAccountRole(roleName)
}

func (s *AccessSyncer) dropRole(roleName string, repo dataAccessRepository) error {
	if isDatabaseRole(roleName) {
		database, cleanedRoleName, err := parseDatabaseRoleName(roleName)
		if err != nil {
			return err
		}

		return repo.DropDatabaseRole(database, cleanedRoleName)
	}

	return repo.DropAccountRole(roleName)
}

func (s *AccessSyncer) renameRole(oldName, newName string, repo dataAccessRepository) error {
	if isDatabaseRole(oldName) || isDatabaseRole(newName) {
		if !isDatabaseRole(newName) || !isDatabaseRole(oldName) {
			return fmt.Errorf("both roles should be a database role newName:%q - oldName:%q", newName, oldName)
		}

		oldDatabase, oldRoleName, err := parseDatabaseRoleName(oldName)
		if err != nil {
			return err
		}

		newDatabase, newRoleName, err := parseDatabaseRoleName(newName)
		if err != nil {
			return err
		}

		if oldDatabase != newDatabase {
			return fmt.Errorf("expected new roleName %q pointing to the same database as old roleName %q", newName, oldName)
		}

		return repo.RenameDatabaseRole(oldDatabase, oldRoleName, newRoleName)
	}

	return repo.RenameAccountRole(oldName, newName)
}

func (s *AccessSyncer) commentOnRoleIfExists(comment, roleName string, repo dataAccessRepository) error {
	if isDatabaseRole(roleName) {
		database, cleanedRoleName, err := parseDatabaseRoleName(roleName)
		if err != nil {
			return err
		}

		return repo.CommentDatabaseRoleIfExists(comment, database, cleanedRoleName)
	}

	return repo.CommentAccountRoleIfExists(comment, roleName)
}

func (s *AccessSyncer) generateAccessControls(ctx context.Context, apMap map[string]*importer.AccessProvider, existingRoles set.Set[string], renameMap map[string]string, repo dataAccessRepository, configMap *config.ConfigMap, feedbackHandler wrappers.AccessProviderFeedbackHandler) error {
	// We always need the meta data
	syncer := DataSourceSyncer{}
	md, err := syncer.GetDataSourceMetaData(ctx, configMap)

	if err != nil {
		return err
	}

	metaData := s.buildMetaDataMap(md)

	rolesCreated := make(map[string]interface{})

	for rn, accessProvider := range apMap {
		externalId := rn
		apType := access_provider.Role

		if accessProvider.Type != nil {
			apType = *accessProvider.Type
		}

		fi := importer.AccessProviderSyncFeedback{
			AccessProvider: accessProvider.Id,
			ActualName:     rn,
			ExternalId:     &externalId,
			Type:           &apType,
		}

		err2 := s.handleAccessProvider(ctx, rn, apMap, existingRoles, renameMap, rolesCreated, repo, metaData)

		err3 := s.handleAccessProviderFeedback(feedbackHandler, &fi, err2)
		if err3 != nil {
			return err3
		}
	}

	return nil
}

func (s *AccessSyncer) handleAccessProviderFeedback(feedbackHandler wrappers.AccessProviderFeedbackHandler, fi *importer.AccessProviderSyncFeedback, err error) error {
	if err != nil {
		logger.Error(err.Error())
		fi.Errors = append(fi.Errors, err.Error())
	}

	return feedbackHandler.AddAccessProviderFeedback(*fi)
}

func (s *AccessSyncer) updateMask(_ context.Context, mask *importer.AccessProvider, roleNameMap map[string]string, repo dataAccessRepository) (string, error) {
	logger.Info(fmt.Sprintf("Updating mask %q", mask.Name))

	globalMaskName := raitoMaskName(mask.Name)
	uniqueMaskName := raitoMaskUniqueName(mask.Name)

	// Step 0: Load beneficieries
	beneficiaries := MaskingBeneficiaries{
		Users: mask.Who.Users,
	}

	for _, role := range mask.Who.InheritFrom {
		if strings.HasPrefix(role, "ID:") {
			if roleName, found := roleNameMap[role[3:]]; found {
				beneficiaries.Roles = append(beneficiaries.Roles, roleName)
			}
		} else {
			beneficiaries.Roles = append(beneficiaries.Roles, role)
		}
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

		schemaFullName := database + "." + schemaName

		dosPerSchema[schemaFullName] = append(dosPerSchema[schemaFullName], do.DataObject.FullName)
	}

	// Step 1: Get existing masking policies with same prefix
	existingPolicies, err := repo.GetPoliciesLike("MASKING", fmt.Sprintf("%s%s", globalMaskName, "%"))
	if err != nil {
		return uniqueMaskName, err
	}

	// Step 2: For each schema create a new masking policy and force the DataObjects to use the new policy
	for schema, dos := range dosPerSchema {
		logger.Info(fmt.Sprintf("Updating mask %q for schema %q", mask.Name, schema))
		namesplit := strings.Split(schema, ".")

		database := namesplit[0]
		schemaName := namesplit[1]

		err = repo.CreateMaskPolicy(database, schemaName, uniqueMaskName, dos, mask.Type, &beneficiaries)
		if err != nil {
			return uniqueMaskName, err
		}
	}

	// Step 3: Remove old policies that we misted in step 1
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

func (s *AccessSyncer) removeMask(_ context.Context, maskName string, repo dataAccessRepository) error {
	logger.Info(fmt.Sprintf("Remove mask %q", maskName))

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
	s.databasesCache, err = repo.GetDatabases()

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
		schemaType = SharedPrefix + schemaType
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
		dbType = SharedPrefix + dbType
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
		tableType = SharedPrefix + tableType
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

			shareNames, err := s.getShareNames(repo)
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

func (s *AccessSyncer) updateOrCreateFilter(ctx context.Context, repo dataAccessRepository, tableFullName string, aps []*importer.AccessProvider, roleNameMap map[string]string) (string, *string, error) {
	tableFullnameSplit := strings.Split(tableFullName, ".")
	database := tableFullnameSplit[0]
	schema := tableFullnameSplit[1]
	table := tableFullnameSplit[2]

	filterExpressions := make([]string, 0, len(aps))
	arguments := set.NewSet[string]()

	for _, ap := range aps {
		fExpression, apArguments, err := filterExpression(ctx, ap)
		if err != nil {
			return "", nil, fmt.Errorf("failed to generate filter expression for access provider %s: %w", ap.Name, err)
		}

		whoExpressionPart, hasWho := filterWhoExpression(ap, roleNameMap)

		if !hasWho {
			continue
		}

		filterExpressions = append(filterExpressions, fmt.Sprintf("(%s) AND (%s)", whoExpressionPart, fExpression))

		arguments.Add(apArguments...)

		logger.Info(fmt.Sprintf("Filter expression for access provider %s: %s (%+v)", ap.Name, fExpression, apArguments))
	}

	if len(filterExpressions) == 0 {
		// No filter expression for example when no who was defined for the filter
		logger.Info("No filter expressions found for table %s.", tableFullName)

		filterExpressions = append(filterExpressions, "FALSE")
	}

	filterName := fmt.Sprintf("raito_%s_%s_%s_filter", schema, table, gonanoid.MustGenerate(idAlphabet, 8))

	err := repo.UpdateFilter(database, schema, table, filterName, arguments.Slice(), strings.Join(filterExpressions, " OR "))
	if err != nil {
		return "", nil, fmt.Errorf("failed to update filter %s: %w", filterName, err)
	}

	return filterName, ptr.String(fmt.Sprintf("%s.%s", tableFullName, filterName)), nil
}

func (s *AccessSyncer) deleteFilter(repo dataAccessRepository, tableFullName string, aps []*importer.AccessProvider) error {
	tableFullnameSplit := strings.Split(tableFullName, ".")
	database := tableFullnameSplit[0]
	schema := tableFullnameSplit[1]
	table := tableFullnameSplit[2]

	filterNames := set.NewSet[string]()

	for _, ap := range aps {
		if ap.ExternalId != nil {
			externalIdSplit := strings.Split(*ap.ExternalId, ".")
			filterNames.Add(externalIdSplit[3])
		}
	}

	var err error

	for filterName := range filterNames {
		deleteErr := repo.DropFilter(database, schema, table, filterName)
		if deleteErr != nil {
			err = multierror.Append(err, fmt.Errorf("failed to delete filter %s: %w", filterName, deleteErr))
		}
	}

	if err != nil {
		return err
	}

	return nil
}

func (s *AccessSyncer) executeGrantOnRole(perm, on, roleName string, repo dataAccessRepository) error {
	if isDatabaseRole(roleName) {
		database, parsedRoleName, err := parseDatabaseRoleName(roleName)
		if err != nil {
			return err
		}

		return repo.ExecuteGrantOnDatabaseRole(perm, on, database, parsedRoleName)
	}

	return repo.ExecuteGrantOnAccountRole(perm, on, roleName)
}

func (s *AccessSyncer) executeRevokeOnRole(perm, on, roleName string, repo dataAccessRepository) error {
	if isDatabaseRole(roleName) {
		database, parsedRoleName, err := parseDatabaseRoleName(roleName)
		if err != nil {
			return err
		}

		return repo.ExecuteRevokeOnDatabaseRole(perm, on, database, parsedRoleName)
	}

	return repo.ExecuteRevokeOnAccountRole(perm, on, roleName)
}

func (s *AccessSyncer) mergeGrants(repo dataAccessRepository, role string, found []Grant, expected []Grant, metaData map[string]map[string]struct{}) error {
	toAdd := slice.SliceDifference(expected, found)
	toRemove := slice.SliceDifference(found, expected)

	logger.Info(fmt.Sprintf("Found %d grants to add and %d grants to remove for role %q", len(toAdd), len(toRemove), role))

	for _, grant := range toAdd {
		if verifyGrant(grant, metaData) {
			err := s.executeGrantOnRole(grant.Permissions, grant.GetGrantOnType()+" "+grant.On, role, repo)
			if err != nil {
				return err
			}
		}
	}

	for _, grant := range toRemove {
		if verifyGrant(grant, metaData) {
			err := s.executeRevokeOnRole(grant.Permissions, grant.GetGrantOnType()+" "+grant.On, role, repo)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func filterExpression(ctx context.Context, ap *importer.AccessProvider) (string, []string, error) {
	if ap.FilterCriteria != nil {
		filterQueryBuilder := NewFilterCriteriaBuilder()

		err := ap.FilterCriteria.Accept(ctx, filterQueryBuilder)
		if err != nil {
			return "", nil, fmt.Errorf("failed to generate filter expression for access provider %s: %w", ap.Name, err)
		}

		query, arguments := filterQueryBuilder.GetQueryAndArguments()

		return query, arguments.Slice(), nil
	} else if ap.PolicyRule != nil {
		query, arguments := filterExpressionOfPolicyRule(*ap.PolicyRule)

		return query, arguments, nil
	}

	return "", nil, errors.New("no filter criteria or policy rule")
}

func filterExpressionOfPolicyRule(policyRule string) (string, []string) {
	argumentRegexp := regexp.MustCompile(`\{([a-zA-Z0-9]+)\}`)

	argumentsSubMatches := argumentRegexp.FindAllStringSubmatch(policyRule, -1)
	query := argumentRegexp.ReplaceAllString(policyRule, "$1")

	arguments := make([]string, 0, len(argumentsSubMatches))
	for _, match := range argumentsSubMatches {
		arguments = append(arguments, match[1])
	}

	return query, arguments
}

func filterWhoExpression(ap *importer.AccessProvider, roleNameMap map[string]string) (string, bool) {
	whoExpressionParts := make([]string, 0, 2)

	{
		users := make([]string, 0, len(ap.Who.Users))

		for _, user := range ap.Who.Users {
			users = append(users, fmt.Sprintf("'%s'", user))
		}

		if len(users) > 0 {
			whoExpressionParts = append(whoExpressionParts, fmt.Sprintf("current_user() IN (%s)", strings.Join(users, ", ")))
		}
	}

	{
		roles := make([]string, 0, len(ap.Who.InheritFrom))

		for _, role := range ap.Who.InheritFrom {
			if strings.HasPrefix(role, "ID:") {
				if roleName, found := roleNameMap[role[3:]]; found {
					roles = append(roles, fmt.Sprintf("'%s'", roleName))
				}
			} else {
				roles = append(roles, fmt.Sprintf("'%s'", role))
			}
		}

		if len(roles) > 0 {
			whoExpressionParts = append(whoExpressionParts, fmt.Sprintf("current_role() IN (%s)", strings.Join(roles, ", ")))
		}
	}

	if len(whoExpressionParts) == 0 {
		return "FALSE", false
	}

	return strings.Join(whoExpressionParts, " OR "), true
}

func groupFiltersByTable(aps map[string]*importer.AccessProvider, feedbackHandler wrappers.AccessProviderFeedbackHandler) (map[string][]*importer.AccessProvider, error) {
	groupedFilters := make(map[string][]*importer.AccessProvider)

	for key, filter := range aps {
		if len(filter.What) != 1 || filter.What[0].DataObject.Type != ds.Table {
			err := feedbackHandler.AddAccessProviderFeedback(importer.AccessProviderSyncFeedback{
				AccessProvider: filter.Id,
				Errors:         []string{"Filters can only be applied to a single table."},
			})

			if err != nil {
				return nil, fmt.Errorf("failed to add access provider feedback: %w", err)
			}

			continue
		}

		table := filter.What[0].DataObject.FullName

		groupedFilters[table] = append(groupedFilters[table], aps[key])
	}

	return groupedFilters, nil
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
	result := fmt.Sprintf("%s%s", maskPrefix, strings.ReplaceAll(strings.ToUpper(name), " ", "_"))

	var validMaskName []rune

	for _, r := range result {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			validMaskName = append(validMaskName, r)
		}
	}

	return string(validMaskName)
}

func raitoMaskUniqueName(name string) string {
	return raitoMaskName(name) + "_" + gonanoid.MustGenerate(idAlphabet, 8)
}

func isDatabaseRole(roleName string) bool {
	return strings.HasPrefix(roleName, databaseRolePrefix)
}

func parseDatabaseRoleName(roleName string) (database string, cleanedRoleName string, err error) {
	if !isDatabaseRole(roleName) {
		return "", "", fmt.Errorf("role %q is not a database role", roleName)
	}

	roleNameWithoutPrefix := strings.TrimPrefix(roleName, databaseRolePrefix)

	parts := strings.Split(roleNameWithoutPrefix, ".")
	if (parts == nil) || (len(parts) < 2) {
		return "", "", fmt.Errorf("role %q is not a database role", roleName)
	}

	database = parts[0]
	cleanedRoleName = parts[1]

	return database, cleanedRoleName, nil
}

func DatabaseRoleActualNameGenerator(databaseRoleName string) string {
	return fmt.Sprintf("%s%s", databaseRolePrefix, databaseRoleName)
}

func DatabaseRoleNameGenerator(database, roleName string) string {
	return fmt.Sprintf("%s.%s", database, roleName)
}
