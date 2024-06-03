package snowflake

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode"

	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/raito-io/cli/base/access_provider/sync_to_target"
	"github.com/raito-io/cli/base/access_provider/sync_to_target/naming_hint"
	"github.com/raito-io/cli/base/tag"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers"
	"github.com/raito-io/golang-set/set"
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
)

//go:generate go run github.com/vektra/mockery/v2 --name=dataAccessRepository --with-expecter --inpackage
type dataAccessRepository interface {
	Close() error
	CommentAccountRoleIfExists(comment, objectName string) error
	CommentDatabaseRoleIfExists(comment, database, roleName string) error
	CreateAccountRole(roleName string) error
	CreateDatabaseRole(database, roleName string) error
	CreateMaskPolicy(databaseName string, schema string, maskName string, columnsFullName []string, maskType *string, beneficiaries *MaskingBeneficiaries) error
	DescribePolicy(policyType, dbName, schema, policyName string) ([]DescribePolicyEntity, error)
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
	GetPolicyReferences(dbName, schema, policyName string) ([]PolicyReferenceEntity, error)
	GetSchemasInDatabase(databaseName string, handleEntity EntityHandler) error
	GetShares() ([]DbEntity, error)
	GetTablesInDatabase(databaseName string, schemaName string, handleEntity EntityHandler) error
	GetTagsByDomain(domain string) (map[string][]*tag.Tag, error)
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

var _ wrappers.AccessProviderSyncer = (*AccessSyncer)(nil)

type AccessSyncer struct {
	repoProvider                  func(params map[string]string, role string) (dataAccessRepository, error)
	tablesPerSchemaCache          map[string][]TableEntity
	schemasPerDataBaseCache       map[string][]SchemaEntity
	databasesCache                []DbEntity
	warehousesCache               []DbEntity
	namingConstraints             naming_hint.NamingConstraints
	uniqueRoleNameGeneratorsCache map[*string]naming_hint.UniqueGenerator
	ignoreLinksToRole             []string
}

func NewDataAccessSyncer(namingConstraints naming_hint.NamingConstraints) *AccessSyncer {
	return &AccessSyncer{
		repoProvider:                  newDataAccessSnowflakeRepo,
		tablesPerSchemaCache:          make(map[string][]TableEntity),
		schemasPerDataBaseCache:       make(map[string][]SchemaEntity),
		uniqueRoleNameGeneratorsCache: make(map[*string]naming_hint.UniqueGenerator),
		namingConstraints:             namingConstraints,
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

	logger.Info("Reading account and database roles from Snowflake")

	shares, err := s.getShareNames(repo)
	if err != nil {
		return err
	}

	logger.Info("Reading account roles from Snowflake")

	err = s.importAllRolesOnAccountLevel(accessProviderHandler, repo, shares, configMap)
	if err != nil {
		return err
	}

	databaseRoleSupportEnabled := configMap.GetBoolWithDefault(SfDatabaseRoles, false)
	if databaseRoleSupportEnabled {
		logger.Info("Reading database roles from Snowflake")
		excludedDatabases := s.extractExcludeDatabases(configMap)

		err = s.importAllRolesOnDatabaseLevel(accessProviderHandler, repo, excludedDatabases, shares, configMap)
		if err != nil {
			return err
		}
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

func (s *AccessSyncer) SyncAccessProviderToTarget(ctx context.Context, accessProviders *sync_to_target.AccessProviderImport, accessProviderFeedbackHandler wrappers.AccessProviderFeedbackHandler, configMap *config.ConfigMap) error {
	repo, err := s.repoProvider(configMap.Parameters, "")
	if err != nil {
		return err
	}

	defer func() {
		logger.Info(fmt.Sprintf("Total snowflake query time:  %s", repo.TotalQueryTime()))
		repo.Close()
	}()

	apList := accessProviders.AccessProviders
	apIdNameMap := make(map[string]string)

	masksMap := make(map[string]*sync_to_target.AccessProvider)
	masksToRemove := make(map[string]*sync_to_target.AccessProvider)

	filtersMap := make(map[string]*sync_to_target.AccessProvider)
	filtersToRemove := make(map[string]*sync_to_target.AccessProvider)

	rolesMap := make(map[string]*sync_to_target.AccessProvider)
	rolesToRemove := make(map[string]*sync_to_target.AccessProvider)

	for _, ap := range apList {
		var err2 error

		switch ap.Action {
		case sync_to_target.Mask:
			_, masksMap, masksToRemove, err2 = s.syncAccessProviderToTargetHandler(ap, masksMap, masksToRemove, accessProviderFeedbackHandler)
		case sync_to_target.Filtered:
			_, filtersMap, filtersToRemove, err2 = s.syncAccessProviderToTargetHandler(ap, filtersMap, filtersToRemove, accessProviderFeedbackHandler)
		case sync_to_target.Grant, sync_to_target.Purpose:
			var externalId string
			externalId, rolesMap, rolesToRemove, err2 = s.syncAccessProviderToTargetHandler(ap, rolesMap, rolesToRemove, accessProviderFeedbackHandler)
			apIdNameMap[ap.Id] = externalId
		case sync_to_target.Deny, sync_to_target.Promise:
		default:
			err2 = accessProviderFeedbackHandler.AddAccessProviderFeedback(sync_to_target.AccessProviderSyncFeedback{
				AccessProvider: ap.Id,
				Errors:         []string{fmt.Sprintf("Unsupported action %s", ap.Action.String())},
			})
		}

		if err2 != nil {
			return err2
		}
	}

	// Step 1 first initiate all the masks
	if len(masksMap)+len(masksToRemove) > 0 {
		err = s.SyncAccessProviderMasksToTarget(ctx, masksToRemove, masksMap, apIdNameMap, accessProviderFeedbackHandler, configMap, repo)
		if err != nil {
			return fmt.Errorf("sync masks to target: %w", err)
		}
	}

	// Step 2 then initialize all filters
	if len(filtersMap)+len(filtersToRemove) > 0 {
		err = s.SyncAccessProviderFiltersToTarget(ctx, filtersToRemove, filtersMap, apIdNameMap, accessProviderFeedbackHandler, configMap, repo)
		if err != nil {
			return fmt.Errorf("sync filters to target: %w", err)
		}
	}

	// Step 3 then initiate all the roles
	err = s.SyncAccessProviderRolesToTarget(ctx, rolesToRemove, rolesMap, accessProviderFeedbackHandler, configMap, repo)
	if err != nil {
		return fmt.Errorf("sync roles to target: %w", err)
	}

	return nil
}

func (s *AccessSyncer) syncAccessProviderToTargetHandler(ap *sync_to_target.AccessProvider, toProcessAps map[string]*sync_to_target.AccessProvider, apToRemoveMap map[string]*sync_to_target.AccessProvider, accessProviderFeedbackHandler wrappers.AccessProviderFeedbackHandler) (string, map[string]*sync_to_target.AccessProvider, map[string]*sync_to_target.AccessProvider, error) {
	var externalId string

	if ap.Delete {
		if ap.ExternalId == nil {
			logger.Warn(fmt.Sprintf("No externalId defined for deleted access provider %q. This will be ignored", ap.Id))

			err := accessProviderFeedbackHandler.AddAccessProviderFeedback(sync_to_target.AccessProviderSyncFeedback{
				AccessProvider: ap.Id,
			})
			if err != nil {
				return "", nil, nil, err
			}

			return "", toProcessAps, apToRemoveMap, nil
		}

		externalId = *ap.ExternalId

		apToRemoveMap[externalId] = ap
	} else {
		uniqueExternalId, err := s.generateUniqueExternalId(ap, "")
		if err != nil {
			return "", nil, nil, err
		}

		externalId = uniqueExternalId
		if _, f := toProcessAps[externalId]; !f {
			toProcessAps[externalId] = ap
		}
	}

	return externalId, toProcessAps, apToRemoveMap, nil
}

func raitoMaskName(roleName string) string {
	roleNameWithoutPrefix := strings.TrimPrefix(roleName, maskPrefix)

	result := fmt.Sprintf("%s%s", maskPrefix, strings.ReplaceAll(strings.ToUpper(roleNameWithoutPrefix), " ", "_"))

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

func (s *AccessSyncer) getAllAvailableDatabases(repo dataAccessRepository) ([]DbEntity, error) {
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

func (s *AccessSyncer) getApplicableDatabases(repo dataAccessRepository, dbExcludes set.Set[string]) ([]DbEntity, error) {
	allDatabases, err := s.getAllAvailableDatabases(repo)
	if err != nil {
		return nil, err
	}

	filteredDatabases := make([]DbEntity, 0)

	for _, db := range allDatabases {
		if !dbExcludes.Contains(db.Name) {
			filteredDatabases = append(filteredDatabases, db)
		}
	}

	return filteredDatabases, nil
}

func (s *AccessSyncer) extractExcludeDatabases(configMap *config.ConfigMap) set.Set[string] {
	excludedDatabases := "SNOWFLAKE"
	if v, ok := configMap.Parameters[SfExcludedDatabases]; ok {
		excludedDatabases = v
	}

	return parseCommaSeparatedList(excludedDatabases)
}
