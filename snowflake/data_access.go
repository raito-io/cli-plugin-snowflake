package snowflake

import (
	"context"
	"fmt"
	"time"

	"github.com/raito-io/cli/base/access_provider/sync_to_target"
	"github.com/raito-io/cli/base/access_provider/sync_to_target/naming_hint"
	"github.com/raito-io/cli/base/tag"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers"
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
	GetDatabaseRoleTags(databaseName string, roleName string) (map[string][]*tag.Tag, error)
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
	namingConstraints naming_hint.NamingConstraints
	repo              dataAccessRepository
	databasesCache    []DbEntity
}

func NewDataAccessSyncer(namingConstraints naming_hint.NamingConstraints) *AccessSyncer {
	return &AccessSyncer{
		namingConstraints: namingConstraints,
	}
}

func (s *AccessSyncer) SyncAccessProvidersFromTarget(ctx context.Context, accessProviderHandler wrappers.AccessProviderHandler, configMap *config.ConfigMap) error {
	if s.repo == nil {
		repo, err := NewSnowflakeRepository(configMap.Parameters, "")
		if err != nil {
			return err
		}

		s.repo = repo
	}

	defer func() {
		logger.Info(fmt.Sprintf("Total snowflake query time:  %s", s.repo.TotalQueryTime()))
		s.repo.Close()
	}()

	fromTargetSyncer := NewAccessFromTargetSyncer(s, s.repo, accessProviderHandler, configMap)

	return fromTargetSyncer.syncFromTarget()
}

func (s *AccessSyncer) SyncAccessProviderToTarget(ctx context.Context, accessProviders *sync_to_target.AccessProviderImport, accessProviderFeedbackHandler wrappers.AccessProviderFeedbackHandler, configMap *config.ConfigMap) error {
	if s.repo == nil {
		repo, err := NewSnowflakeRepository(configMap.Parameters, "")
		if err != nil {
			return err
		}

		s.repo = repo
	}

	defer func() {
		logger.Info(fmt.Sprintf("Total snowflake query time:  %s", s.repo.TotalQueryTime()))
		s.repo.Close()
	}()

	toTargetSyncer := NewAccessToTargetSyncer(s, s.namingConstraints, s.repo, accessProviders, accessProviderFeedbackHandler, configMap)

	return toTargetSyncer.syncToTarget(ctx)
}

//
// Functions used in both the from target and the to target syncers
//

func (s *AccessSyncer) getShareNames() ([]string, error) {
	dbShares, err := s.repo.GetShares()
	if err != nil {
		return nil, err
	}

	shareNames := make([]string, len(dbShares))
	for _, e := range dbShares {
		shareNames = append(shareNames, e.Name)
	}

	return shareNames, nil
}

func (s *AccessSyncer) getGrantsToRole(externalId string, apType *string) ([]GrantToRole, error) {
	if isDatabaseRole(apType) {
		database, parsedRoleName, err := parseDatabaseRoleExternalId(externalId)
		if err != nil {
			return nil, err
		}

		return s.repo.GetGrantsToDatabaseRole(database, parsedRoleName)
	}

	return s.repo.GetGrantsToAccountRole(externalId)
}

func (s *AccessSyncer) getAllAvailableDatabases() ([]DbEntity, error) {
	if s.databasesCache != nil {
		return s.databasesCache, nil
	}

	var err error
	s.databasesCache, err = s.repo.GetDatabases()

	if err != nil {
		s.databasesCache = nil
		return nil, err
	}

	return s.databasesCache, nil
}

func (s *AccessSyncer) retrieveGrantsOfRole(externalId string, apType *string) (grantOfEntities []GrantOfRole, err error) {
	if isDatabaseRole(apType) {
		database, parsedRoleName, err2 := parseDatabaseRoleExternalId(externalId)
		if err2 != nil {
			return nil, err2
		}

		grantOfEntities, err = s.repo.GetGrantsOfDatabaseRole(database, parsedRoleName)
	} else {
		grantOfEntities, err = s.repo.GetGrantsOfAccountRole(externalId)
	}

	return grantOfEntities, err
}
