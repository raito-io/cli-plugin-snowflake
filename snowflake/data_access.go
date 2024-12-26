package snowflake

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/smithy-go/ptr"
	"github.com/raito-io/cli-plugin-snowflake/common"
	"github.com/raito-io/cli/base/access_provider/sync_to_target"
	"github.com/raito-io/cli/base/access_provider/sync_to_target/naming_hint"
	"github.com/raito-io/cli/base/tag"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers"
	"github.com/raito-io/golang-set/set"
)

var RolesNotInternalizable = []string{"ORGADMIN", "ACCOUNTADMIN", "SECURITYADMIN", "USERADMIN", "SYSADMIN", "PUBLIC"}
var AcceptedTypes = map[string]struct{}{"ACCOUNT": {}, "WAREHOUSE": {}, "DATABASE": {}, "SCHEMA": {}, "TABLE": {}, "VIEW": {}, "COLUMN": {}, "SHARED-DATABASE": {}, "EXTERNAL_TABLE": {}, "MATERIALIZED_VIEW": {}, "FUNCTION": {}}

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
	GetSnowFlakeAccountName() (string, error)
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
	ExecuteGrantOnAccountRole(perm, on, role string, isSystemGrant bool) error
	ExecuteGrantOnDatabaseRole(perm, on, database, databaseRole string) error
	ExecuteRevokeOnAccountRole(perm, on, role string, isSystemGrant bool) error
	ExecuteRevokeOnDatabaseRole(perm, on, database, databaseRole string) error
	GetAccountRoles() ([]RoleEntity, error)
	GetOutboundShares() ([]ShareEntity, error)
	GetAccountRolesWithPrefix(prefix string) ([]RoleEntity, error)
	GetDatabaseRoles(database string) ([]RoleEntity, error)
	GetDatabaseRolesWithPrefix(database string, prefix string) ([]RoleEntity, error)
	GetDatabases() ([]DbEntity, error)
	GetGrantsOfAccountRole(roleName string) ([]GrantOfRole, error)
	GetGrantsOfDatabaseRole(database, roleName string) ([]GrantOfRole, error)
	GetGrantsToAccountRole(roleName string) ([]GrantToRole, error)
	GetGrantsToShare(shareName string) ([]GrantToRole, error)
	GetGrantsToDatabaseRole(database, roleName string) ([]GrantToRole, error)
	GetPolicies(policy string) ([]PolicyEntity, error)
	GetPoliciesLike(policy string, like string) ([]PolicyEntity, error)
	GetPolicyReferences(dbName, schema, policyName string) ([]PolicyReferenceEntity, error)
	GetSchemasInDatabase(databaseName string, handleEntity EntityHandler) error
	GetInboundShares() ([]DbEntity, error)
	GetTablesInDatabase(databaseName string, schemaName string, handleEntity EntityHandler) error
	GetFunctionsInDatabase(databaseName string, handleEntity EntityHandler) error
	GetTagsByDomain(domain string) (map[string][]*tag.Tag, error)
	GetDatabaseRoleTags(databaseName string, roleName string) (map[string][]*tag.Tag, error)
	GetWarehouses() ([]DbEntity, error)
	GrantAccountRolesToAccountRole(ctx context.Context, role string, roles ...string) error
	GrantAccountRolesToDatabaseRole(ctx context.Context, database string, databaseRole string, accountRoles ...string) error
	GrantDatabaseRolesToDatabaseRole(ctx context.Context, database string, databaseRole string, databaseRoles ...string) error
	GrantSharesToDatabaseRole(ctx context.Context, database string, databaseRole string, shares ...string) error
	GrantUsersToAccountRole(ctx context.Context, role string, users ...string) error
	RenameAccountRole(oldName, newName string) error
	RenameDatabaseRole(database, oldName, newName string) error
	RevokeAccountRolesFromAccountRole(ctx context.Context, role string, roles ...string) error
	RevokeAccountRolesFromDatabaseRole(ctx context.Context, database string, databaseRole string, accountRoles ...string) error
	RevokeDatabaseRolesFromDatabaseRole(ctx context.Context, database string, databaseRole string, databaseRoles ...string) error
	RevokeSharesFromDatabaseRole(ctx context.Context, database string, databaseRole string, shares ...string) error
	RevokeUsersFromAccountRole(ctx context.Context, role string, users ...string) error
	TotalQueryTime() time.Duration
	UpdateFilter(databaseName string, schema string, tableName string, filterName string, argumentNames []string, expression string) error
}

var _ wrappers.AccessProviderSyncer = (*AccessSyncer)(nil)

type AccessSyncer struct {
	namingConstraints naming_hint.NamingConstraints
	repoProvider      func(params map[string]string, role string) (dataAccessRepository, error)
	repo              dataAccessRepository
}

func NewDataAccessSyncer(namingConstraints naming_hint.NamingConstraints) *AccessSyncer {
	return &AccessSyncer{
		namingConstraints: namingConstraints,
		repoProvider:      newDataAccessSnowflakeRepo,
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

	s.repo = repo

	defer func() {
		logger.Info(fmt.Sprintf("Total snowflake query time:  %s", s.repo.TotalQueryTime()))
		s.repo.Close()
	}()

	fromTargetSyncer := NewAccessFromTargetSyncer(s, s.repo, accessProviderHandler, configMap)

	return fromTargetSyncer.syncFromTarget()
}

func (s *AccessSyncer) SyncAccessProviderToTarget(ctx context.Context, accessProviders *sync_to_target.AccessProviderImport, accessProviderFeedbackHandler wrappers.AccessProviderFeedbackHandler, configMap *config.ConfigMap) error {
	repo, err := s.repoProvider(configMap.Parameters, "")
	if err != nil {
		return err
	}

	s.repo = repo

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

func (s *AccessSyncer) getInboundShareNames() ([]string, error) {
	dbShares, err := s.repo.GetInboundShares()
	if err != nil {
		return nil, err
	}

	shareNames := make([]string, 0)
	for _, e := range dbShares {
		shareNames = append(shareNames, e.Name)
	}

	return shareNames, nil
}

func (s *AccessSyncer) getDatabaseNames() ([]string, error) {
	databases, err := s.repo.GetDatabases()
	if err != nil {
		return nil, err
	}

	databaseNames := make([]string, 0)
	for _, e := range databases {
		databaseNames = append(databaseNames, e.Name)
	}

	return databaseNames, nil
}

func (s *AccessSyncer) getAllDatabaseAndShareNames() (set.Set[string], error) {
	databases, err := s.getDatabaseNames()
	if err != nil {
		return nil, err
	}

	inboundShares, err := s.getInboundShareNames()
	if err != nil {
		return nil, err
	}

	combinedList := set.NewSet[string]()
	combinedList.Add(databases...)
	combinedList.Add(inboundShares...)

	return combinedList, nil
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

// getFullNameFromGrant creates the full name for Raito WHAT item based on the name and type from the grant definition in Snowflake
func (s *AccessSyncer) getFullNameFromGrant(name, objectType string) string {
	if strings.EqualFold(objectType, "ACCOUNT") {
		accountName, err := s.repo.GetSnowFlakeAccountName()
		if err != nil {
			logger.Error(fmt.Sprintf("Failed to get account name from Snowflake: %s", err.Error()))

			return "UNKNOWN"
		}

		return accountName
	}

	sfObject := common.ParseFullName(name)

	if strings.EqualFold(objectType, Function) && sfObject.Table != nil {
		function := *sfObject.Table

		if strings.Contains(function, "(") {
			funcName := function[:strings.Index(function, "(")] //nolint:gocritic

			paramString := function[strings.Index(function, "(")+1:]
			if strings.Contains(paramString, "):") {
				paramString = paramString[:strings.Index(paramString, "):")] //nolint:gocritic
			}

			paramString = strings.TrimSuffix(paramString, ")")

			params := strings.Split(paramString, ",")
			for i, param := range params {
				p := strings.TrimSpace(param)
				params[i] = p[strings.LastIndex(p, " ")+1:]
			}

			sfObject.Table = ptr.String(fmt.Sprintf(`%q(%s)`, funcName, strings.Join(params, ", ")))
		}
	}

	return sfObject.GetFullName(false)
}
