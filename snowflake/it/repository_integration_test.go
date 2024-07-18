//go:build integration

package it

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/smithy-go/ptr"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/raito-io/cli-plugin-snowflake/snowflake"
)

type RepositoryTestSuite struct {
	SnowflakeTestSuite
	repo *snowflake.SnowflakeRepository
}

func TestRepositoryTestSuite(t *testing.T) {
	ts := RepositoryTestSuite{}
	repo, err := snowflake.NewSnowflakeRepository(ts.getConfig().Parameters, "")

	if err != nil {
		panic(err)
	}

	defer repo.Close()

	ts.repo = repo
	suite.Run(t, &ts)
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_GetAccountRoles() {
	//When
	roles, err := s.repo.GetAccountRoles()

	//Then
	s.NoError(err)
	s.True(len(roles) >= 6)

	roleNames := make([]string, 0, len(roles))
	for _, role := range roles {
		roleNames = append(roleNames, role.Name)
	}

	s.Contains(roleNames, "SYSADMIN")
	s.Contains(roleNames, "ORGADMIN")
	s.Contains(roleNames, "ACCOUNTADMIN")
	s.Contains(roleNames, "SECURITYADMIN")
	s.Contains(roleNames, "USERADMIN")
	s.Contains(roleNames, "PUBLIC")
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_GetAccountRolesWithPrefix() {
	//When
	roles, err := s.repo.GetAccountRolesWithPrefix("SYS")

	//Then
	s.NoError(err)
	s.True(len(roles) >= 1)

	roleNames := make([]string, 0, len(roles))
	for _, role := range roles {
		roleNames = append(roleNames, role.Name)

		if !strings.HasPrefix(role.Name, "SYS") {
			s.Failf("Role %s should have prefix 'SYS'", role.Name)
		}
	}

	s.Contains(roleNames, "SYSADMIN")
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_CreateAccountRole() {
	//Given
	roleName := fmt.Sprintf("%s_REPO_TEST_CREATE_ROLE_TEST", testId)

	//When
	err := s.repo.CreateAccountRole(roleName)

	//Then
	s.NoError(err)

	roles, err := s.repo.GetAccountRolesWithPrefix(testId)
	s.NoError(err)
	s.Contains(roles, snowflake.RoleEntity{
		Name:            roleName,
		Owner:           "ACCOUNTADMIN",
		GrantedRoles:    0,
		GrantedToRoles:  0,
		AssignedToUsers: 0,
	})
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_DropAccountRole() {
	//Given
	roleName := fmt.Sprintf("%s_REPO_TEST_CREATE_ROLE_TEST", testId)
	err := s.repo.CreateAccountRole(roleName)
	s.NoError(err)

	//When
	err = s.repo.DropAccountRole(roleName)

	//Then
	s.NoError(err)
	roles, err := s.repo.GetAccountRolesWithPrefix(testId)
	s.NoError(err)

	roleNames := make([]string, 0, len(roles))
	for _, role := range roles {
		roleNames = append(roleNames, role.Name)
	}

	s.NotContains(roleNames, roleName)
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_RenameAccountRole() {
	//Given
	originalRoleName := fmt.Sprintf("%s_REPO_TEST_RENAME_ROLE_TEST", testId)
	newExpectedRoleName := fmt.Sprintf("%s_REPO_TEST_RENAME_ROLE_TEST_NEW", testId)
	err := s.repo.CreateAccountRole(originalRoleName)
	s.NoError(err)

	//When
	err = s.repo.RenameAccountRole(originalRoleName, newExpectedRoleName)

	//Then
	s.NoError(err)
	roles, err := s.repo.GetAccountRolesWithPrefix(testId)
	s.NoError(err)

	roleNames := make([]string, 0, len(roles))
	for _, role := range roles {
		roleNames = append(roleNames, role.Name)
	}

	s.NotContains(roleNames, originalRoleName)
	s.Contains(roleNames, newExpectedRoleName)
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_GetGrantsToAccountRole() {
	//When
	grantsToRole, err := s.repo.GetGrantsToAccountRole("DATA_ANALYST")

	//Then
	s.NoError(err)

	s.Contains(grantsToRole, snowflake.GrantToRole{
		Privilege: "USAGE",
		GrantedOn: "DATABASE",
		Name:      "RAITO_DATABASE",
	})
	s.Contains(grantsToRole, snowflake.GrantToRole{
		Privilege: "USAGE",
		GrantedOn: "SCHEMA",
		Name:      "RAITO_DATABASE.ORDERING",
	})
	s.Contains(grantsToRole, snowflake.GrantToRole{
		Privilege: "SELECT",
		GrantedOn: "TABLE",
		Name:      "RAITO_DATABASE.ORDERING.SUPPLIER",
	})
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_GetGrantsOfAccountRole() {
	//When
	grantsOfRolePublic, err := s.repo.GetGrantsOfAccountRole("ACCOUNTADMIN")

	//Then
	s.NoError(err)
	s.True(len(grantsOfRolePublic) >= 1)
	s.Contains(grantsOfRolePublic, snowflake.GrantOfRole{
		GrantedTo:   "USER",
		GranteeName: "RAITO",
	})
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_GrantAccountRolesToAccountRole() {
	//When
	originalRoleName := fmt.Sprintf("%s_REPO_TEST_GRANT_R2R", testId)
	rolesToGrants := make([]string, 0, 5)

	for i := 1; i <= 5; i++ {
		rolesToGrants = append(rolesToGrants, fmt.Sprintf("%s_REPO_TEST_GRANT_R2R_%d", testId, i))
	}

	err := s.repo.CreateAccountRole(originalRoleName)
	s.NoError(err)

	//When
	err = s.repo.GrantAccountRolesToAccountRole(context.Background(), originalRoleName, rolesToGrants...)

	//Then
	s.NoError(err)

	grants, err := s.repo.GetGrantsOfAccountRole(originalRoleName)
	s.NoError(err)

	expectedGrants := make([]snowflake.GrantOfRole, 0, len(grants))

	for _, granteeName := range rolesToGrants {
		expectedGrants = append(expectedGrants, snowflake.GrantOfRole{
			GrantedTo:   "ROLE",
			GranteeName: granteeName,
		})
	}

	s.Equal(grants, expectedGrants)
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_RevokeAccountRolesFromAccountRole() {
	//When
	originalRoleName := fmt.Sprintf("%s_REPO_TEST_REVOKE_R2R", testId)
	rolesToGrants := make([]string, 0, 5)

	for i := 1; i <= 5; i++ {
		rolesToGrants = append(rolesToGrants, fmt.Sprintf("%s_REPO_TEST_REVOKE_R2R_%d", testId, i))
	}

	err := s.repo.CreateAccountRole(originalRoleName)
	s.NoError(err)

	err = s.repo.GrantAccountRolesToAccountRole(context.Background(), originalRoleName, rolesToGrants...)
	s.NoError(err)

	//When
	err = s.repo.RevokeAccountRolesFromAccountRole(context.Background(), originalRoleName, rolesToGrants...)

	//Then
	s.NoError(err)

	grants, err := s.repo.GetGrantsOfAccountRole(originalRoleName)
	s.NoError(err)
	s.Empty(grants)
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_GrantUsersToAccountRole() {
	//Given
	roleName := fmt.Sprintf("%s_REPO_TEST_GRANT_USER_TEST", testId)
	err := s.repo.CreateAccountRole(roleName)

	s.NoError(err)

	//When
	err = s.repo.GrantUsersToAccountRole(context.Background(), roleName, snowflakeUserName)

	//Then
	s.NoError(err)

	grants, err := s.repo.GetGrantsOfAccountRole(roleName)
	s.NoError(err)

	s.Equal(grants, []snowflake.GrantOfRole{
		{
			GrantedTo:   "USER",
			GranteeName: snowflakeUserName,
		},
	})
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_RevokeUsersFromAccountRole() {
	//Given
	roleName := fmt.Sprintf("%s_REPO_TEST_GRANT_USER_TEST", testId)
	err := s.repo.CreateAccountRole(roleName)
	s.NoError(err)

	err = s.repo.GrantUsersToAccountRole(context.Background(), roleName, snowflakeUserName)
	s.NoError(err)

	//When
	err = s.repo.RevokeUsersFromAccountRole(context.Background(), roleName, snowflakeUserName)

	//Then
	s.NoError(err)

	grants, err := s.repo.GetGrantsOfAccountRole(roleName)
	s.NoError(err)
	s.Empty(grants)
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_ExecuteGrantOnAccountRole() {
	//Given
	roleName := fmt.Sprintf("%s_REPO_TEST_EXECUTE_GRANT_TEST", testId)
	err := s.repo.CreateAccountRole(roleName)
	s.NoError(err)

	//When
	err = s.repo.ExecuteGrantOnAccountRole("SELECT", "RAITO_DATABASE.ORDERING.ORDERS", roleName)

	//Then
	s.NoError(err)

	grantsTo, err := s.repo.GetGrantsToAccountRole(roleName)
	s.NoError(err)

	s.Equal(grantsTo, []snowflake.GrantToRole{
		{
			Name:      "RAITO_DATABASE.ORDERING.ORDERS",
			GrantedOn: "TABLE",
			Privilege: "SELECT",
		},
	})
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_ExecuteRevokeOnAccountRole() {
	//Given
	roleName := fmt.Sprintf("%s_REPO_TEST_EXECUTE_REVOKE_TEST", testId)
	err := s.repo.CreateAccountRole(roleName)
	s.NoError(err)
	err = s.repo.ExecuteGrantOnAccountRole("SELECT", "RAITO_DATABASE.ORDERING.ORDERS", roleName)
	s.NoError(err)

	//When
	err = s.repo.ExecuteRevokeOnAccountRole("SELECT", "RAITO_DATABASE.ORDERING.ORDERS", roleName)

	//Then
	s.NoError(err)

	grantsTo, err := s.repo.GetGrantsToAccountRole(roleName)
	s.NoError(err)
	s.Empty(grantsTo)
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_GetDatabaseRolesWithPrefix() {
	//When
	roles, err := s.repo.GetDatabaseRolesWithPrefix("RAITO_DATABASE", "RAITO_DB_ROLE_")

	//Then
	s.NoError(err)
	s.True(len(roles) >= 1)

	roleNames := make([]string, 0, len(roles))
	for _, role := range roles {
		roleNames = append(roleNames, role.Name)

		if !strings.HasPrefix(role.Name, "RAITO_DB_ROLE_") {
			s.Failf("Role %s should have prefix 'RAITO_DB_ROLE_'", role.Name)
		}
	}

	s.Contains(roleNames, "RAITO_DB_ROLE_1")
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_CreateDatabaseRole() {
	//Given
	roleName := fmt.Sprintf("%s_REPO_TEST_CREATE_DATABASE_ROLE_TEST", testId)
	database := "RAITO_DATABASE"

	//When
	err := s.repo.CreateDatabaseRole(database, roleName)

	//Then
	s.NoError(err)

	roles, err := s.repo.GetDatabaseRolesWithPrefix(database, testId)
	s.NoError(err)
	s.Contains(roles, snowflake.RoleEntity{
		Name:            roleName,
		Owner:           "ACCOUNTADMIN",
		GrantedRoles:    0,
		GrantedToRoles:  0,
		AssignedToUsers: 0,
	})
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_DropDatabaseRole() {
	//Given
	roleName := fmt.Sprintf("%s_REPO_TEST_DROP_DATABASE_ROLE_TEST", testId)
	database := "RAITO_DATABASE"

	//When
	err := s.repo.CreateDatabaseRole(database, roleName)
	s.NoError(err)

	//Then
	roles, err := s.repo.GetDatabaseRolesWithPrefix(database, testId)
	s.NoError(err)

	roleNames := make([]string, 0, len(roles))
	for _, role := range roles {
		roleNames = append(roleNames, role.Name)
	}

	s.Contains(roleNames, roleName)

	//When
	err = s.repo.DropDatabaseRole(database, roleName)

	//Then
	s.NoError(err)
	roles, err = s.repo.GetDatabaseRolesWithPrefix(database, testId)
	s.NoError(err)

	roleNames = make([]string, 0, len(roles))
	for _, role := range roles {
		roleNames = append(roleNames, role.Name)
	}

	s.NotContains(roleNames, roleName)
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_RenameDatabaseRole() {
	//Given
	originalRoleName := fmt.Sprintf("%s_REPO_TEST_RENAME_DATABASE_ROLE_TEST", testId)
	newExpectedRoleName := fmt.Sprintf("%s_REPO_TEST_RENAME_ROLE_DATABASE_TEST_NEW", testId)
	database := "RAITO_DATABASE"
	err := s.repo.CreateDatabaseRole(database, originalRoleName)
	s.NoError(err)

	//When
	err = s.repo.RenameDatabaseRole(database, originalRoleName, newExpectedRoleName)

	//Then
	s.NoError(err)
	roles, err := s.repo.GetDatabaseRolesWithPrefix(database, testId)
	s.NoError(err)

	roleNames := make([]string, 0, len(roles))
	for _, role := range roles {
		roleNames = append(roleNames, role.Name)
	}

	s.NotContains(roleNames, originalRoleName)
	s.Contains(roleNames, newExpectedRoleName)
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_GetUsers() {
	//When
	users, err := s.repo.GetUsers()

	//Then
	s.NoError(err)
	s.True(len(users) >= 3)

	s.Contains(users, snowflake.UserEntity{
		Name:        "SNOWFLAKE",
		Email:       "",
		Owner:       "",
		DisplayName: "SNOWFLAKE",
		LoginName:   "SNOWFLAKE",
		Type:        "",
	})

	s.Contains(users, snowflake.UserEntity{
		Name:        snowflakeUserName,
		Email:       "",
		Owner:       "ACCOUNTADMIN",
		DisplayName: snowflakeUserName,
		LoginName:   snowflakeUserName,
		Type:        "null",
	})

	s.Contains(users, snowflake.UserEntity{
		Name:        "atkison_a",
		Email:       "a_abbotatkinson7576@raito.io",
		Owner:       "ACCOUNTADMIN",
		DisplayName: "Angelica Abbot Atkinson",
		LoginName:   "ATKISON_A",
		Type:        "SERVICE",
	})
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_GetGrantsToDatabaseRole() {
	//Given
	roleName := "RAITO_DB_ROLE_1"
	database := "RAITO_DATABASE"

	//When
	grantsToRole, err := s.repo.GetGrantsToDatabaseRole(database, roleName)

	//Then
	s.NoError(err)
	s.True(len(grantsToRole) >= 2, "grantsToRole only has %d grants: %+v", len(grantsToRole), grantsToRole)

	s.Contains(grantsToRole, snowflake.GrantToRole{
		Privilege: "USAGE",
		GrantedOn: "DATABASE",
		Name:      "RAITO_DATABASE",
	})
	s.Contains(grantsToRole, snowflake.GrantToRole{
		Privilege: "SELECT",
		GrantedOn: "TABLE",
		Name:      "RAITO_DATABASE.ORDERING.ORDERS",
	})
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_GetGrantsOfDatabaseRole() {
	//Given
	roleName := "RAITO_DB_ROLE_1"
	database := "RAITO_DATABASE"

	//When
	grantsOfRole, err := s.repo.GetGrantsOfDatabaseRole(database, roleName)

	//Then
	s.NoError(err)
	s.True(len(grantsOfRole) >= 1)
	s.Contains(grantsOfRole, snowflake.GrantOfRole{
		GrantedTo:   "ROLE",
		GranteeName: "HUMAN_RESOURCES",
	})
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_GrantAccountRolesToDatabaseRole() {
	//Given
	database := "RAITO_DATABASE"
	originalRoleName := fmt.Sprintf("%s_REPO_TEST_GRANT_R2DBR", testId)
	accountRolesToGrant := make([]string, 0, 5)

	//When

	for i := 1; i <= 5; i++ {
		accountRolesToGrant = append(accountRolesToGrant, fmt.Sprintf("%s_REPO_TEST_GRANT_R2DBR_%d", testId, i))
	}

	err := s.repo.CreateDatabaseRole(database, originalRoleName)
	s.NoError(err)

	//When
	err = s.repo.GrantAccountRolesToDatabaseRole(context.Background(), database, originalRoleName, accountRolesToGrant...)

	//Then
	s.NoError(err)

	grants, err := s.repo.GetGrantsOfDatabaseRole(database, originalRoleName)
	s.NoError(err)

	expectedGrants := make([]snowflake.GrantOfRole, 0, len(grants))

	for _, granteeName := range accountRolesToGrant {
		expectedGrants = append(expectedGrants, snowflake.GrantOfRole{
			GrantedTo:   "ROLE",
			GranteeName: granteeName,
		})
	}

	s.Equal(grants, expectedGrants)
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_GrantDatabaseRolesToDatabaseRole() {
	//Given
	database := "RAITO_DATABASE"
	originalRoleName := fmt.Sprintf("%s_REPO_TEST_GRANT_DBR2DBR", testId)
	databaseRolesToGrant := make([]string, 0, 5)

	//When

	for i := 1; i <= 5; i++ {
		databaseRolesToGrant = append(databaseRolesToGrant, fmt.Sprintf("%s_REPO_TEST_GRANT_DBR2DBR_%d", testId, i))
	}

	err := s.repo.CreateDatabaseRole(database, originalRoleName)
	s.NoError(err)

	//When
	err = s.repo.GrantDatabaseRolesToDatabaseRole(context.Background(), database, originalRoleName, databaseRolesToGrant...)

	//Then
	s.NoError(err)

	grants, err := s.repo.GetGrantsOfDatabaseRole(database, originalRoleName)
	s.NoError(err)

	expectedGrants := make([]snowflake.GrantOfRole, 0, len(grants))

	for _, granteeName := range databaseRolesToGrant {
		expectedGrants = append(expectedGrants, snowflake.GrantOfRole{
			GrantedTo:   "DATABASE_ROLE",
			GranteeName: fmt.Sprintf("%s.%s", database, granteeName),
		})
	}

	s.Equal(grants, expectedGrants)
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_RevokeAccountRolesFromDatabaseRole() {
	//When
	database := "RAITO_DATABASE"
	originalRoleName := fmt.Sprintf("%s_REPO_TEST_REVOKE_R2DBR", testId)
	accountRolesToGrants := make([]string, 0, 5)

	for i := 1; i <= 5; i++ {
		accountRolesToGrants = append(accountRolesToGrants, fmt.Sprintf("%s_REPO_TEST_REVOKE_R2DBR_%d", testId, i))
	}

	err := s.repo.CreateDatabaseRole(database, originalRoleName)
	s.NoError(err)

	err = s.repo.GrantAccountRolesToDatabaseRole(context.Background(), database, originalRoleName, accountRolesToGrants...)
	s.NoError(err)

	//When
	err = s.repo.RevokeAccountRolesFromDatabaseRole(context.Background(), database, originalRoleName, accountRolesToGrants...)

	//Then
	s.NoError(err)

	grants, err := s.repo.GetGrantsOfDatabaseRole(database, originalRoleName)
	s.NoError(err)
	s.Empty(grants)
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_RevokeDatabaseRolesFromDatabaseRole() {
	//When
	database := "RAITO_DATABASE"
	originalRoleName := fmt.Sprintf("%s_REPO_TEST_REVOKE_DBR2DBR", testId)
	databaseRolesToGrants := make([]string, 0, 5)

	for i := 1; i <= 5; i++ {
		databaseRolesToGrants = append(databaseRolesToGrants, fmt.Sprintf("%s_REPO_TEST_REVOKE_DBR2DBR_%d", testId, i))
	}

	err := s.repo.CreateDatabaseRole(database, originalRoleName)
	s.NoError(err)

	err = s.repo.GrantDatabaseRolesToDatabaseRole(context.Background(), database, originalRoleName, databaseRolesToGrants...)
	s.NoError(err)

	//When
	err = s.repo.RevokeDatabaseRolesFromDatabaseRole(context.Background(), database, originalRoleName, databaseRolesToGrants...)

	//Then
	s.NoError(err)

	grants, err := s.repo.GetGrantsOfDatabaseRole(database, originalRoleName)
	s.NoError(err)
	s.Empty(grants)
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_ExecuteGrantOnDatabaseRole() {
	//Given
	database := "RAITO_DATABASE"
	roleName := fmt.Sprintf("%s_REPO_TEST_EXECUTE_GRANT_DATABASEROLE_TEST", testId)
	err := s.repo.CreateDatabaseRole(database, roleName)
	s.NoError(err)

	//When
	err = s.repo.ExecuteGrantOnDatabaseRole("SELECT", "RAITO_DATABASE.ORDERING.ORDERS", database, roleName)

	//Then
	s.NoError(err)

	grantsTo, err := s.repo.GetGrantsToDatabaseRole(database, roleName)
	s.NoError(err)

	s.Equal(grantsTo, []snowflake.GrantToRole{
		{
			Name:      "RAITO_DATABASE",
			GrantedOn: "DATABASE",
			Privilege: "USAGE",
		},
		{
			Name:      "RAITO_DATABASE.ORDERING.ORDERS",
			GrantedOn: "TABLE",
			Privilege: "SELECT",
		},
	})
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_ExecuteRevokeOnDatabaseRole() {
	//Given
	database := "RAITO_DATABASE"
	roleName := fmt.Sprintf("%s_REPO_TEST_EXECUTE_REVOKE_DATABASEROLE_TEST", testId)
	err := s.repo.CreateDatabaseRole(database, roleName)
	s.NoError(err)
	err = s.repo.ExecuteGrantOnDatabaseRole("SELECT", "RAITO_DATABASE.ORDERING.ORDERS", database, roleName)
	s.NoError(err)

	//When
	err = s.repo.ExecuteRevokeOnDatabaseRole("SELECT", "RAITO_DATABASE.ORDERING.ORDERS", database, roleName)

	//Then
	s.NoError(err)

	grantsTo, err := s.repo.GetGrantsToDatabaseRole(database, roleName)
	s.NoError(err)
	s.Equal(grantsTo, []snowflake.GrantToRole{
		{
			Name:      "RAITO_DATABASE",
			GrantedOn: "DATABASE",
			Privilege: "USAGE",
		},
	})
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_GetPolicies() {
	if sfStandardEdition {
		s.T().Skip("Standard edition do not support masking policies")
	}

	//When
	policies, err := s.repo.GetPolicies("MASKING")

	//Then
	require.NoError(s.T(), err)

	s.Contains(policies, snowflake.PolicyEntity{
		Name:         "ORDERING_MASKING_POLICY",
		DatabaseName: "RAITO_DATABASE",
		SchemaName:   "ORDERING",
		Kind:         "MASKING_POLICY",
		Owner:        "ACCOUNTADMIN",
	})
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_DescribePolicy() {
	if sfStandardEdition {
		s.T().Skip("Standard edition do not support masking policies")
	}

	entries, err := s.repo.DescribePolicy("MASKING", "RAITO_DATABASE", "ORDERING", "ORDERING_MASKING_POLICY")
	assert.NoError(s.T(), err)

	assert.Len(s.T(), entries, 1)
	s.Equal(snowflake.DescribePolicyEntity{Name: "ORDERING_MASKING_POLICY", Body: "case\n  when current_role() in ('ACCOUNTADMIN', 'SYSADMIN') then\n    val\n  else\n    '******'\nend"}, entries[0])
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_GetPolicyReferences() {
	if sfStandardEdition {
		s.T().Skip("Standard edition do not support policy references")
	}

	entries, err := s.repo.GetPolicyReferences("RAITO_DATABASE", "ORDERING", "ORDERING_MASKING_POLICY")
	assert.NoError(s.T(), err)

	assert.Len(s.T(), entries, 1)

	s.Equal(snowflake.PolicyReferenceEntity{
		POLICY_DB:            "RAITO_DATABASE",
		POLICY_SCHEMA:        "ORDERING",
		POLICY_NAME:          "ORDERING_MASKING_POLICY",
		POLICY_KIND:          "MASKING_POLICY",
		REF_DATABASE_NAME:    "RAITO_DATABASE",
		REF_SCHEMA_NAME:      "ORDERING",
		REF_ENTITY_NAME:      "SUPPLIER",
		REF_ENTITY_DOMAIN:    "TABLE",
		REF_COLUMN_NAME:      snowflake.NullString{String: "NAME", Valid: true},
		REF_ARG_COLUMN_NAMES: snowflake.NullString{String: "", Valid: false},
		TAG_DATABASE:         snowflake.NullString{String: "", Valid: false},
		TAG_SCHEMA:           snowflake.NullString{String: "", Valid: false},
		TAG_NAME:             snowflake.NullString{String: "", Valid: false},
		POLICY_STATUS:        "ACTIVE",
	}, entries[0])
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_GetSnowFlakeAccountName() {
	//When
	accountName, err := s.repo.GetSnowFlakeAccountName()

	//Then
	s.NoError(err)
	s.Equal(strings.ToUpper(strings.Split(sfAccount, ".")[0]), accountName)
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_GetWarehouses() {
	//When
	warehouses, err := s.repo.GetWarehouses()

	//Then
	s.NoError(err)
	s.True(len(warehouses) >= 1)

	comment := ""

	s.Contains(warehouses, snowflake.DbEntity{
		Name:    "RAITO_WAREHOUSE",
		Comment: &comment,
	})
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_GetShares() {
	//When
	shares, err := s.repo.GetShares()

	//Then
	s.NoError(err)
	s.True(len(shares) >= 2)

	s.Contains(shares, snowflake.DbEntity{
		Name: "SNOWFLAKE",
	})

	s.Contains(shares, snowflake.DbEntity{
		Name: "SNOWFLAKE_SAMPLE_DATA",
	})
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_GetDatabases() {
	//When
	databases, err := s.repo.GetDatabases()

	//Then
	s.NoError(err)
	s.Len(databases, 3)

	comment := ""

	s.Contains(databases, snowflake.DbEntity{
		Name:    "SNOWFLAKE",
		Comment: &comment,
	})

	s.Contains(databases, snowflake.DbEntity{
		Name:    "SNOWFLAKE_SAMPLE_DATA",
		Comment: &comment,
	})

	comment = "Database for RAITO testing and demo"

	s.Contains(databases, snowflake.DbEntity{
		Name:    "RAITO_DATABASE",
		Comment: &comment,
	})
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_GetSchemasInDatabase() {
	//Given
	database := "RAITO_DATABASE"

	//When
	schemas := make([]snowflake.SchemaEntity, 0)
	err := s.repo.GetSchemasInDatabase(database, func(entity interface{}) error {
		schemas = append(schemas, *entity.(*snowflake.SchemaEntity))
		return nil
	})

	//Then
	s.NoError(err)
	s.Len(schemas, 3)

	comment := "Schema for RAITO testing and demo"

	s.Contains(schemas, snowflake.SchemaEntity{
		Database: "RAITO_DATABASE",
		Name:     "PUBLIC",
		Comment:  nil,
	})

	s.Contains(schemas, snowflake.SchemaEntity{
		Database: "RAITO_DATABASE",
		Name:     "ORDERING",
		Comment:  &comment,
	})

	comment = "Views describing the contents of schemas in this database"

	s.Contains(schemas, snowflake.SchemaEntity{
		Database: "RAITO_DATABASE",
		Name:     "INFORMATION_SCHEMA",
		Comment:  &comment,
	})
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_GetTablesInSchema() {
	//Given
	database := "RAITO_DATABASE"
	schema := "ORDERING"

	//When
	tables := make([]snowflake.TableEntity, 0)
	err := s.repo.GetTablesInDatabase(database, schema, func(entity interface{}) error {
		tables = append(tables, *entity.(*snowflake.TableEntity))
		return nil
	})

	//Then
	s.NoError(err)
	s.Len(tables, 5)

	expected := []snowflake.TableEntity{
		{
			Database:  "RAITO_DATABASE",
			Schema:    "ORDERING",
			Name:      "ORDERS_LIMITED",
			TableType: "VIEW",
			Comment:   ptr.String("Non-materialized view with limited data"),
		},
		{
			Database:  "RAITO_DATABASE",
			Schema:    "ORDERING",
			Name:      "ORDERS",
			TableType: "BASE TABLE",
		},
		{
			Database:  "RAITO_DATABASE",
			Schema:    "ORDERING",
			Name:      "CUSTOMER",
			TableType: "BASE TABLE",
		},
		{
			Database:  "RAITO_DATABASE",
			Schema:    "ORDERING",
			Name:      "SUPPLIER",
			TableType: "BASE TABLE",
		},
		{
			Database:  "RAITO_DATABASE",
			Schema:    "ORDERING",
			Name:      "CUSTOMERS_LIMITED",
			TableType: "MATERIALIZED VIEW",
			Comment:   ptr.String("Materialized view with limited data"),
		},
	}

	s.ElementsMatch(expected, tables)
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_GetColumnsInTable() {
	//Given
	database := "RAITO_DATABASE"
	schema := "ORDERING"
	table := "ORDERS"

	//When
	columns := make([]snowflake.ColumnEntity, 0)
	err := s.repo.GetColumnsInDatabase(database, func(entity interface{}) error {
		column := entity.(*snowflake.ColumnEntity)
		if column.Schema == schema && column.Table == table {
			columns = append(columns, *column)
		}
		return nil
	})

	//Then
	s.NoError(err)
	s.Len(columns, 9)

	s.ElementsMatch(columns, []snowflake.ColumnEntity{
		{
			Database: database,
			Schema:   schema,
			Table:    table,
			Name:     "CLERK",
			Comment:  ptr.String(""),
			DataType: "TEXT",
		},
		{
			Database: database,
			Schema:   schema,
			Table:    table,
			Name:     "COMMENT",
			Comment:  ptr.String(""),
			DataType: "TEXT",
		},
		{
			Database: database,
			Schema:   schema,
			Table:    table,
			Name:     "CUSTKEY",
			Comment:  ptr.String(""),
			DataType: "NUMBER",
		},
		{
			Database: database,
			Schema:   schema,
			Table:    table,
			Name:     "ORDERDATE",
			Comment:  ptr.String(""),
			DataType: "DATE",
		},
		{
			Database: database,
			Schema:   schema,
			Table:    table,
			Name:     "ORDERKEY",
			Comment:  ptr.String(""),
			DataType: "NUMBER",
		},
		{
			Database: database,
			Schema:   schema,
			Table:    table,
			Name:     "ORDERPRIORITY",
			Comment:  ptr.String(""),
			DataType: "TEXT",
		},
		{
			Database: database,
			Schema:   schema,
			Table:    table,
			Name:     "ORDERSTATUS",
			Comment:  ptr.String(""),
			DataType: "TEXT",
		},
		{
			Database: database,
			Schema:   schema,
			Table:    table,
			Name:     "SHIPPRIORITY",
			Comment:  ptr.String(""),
			DataType: "NUMBER",
		},
		{
			Database: database,
			Schema:   schema,
			Table:    table,
			Name:     "TOTALPRICE",
			Comment:  ptr.String(""),
			DataType: "NUMBER",
		},
	})
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_CommentAccountRoleIfExists_NonExistingRole() {
	//Given
	roleName := fmt.Sprintf("%s_REPO_TEST_COMMENT_NON_EXISTING_ROLE", testId)

	//When
	err := s.repo.CommentAccountRoleIfExists("SomeComment", roleName)

	//Then
	s.NoError(err)
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_CommentAccountRoleIfExists_Role() {
	//Given
	roleName := fmt.Sprintf("%s_REPO_TEST_COMMENT_EXISTING_ROLE", testId)
	err := s.repo.CreateAccountRole(roleName)

	comment := "Some comment"

	//When
	err = s.repo.CommentAccountRoleIfExists(comment, roleName)

	//Then
	s.NoError(err)
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_CommentDatabaseRoleIfExists_NonExistingRole() {
	//Given
	database := "SNOWFLAKE_INTEGRATION_TEST"
	roleName := fmt.Sprintf("%s_REPO_TEST_COMMENT_NON_EXISTING_DB_ROLE", testId)

	//When
	err := s.repo.CommentDatabaseRoleIfExists("SomeComment", database, roleName)

	//Then
	s.NoError(err)
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_CommentDatabaseRoleIfExists_Role() {
	//Given
	database := "SNOWFLAKE_INTEGRATION_TEST"
	roleName := fmt.Sprintf("%s_REPO_TEST_COMMENT_EXISTING_DB_ROLE", testId)
	err := s.repo.CreateDatabaseRole(database, roleName)

	comment := "Some comment"

	//When
	err = s.repo.CommentDatabaseRoleIfExists(comment, database, roleName)

	//Then
	s.NoError(err)
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_CreateMaskPolicy() {
	if sfStandardEdition {
		s.T().Skip("Skip test as Masking is a non standard edition feature")
	}

	// Given
	database := "RAITO_DATABASE"
	schema := "ORDERING"
	table := "SUPPLIER"
	column := "ADDRESS"

	beneficiary := snowflake.MaskingBeneficiaries{
		Roles: []string{"HUMAN_RESOURCES"},
	}

	maskName := strings.Join([]string{"MaskingTest", gonanoid.MustGenerate("0123456789ABCDEF", 8)}, "_")

	// When
	err := s.repo.CreateMaskPolicy(database, schema, maskName, []string{fmt.Sprintf("%s.%s.%s.%s", database, schema, table, column)}, ptr.String("NULL_MASK"), &beneficiary)

	// Then
	require.NoError(s.T(), err)

	policyEntries, err := s.repo.GetPoliciesLike("MASKING", fmt.Sprintf("%s%s", maskName, "%"))
	require.NoError(s.T(), err)
	require.Len(s.T(), policyEntries, 1)
	assert.True(s.T(), strings.HasPrefix(policyEntries[0].Name, strings.ToUpper(maskName)))

	// When
	err = s.repo.DropMaskingPolicy(database, schema, maskName)

	// Then
	require.NoError(s.T(), err)

	policyEntries, err = s.repo.GetPoliciesLike("MASKING", fmt.Sprintf("%s%s", maskName, "%"))
	require.NoError(s.T(), err)
	assert.Len(s.T(), policyEntries, 0)
}
