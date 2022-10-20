//go:build integration

package it

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/raito-io/cli-plugin-snowflake/common"
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

func (s *RepositoryTestSuite) TestSnowflakeRepository_BatchingInformation() {
	//given
	queryHistoryTable := "SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY"
	startDate := time.Now().Truncate(24*time.Hour).AddDate(0, 0, -14)

	//When
	minTimeStr, maxTimeStr, numRows, err := s.repo.BatchingInformation(&startDate, queryHistoryTable)

	//Then
	s.NoError(err)
	s.True(numRows > 0)
	s.NotNil(minTimeStr)
	s.NotNil(maxTimeStr)

	minTime, err := time.Parse(time.RFC3339, *minTimeStr)
	s.NoError(err)
	s.True(startDate.Before(minTime))
	s.True(minTime.Before(time.Now()))

	maxTime, err := time.Parse(time.RFC3339, *maxTimeStr)
	s.NoError(err)
	s.True(startDate.Before(minTime))
	s.True(minTime.Before(maxTime))
	s.True(maxTime.Before(time.Now()))
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_DataUsage() {
	//given
	queryHistoryTable := "SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY"
	startDate := time.Now().Truncate(24*time.Hour).AddDate(0, 0, -14)
	minTimeStr, maxTimeStr, _, err := s.repo.BatchingInformation(&startDate, queryHistoryTable)

	columns := []string{"QUERY_ID", "EXECUTION_STATUS", "QUERY_TEXT", "START_TIME"}
	limit := 10
	offset := 5

	//When
	entities, err := s.repo.DataUsage(columns, limit, offset, queryHistoryTable, minTimeStr, maxTimeStr, true)

	//Then
	s.NoError(err)
	s.Len(entities, 10)
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_DataUsage_NoHistoryTable() {
	//given
	queryHistoryTable := "SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY"
	startDate := time.Now().Truncate(24*time.Hour).AddDate(0, 0, -14)
	minTimeStr, maxTimeStr, _, err := s.repo.BatchingInformation(&startDate, queryHistoryTable)

	columns := snowflake.GetQueryDbEntitiesColumnNames("db", "useColumnName")
	limit := 10
	offset := 5

	//When
	entities, err := s.repo.DataUsage(columns, limit, offset, queryHistoryTable, minTimeStr, maxTimeStr, false)

	//Then
	s.NoError(err)
	s.Len(entities, 10)
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_CheckAccessHistoryAvailability() {
	//given
	queryHistoryTable := "SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY"

	//When
	result, err := s.repo.CheckAccessHistoryAvailability(queryHistoryTable)

	//Then
	s.NoError(err)
	s.False(result)
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_GetRoles() {
	//When
	roles, err := s.repo.GetRoles()

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

func (s *RepositoryTestSuite) TestSnowflakeRepository_GetRolesWithPrefix() {
	//When
	roles, err := s.repo.GetRolesWithPrefix("SYS")

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

func (s *RepositoryTestSuite) TestSnowflakeRepository_CreateRole() {
	//Given
	roleName := fmt.Sprintf("%s_REPO_TEST_CREATE_ROLE_TEST", testId)

	//When
	err := s.repo.CreateRole(roleName, "TEST REPOSITORY")

	//Then
	s.NoError(err)

	roles, err := s.repo.GetRolesWithPrefix(testId)
	s.NoError(err)
	s.Contains(roles, snowflake.RoleEntity{
		Name:            roleName,
		Owner:           "ACCOUNTADMIN",
		GrantedRoles:    0,
		GrantedToRoles:  0,
		AssignedToUsers: 0,
	})
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_DropRole() {
	//Given
	roleName := fmt.Sprintf("%s_REPO_TEST_CREATE_ROLE_TEST", testId)
	err := s.repo.CreateRole(roleName, "TEST REPOSITORY")
	s.NoError(err)

	//When
	err = s.repo.DropRole(roleName)

	//Then
	s.NoError(err)
	roles, err := s.repo.GetRolesWithPrefix(testId)
	s.NoError(err)

	roleNames := make([]string, 0, len(roles))
	for _, role := range roles {
		roleNames = append(roleNames, role.Name)
	}

	s.NotContains(roleNames, roleName)
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_GetGrantsToRole() {
	//When
	grantsToRole, err := s.repo.GetGrantsToRole("PUBLIC")

	//Then
	s.NoError(err)
	s.True(len(grantsToRole) >= 94)

	s.Contains(grantsToRole, snowflake.GrantToRole{
		Privilege: "USAGE",
		GrantedOn: "DATABASE",
		Name:      "SNOWFLAKE_SAMPLE_DATA",
	})
	s.Contains(grantsToRole, snowflake.GrantToRole{
		Privilege: "USAGE",
		GrantedOn: "SCHEMA",
		Name:      "SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL",
	})
	s.Contains(grantsToRole, snowflake.GrantToRole{
		Privilege: "SELECT",
		GrantedOn: "TABLE",
		Name:      "SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CALL_CENTER",
	})
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_GetGrantsOfRole() {
	//When
	grantsOfRolePublic, err := s.repo.GetGrantsOfRole("ACCOUNTADMIN")

	//Then
	s.NoError(err)
	s.True(len(grantsOfRolePublic) >= 1)
	s.Contains(grantsOfRolePublic, snowflake.GrantOfRole{
		GrantedTo:   "USER",
		GranteeName: "RAITO",
	})
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_GrantRolesToRole() {
	//When
	originalRoleName := fmt.Sprintf("%s_REPO_TEST_GRANT_R2R", testId)
	rolesToGrants := make([]string, 0, 5)

	for i := 1; i <= 5; i++ {
		rolesToGrants = append(rolesToGrants, fmt.Sprintf("%s_REPO_TEST_GRANT_R2R_%d", testId, i))
	}

	err := s.repo.CreateRole(originalRoleName, "Integration testing")
	s.NoError(err)

	//When
	err = s.repo.GrantRolesToRole(context.Background(), originalRoleName, rolesToGrants...)

	//Then
	s.NoError(err)

	grants, err := s.repo.GetGrantsOfRole(originalRoleName)
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

func (s *RepositoryTestSuite) TestSnowflakeRepository_RevokeRolesFromRole() {
	//When
	originalRoleName := fmt.Sprintf("%s_REPO_TEST_REVOKE_R2R", testId)
	rolesToGrants := make([]string, 0, 5)

	for i := 1; i <= 5; i++ {
		rolesToGrants = append(rolesToGrants, fmt.Sprintf("%s_REPO_TEST_REVOKE_R2R_%d", testId, i))
	}

	err := s.repo.CreateRole(originalRoleName, "Integration testing")
	s.NoError(err)

	err = s.repo.GrantRolesToRole(context.Background(), originalRoleName, rolesToGrants...)
	s.NoError(err)

	//When
	err = s.repo.RevokeRolesFromRole(context.Background(), originalRoleName, rolesToGrants...)

	//Then
	s.NoError(err)

	grants, err := s.repo.GetGrantsOfRole(originalRoleName)
	s.NoError(err)
	s.Empty(grants)
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_GrantUsersToRole() {
	//Given
	roleName := fmt.Sprintf("%s_REPO_TEST_GRANT_USER_TEST", testId)
	err := s.repo.CreateRole(roleName, "Integration test")

	s.NoError(err)

	//When
	err = s.repo.GrantUsersToRole(context.Background(), roleName, snowflakeUserName)

	//Then
	s.NoError(err)

	grants, err := s.repo.GetGrantsOfRole(roleName)
	s.NoError(err)

	s.Equal(grants, []snowflake.GrantOfRole{
		{
			GrantedTo:   "USER",
			GranteeName: snowflakeUserName,
		},
	})
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_RevokeUsersFromRole() {
	//Given
	roleName := fmt.Sprintf("%s_REPO_TEST_GRANT_USER_TEST", testId)
	err := s.repo.CreateRole(roleName, "Integration test")
	s.NoError(err)

	err = s.repo.GrantUsersToRole(context.Background(), roleName, snowflakeUserName)
	s.NoError(err)

	//When
	err = s.repo.RevokeUsersFromRole(context.Background(), roleName, snowflakeUserName)

	//Then
	s.NoError(err)

	grants, err := s.repo.GetGrantsOfRole(roleName)
	s.NoError(err)
	s.Empty(grants)
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_ExecuteGrant() {
	//Given
	roleName := fmt.Sprintf("%s_REPO_TEST_EXECUTE_GRANT_TEST", testId)
	err := s.repo.CreateRole(roleName, "Integration test execute grant")
	s.NoError(err)

	//When
	err = s.repo.ExecuteGrant("SELECT", "SNOWFLAKE_INTEGRATION_TEST.ORDERING.ORDERS", roleName)

	//Then
	s.NoError(err)

	grantsTo, err := s.repo.GetGrantsToRole(roleName)
	s.NoError(err)

	s.Equal(grantsTo, []snowflake.GrantToRole{
		{
			Name:      "SNOWFLAKE_INTEGRATION_TEST.ORDERING.ORDERS",
			GrantedOn: "TABLE",
			Privilege: "SELECT",
		},
	})
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_ExecuteRevoke() {
	//Given
	roleName := fmt.Sprintf("%s_REPO_TEST_EXECUTE_REVOKE_TEST", testId)
	err := s.repo.CreateRole(roleName, "Integration test execute grant")
	s.NoError(err)
	err = s.repo.ExecuteGrant("SELECT", "SNOWFLAKE_INTEGRATION_TEST.ORDERING.ORDERS", roleName)
	s.NoError(err)

	//When
	err = s.repo.ExecuteRevoke("SELECT", "SNOWFLAKE_INTEGRATION_TEST.ORDERING.ORDERS", roleName)

	//Then
	s.NoError(err)

	grantsTo, err := s.repo.GetGrantsToRole(roleName)
	s.NoError(err)
	s.Empty(grantsTo)
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
	})

	s.Contains(users, snowflake.UserEntity{
		Name:        snowflakeUserName,
		Email:       "",
		Owner:       "ACCOUNTADMIN",
		DisplayName: snowflakeUserName,
		LoginName:   snowflakeUserName,
	})
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_GetPolicies_Masking() {
	if sfStandardEdition {
		s.T().Skip("Standard edition do not support masking policies")
	}

	//When
	_, err := s.repo.GetPolicies("MASKING")

	//Then
	s.NoError(err)

	//TODO
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_GetPolicies_RowAccess() {
	if sfStandardEdition {
		s.T().Skip("Standard edition do not support row access policies")
	}

	//When
	_, err := s.repo.GetPolicies("ROW ACCESS")

	//Then
	s.NoError(err)

	//TODO
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_DescribePolicy_Masking() {
	if sfStandardEdition {
		s.T().Skip("Standard edition do not support masking policies")
	}

	//TODO
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_DescribePolicy_RowAccess() {
	if sfStandardEdition {
		s.T().Skip("Standard edition do not support row access policies")
	}

	//TODO
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_GetPolicyReferences() {
	if sfStandardEdition {
		s.T().Skip("Standard edition do not support policy references")
	}

	//TODO
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
		Name:    "TESTING_WAREHOUSE",
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

func (s *RepositoryTestSuite) TestSnowflakeRepository_GetDataBases() {
	//When
	databases, err := s.repo.GetDataBases()

	//Then
	s.NoError(err)
	s.True(len(databases) >= 3)

	comment := ""

	s.Contains(databases, snowflake.DbEntity{
		Name:    "SNOWFLAKE",
		Comment: &comment,
	})

	comment = "Provided by Snowflake during account provisioning"

	s.Contains(databases, snowflake.DbEntity{
		Name:    "SNOWFLAKE_SAMPLE_DATA",
		Comment: &comment,
	})

	comment = "Database created for integration testing"

	s.Contains(databases, snowflake.DbEntity{
		Name:    "SNOWFLAKE_INTEGRATION_TEST",
		Comment: &comment,
	})
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_GetSchemaInDatabase() {
	//Given
	database := "SNOWFLAKE_INTEGRATION_TEST"

	//When
	schemas, err := s.repo.GetSchemaInDatabase(database)

	//Then
	s.NoError(err)
	s.Len(schemas, 3)

	comment := ""

	s.Contains(schemas, snowflake.DbEntity{
		Name:    "PUBLIC",
		Comment: &comment,
	})

	s.Contains(schemas, snowflake.DbEntity{
		Name:    "ORDERING",
		Comment: &comment,
	})

	comment = "Views describing the contents of schemas in this database"

	s.Contains(schemas, snowflake.DbEntity{
		Name:    "INFORMATION_SCHEMA",
		Comment: &comment,
	})
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_GetTablesInSchema() {
	//Given
	database := "SNOWFLAKE_INTEGRATION_TEST"
	schema := "ORDERING"
	sfObjectSchema := common.SnowflakeObject{
		Database: &database,
		Schema:   &schema,
	}

	//When
	tables, err := s.repo.GetTablesInSchema(&sfObjectSchema)

	//Then
	s.NoError(err)
	s.Len(tables, 1)

	comment := ""

	s.Equal([]snowflake.DbEntity{
		{
			Name:    "ORDERS",
			Comment: &comment,
		},
	}, tables)
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_GetViewsInSchema() {
	//Given
	database := "SNOWFLAKE"
	schema := "ACCOUNT_USAGE"
	sfObjectSchema := common.SnowflakeObject{
		Database: &database,
		Schema:   &schema,
	}

	//When
	views, err := s.repo.GetViewsInSchema(&sfObjectSchema)

	//Then
	s.NoError(err)
	s.Len(views, 51)

	comment := ""

	s.Contains(views, snowflake.DbEntity{
		Name:    "ACCESS_HISTORY",
		Comment: &comment,
	})
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_GetColumnsInTable() {
	//Given
	database := "SNOWFLAKE_INTEGRATION_TEST"
	schema := "ORDERING"
	table := "ORDERS"
	sfObjectTable := common.SnowflakeObject{
		Database: &database,
		Schema:   &schema,
		Table:    &table,
	}

	//When
	columns, err := s.repo.GetColumnsInTable(&sfObjectTable)

	//Then
	s.NoError(err)
	s.Len(columns, 9)

	s.ElementsMatch(columns, []snowflake.DbEntity{
		{
			Name: "CLERK",
		},
		{
			Name: "COMMENT",
		},
		{
			Name: "CUSTKEY",
		},
		{
			Name: "ORDERDATE",
		},
		{
			Name: "ORDERKEY",
		},
		{
			Name: "ORDERPRIORITY",
		},
		{
			Name: "ORDERSTATUS",
		},
		{
			Name: "SHIPPRIORITY",
		},
		{
			Name: "TOTALPRICE",
		},
	})
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_CommentIfExists_NonExistingRole() {
	//Given
	roleName := fmt.Sprintf("%s_REPO_TEST_COMMENT_NON_EXISTING_ROLE", testId)

	//When
	err := s.repo.CommentIfExists("SomeComment", "ROLE", roleName)

	//Then
	s.NoError(err)
}

func (s *RepositoryTestSuite) TestSnowflakeRepository_CommentIfExists_Role() {
	//Given
	roleName := fmt.Sprintf("%s_REPO_TEST_COMMENT_EXISTING_ROLE", testId)
	err := s.repo.CreateRole(roleName, "Integration testing comment if exists")

	comment := "Some comment"

	//When
	err = s.repo.CommentIfExists(comment, "ROLE", roleName)

	//Then
	s.NoError(err)
}