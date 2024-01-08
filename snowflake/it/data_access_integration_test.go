//go:build integration

package it

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/smithy-go/ptr"
	"github.com/raito-io/cli/base/access_provider/sync_to_target"
	"github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/wrappers/mocks"
	"github.com/stretchr/testify/suite"

	"github.com/raito-io/cli-plugin-snowflake/snowflake"
)

type DataAccessTestSuite struct {
	SnowflakeTestSuite
	sfRepo *snowflake.SnowflakeRepository
}

func TestDataAccessTestSuite(t *testing.T) {
	ts := DataAccessTestSuite{}

	var err error
	ts.sfRepo, err = snowflake.NewSnowflakeRepository(ts.getConfig().Parameters, "")

	if err != nil {
		panic(err)
	}

	defer ts.sfRepo.Close()

	suite.Run(t, &ts)
}

func (s *DataAccessTestSuite) TestAccessSyncer_SyncAccessProvidersFromTarget() {
	//Given
	dataAccessProviderHandler := mocks.NewSimpleAccessProviderHandler(s.T(), 1)
	dataAccessSyncer := snowflake.NewDataAccessSyncer()

	config := s.getConfig()

	//When
	err := dataAccessSyncer.SyncAccessProvidersFromTarget(context.Background(), dataAccessProviderHandler, config)

	//Then
	s.NoError(err)
	s.True(len(dataAccessProviderHandler.AccessProviders) >= 6)

	externalIds := make([]string, len(dataAccessProviderHandler.AccessProviders))

	for i := range dataAccessProviderHandler.AccessProviders {
		ap := dataAccessProviderHandler.AccessProviders[i]
		externalIds[i] = ap.ExternalId
	}

	s.Contains(externalIds, "SYSADMIN")
	s.Contains(externalIds, "ORGADMIN")
	s.Contains(externalIds, "ACCOUNTADMIN")
	s.Contains(externalIds, "SECURITYADMIN")
	s.Contains(externalIds, "USERADMIN")
	s.Contains(externalIds, "PUBLIC")
	s.Contains(externalIds, "SNOWFLAKE_INTEGRATION_TEST.IT_TEST_ROLE1")
	s.Contains(externalIds, "SNOWFLAKE_INTEGRATION_TEST.IT_TEST_ROLE2")
}

func (s *DataAccessTestSuite) TestAssessSyncer_SyncAccessProvidersToTarget() {
	//Given
	dataAccessFeedbackHandler := mocks.NewSimpleAccessProviderFeedbackHandler(s.T())

	rolesToRemove := map[string]*sync_to_target.AccessProvider{}

	actualRoleName := generateRole("TESTROLE1", "")
	AccountRoleId := fmt.Sprintf("%s_ACCOUNT_ID1", testId)

	DatabaseRoleName1 := generateRole("TESTDATABASEROLE1", "")
	DatabaseRoleName2 := generateRole("TESTDATABASEROLE2", "")

	DatabaseRoleActualName1 := fmt.Sprintf("DATABASEROLE###SNOWFLAKE_INTEGRATION_TEST.%s", DatabaseRoleName1)
	DatabaseRoleActualName2 := fmt.Sprintf("DATABASEROLE###SNOWFLAKE_INTEGRATION_TEST.%s", DatabaseRoleName2)

	access := map[string]*sync_to_target.AccessProvider{
		actualRoleName: {
			Id:          AccountRoleId,
			Name:        fmt.Sprintf("%s_ap1", testId),
			Action:      sync_to_target.Grant,
			NamingHint:  actualRoleName,
			Delete:      false,
			Description: fmt.Sprintf("Integration testing for test %s", testId),
			Who: sync_to_target.WhoItem{
				Users: []string{snowflakeUserName},
			},
			What: []sync_to_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "SNOWFLAKE_INTEGRATION_TEST.ORDERING.ORDERS",
						Type:     "table",
					},
					Permissions: []string{"SELECT"},
				},
			},
		},
		DatabaseRoleActualName1: {
			Id:          fmt.Sprintf("%s_TESTDATABASEROLE1_ID1", testId),
			Name:        fmt.Sprintf("%s.TESTDATABASEROLE1_ID1", testId),
			Action:      sync_to_target.Grant,
			NamingHint:  DatabaseRoleActualName1,
			ActualName:  &DatabaseRoleActualName1,
			Delete:      false,
			Description: fmt.Sprintf("Integration testing for test %s", testId),
			Who:         sync_to_target.WhoItem{},
			Type:        ptr.String("DATABASE_ROLE"),
			What:        []sync_to_target.WhatItem{},
			WhoLocked:   ptr.Bool(true),
		},
		DatabaseRoleActualName2: {
			Id:          fmt.Sprintf("%s_TESTDATABASEROLE1_ID2", testId),
			Name:        fmt.Sprintf("%s.TESTDATABASEROLE1_ID2", testId),
			Action:      sync_to_target.Grant,
			NamingHint:  DatabaseRoleActualName2,
			ActualName:  &DatabaseRoleActualName2,
			Delete:      false,
			Description: fmt.Sprintf("Integration testing for test %s", testId),
			Who: sync_to_target.WhoItem{
				InheritFrom: []string{
					fmt.Sprintf("ID:%s", AccountRoleId),
				},
			},
			Type:      ptr.String("DATABASE_ROLE"),
			What:      []sync_to_target.WhatItem{},
			WhoLocked: ptr.Bool(true),
		},
	}

	dataAccessSyncer := snowflake.NewDataAccessSyncer()

	config := s.getConfig()

	//When
	err := dataAccessSyncer.SyncAccessProviderRolesToTarget(context.Background(), rolesToRemove, access, dataAccessFeedbackHandler, config)

	//Then
	s.NoError(err)
	s.True(len(dataAccessFeedbackHandler.AccessProviderFeedback) >= 3)

	accessProviderFeedback := filterFeedbackInformation(dataAccessFeedbackHandler.AccessProviderFeedback)

	s.Len(accessProviderFeedback, 3)
	s.Equal([]sync_to_target.AccessProviderSyncFeedback{
		{
			ActualName:     actualRoleName,
			AccessProvider: AccountRoleId,
			ExternalId:     &actualRoleName,
			Type:           ptr.String("role"),
		},
		{
			ActualName:     DatabaseRoleActualName1,
			AccessProvider: fmt.Sprintf("%s_TESTDATABASEROLE1_ID1", testId),
			ExternalId:     &DatabaseRoleActualName1,
			Type:           ptr.String("DATABASE_ROLE"),
		},
		{
			ActualName:     DatabaseRoleActualName2,
			AccessProvider: fmt.Sprintf("%s_TESTDATABASEROLE1_ID2", testId),
			ExternalId:     &DatabaseRoleActualName2,
			Type:           ptr.String("DATABASE_ROLE"),
		},
	}, accessProviderFeedback)

	roles, err := s.sfRepo.GetAccountRoles()
	s.NoError(err)
	s.Contains(roles, snowflake.RoleEntity{
		Name:            actualRoleName,
		AssignedToUsers: 1,
		GrantedToRoles:  0,
		GrantedRoles:    0,
		Owner:           "ACCOUNTADMIN",
	})

	databaseRoles, err := s.sfRepo.GetDatabaseRoles("SNOWFLAKE_INTEGRATION_TEST")
	s.NoError(err)
	s.Contains(databaseRoles, snowflake.RoleEntity{
		Name:            DatabaseRoleName1,
		AssignedToUsers: 0,
		GrantedToRoles:  0,
		GrantedRoles:    0,
		Owner:           "ACCOUNTADMIN",
	})
	s.Contains(databaseRoles, snowflake.RoleEntity{
		Name:            DatabaseRoleName2,
		AssignedToUsers: 0,
		GrantedToRoles:  1,
		GrantedRoles:    0,
		Owner:           "ACCOUNTADMIN",
	})

	//Update database role 1 to attach it to the account role
	//Given
	dataAccessFeedbackHandler = mocks.NewSimpleAccessProviderFeedbackHandler(s.T())
	access = map[string]*sync_to_target.AccessProvider{
		actualRoleName: {
			Id:          AccountRoleId,
			Name:        fmt.Sprintf("%s_ap1", testId),
			Action:      sync_to_target.Grant,
			NamingHint:  actualRoleName,
			Delete:      false,
			Description: fmt.Sprintf("Integration testing for test %s", testId),
			Who: sync_to_target.WhoItem{
				Users: []string{snowflakeUserName},
			},
			What: []sync_to_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "SNOWFLAKE_INTEGRATION_TEST.ORDERING.ORDERS",
						Type:     "table",
					},
					Permissions: []string{"SELECT"},
				},
			},
		},
		DatabaseRoleActualName1: {
			Id:          fmt.Sprintf("%s_TESTDATABASEROLE1_ID1", testId),
			Name:        fmt.Sprintf("%s.TESTDATABASEROLE1_ID1", testId),
			Action:      sync_to_target.Grant,
			NamingHint:  DatabaseRoleActualName1,
			ActualName:  &DatabaseRoleActualName1,
			Delete:      false,
			Description: fmt.Sprintf("Integration testing for test %s", testId),
			Who: sync_to_target.WhoItem{
				InheritFrom: []string{
					fmt.Sprintf("ID:%s", AccountRoleId),
				},
			}, Type: ptr.String("DATABASE_ROLE"),
			What:      []sync_to_target.WhatItem{},
			WhoLocked: ptr.Bool(true),
		}}

	//When
	err = dataAccessSyncer.SyncAccessProviderRolesToTarget(context.Background(), rolesToRemove, access, dataAccessFeedbackHandler, config)

	//Then
	s.NoError(err)
	s.True(len(dataAccessFeedbackHandler.AccessProviderFeedback) >= 2)

	accessProviderFeedback = filterFeedbackInformation(dataAccessFeedbackHandler.AccessProviderFeedback)

	s.Len(accessProviderFeedback, 2)
	s.Equal([]sync_to_target.AccessProviderSyncFeedback{
		{
			ActualName:     actualRoleName,
			AccessProvider: AccountRoleId,
			ExternalId:     &actualRoleName,
			Type:           ptr.String("role"),
		},
		{
			ActualName:     DatabaseRoleActualName1,
			AccessProvider: fmt.Sprintf("%s_TESTDATABASEROLE1_ID1", testId),
			ExternalId:     &DatabaseRoleActualName1,
			Type:           ptr.String("DATABASE_ROLE"),
		},
	}, accessProviderFeedback)

	roles, err = s.sfRepo.GetAccountRoles()
	s.NoError(err)
	s.Contains(roles, snowflake.RoleEntity{
		Name:            actualRoleName,
		AssignedToUsers: 1,
		GrantedToRoles:  0,
		GrantedRoles:    0,
		Owner:           "ACCOUNTADMIN",
	})

	databaseRoles, err = s.sfRepo.GetDatabaseRoles("SNOWFLAKE_INTEGRATION_TEST")
	s.NoError(err)
	s.Contains(databaseRoles, snowflake.RoleEntity{
		Name:            DatabaseRoleName1,
		AssignedToUsers: 0,
		GrantedToRoles:  1,
		GrantedRoles:    0,
		Owner:           "ACCOUNTADMIN",
	})
	s.Contains(databaseRoles, snowflake.RoleEntity{
		Name:            DatabaseRoleName2,
		AssignedToUsers: 0,
		GrantedToRoles:  1,
		GrantedRoles:    0,
		Owner:           "ACCOUNTADMIN",
	})

	//Given
	id := AccountRoleId
	dataAccessFeedbackHandler = mocks.NewSimpleAccessProviderFeedbackHandler(s.T())
	rolesToRemove = map[string]*sync_to_target.AccessProvider{actualRoleName: {Id: id}}
	access = make(map[string]*sync_to_target.AccessProvider)

	//When
	err = dataAccessSyncer.SyncAccessProviderRolesToTarget(context.Background(), rolesToRemove, access, dataAccessFeedbackHandler, config)

	//Then
	s.NoError(err)
	s.ElementsMatch(dataAccessFeedbackHandler.AccessProviderFeedback, []sync_to_target.AccessProviderSyncFeedback{
		{
			AccessProvider: id,
			ActualName:     actualRoleName,
			ExternalId:     &actualRoleName,
		},
	})

	roles, err = s.sfRepo.GetAccountRoles()
	s.NoError(err)
	s.NotContains(roles, snowflake.RoleEntity{
		Name:            actualRoleName,
		AssignedToUsers: 1,
		GrantedToRoles:  0,
		GrantedRoles:    0,
		Owner:           "ACCOUNTADMIN",
	})
}

func (s *DataAccessTestSuite) TestAssessSyncer_SyncAccessAsCodeToTarget() {
	//Given
	prefix := fmt.Sprintf("%s$AAC_", testId)

	actualRoleName := generateRole("TESTROLE1", prefix)

	access := map[string]*sync_to_target.AccessProvider{
		actualRoleName: {
			Id:          fmt.Sprintf("%s_ap_id1", testId),
			Name:        fmt.Sprintf("%s_ap1", testId),
			Action:      sync_to_target.Grant,
			NamingHint:  actualRoleName,
			Delete:      false,
			Description: fmt.Sprintf("Integration testing for test %s", testId),
			Who: sync_to_target.WhoItem{
				Users: []string{snowflakeUserName},
			},
			What: []sync_to_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "SNOWFLAKE_INTEGRATION_TEST.ORDERING.ORDERS",
						Type:     "table",
					},
					Permissions: []string{"SELECT"},
				},
			},
		},
	}

	// for key, generatedAp := range generateDefaultDatabaseRolesSnowflake() {
	// 	access[key] = generatedAp
	// }

	dataAccessSyncer := snowflake.NewDataAccessSyncer()

	config := s.getConfig()

	//When
	err := dataAccessSyncer.SyncAccessAsCodeToTarget(context.Background(), access, prefix, config)

	//Then
	s.NoError(err)

	roles, err := s.sfRepo.GetAccountRoles()
	s.NoError(err)
	s.Contains(roles, snowflake.RoleEntity{
		Name:            actualRoleName,
		AssignedToUsers: 1,
		GrantedToRoles:  0,
		GrantedRoles:    0,
		Owner:           "ACCOUNTADMIN",
	})

	//Given
	access = make(map[string]*sync_to_target.AccessProvider)

	//When
	err = dataAccessSyncer.SyncAccessAsCodeToTarget(context.Background(), access, prefix, config)

	//Then
	s.NoError(err)

	roles, err = s.sfRepo.GetAccountRoles()
	s.NoError(err)
	s.NotContains(roles, snowflake.RoleEntity{
		Name:            actualRoleName,
		AssignedToUsers: 1,
		GrantedToRoles:  0,
		GrantedRoles:    0,
		Owner:           "ACCOUNTADMIN",
	})
}

func (s *DataAccessTestSuite) TestAccessSyncer_SyncAccessProviderMasksToTarget() {
	s.T().Skip("Skip test as Masking is a non standard edition feature")

	//Given
	dataAccessFeedbackHandler := mocks.NewSimpleAccessProviderFeedbackHandler(s.T())

	database := "RUBEN_TEST"
	schema := "TESTING"
	table := "CITIES"
	column := "CITY"

	doFullname := fmt.Sprintf("%s.%s.%s.%s", database, schema, table, column)

	masksToRemove := map[string]*sync_to_target.AccessProvider{}

	maskName := fmt.Sprintf("%s_mask_id1", testId)

	masks := map[string]*sync_to_target.AccessProvider{
		maskName: {
			Id:          maskName,
			Name:        maskName,
			Action:      sync_to_target.Mask,
			Delete:      false,
			Description: fmt.Sprintf("Mask integration testing for test %s", testId),
			Who: sync_to_target.WhoItem{
				Users: []string{snowflakeUserName},
			},
			What: []sync_to_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: doFullname,
						Type:     "column",
					},
				},
			},
		},
	}

	dataAccessSyncer := snowflake.NewDataAccessSyncer()

	config := s.getConfig()

	//When
	err := dataAccessSyncer.SyncAccessProviderMasksToTarget(context.Background(), masksToRemove, masks, map[string]string{}, dataAccessFeedbackHandler, config)

	//Then
	s.NoError(err)
	s.True(len(dataAccessFeedbackHandler.AccessProviderFeedback) >= 1)

	accessProviderFeedback := filterFeedbackInformation(dataAccessFeedbackHandler.AccessProviderFeedback)

	s.Len(accessProviderFeedback, 1)

	s.True(strings.HasPrefix(accessProviderFeedback[0].ActualName, fmt.Sprintf("RAITO_%s", strings.ToUpper(maskName))))

	maskPolicies, err := s.sfRepo.GetPolicies("MASKING")
	s.NoError(err)
	s.Contains(maskPolicies, snowflake.PolicyEntity{
		Name:         fmt.Sprintf("%s_TEXT", strings.ToUpper(accessProviderFeedback[0].ActualName)),
		SchemaName:   schema,
		DatabaseName: database,
		Kind:         "MASKING_POLICY",
		Owner:        "ACCOUNTADMIN",
	})

	dataAccessFeedbackHandler = mocks.NewSimpleAccessProviderFeedbackHandler(s.T())

	//When updating the mask will be recreated
	err = dataAccessSyncer.SyncAccessProviderMasksToTarget(context.Background(), masksToRemove, masks, map[string]string{}, dataAccessFeedbackHandler, config)

	//Then
	s.NoError(err)
	s.True(len(dataAccessFeedbackHandler.AccessProviderFeedback) >= 1)

	accessProviderFeedback = filterFeedbackInformation(dataAccessFeedbackHandler.AccessProviderFeedback)

	s.Len(accessProviderFeedback, 1)

	s.True(strings.HasPrefix(accessProviderFeedback[0].ActualName, fmt.Sprintf("RAITO_%s", strings.ToUpper(maskName))))

	maskPolicies, err = s.sfRepo.GetPolicies("MASKING")
	s.NoError(err)
	s.Contains(maskPolicies, snowflake.PolicyEntity{
		Name:         fmt.Sprintf("%s_TEXT", strings.ToUpper(accessProviderFeedback[0].ActualName)),
		SchemaName:   schema,
		DatabaseName: database,
		Kind:         "MASKING_POLICY",
		Owner:        "ACCOUNTADMIN",
	})

	//Given
	dataAccessFeedbackHandler = mocks.NewSimpleAccessProviderFeedbackHandler(s.T())
	masksToRemove["RAITO_"+maskName] = &sync_to_target.AccessProvider{}
	masks = nil

	dataAccessFeedbackHandler = mocks.NewSimpleAccessProviderFeedbackHandler(s.T())

	//When
	err = dataAccessSyncer.SyncAccessProviderMasksToTarget(context.Background(), masksToRemove, masks, map[string]string{}, dataAccessFeedbackHandler, config)

	//Then
	s.NoError(err)
	s.Len(accessProviderFeedback, 1)

	s.True(strings.HasPrefix(accessProviderFeedback[0].ActualName, fmt.Sprintf("RAITO_%s", strings.ToUpper(maskName))))

	maskPolicies, err = s.sfRepo.GetPolicies("MASKING")
	s.NoError(err)
	s.NotContains(maskPolicies, snowflake.PolicyEntity{
		Name:         fmt.Sprintf("RAITO_%s_TEXT", strings.ToUpper(accessProviderFeedback[0].ActualName)),
		SchemaName:   schema,
		DatabaseName: database,
		Owner:        "ACCOUNTADMIN",
	})
}

func generateRole(username string, prefix string) string {
	if prefix == "" {
		prefix = fmt.Sprintf("%s_", testId)
	}
	return strings.ToUpper(fmt.Sprintf("%s_%s", prefix, username))
}

func filterFeedbackInformation(feedbackInformation []sync_to_target.AccessProviderSyncFeedback) []sync_to_target.AccessProviderSyncFeedback {
	result := make([]sync_to_target.AccessProviderSyncFeedback, 0, len(feedbackInformation))

	for _, feedbackList := range feedbackInformation {
		if strings.HasPrefix(feedbackList.AccessProvider, testId) {
			result = append(result, feedbackList)
		}
	}

	return result
}

func generateDefaultDatabaseRolesSnowflake() map[string]*sync_to_target.AccessProvider {

	expectedDatabase := "SNOWFLAKE"
	expectedDatabaseRoles := []string{
		"ALERT_VIEWER",
		"BUDGET_CREATOR",
		"CLASSIFICATION_ADMIN",
		"CORE_VIEWER",
		"DATA_PRIVACY_VIEWER",
		"GOVERNANCE_ADMIN",
		"GOVERNANCE_VIEWER",
		"ML_USER",
		"MONITORING_VIEWER",
		"OBJECT_VIEWER",
		"ORGANIZATION_ACCOUNTS_VIEWER",
		"ORGANIZATION_BILLING_VIEWER",
		"ORGANIZATION_GOVERNANCE_VIEWER",
		"ORGANIZATION_USAGE_VIEWER",
		"READER_USAGE_VIEWER",
		"SECURITY_VIEWER",
		"SHARING_USAGE_VIEWER",
		"USAGE_VIEWER",
	}

	aps := map[string]*sync_to_target.AccessProvider{}

	for _, role := range expectedDatabaseRoles {
		databaseRoleName := snowflake.DatabaseRoleNameGenerator(role, expectedDatabase)
		actualName := snowflake.DatabaseRoleActualNameGenerator(databaseRoleName)

		aps[actualName] = &sync_to_target.AccessProvider{
			Id:     actualName,
			Name:   role,
			Action: sync_to_target.Grant,
			Delete: false,
			Who:    sync_to_target.WhoItem{},
			What:   []sync_to_target.WhatItem{},
		}
	}

	return aps
}
