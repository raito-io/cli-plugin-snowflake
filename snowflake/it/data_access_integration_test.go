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
	dataAccessProviderHandler := mocks.NewSimpleAccessProviderHandler(s.T(), 8)
	dataAccessSyncer := snowflake.NewDataAccessSyncer(snowflake.RoleNameConstraints)

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
	s.Contains(externalIds, "DATABASEROLE###DATABASE:SNOWFLAKE_INTEGRATION_TEST###ROLE:IT_TEST_ROLE1")
	s.Contains(externalIds, "DATABASEROLE###DATABASE:SNOWFLAKE_INTEGRATION_TEST###ROLE:IT_TEST_ROLE2")
}

func (s *DataAccessTestSuite) TestAssessSyncer_SyncAccessProvidersToTarget() {
	//Given
	dataAccessFeedbackHandler := mocks.NewSimpleAccessProviderFeedbackHandler(s.T())

	actualRoleName := generateRole("TESTROLE1", testId)
	accountRoleId := fmt.Sprintf("%s_ACCOUNT_ID1", testId)

	databaseRoleName1 := generateRole("TESTDATABASEROLE1", testId)
	databaseRoleName2 := generateRole("TESTDATABASEROLE2", testId)

	databaseRoleExternalId1 := fmt.Sprintf("DATABASEROLE###DATABASE:SNOWFLAKE_INTEGRATION_TEST###ROLE:%s", databaseRoleName1)
	databaseRoleExternalId2 := fmt.Sprintf("DATABASEROLE###DATABASE:SNOWFLAKE_INTEGRATION_TEST###ROLE:%s", databaseRoleName2)

	accessProviderImport := &sync_to_target.AccessProviderImport{
		AccessProviders: []*sync_to_target.AccessProvider{
			{
				Id:          accountRoleId,
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
			{
				Id:          databaseRoleName1,
				Name:        databaseRoleName1,
				Action:      sync_to_target.Grant,
				NamingHint:  databaseRoleName1,
				ActualName:  ptr.String(databaseRoleName1),
				Delete:      false,
				Description: fmt.Sprintf("Integration testing for test %s", testId),
				Who:         sync_to_target.WhoItem{},
				Type:        ptr.String("databaseRole"),
				What: []sync_to_target.WhatItem{
					{
						DataObject: &data_source.DataObjectReference{
							FullName: "SNOWFLAKE_INTEGRATION_TEST.ORDERING.ORDERS",
							Type:     "table",
						},
						Permissions: []string{"SELECT"},
					},
				},
				WhoLocked: ptr.Bool(true),
			},
			{
				Id:          databaseRoleName2,
				Name:        databaseRoleName2,
				NamingHint:  databaseRoleName2,
				Action:      sync_to_target.Grant,
				ActualName:  ptr.String(databaseRoleName2),
				Delete:      false,
				Description: fmt.Sprintf("Integration testing for test %s", testId),
				Who: sync_to_target.WhoItem{
					InheritFrom: []string{
						fmt.Sprintf("ID:%s", accountRoleId),
					},
				},
				Type: ptr.String("databaseRole"),
				What: []sync_to_target.WhatItem{
					{
						DataObject: &data_source.DataObjectReference{
							FullName: "SNOWFLAKE_INTEGRATION_TEST.ORDERING.ORDERS",
							Type:     "table",
						},
						Permissions: []string{"SELECT"},
					},
				},
				WhoLocked: ptr.Bool(true),
			},
		},
	}

	dataAccessSyncer := snowflake.NewDataAccessSyncer(snowflake.RoleNameConstraints)

	config := s.getConfig()

	//When
	err := dataAccessSyncer.SyncAccessProviderToTarget(context.Background(), accessProviderImport, dataAccessFeedbackHandler, config)

	//Then
	s.NoError(err)
	s.True(len(dataAccessFeedbackHandler.AccessProviderFeedback) >= 3)

	accessProviderFeedback := filterFeedbackInformation(dataAccessFeedbackHandler.AccessProviderFeedback)

	s.Len(accessProviderFeedback, 3)
	s.ElementsMatch([]sync_to_target.AccessProviderSyncFeedback{
		{
			AccessProvider: accountRoleId,
			ActualName:     actualRoleName,
			ExternalId:     &actualRoleName,
		},
		{
			AccessProvider: fmt.Sprintf("%s_TESTDATABASEROLE1", testId),
			ActualName:     databaseRoleName1,
			ExternalId:     &databaseRoleExternalId1,
			Type:           ptr.String("databaseRole"),
		},
		{
			AccessProvider: fmt.Sprintf("%s_TESTDATABASEROLE2", testId),
			ActualName:     databaseRoleName2,
			ExternalId:     &databaseRoleExternalId2,
			Type:           ptr.String("databaseRole"),
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
		Name:            databaseRoleName1,
		AssignedToUsers: 0,
		GrantedToRoles:  0,
		GrantedRoles:    0,
		Owner:           "ACCOUNTADMIN",
	})
	s.Contains(databaseRoles, snowflake.RoleEntity{
		Name:            databaseRoleName2,
		AssignedToUsers: 0,
		GrantedToRoles:  1,
		GrantedRoles:    0,
		Owner:           "ACCOUNTADMIN",
	})

	//Update database role 1 to attach it to the account role
	//Given
	dataAccessFeedbackHandler = mocks.NewSimpleAccessProviderFeedbackHandler(s.T())
	accessProviderImport.AccessProviders = []*sync_to_target.AccessProvider{
		{
			Id:          accountRoleId,
			Name:        fmt.Sprintf("%s_ap1", testId),
			Action:      sync_to_target.Grant,
			NamingHint:  actualRoleName,
			ExternalId:  &actualRoleName,
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
		{
			Id:          databaseRoleName1,
			Name:        databaseRoleName1,
			Action:      sync_to_target.Grant,
			ExternalId:  &databaseRoleExternalId1,
			NamingHint:  databaseRoleName1,
			ActualName:  ptr.String(databaseRoleName1),
			Delete:      false,
			Description: fmt.Sprintf("Integration testing for test %s", testId),
			Who: sync_to_target.WhoItem{
				InheritFrom: []string{
					fmt.Sprintf("ID:%s", accountRoleId),
				},
			},
			Type:      ptr.String("databaseRole"),
			What:      []sync_to_target.WhatItem{},
			WhoLocked: ptr.Bool(true),
		},
	}

	//When
	dataAccessSyncer = snowflake.NewDataAccessSyncer(snowflake.RoleNameConstraints)
	err = dataAccessSyncer.SyncAccessProviderToTarget(context.Background(), accessProviderImport, dataAccessFeedbackHandler, config)

	//Then
	s.NoError(err)
	s.True(len(dataAccessFeedbackHandler.AccessProviderFeedback) >= 2)

	accessProviderFeedback = filterFeedbackInformation(dataAccessFeedbackHandler.AccessProviderFeedback)

	s.Len(accessProviderFeedback, 2)
	s.ElementsMatch([]sync_to_target.AccessProviderSyncFeedback{
		{
			AccessProvider: accountRoleId,
			ActualName:     actualRoleName,
			ExternalId:     &actualRoleName,
		},
		{
			AccessProvider: fmt.Sprintf("%s_TESTDATABASEROLE1", testId),
			ActualName:     databaseRoleName1,
			ExternalId:     &databaseRoleExternalId1,
			Type:           ptr.String("databaseRole"),
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
		Name:            databaseRoleName1,
		AssignedToUsers: 0,
		GrantedToRoles:  1,
		GrantedRoles:    0,
		Owner:           "ACCOUNTADMIN",
	})
	s.Contains(databaseRoles, snowflake.RoleEntity{
		Name:            databaseRoleName2,
		AssignedToUsers: 0,
		GrantedToRoles:  1,
		GrantedRoles:    0,
		Owner:           "ACCOUNTADMIN",
	})

	//Given
	id := accountRoleId
	dataAccessFeedbackHandler = mocks.NewSimpleAccessProviderFeedbackHandler(s.T())
	accessProviderImport.AccessProviders = []*sync_to_target.AccessProvider{
		{
			Id:          accountRoleId,
			Name:        fmt.Sprintf("%s_ap1", testId),
			Action:      sync_to_target.Grant,
			NamingHint:  actualRoleName,
			ExternalId:  &actualRoleName,
			Delete:      true,
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

	//When
	dataAccessSyncer = snowflake.NewDataAccessSyncer(snowflake.RoleNameConstraints)
	err = dataAccessSyncer.SyncAccessProviderToTarget(context.Background(), accessProviderImport, dataAccessFeedbackHandler, config)

	//Then
	s.NoError(err)
	s.ElementsMatch(dataAccessFeedbackHandler.AccessProviderFeedback, []sync_to_target.AccessProviderSyncFeedback{
		{
			AccessProvider: id,
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

func (s *DataAccessTestSuite) TestAccessSyncer_SyncAccessProviderMasksToTarget() {
	s.T().Skip("Skip test as Masking is a non standard edition feature")

	//Given
	dataAccessFeedbackHandler := mocks.NewSimpleAccessProviderFeedbackHandler(s.T())

	database := "RUBEN_TEST"
	schema := "TESTING"
	table := "CITIES"
	column := "CITY"

	doFullname := fmt.Sprintf("%s.%s.%s.%s", database, schema, table, column)

	maskName := fmt.Sprintf("%s_mask_id1", testId)

	accessProviderImport := &sync_to_target.AccessProviderImport{
		AccessProviders: []*sync_to_target.AccessProvider{{
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
		},
	}

	dataAccessSyncer := snowflake.NewDataAccessSyncer(snowflake.RoleNameConstraints)

	config := s.getConfig()

	//When
	err := dataAccessSyncer.SyncAccessProviderToTarget(context.Background(), accessProviderImport, dataAccessFeedbackHandler, config)

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
	err = dataAccessSyncer.SyncAccessProviderToTarget(context.Background(), accessProviderImport, dataAccessFeedbackHandler, config)

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
	// masksToRemove["RAITO_"+maskName] = &sync_to_target.AccessProvider{}
	accessProviderImport.AccessProviders = []*sync_to_target.AccessProvider{}

	dataAccessFeedbackHandler = mocks.NewSimpleAccessProviderFeedbackHandler(s.T())

	//When
	err = dataAccessSyncer.SyncAccessProviderToTarget(context.Background(), accessProviderImport, dataAccessFeedbackHandler, config)

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
