//go:build integration

package it

import (
	"context"
	"fmt"
	"github.com/aws/smithy-go/ptr"
	"strings"
	"testing"

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
}

func (s *DataAccessTestSuite) TestAssessSyncer_SyncAccessProvidersToTarget() {
	//Given
	dataAccessFeedbackHandler := mocks.NewSimpleAccessProviderFeedbackHandler(s.T())

	rolesToRemove := map[string]*sync_to_target.AccessProvider{}

	actualRoleName := generateRole("TESTROLE1", "")

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

	dataAccessSyncer := snowflake.NewDataAccessSyncer()

	config := s.getConfig()

	//When
	err := dataAccessSyncer.SyncAccessProviderRolesToTarget(context.Background(), rolesToRemove, access, dataAccessFeedbackHandler, config)

	//Then
	s.NoError(err)
	s.True(len(dataAccessFeedbackHandler.AccessProviderFeedback) >= 1)

	accessProviderFeedback := filterFeedbackInformation(dataAccessFeedbackHandler.AccessProviderFeedback)

	s.Len(accessProviderFeedback, 1)
	s.Equal([]sync_to_target.AccessProviderSyncFeedback{
		{
			ActualName:     actualRoleName,
			AccessProvider: fmt.Sprintf("%s_ap_id1", testId),
			ExternalId:     &actualRoleName,
			Type:           ptr.String("role"),
		},
	}, accessProviderFeedback)

	roles, err := s.sfRepo.GetRoles()
	s.NoError(err)
	s.Contains(roles, snowflake.RoleEntity{
		Name:            actualRoleName,
		AssignedToUsers: 1,
		GrantedToRoles:  0,
		GrantedRoles:    0,
		Owner:           "ACCOUNTADMIN",
	})

	//Given
	id := fmt.Sprintf("%s_ap_id1", testId)
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

	roles, err = s.sfRepo.GetRoles()
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

	dataAccessSyncer := snowflake.NewDataAccessSyncer()

	config := s.getConfig()

	//When
	err := dataAccessSyncer.SyncAccessAsCodeToTarget(context.Background(), access, prefix, config)

	//Then
	s.NoError(err)

	roles, err := s.sfRepo.GetRoles()
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

	roles, err = s.sfRepo.GetRoles()
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

	var masksToRemove map[string]*sync_to_target.AccessProvider

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
	err := dataAccessSyncer.SyncAccessProviderMasksToTarget(context.Background(), masksToRemove, masks, dataAccessFeedbackHandler, config)

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

	//When updating the mask will be recreated
	err = dataAccessSyncer.SyncAccessProviderMasksToTarget(context.Background(), masksToRemove, masks, dataAccessFeedbackHandler, config)

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
	masksToRemove[maskName] = &sync_to_target.AccessProvider{}
	masks = nil

	//When
	err = dataAccessSyncer.SyncAccessProviderMasksToTarget(context.Background(), masksToRemove, masks, dataAccessFeedbackHandler, config)

	//Then
	s.NoError(err)
	s.Empty(dataAccessFeedbackHandler.AccessProviderFeedback)

	maskPolicies, err = s.sfRepo.GetPolicies("MASKING")
	s.NoError(err)
	s.NotContains(maskPolicies, snowflake.PolicyEntity{
		Name:         fmt.Sprintf("%s_TEXT", accessProviderFeedback[0].ActualName),
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
