//go:build integration

package it

import (
	"context"
	"testing"

	"github.com/raito-io/cli/base/access_provider/sync_to_target"
	"github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/wrappers/mocks"
	"github.com/stretchr/testify/suite"

	"github.com/raito-io/cli-plugin-snowflake/snowflake"
)

type DataAccessTestSuite struct {
	SnowflakeTestSuite
}

func TestDataAccessTestSuite(t *testing.T) {
	ts := DataAccessTestSuite{}
	suite.Run(t, &ts)
}

func (s *DataAccessTestSuite) TestAssessSyncer_SyncAccessProvidersFromTarget() {
	//Given
	dataAccessProviderHandler := mocks.NewSimpleAccessProviderHandler(s.T(), 1)
	dataAccessSyncer := snowflake.NewDataAccessSyncer()

	config := s.getConfig()

	//When
	err := dataAccessSyncer.SyncAccessProvidersFromTarget(context.Background(), dataAccessProviderHandler, config)

	//Then
	s.NoError(err)
	s.True(len(dataAccessProviderHandler.AccessProviders) > 0)

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
	dataAccessFeedbackHandler := mocks.NewSimpleAccessProviderFeedbackHandler(s.T(), 1)

	rolesToRemove := []string{}

	access1 := &sync_to_target.Access{
		Who: sync_to_target.WhoItem{
			Users: []string{"DIETER@RAITO.IO"},
		},
		What: []sync_to_target.WhatItem{
			{
				DataObject: &data_source.DataObjectReference{
					FullName: "ANALYTICS.ANALYTICS.MY_FIRST_DBT_MODEL",
					Type:     "table",
				},
				Permissions: []string{"SELECT"},
			},
		},
		Id: "AccessRole1",
	}

	access := map[string]sync_to_target.EnrichedAccess{
		"TESTROLE1": {
			Access: access1,
			AccessProvider: &sync_to_target.AccessProvider{
				Id:          "AccessProvider1",
				Access:      []*sync_to_target.Access{access1},
				Name:        "AccessProvider1",
				Action:      sync_to_target.Grant,
				NamingHint:  "TESTROLE1",
				Delete:      false,
				Description: "Integration testing",
			},
		},
	}

	dataAccessSyncer := snowflake.NewDataAccessSyncer()

	config := s.getConfig()

	//When
	err := dataAccessSyncer.SyncAccessProvidersToTarget(context.Background(), rolesToRemove, access, dataAccessFeedbackHandler, config)

	//Then
	s.NoError(err)
	s.Len(dataAccessFeedbackHandler.AccessProviderFeedback, 1)
	s.Equal(map[string][]sync_to_target.AccessSyncFeedbackInformation{
		"AccessProvider1": {{
			ActualName: "TESTROLE1",
			AccessId:   "AccessRole1",
		}},
	}, dataAccessFeedbackHandler.AccessProviderFeedback)

	//Given
	dataAccessFeedbackHandler = mocks.NewSimpleAccessProviderFeedbackHandler(s.T(), 1)
	rolesToRemove = append(rolesToRemove, "TESTROLE1")
	access = make(map[string]sync_to_target.EnrichedAccess)

	//When
	err = dataAccessSyncer.SyncAccessProvidersToTarget(context.Background(), rolesToRemove, access, dataAccessFeedbackHandler, config)

	//Then
	s.NoError(err)
	s.Empty(dataAccessFeedbackHandler.AccessProviderFeedback)
}

func (s *DataAccessTestSuite) TestAssessSyncer_SyncAccessAsCodeToTarget() {
	//Given
	prefix := "IT_"

	access1 := &sync_to_target.Access{
		Who: sync_to_target.WhoItem{
			Users: []string{"DIETER@RAITO.IO"},
		},
		What: []sync_to_target.WhatItem{
			{
				DataObject: &data_source.DataObjectReference{
					FullName: "ANALYTICS.ANALYTICS.MY_FIRST_DBT_MODEL",
					Type:     "table",
				},
				Permissions: []string{"SELECT"},
			},
		},
		Id: "AccessRole1",
	}

	access := map[string]sync_to_target.EnrichedAccess{
		"IT_TESTROLE1": {
			Access: access1,
			AccessProvider: &sync_to_target.AccessProvider{
				Id:          "AccessProvider1",
				Access:      []*sync_to_target.Access{access1},
				Name:        "AccessProvider1",
				Action:      sync_to_target.Grant,
				NamingHint:  "TESTROLE1",
				Delete:      false,
				Description: "Integration testing",
			},
		},
	}

	dataAccessSyncer := snowflake.NewDataAccessSyncer()

	config := s.getConfig()

	//When
	err := dataAccessSyncer.SyncAccessAsCodeToTarget(context.Background(), access, prefix, config)

	//Then
	s.NoError(err)

	//Given
	access = make(map[string]sync_to_target.EnrichedAccess)

	//When
	err = dataAccessSyncer.SyncAccessAsCodeToTarget(context.Background(), access, prefix, config)

	//Then
	s.NoError(err)
}
