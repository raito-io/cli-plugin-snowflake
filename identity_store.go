package main

import (
	"fmt"
	"github.com/blockloop/scan"
	isb "github.com/raito-io/cli/base/identity_store"
	"github.com/raito-io/cli/common/api"
	"github.com/raito-io/cli/common/api/identity_store"
	"strings"
	"time"
)

type IdentityStoreSyncer struct {
}

func (s *IdentityStoreSyncer) SyncIdentityStore(config *identity_store.IdentityStoreSyncConfig) identity_store.IdentityStoreSyncResult {
	fileCreator, err := isb.NewIdentityStoreFileCreator(config)
	if err != nil {
		return identity_store.IdentityStoreSyncResult {
			Error: api.ToErrorResult(err),
		}
	}
	defer fileCreator.Close()

	start := time.Now()

	q := "SHOW USERS"
	rows, err := ConnectAndQuery(config.Parameters, "", q)
	if err != nil {
		return identity_store.IdentityStoreSyncResult {
			Error: api.ToErrorResult(err),
		}
	}

	excludedOwners := ""
	if v, ok := config.Parameters[SfExcludedOwners]; ok && v != nil {
		excludedOwners = v.(string)
	}

	ownerExclusions := make(map[string]struct{})
	if excludedOwners != "" {
		for _, o := range strings.Split(excludedOwners, ",") {
			ownerExclusions[strings.TrimSpace(o)] = struct{}{}
		}
	}

	var userRows []userEntity
	err = scan.Rows(&userRows, rows)
	if err != nil {
		return identity_store.IdentityStoreSyncResult {
			Error: api.ToErrorResult(fmt.Errorf("Error while parsing result from users query: %s", err.Error())),
		}
	}
	err = CheckSFLimitExceeded(q, len(userRows))
	if err != nil {
		return identity_store.IdentityStoreSyncResult {
			Error: api.ToErrorResult(fmt.Errorf("Error while fetching users: %s", err.Error())),
		}
	}

	users := make([]isb.User, 0, 20)
	for _, userRow := range userRows {
		logger.Debug(fmt.Sprintf("Handling user %q", userRow.UserName))
		if _, f := ownerExclusions[userRow.Owner]; f {
			logger.Debug("Skipping user as it's owned by an excluded owner")
			continue
		}
		user := isb.User{
			ExternalId: userRow.UserName,
			UserName: userRow.UserName,
			Name: userRow.DisplayName,
			Email: userRow.Email,
		}
		users = append(users, user)
	}
	err = fileCreator.AddUsers(users)
	if err != nil {
		return identity_store.IdentityStoreSyncResult {
			Error: api.ToErrorResult(err),
		}
	}

	sec := time.Since(start).Round(time.Millisecond)

	logger.Info(fmt.Sprintf("Fetched %d users from Snowflake in %s", fileCreator.GetUserCount(), sec))

	return identity_store.IdentityStoreSyncResult {	}
}

type userEntity struct {
	UserName string `db:"login_name"`
	DisplayName string `db:"display_name"`
	Email string `db:"email"`
	Owner string `db:"owner"`
}
