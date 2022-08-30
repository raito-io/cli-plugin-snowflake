package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/blockloop/scan"
	is "github.com/raito-io/cli/base/identity_store"
	e "github.com/raito-io/cli/base/util/error"
)

type IdentityStoreSyncer struct {
}

func (s *IdentityStoreSyncer) SyncIdentityStore(config *is.IdentityStoreSyncConfig) is.IdentityStoreSyncResult {
	fileCreator, err := is.NewIdentityStoreFileCreator(config)
	if err != nil {
		return is.IdentityStoreSyncResult{
			Error: e.ToErrorResult(err),
		}
	}
	defer fileCreator.Close()

	start := time.Now()
	q := "SHOW USERS"

	rows, err := ConnectAndQuery(config.Parameters, "", q)
	if err != nil {
		return is.IdentityStoreSyncResult{
			Error: e.ToErrorResult(err),
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
		return is.IdentityStoreSyncResult{
			Error: e.ToErrorResult(fmt.Errorf("error while parsing result from users query: %s", err.Error())),
		}
	}

	err = CheckSFLimitExceeded(q, len(userRows))
	if err != nil {
		return is.IdentityStoreSyncResult{
			Error: e.ToErrorResult(fmt.Errorf("error while fetching users: %s", err.Error())),
		}
	}

	users := make([]is.User, 0, 20)

	for _, userRow := range userRows {
		logger.Debug(fmt.Sprintf("Handling user %q", userRow.UserName))

		if _, f := ownerExclusions[userRow.Owner]; f {
			logger.Debug("Skipping user as it's owned by an excluded owner")
			continue
		}
		user := is.User{
			ExternalId: userRow.UserName,
			UserName:   userRow.UserName,
			Name:       userRow.DisplayName,
			Email:      userRow.Email,
		}
		users = append(users, user)
	}

	err = fileCreator.AddUsers(users)
	if err != nil {
		return is.IdentityStoreSyncResult{
			Error: e.ToErrorResult(err),
		}
	}

	sec := time.Since(start).Round(time.Millisecond)

	logger.Info(fmt.Sprintf("Fetched %d users from Snowflake in %s", fileCreator.GetUserCount(), sec))

	return is.IdentityStoreSyncResult{}
}

type userEntity struct {
	UserName    string `db:"login_name"`
	DisplayName string `db:"display_name"`
	Email       string `db:"email"`
	Owner       string `db:"owner"`
}
