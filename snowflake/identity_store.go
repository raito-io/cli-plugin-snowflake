package snowflake

import (
	"context"
	"fmt"
	"strings"
	"time"

	is "github.com/raito-io/cli/base/identity_store"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers"
)

//go:generate go run github.com/vektra/mockery/v2 --name=identityStoreRepository --with-expecter --inpackage
type identityStoreRepository interface {
	Close() error
	TotalQueryTime() time.Duration
	GetUsers() ([]UserEntity, error)
}

type IdentityStoreSyncer struct {
	repoProvider func(params map[string]interface{}, role string) (identityStoreRepository, error)
}

func NewIdentityStoreSyncer() *IdentityStoreSyncer {
	return &IdentityStoreSyncer{repoProvider: newIdentityStoreSnowflakeRepo}
}

func newIdentityStoreSnowflakeRepo(params map[string]interface{}, role string) (identityStoreRepository, error) {
	return NewSnowflakeRepository(params, role)
}

func (s *IdentityStoreSyncer) SyncIdentityStore(ctx context.Context, identityHandler wrappers.IdentityStoreIdentityHandler, configMap *config.ConfigMap) error {
	repo, err := s.repoProvider(configMap.Parameters, "")
	if err != nil {
		return err
	}

	defer func() {
		logger.Info(fmt.Sprintf("Total snowflake query time:  %s", repo.TotalQueryTime()))
		repo.Close()
	}()

	excludedOwners := ""
	if v, ok := configMap.Parameters[SfExcludedOwners]; ok && v != nil {
		excludedOwners = v.(string)
	}

	ownerExclusions := make(map[string]struct{})

	if excludedOwners != "" {
		for _, o := range strings.Split(excludedOwners, ",") {
			ownerExclusions[strings.TrimSpace(o)] = struct{}{}
		}
	}

	userRows, err := repo.GetUsers()
	if err != nil {
		return err
	}

	for _, userRow := range userRows {
		logger.Debug(fmt.Sprintf("Handling user %q", userRow.Name))

		if _, f := ownerExclusions[userRow.Owner]; f {
			logger.Debug("Skipping user as it's owned by an excluded owner")
			continue
		}
		user := is.User{
			ExternalId: userRow.LoginName,
			UserName:   userRow.Name,
			Name:       userRow.DisplayName,
			Email:      userRow.Email,
		}

		err = identityHandler.AddUsers(&user)
		if err != nil {
			return err
		}
	}

	return nil
}
