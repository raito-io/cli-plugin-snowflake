package snowflake

import (
	"context"
	"fmt"
	"strings"
	"time"

	is "github.com/raito-io/cli/base/identity_store"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers"
	"github.com/raito-io/golang-set/set"
)

//go:generate go run github.com/vektra/mockery/v2 --name=identityStoreRepository --with-expecter --inpackage
type identityStoreRepository interface {
	Close() error
	TotalQueryTime() time.Duration
	GetUsers() ([]UserEntity, error)
}

type IdentityStoreSyncer struct {
	repoProvider func(params map[string]string, role string) (identityStoreRepository, error)
}

func NewIdentityStoreSyncer() *IdentityStoreSyncer {
	return &IdentityStoreSyncer{repoProvider: newIdentityStoreSnowflakeRepo}
}

func newIdentityStoreSnowflakeRepo(params map[string]string, role string) (identityStoreRepository, error) {
	return NewSnowflakeRepository(params, role)
}

func (s *IdentityStoreSyncer) GetIdentityStoreMetaData(_ context.Context) (*is.MetaData, error) {
	logger.Debug("Returning meta data for Snowflake identity store")

	return &is.MetaData{
		Type: "snowflake",
	}, nil
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

	userRows, err := repo.GetUsers()
	if err != nil {
		return err
	}

	visitedEmailSet := set.NewSet[string]()

	for _, userRow := range userRows {
		logger.Debug(fmt.Sprintf("Handling user %q", userRow.Name))

		userRow.Email = strings.ToLower(userRow.Email)

		// this is a PATCH for RAITO-349, will be removed after appserver fix is in production
		if userRow.Email != "" {
			if !visitedEmailSet.Contains(userRow.Email) {
				visitedEmailSet.Add(userRow.Email)
			} else {
				emailParts := strings.Split(userRow.Email, "@")
				userRow.Email = fmt.Sprintf("%s+%s@%s", emailParts[0], strings.ToLower(userRow.LoginName), emailParts[1])
				visitedEmailSet.Add(userRow.Email)
			}
		}

		displayName := userRow.DisplayName
		if displayName == "" {
			displayName = userRow.Name
		}

		user := is.User{
			ExternalId: cleanDoubleQuotes(userRow.LoginName),
			UserName:   cleanDoubleQuotes(userRow.Name),
			Name:       cleanDoubleQuotes(displayName),
			Email:      userRow.Email,
		}

		err = identityHandler.AddUsers(&user)
		if err != nil {
			return err
		}
	}

	return nil
}

func cleanDoubleQuotes(input string) string {
	if len(input) > 0 && strings.HasPrefix(input, "\"") && strings.HasSuffix(input, "\"") {
		return input[1 : len(input)-1]
	}

	return input
}
