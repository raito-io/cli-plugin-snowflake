package snowflake

import (
	"context"
	"fmt"
	"strings"
	"time"

	is "github.com/raito-io/cli/base/identity_store"
	"github.com/raito-io/cli/base/tag"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers"
	"github.com/raito-io/golang-set/set"
)

//go:generate go run github.com/vektra/mockery/v2 --name=identityStoreRepository --with-expecter --inpackage
type identityStoreRepository interface {
	Close() error
	TotalQueryTime() time.Duration
	GetUsers() ([]UserEntity, error)
	GetTagsByDomain(domain string) (map[string][]*tag.Tag, error)
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

func (s *IdentityStoreSyncer) GetIdentityStoreMetaData(_ context.Context, _ *config.ConfigMap) (*is.MetaData, error) {
	Logger.Debug("Returning meta data for Snowflake identity store")

	return &is.MetaData{
		Type:        "snowflake",
		CanBeMaster: false,
		CanBeLinked: false,
	}, nil
}

func (s *IdentityStoreSyncer) retrieveAdditionalUserTags(repo identityStoreRepository, configMap *config.ConfigMap) (map[string][]*tag.Tag, error) {
	standard := configMap.GetBoolWithDefault(SfStandardEdition, false)
	skipTags := configMap.GetBoolWithDefault(SfSkipTags, false)

	shouldRetrieveTags := !standard && !skipTags
	if !shouldRetrieveTags {
		return nil, nil
	}

	allUserTags, err := repo.GetTagsByDomain("USER")
	if err != nil {
		return nil, err
	}

	return allUserTags, nil
}

func (s *IdentityStoreSyncer) SyncIdentityStore(ctx context.Context, identityHandler wrappers.IdentityStoreIdentityHandler, configMap *config.ConfigMap) error {
	repo, err := s.repoProvider(configMap.Parameters, "")
	if err != nil {
		return err
	}

	defer func() {
		Logger.Info(fmt.Sprintf("Total snowflake query time:  %s", repo.TotalQueryTime()))
		repo.Close()
	}()

	userRows, err := repo.GetUsers()
	if err != nil {
		return err
	}

	visitedEmailSet := set.NewSet[string]()

	allUserTags, err := s.retrieveAdditionalUserTags(repo, configMap)
	if err != nil {
		return err
	}

	for _, userRow := range userRows {
		Logger.Debug(fmt.Sprintf("Handling user %q", userRow.Name))

		var tags []*tag.Tag
		if len(allUserTags[userRow.Name]) > 0 {
			tags = allUserTags[userRow.Name]
		}

		displayName := userRow.DisplayName
		if displayName == "" {
			displayName = userRow.Name
		}

		isMachine := userRow.Type != nil && (strings.EqualFold(*userRow.Type, "SERVICE") || strings.EqualFold(*userRow.Type, "LEGACY_SERVICE"))

		email := ""

		if userRow.Email != nil {
			email = strings.ToLower(*userRow.Email)

			// this is a PATCH for RAITO-349, will be removed after appserver fix is in production
			if email != "" {
				if !visitedEmailSet.Contains(email) {
					visitedEmailSet.Add(email)
				} else {
					emailParts := strings.Split(email, "@")
					email = fmt.Sprintf("%s+%s@%s", emailParts[0], strings.ToLower(userRow.LoginName), emailParts[1])
					visitedEmailSet.Add(email)
				}
			}
		}

		user := is.User{
			ExternalId: cleanDoubleQuotes(userRow.LoginName),
			UserName:   cleanDoubleQuotes(userRow.Name),
			Name:       cleanDoubleQuotes(displayName),
			Email:      email,
			Tags:       tags,
			IsMachine:  &isMachine,
		}

		err = identityHandler.AddUsers(&user)
		if err != nil {
			return err
		}
	}

	return nil
}
