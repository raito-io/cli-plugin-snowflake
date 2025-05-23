package snowflake

import (
	"fmt"
	"strings"

	"github.com/aws/smithy-go/ptr"
	importer "github.com/raito-io/cli/base/access_provider/sync_to_target"
	"github.com/raito-io/cli/base/util/slice"
	"github.com/raito-io/golang-set/set"
)

func (s *AccessToTargetSyncer) processSharesToTarget(toProcessApIds []string, apsById map[string]*ApSyncToTargetItem) error {
	// do combine update action to create, rename and update shares
	toUpdateItems := s.accessProvidersForMutationActions(toProcessApIds, apsById, []ApMutationAction{ApMutationActionCreate, ApMutationActionRename, ApMutationActionUpdate})
	toRemoveItems := s.accessProvidersForMutationActions(toProcessApIds, apsById, []ApMutationAction{ApMutationActionDelete})

	Logger.Info(fmt.Sprintf("Configuring access provider as shares in Snowflake. Update %d shares remove %d shares", len(toUpdateItems), len(toRemoveItems)))

	// build a metadata permission map
	metadata := s.buildMetaDataMap()

	// create/update shares
	if err := s.sharesCreateOrUpdateAll(toUpdateItems, metadata); err != nil {
		return fmt.Errorf("creating/updating shares on Snowflake: %w", err)
	}

	// remove old shares
	if err := s.sharesRemoveAll(toRemoveItems); err != nil {
		return fmt.Errorf("deleting shares on Snowflake: %w", err)
	}

	return nil
}

func (s *AccessToTargetSyncer) sharesCreateOrUpdateAll(toUpdateItems []*ApSyncToTargetItem, metadata map[string]map[string]struct{}) error {
	for _, share := range toUpdateItems {
		shareName, err := s.shareCreateOrUpdateItem(share.accessProvider, metadata)
		fi := importer.AccessProviderSyncFeedback{AccessProvider: share.accessProvider.Id, ActualName: shareName, ExternalId: ptr.String(apTypeSharePrefix + shareName)}

		if err != nil {
			fi.Errors = append(fi.Errors, err.Error())
		}

		err = s.accessProviderFeedbackHandler.AddAccessProviderFeedback(fi)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *AccessToTargetSyncer) sharesRemoveAll(toRemoveItems []*ApSyncToTargetItem) error {
	for _, shareAp := range toRemoveItems {
		fi := importer.AccessProviderSyncFeedback{AccessProvider: shareAp.accessProvider.Id, ActualName: shareAp.calculatedExternalId, ExternalId: ptr.String(shareAp.calculatedExternalId)}

		err := s.shareRemoveItem(shareAp.calculatedExternalId)
		if err != nil {
			fi.Errors = append(fi.Errors, err.Error())
		}

		err = s.accessProviderFeedbackHandler.AddAccessProviderFeedback(fi)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *AccessToTargetSyncer) shareCreateOrUpdateItem(share *importer.AccessProvider, metaData map[string]map[string]struct{}) (string, error) {
	Logger.Info(fmt.Sprintf("Updating share %q", share.Name))

	databases := set.NewSet[string]()

	shareName := maskPrefix + strings.ToUpper(share.NamingHint)

	if share.ActualName != nil {
		shareName = *share.ActualName
	}

	for _, do := range share.What {
		database := strings.SplitN(do.DataObject.FullName, ".", 2)[0]
		databases.Add(database)
	}

	err := s.repo.CreateShare(shareName)
	if err != nil {
		return shareName, fmt.Errorf("upsert share: %w", err)
	}

	var foundGrants []Grant

	if share.ExternalId != nil {
		existingGrants, err2 := s.repo.GetGrantsToShare(shareName)
		if err2 != nil {
			return shareName, fmt.Errorf("get grants to share: %w", err2)
		}

		foundGrants = make([]Grant, 0, len(existingGrants))

		for _, grant := range existingGrants {
			if strings.EqualFold(grant.Privilege, "OWNERSHIP") {
				Logger.Info(fmt.Sprintf("Ignoring permission %q on %q for Share %q as this will remain untouched", grant.Privilege, grant.Name, share.Name))
			} else {
				onType := convertSnowflakeGrantTypeToRaito(grant.GrantedOn)
				name := grant.Name

				if onType == Function { // For functions, we need to do a special conversion
					name = s.accessSyncer.getFullNameFromGrant(name, onType)
				}

				foundGrants = append(foundGrants, Grant{grant.Privilege, onType, name})
			}
		}
	}

	grants, err := s.createGrantsForWhatObjects(share, s.buildMetaDataMap())
	if err != nil {
		return "", fmt.Errorf("create grants for what objects: %w", err)
	}

	grantsToAdd := slice.SliceDifference(grants.Slice(), foundGrants)
	grantsToRemove := slice.SliceDifference(foundGrants, grants.Slice())

	for _, grant := range grantsToAdd {
		if verifyGrant(grant, metaData) {
			err = s.repo.ExecuteGrantOnShare(grant.Permissions, grant.OnWithType(), shareName)
			if err != nil {
				return shareName, fmt.Errorf("execute grant on share: %w", err)
			}
		}
	}

	for _, grant := range grantsToRemove {
		if verifyGrant(grant, metaData) {
			err = s.repo.ExecuteRevokeOnShare(grant.Permissions, grant.OnWithType(), shareName)
			if err != nil {
				return shareName, fmt.Errorf("execute revoke on share: %w", err)
			}
		}
	}

	if grants.Size() > 0 {
		err = s.repo.SetShareAccounts(shareName, share.Who.Recipients)
		if err != nil {
			return shareName, fmt.Errorf("set share accounts: %w", err)
		}
	} else {
		Logger.Warn(fmt.Sprintf("Share %s has no database assigned. Cannot add accounts to share", shareName))
	}

	return shareName, nil
}

func (s *AccessToTargetSyncer) shareRemoveItem(shareId string) error {
	Logger.Info(fmt.Sprintf("Remove share %q", shareId))

	shareName := strings.TrimPrefix(shareId, maskPrefix)

	err := s.repo.DropShare(shareName)
	if err != nil {
		return fmt.Errorf("drop share: %w", err)
	}

	return nil
}
