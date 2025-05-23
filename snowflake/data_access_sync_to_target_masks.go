package snowflake

import (
	"fmt"

	"strings"
	"unicode"

	"github.com/aws/smithy-go/ptr"
	gonanoid "github.com/matoous/go-nanoid/v2"
	importer "github.com/raito-io/cli/base/access_provider/sync_to_target"
)

func (s *AccessToTargetSyncer) processMasksToTarget(toProcessApIds []string, apsById map[string]*ApSyncToTargetItem, mappedGrantExternalIdById map[string]string) error {
	// Do combine update action to create, rename and update masks
	toUpdateItems := s.accessProvidersForMutationActions(toProcessApIds, apsById, []ApMutationAction{ApMutationActionCreate, ApMutationActionRename, ApMutationActionUpdate})
	toRemoveItems := s.accessProvidersForMutationActions(toProcessApIds, apsById, []ApMutationAction{ApMutationActionDelete})

	// Validate if we can set masks on this SF account
	if s.configMap.GetBoolWithDefault(SfStandardEdition, false) {
		if len(toUpdateItems) > 0 || len(toRemoveItems) > 0 {
			Logger.Error("Skipping masking policies due to Snowflake Standard Edition.")
		}

		return nil
	}

	// Count all masks to be created, updated or removed
	Logger.Info(fmt.Sprintf("Configuring access provider as masks in Snowflake. Create/update %d masks and remove %d masks", len(toUpdateItems), len(toRemoveItems)))

	// Execute update/create masks actions
	if err := s.masksCreateOrUpdateAll(toUpdateItems, mappedGrantExternalIdById); err != nil {
		return fmt.Errorf("creating/updating masks on Snowflake: %w", err)
	}

	// Remove all expected masks
	if err := s.masksRemoveAll(toRemoveItems); err != nil {
		return fmt.Errorf("removing masks on Snowflake: %w", err)
	}

	Logger.Info("Finalized masks updates and removals on Snowflake")

	return nil
}

func (s *AccessToTargetSyncer) masksCreateOrUpdateAll(toProcessMasks []*ApSyncToTargetItem, mappedGrantExternalIdById map[string]string) error {
	if len(toProcessMasks) == 0 {
		return nil
	}

	for _, mask := range toProcessMasks {
		maskName, err2 := s.maskCreateOrUpdateItem(mask.accessProvider, mappedGrantExternalIdById)
		fi := importer.AccessProviderSyncFeedback{AccessProvider: mask.accessProvider.Id, ActualName: maskName, ExternalId: &maskName}

		if err2 != nil {
			fi.Errors = append(fi.Errors, err2.Error())
		}

		err := s.accessProviderFeedbackHandler.AddAccessProviderFeedback(fi)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *AccessToTargetSyncer) masksRemoveAll(toRemoveMasks []*ApSyncToTargetItem) error {
	if len(toRemoveMasks) == 0 {
		return nil
	}

	for _, mask := range toRemoveMasks {
		maskExternalId := mask.calculatedExternalId
		fi := importer.AccessProviderSyncFeedback{AccessProvider: mask.accessProvider.Id, ActualName: maskExternalId, ExternalId: ptr.String(maskExternalId)}

		err := s.maskRemoveItem(maskExternalId)
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

func (s *AccessToTargetSyncer) maskCreateOrUpdateItem(mask *importer.AccessProvider, roleNameMap map[string]string) (string, error) {
	Logger.Info(fmt.Sprintf("Updating mask %q", mask.Name))

	globalMaskName := maskNamePrefixer(mask.Name)
	uniqueMaskName := maskUniqueNameGenerator(mask.Name)

	// Step 0: Load beneficiaries
	beneficiaries := MaskingBeneficiaries{
		Users: mask.Who.Users,
	}

	for _, role := range mask.Who.InheritFrom {
		if strings.HasPrefix(role, "ID:") {
			if roleName, found := roleNameMap[role[3:]]; found {
				beneficiaries.Roles = append(beneficiaries.Roles, roleName)
			}
		} else {
			beneficiaries.Roles = append(beneficiaries.Roles, role)
		}
	}

	dosPerSchema := map[string][]string{}

	for _, do := range mask.What {
		fullNameSplit := strings.Split(do.DataObject.FullName, ".")

		if len(fullNameSplit) != 4 {
			Logger.Error(fmt.Sprintf("Invalid fullname for column %s in mask %s", do.DataObject.FullName, mask.Name))

			continue
		}

		schemaName := fullNameSplit[1]
		database := fullNameSplit[0]

		schemaFullName := database + "." + schemaName

		dosPerSchema[schemaFullName] = append(dosPerSchema[schemaFullName], do.DataObject.FullName)
	}

	// Step 1: Get existing masking policies with the same prefix
	existingPolicies, err := s.repo.GetPoliciesLike("MASKING", fmt.Sprintf("%s%s", globalMaskName, "%"))
	if err != nil {
		return uniqueMaskName, err
	}

	// Step 2: For each schema create a new masking policy and force the DataObjects to use the new policy
	for schema, dos := range dosPerSchema {
		Logger.Info(fmt.Sprintf("Updating mask %q for schema %q", mask.Name, schema))
		nameSplit := strings.Split(schema, ".")

		database := nameSplit[0]
		schemaName := nameSplit[1]

		err = s.repo.CreateMaskPolicy(database, schemaName, uniqueMaskName, dos, mask.Type, &beneficiaries)
		if err != nil {
			return uniqueMaskName, err
		}
	}

	// Step 3: Remove old policies that we misted in step 1
	for _, policy := range existingPolicies {
		existingUniqueMaskNameSpit := strings.Split(policy.Name, "_")
		existingUniqueMaskName := strings.Join(existingUniqueMaskNameSpit[:len(existingUniqueMaskNameSpit)-1], "_")

		err = s.repo.DropMaskingPolicy(policy.DatabaseName, policy.SchemaName, existingUniqueMaskName)
		if err != nil {
			return uniqueMaskName, err
		}
	}

	return uniqueMaskName, nil
}

func (s *AccessToTargetSyncer) maskRemoveItem(maskName string) error {
	Logger.Info(fmt.Sprintf("Remove mask %q", maskName))

	existingPolicies, err := s.repo.GetPoliciesLike("MASKING", fmt.Sprintf("%s%s", maskName, "%"))
	if err != nil {
		return err
	}

	for _, policy := range existingPolicies {
		err = s.repo.DropMaskingPolicy(policy.DatabaseName, policy.SchemaName, maskName)
		if err != nil {
			return err
		}
	}

	return nil
}

func maskNamePrefixer(roleName string) string {
	roleNameWithoutPrefix := strings.TrimPrefix(roleName, maskPrefix)

	result := fmt.Sprintf("%s%s", maskPrefix, strings.ReplaceAll(strings.ToUpper(roleNameWithoutPrefix), " ", "_"))

	var validMaskName []rune

	for _, r := range result {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			validMaskName = append(validMaskName, r)
		}
	}

	return string(validMaskName)
}

func maskUniqueNameGenerator(name string) string {
	return maskNamePrefixer(name) + "_" + gonanoid.MustGenerate(idAlphabet, 8)
}
