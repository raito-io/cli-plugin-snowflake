package snowflake

import (
	"fmt"
	"strings"

	"github.com/hashicorp/go-hclog"
	"github.com/raito-io/cli/base"
)

const (
	externalIdDatabaseRolePrefix  = "DATABASEROLE###DATABASE:"
	externalIdDatabaseRoleDivider = "###ROLE:"
)

var logger hclog.Logger

func init() {
	logger = base.Logger()
}

func cleanDoubleQuotes(input string) string {
	if len(input) >= 2 && strings.HasPrefix(input, "\"") && strings.HasSuffix(input, "\"") {
		return input[1 : len(input)-1]
	}

	return input
}

func parseDatabaseRoleExternalId(externalId string) (database string, cleanedRoleName string, err error) {
	if strings.HasPrefix(externalId, externalIdDatabaseRolePrefix) {
		externalIdWithoutPrefix := strings.TrimPrefix(externalId, externalIdDatabaseRolePrefix)
		parts := strings.Split(externalIdWithoutPrefix, externalIdDatabaseRoleDivider)

		if len(parts) == 2 && !strings.EqualFold(parts[0], "") && !strings.EqualFold(parts[1], "") {
			database = parts[0]
			cleanedRoleName = parts[1]

			return database, cleanedRoleName, nil
		}
	}

	return "", "", fmt.Errorf("role %q is not in the expected database role format", externalId)
}

func parseDatabaseRoleRoleName(sfRoleName string) (database string, cleanedRoleName string, err error) {
	parts := strings.Split(sfRoleName, ".")
	if (parts == nil) || (len(parts) < 2) {
		return "", "", fmt.Errorf("role %q is not a database role", sfRoleName)
	}

	database = parts[0]
	cleanedRoleName = parts[1]

	return database, cleanedRoleName, nil
}

func databaseRoleExternalIdGenerator(database, roleName string) string {
	return fmt.Sprintf("%s%s%s%s", externalIdDatabaseRolePrefix, database, externalIdDatabaseRoleDivider, roleName)
}

func accountRoleExternalIdGenerator(roleName string) string {
	return roleName
}

func isDatabaseRole(apType *string) bool {
	return apType != nil && strings.EqualFold(*apType, apTypeDatabaseRole)
}

func isDatabaseRoleByExternalId(externalId string) bool {
	if strings.HasPrefix(externalId, externalIdDatabaseRolePrefix) {
		externalIdWithoutPrefix := strings.TrimPrefix(externalId, externalIdDatabaseRolePrefix)
		parts := strings.Split(externalIdWithoutPrefix, externalIdDatabaseRoleDivider)

		return len(parts) == 2 && !strings.EqualFold(parts[0], "") && !strings.EqualFold(parts[1], "")
	}

	return false
}
