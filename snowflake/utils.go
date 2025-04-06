package snowflake

import (
	"crypto/rsa"
	"encoding/pem"
	"fmt"
	"os"
	"runtime/debug"
	"strings"

	"github.com/hashicorp/go-hclog"
	"github.com/raito-io/cli/base"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/golang-set/set"
	"github.com/youmark/pkcs8"
)

const (
	externalIdDatabaseRolePrefix    = "DATABASEROLE###DATABASE:"
	externalIdRoleDivider           = "###ROLE:"
	externalIdApplicationRolePrefix = "APPLICATIONROLE###APPLICATION:"
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
	return parseRoleWithPrefixFromExternalId(externalId, externalIdDatabaseRolePrefix)
}

func parseApplicationRoleExternalId(externalId string) (database string, cleanedRoleName string, err error) {
	return parseRoleWithPrefixFromExternalId(externalId, externalIdApplicationRolePrefix)
}

func parseRoleWithPrefixFromExternalId(externalId string, prefix string) (database string, cleanedRoleName string, err error) {
	if strings.HasPrefix(externalId, prefix) {
		externalIdWithoutPrefix := strings.TrimPrefix(externalId, prefix)
		parts := strings.Split(externalIdWithoutPrefix, externalIdRoleDivider)

		if len(parts) == 2 && !strings.EqualFold(parts[0], "") && !strings.EqualFold(parts[1], "") {
			database = parts[0]
			cleanedRoleName = parts[1]

			return database, cleanedRoleName, nil
		}
	}

	stack := debug.Stack()
	logger.Info("Stack trace", "stack", string(stack))

	return "", "", fmt.Errorf("role %q is not in the expected format", externalId)
}

func parseNamespacedRoleRoleName(sfRoleName string) (namespace string, cleanedRoleName string, err error) {
	parts := strings.Split(sfRoleName, ".")
	if (parts == nil) || (len(parts) < 2) {
		return "", "", fmt.Errorf("role %q is not a namespaced role", sfRoleName)
	}

	namespace = parts[0]
	cleanedRoleName = parts[1]

	return namespace, cleanedRoleName, nil
}

func databaseRoleExternalIdGenerator(database, roleName string) string {
	return fmt.Sprintf("%s%s%s%s", externalIdDatabaseRolePrefix, database, externalIdRoleDivider, roleName)
}

func applicationRoleExternalIdGenerator(database, roleName string) string {
	return fmt.Sprintf("%s%s%s%s", externalIdApplicationRolePrefix, database, externalIdRoleDivider, roleName)
}

func accountRoleExternalIdGenerator(roleName string) string {
	return roleName
}

func shareExternalIdGenerator(name string) string {
	return fmt.Sprintf("%s%s", apTypeSharePrefix, name)
}

func isDatabaseRole(apType *string) bool {
	return apType != nil && strings.EqualFold(*apType, apTypeDatabaseRole)
}

func isApplicationRole(apType *string) bool {
	return apType != nil && strings.EqualFold(*apType, apTypeApplicationRole)
}

func getWorkerPoolSize(configMap *config.ConfigMap) int {
	size := configMap.GetInt(SfWorkerPoolSize)
	if size <= 0 {
		return 10
	}

	return size
}

func isDatabaseRoleByExternalId(externalId string) bool {
	return isRoleWithPrefixByExternalId(externalId, externalIdDatabaseRolePrefix)
}

func isApplicationRoleByExternalId(externalId string) bool {
	return isRoleWithPrefixByExternalId(externalId, externalIdApplicationRolePrefix)
}

func isRoleWithPrefixByExternalId(externalId string, prefix string) bool {
	if strings.HasPrefix(externalId, prefix) {
		externalIdWithoutPrefix := strings.TrimPrefix(externalId, prefix)
		parts := strings.Split(externalIdWithoutPrefix, externalIdRoleDivider)

		return len(parts) == 2 && !strings.EqualFold(parts[0], "") && !strings.EqualFold(parts[1], "")
	}

	return false
}

func parseCommaSeparatedList(list string) set.Set[string] {
	list = strings.TrimSpace(list)

	if list == "" {
		return set.NewSet[string]()
	}

	ret := set.NewSet[string]()

	for _, v := range strings.Split(list, ",") {
		ret.Add(strings.TrimSpace(v))
	}

	return ret
}

func LoadPrivateKeyFromFile(file string, passphrase string) (*rsa.PrivateKey, error) {
	pemData, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("opening file %q: %w", file, err)
	}

	return LoadPrivateKey(pemData, passphrase)
}

func LoadPrivateKey(pemData []byte, passphrase string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(pemData)
	if block == nil {
		return nil, fmt.Errorf("failed to decode PEM block containing the private key; block is nil")
	}

	var key interface{}
	var err error

	if block.Type == "ENCRYPTED PRIVATE KEY" {
		if passphrase == "" {
			return nil, fmt.Errorf("private key is encrypted but the parameter %s is not provided", SfPrivateKeyPassphrase)
		}

		key, err = pkcs8.ParsePKCS8PrivateKey(block.Bytes, []byte(passphrase))
	} else if block.Type == "PRIVATE KEY" {
		key, err = pkcs8.ParsePKCS8PrivateKey(block.Bytes)
	} else {
		return nil, fmt.Errorf("unsupported private key block type %q", block.Type)
	}

	if err != nil {
		return nil, fmt.Errorf("parsing private key: %w", err)
	}

	return key.(*rsa.PrivateKey), nil
}
