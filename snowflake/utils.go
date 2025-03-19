package snowflake

import (
	"crypto/rsa"
	"encoding/pem"
	"fmt"
	"os"
	"strings"

	"github.com/hashicorp/go-hclog"
	"github.com/raito-io/cli/base"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/golang-set/set"
	"github.com/youmark/pkcs8"
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

func shareExternalIdGenerator(name string) string {
	return fmt.Sprintf("%s%s", apTypeSharePrefix, name)
}

func isDatabaseRole(apType *string) bool {
	return apType != nil && strings.EqualFold(*apType, apTypeDatabaseRole)
}

func getWorkerPoolSize(configMap *config.ConfigMap) int {
	size := configMap.GetInt(SfWorkerPoolSize)
	if size <= 0 {
		return 10
	}

	return size
}

func isDatabaseRoleByExternalId(externalId string) bool {
	if strings.HasPrefix(externalId, externalIdDatabaseRolePrefix) {
		externalIdWithoutPrefix := strings.TrimPrefix(externalId, externalIdDatabaseRolePrefix)
		parts := strings.Split(externalIdWithoutPrefix, externalIdDatabaseRoleDivider)

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
