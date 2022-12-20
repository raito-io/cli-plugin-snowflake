package main

import (
	"fmt"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/raito-io/cli/base"
	"github.com/raito-io/cli/base/access_provider/sync_to_target/naming_hint"
	"github.com/raito-io/cli/base/info"
	"github.com/raito-io/cli/base/util/plugin"
	"github.com/raito-io/cli/base/wrappers"
	"github.com/raito-io/cli/base/wrappers/role_based"

	"github.com/raito-io/cli-plugin-snowflake/snowflake"
)

var version = "0.0.0"

var logger hclog.Logger

// https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html#identifier-requirements
var roleNameConstraints = naming_hint.NamingConstraints{
	UpperCaseLetters:  true,
	LowerCaseLetters:  false,
	Numbers:           true,
	SpecialCharacters: "_$",
	MaxLength:         255,
}

func main() {
	logger = base.Logger()
	logger.SetLevel(hclog.Debug)

	err := base.RegisterPlugins(wrappers.IdentityStoreSync(snowflake.NewIdentityStoreSyncer()),
		wrappers.DataSourceSync(snowflake.NewDataSourceSyncer()),
		role_based.AccessProviderRoleSync(snowflake.NewDataAccessSyncer(), roleNameConstraints),
		wrappers.DataUsageSync(snowflake.NewDataUsageSyncer()), &info.InfoImpl{
			Info: plugin.PluginInfo{
				Name:    "Snowflake",
				Version: plugin.ParseVersion(version),
				Parameters: []plugin.ParameterInfo{
					{Name: "sf-account", Description: "The account name of the Snowflake account to connect to. For example, xy123456.eu-central-1", Mandatory: true},
					{Name: "sf-user", Description: "The username to authenticate against the Snowflake account.", Mandatory: true},
					{Name: "sf-password", Description: "The username to authenticate against the Snowflake account.", Mandatory: true},
					{Name: "sf-role", Description: "The name of the role to use for executing the necessary queries. If not specified 'ACCOUNTADMIN' is used.", Mandatory: false},
					{Name: "sf-excluded-databases", Description: "The optional comma-separated list of databases that should be skipped.", Mandatory: false},
					{Name: "sf-excluded-schemas", Description: "The optional comma-separated list of schemas that should be skipped. This can either be in a specific database (as <database>.<schema>) or a just a schema name that should be skipped in all databases. By default INFORMATION_SCHEMA is skipped since there are no access controls to manage", Mandatory: false},
					{Name: "sf-external-identity-store-owners", Description: "The optional comma-separated list of owners of SCIM integrations with external identity stores (e.g. Okta or Active Directory). Roles which are imported from groups from these identity stores will be partially or fully locked in Raito to avoid a conflict with the SCIM integration.", Mandatory: false},
					{Name: "sf-link-to-external-identity-store-groups", Description: "This boolean parameter can be set when the 'sf-external-identity-store-owners' parameter is set. When 'true', the 'who' of roles coming from the external access provider will refer to the group of the external access provider and the 'what' of the access provider will still be editable in Raito Cloud. When 'false' (default) the 'who' will contain the unpacked users of the group and the access provider in Raito Cloud will be locked entirely.", Mandatory: false},
					{Name: "sf-standard-edition", Description: "If set enterprise features will be disabled", Mandatory: false},
				},
			},
		})

	if err != nil {
		logger.Error(fmt.Sprintf("error while registering plugins: %s", err.Error()))
	}
}
