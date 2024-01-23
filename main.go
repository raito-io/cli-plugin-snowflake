package main

import (
	"fmt"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/raito-io/cli/base"
	"github.com/raito-io/cli/base/access_provider"
	"github.com/raito-io/cli/base/info"
	"github.com/raito-io/cli/base/util/plugin"
	"github.com/raito-io/cli/base/wrappers"

	"github.com/raito-io/cli-plugin-snowflake/snowflake"
)

var version = "0.0.0"

var logger hclog.Logger

func main() {
	logger = base.Logger()
	logger.SetLevel(hclog.Debug)

	err := base.RegisterPlugins(
		wrappers.IdentityStoreSync(snowflake.NewIdentityStoreSyncer()),
		wrappers.DataSourceSync(snowflake.NewDataSourceSyncer()),
		wrappers.DataAccessSync(snowflake.NewDataAccessSyncer(snowflake.RoleNameConstraints), access_provider.WithSupportPartialSync()),
		wrappers.DataUsageSync(snowflake.NewDataUsageSyncer()),
		&info.InfoImpl{
			Info: &plugin.PluginInfo{
				Name:    "Snowflake",
				Version: plugin.ParseVersion(version),
				Parameters: []*plugin.ParameterInfo{
					{Name: snowflake.SfAccount, Description: "The account name of the Snowflake account to connect to. For example, xy123456.eu-central-1", Mandatory: true},
					{Name: snowflake.SfUser, Description: "The username to authenticate against the Snowflake account.", Mandatory: true},
					{Name: snowflake.SfPassword, Description: "The username to authenticate against the Snowflake account.", Mandatory: true},
					{Name: snowflake.SfRole, Description: "The name of the role to use for executing the necessary queries. If not specified 'ACCOUNTADMIN' is used.", Mandatory: false},
					{Name: snowflake.SfExcludedDatabases, Description: "The optional comma-separated list of databases that should be skipped.", Mandatory: false},
					{Name: snowflake.SfExcludedSchemas, Description: "The optional comma-separated list of schemas that should be skipped. This can either be in a specific database (as <database>.<schema>) or a just a schema name that should be skipped in all databases. By default INFORMATION_SCHEMA is skipped since there are no access controls to manage", Mandatory: false},
					{Name: snowflake.SfExcludedRoles, Description: "The optional comma-separated list of roles that should be skipped. You should not exclude roles which others (not-excluded) roles depend on as that would break the hierarchy.", Mandatory: false},
					{Name: snowflake.SfExternalIdentityStoreOwners, Description: "The optional comma-separated list of owners of SCIM integrations with external identity stores (e.g. Okta or Active Directory). Roles which are imported from groups from these identity stores will be partially or fully locked in Raito to avoid a conflict with the SCIM integration.", Mandatory: false},
					{Name: snowflake.SfLinkToExternalIdentityStoreGroups, Description: "This boolean parameter can be set when the 'sf-external-identity-store-owners' parameter is set. When 'true', the 'who' of roles coming from the external access provider will refer to the group of the external access provider and the 'what' of the access provider will still be editable in Raito Cloud. When 'false' (default) the 'who' will contain the unpacked users of the group and the access provider in Raito Cloud will be locked entirely.", Mandatory: false},
					{Name: snowflake.SfStandardEdition, Description: "If set enterprise features will be disabled", Mandatory: false},
					{Name: snowflake.SfSkipTags, Description: "If set, tags will not be fetched", Mandatory: false},
					{Name: snowflake.SfSkipColumns, Description: "If set, columns and column masking policies will not be imported.", Mandatory: false},
					{Name: snowflake.SfDataUsageWindow, Description: "The maximum number of days of usage data to retrieve. Default is 90. Maximum is 90 days. ", Mandatory: false},
					{Name: snowflake.SfDatabaseRoles, Description: "If set, database-roles for all databases will be fetched. ", Mandatory: false},
				},
			},
		})

	if err != nil {
		logger.Error(fmt.Sprintf("error while registering plugins: %s", err.Error()))
	}
}
