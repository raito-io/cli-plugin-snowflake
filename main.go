package main

import (
	"fmt"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/raito-io/cli/base"
	"github.com/raito-io/cli/base/info"
	"github.com/raito-io/cli/base/util/plugin"
)

var version = "0.0.0"

var logger hclog.Logger

func main() {
	logger = base.Logger()
	logger.SetLevel(hclog.Debug)

	err := base.RegisterPlugins(&IdentityStoreSyncer{}, &DataSourceSyncer{}, &AccessSyncer{}, &DataUsageSyncer{}, &info.InfoImpl{
		Info: plugin.PluginInfo{
			Name:    "Snowflake",
			Version: plugin.ParseVersion(version),
			Parameters: []plugin.ParameterInfo{
				{Name: "sf-account", Description: "The account name of the Snowflake account to connect to. For example, xy123456.eu-central-1", Mandatory: true},
				{Name: "sf-user", Description: "The username to authenticate against the Snowflake account.", Mandatory: true},
				{Name: "sf-password", Description: "The username to authenticate against the Snowflake account.", Mandatory: true},
				{Name: "sf-role", Description: "The name of the role to use for executing the necessary queries. If not specified 'ACCOUNTADMIN' is used.", Mandatory: false},
				{Name: "sf-database", Description: "The name of the main database to connect to. This is necessary for building the connection string.", Mandatory: true},
				{Name: "sf-excluded-databases", Description: "The optional comma-separated list of databases that should be skipped.", Mandatory: false},
				{Name: "sf-excluded-schemas", Description: "The optional comma-separated list of schemas that should be skipped. This can either be in a specific database (as <database>.<schema>) or a just a schema name that should be skipped in all databases.", Mandatory: false},
				{Name: "sf-excluded-owners", Description: "The optional comma-separated list of owners that need to be skipped when syncing users or marked as read-only when importing roles as Access Providers. This is typically  used to not synchronize the users that were imported from an external Identity Store (like Okta, Active Directory, ...).", Mandatory: false},
				{Name: "sf-create-future-grants", Description: "If set, future grants will be created for all the created grants. This may lead to a less controlled situation, so only use when you are sure.", Mandatory: false},
			},
		},
	})

	if err != nil {
		logger.Error(fmt.Sprintf("error while registering plugins: %s", err.Error()))
	}
}
