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
					{Name: snowflake.SfPassword, Description: fmt.Sprintf("The password to authenticate against the Snowflake account. Either this or %s must be specified", snowflake.SfPrivateKey), Mandatory: false},
					{Name: snowflake.SfPrivateKey, Description: fmt.Sprintf("The path of the file containing the private key to authenticate against the Snowflake account. Either this or %s must be specified.", snowflake.SfPassword), Mandatory: false},
					{Name: snowflake.SfPrivateKeyPassphrase, Description: "The passphrase for the private key in case it is encrypted.", Mandatory: false},
					{Name: snowflake.SfRole, Description: "The name of the role to use for executing the necessary queries. If not specified 'ACCOUNTADMIN' is used.", Mandatory: false},
					{Name: snowflake.SfWarehouse, Description: "The name of the warehouse to use for executing the necessary queries. If not specified, the default warehouse for the user is used.", Mandatory: false},
					{Name: snowflake.SfExcludedDatabases, Description: "The optional comma-separated list of databases that should be skipped.", Mandatory: false},
					{Name: snowflake.SfExcludedSchemas, Description: "The optional comma-separated list of schemas that should be skipped. This can either be in a specific database (as <database>.<schema>) or a just a schema name that should be skipped in all databases. By default INFORMATION_SCHEMA is skipped since there are no access controls to manage", Mandatory: false},
					{Name: snowflake.SfExcludedRoles, Description: "The optional comma-separated list of roles that should be skipped. Roles containing excluded roles will be imported as incomplete because this breaks the hierarchy", Mandatory: false},
					{Name: snowflake.SfExternalIdentityStoreOwners, Description: "The optional comma-separated list of owners of SCIM integrations with external identity stores (e.g. Okta or Active Directory). Roles which are imported from groups from these identity stores will be partially or fully locked in Raito to avoid a conflict with the SCIM integration.", Mandatory: false},
					{Name: snowflake.SfLinkToExternalIdentityStoreGroups, Description: "This boolean parameter can be set when the 'sf-external-identity-store-owners' parameter is set. When 'true', the 'who' of roles coming from the external access provider will refer to the group of the external access provider and the 'what' of the access provider will still be editable in Raito Cloud. When 'false' (default) the 'who' will contain the unpacked users of the group and the access provider in Raito Cloud will be locked entirely.", Mandatory: false},
					{Name: snowflake.SfStandardEdition, Description: "If set enterprise features will be disabled", Mandatory: false},
					{Name: snowflake.SfSkipTags, Description: "If set, tags will not be fetched", Mandatory: false},
					{Name: snowflake.SfSkipColumns, Description: "If set, columns and column masking policies will not be imported.", Mandatory: false},
					{Name: snowflake.SfDataUsageWindow, Description: "The maximum number of days of usage data to retrieve. Default is 90. Maximum is 90 days.", Mandatory: false},
					{Name: snowflake.SfDatabaseRoles, Description: "If set, database-roles for all databases will be fetched.", Mandatory: false},
					{Name: snowflake.SfIgnoreLinksToRoles, Description: "This comma separated list of regular expressions can be used to indicate that role hierarchy links to certain roles are never added or removed. e.g. 'SYS.+,ADMIN.+' will match all roles starting with 'SYS' or 'ADMIN', meaning that all grants to these roles will remain untouched during the sync.", Mandatory: false},
					{Name: snowflake.SfUsageBatchSize, Description: "If not set, no batching is done when fetching usage statements. This will be the fastest, however it uses more memory. If memory usage is a problem, this can be set to a number between 10.000 and 1.000.000 (higher is recommended) to fetch usage in batches of that size.", Mandatory: false},
					{Name: snowflake.SfUsageUserExcludes, Description: "The optional comma-separated list of user names to exclude when fetching data. This is typically used for service accounts that do a large amount of operations.", Mandatory: false},
					{Name: snowflake.SfMaskDecryptFunction, Description: "This allows you to have an additional column masking option which will decrypt data in a column. You do this by specifying the function name that will do the decryption. This function one parameter containing the value to decrypt. e.g. MY_DATABASE.MY_SCHEMA.DECRYPT_IT. Note: Make sure the role used to connect to Snowflake (as specified in sf-role) has USAGE permissions on the decryption method.", Mandatory: false},
					{Name: snowflake.SfMaskDecryptColumnTag, Description: fmt.Sprintf("When using the '%s' option, you can also additionally specify a tag name that contains the encryption type to use. This will pass a second argument to your decryption function with the value of this tag on the column. Use the fully qualified name for the tag. e.g. MY_DATABASE.MY_SCHEMA.MY_TAG", snowflake.SfMaskDecryptFunction), Mandatory: false},

					{Name: snowflake.SfRoleOwnerEmailTag, Description: "Can be used to set a tag on roles generated from grants in Raito Cloud, to indicate the owners. The access control owner users will be exported in the format 'email:owner1@raito.io,email:owner2@raito.io'. The tag should already exist and should be referenced using the full name. E.g. MY_DATABASE.MY_SCHEMA.MY_TAG_NAME", Mandatory: false},
					{Name: snowflake.SfRoleOwnerNameTag, Description: "Can be used to set a tag on roles generated from grants  in Raito Cloud, to indicate the owners. The access control owner users will be exported in the format 'owner1Account,owner2Account'. The tag should already exist and should be referenced using the full name. E.g. MY_DATABASE.MY_SCHEMA.MY_TAG_NAME", Mandatory: false},
					{Name: snowflake.SfRoleOwnerGroupTag, Description: "Can be used to set a tag on roles generated from grants in Raito Cloud, to indicate the owners. The access control owner groups will be exported in the format 'groupName1,groupName2'. The tag should already exist and should be referenced using the full name. E.g. MY_DATABASE.MY_SCHEMA.MY_TAG_NAME", Mandatory: false},
				},
				TagSource: snowflake.TagSource,
			},
		})

	if err != nil {
		logger.Error(fmt.Sprintf("error while registering plugins: %s", err.Error()))
	}
}
