package snowflake

const (
	SfAccount                           = "sf-account"
	SfUser                              = "sf-user"
	SfPassword                          = "sf-password"
	SfPrivateKey                        = "sf-private-key"
	SfPrivateKeyPassphrase              = "sf-private-key-passphrase" //nolint:gosec
	SfRole                              = "sf-role"
	SfWarehouse                         = "sf-warehouse"
	SfExcludedDatabases                 = "sf-excluded-databases"
	SfExcludedSchemas                   = "sf-excluded-schemas"
	SfExcludedRoles                     = "sf-excluded-roles"
	SfExternalIdentityStoreOwners       = "sf-external-identity-store-owners"
	SfStandardEdition                   = "sf-standard-edition"
	SfLinkToExternalIdentityStoreGroups = "sf-link-to-external-identity-store-groups"
	SfSkipTags                          = "sf-skip-tags"
	SfSkipColumns                       = "sf-skip-columns"
	SfDataUsageWindow                   = "sf-data-usage-window"
	SfDatabaseRoles                     = "sf-database-roles"
	SfDriverDebug                       = "sf-driver-debug"
	SfDriverInsecureMode                = "sf-driver-insecure-mode"
	SfIgnoreLinksToRoles                = "sf-ignore-links-to-roles"
	SfUsageBatchSize                    = "sf-usage-batch-size"
	SfUsageUserExcludes                 = "sf-usage-user-excludes"
	SfWorkerPoolSize                    = "sf-worker-pool-size"

	SfMaskDecryptFunction  = "sf-mask-decrypt-function"
	SfMaskDecryptColumnTag = "sf-mask-decrypt-column-tag"

	TagSource = "Snowflake"

	SharedPrefix = "shared-"

	AccountAdminRole = "ACCOUNTADMIN"

	GrantTypeDatabaseRole = "DATABASE_ROLE"
)
