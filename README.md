<h1 align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://github.com/raito-io/raito-io.github.io/raw/master/assets/images/logo-vertical-dark%402x.png">
    <img height="250px" src="https://github.com/raito-io/raito-io.github.io/raw/master/assets/images/logo-vertical%402x.png">
  </picture>
</h1>

<h4 align="center">
  Snowflake plugin for the Raito CLI
</h4>

<p align="center">
    <a href="/LICENSE.md" target="_blank"><img src="https://img.shields.io/badge/license-Apache%202-brightgreen.svg" alt="Software License" /></a>
    <a href="https://github.com/raito-io/cli-plugin-snowflake/actions/workflows/build.yml" target="_blank"><img src="https://img.shields.io/github/actions/workflow/status/raito-io/cli-plugin-snowflake/build.yml?branch=main" alt="Build status" /></a>
    <a href="https://codecov.io/gh/raito-io/cli-plugin-snowflake" target="_blank"><img src="https://img.shields.io/codecov/c/github/raito-io/cli-plugin-snowflake" alt="Code Coverage" /></a>
</p>

<hr/>

# Raito CLI Plugin - Snowflake

This Raito CLI plugin implements the integration with Snowflake. It can
 - Synchronize the users in Snowflake to an identity store in Raito Cloud.
 - Synchronize the Snowflake meta data (data structure, known permissions, ...) to a data source in Raito Cloud.
 - Synchronize the Snowflake role, masking rules and row-level security rules with access providers in Raito Cloud.
 - Synchronize the data usage information to Raito Cloud.


## Prerequisites
To use this plugin, you will need

1. The Raito CLI to be correctly installed. You can check out our [documentation](http://docs.raito.io/docs/cli/installation) for help on this.
2. A Raito Cloud account to synchronize your Snowflake account with. If you don't have this yet, visit our webpage at (https://raito.io) and request a trial account.
3. At least one Snowflake account.

A full example on how to start using Raito Cloud with Snowflake can be found as a [guide in our documentation](http://docs.raito.io/docs/guide/cloud).

## Usage
To use the plugin, add the following snippet to your Raito CLI configuration file (`raito.yml`, by default) under the `targets` section:

```yaml
  - name: snowflake1
    connector-name: raito-io/cli-plugin-snowflake
    data-source-id: <<Snowflake DataSource ID>>
    identity-store-id: <<Snowflake IdentityStore ID>>

    # Specifying the Snowflake specific config parameters
    sf-account: <<Your Snowflake Account>>
    sf-user: <<Your Snowflake User>>
    # Specify either a password or a private key
    sf-password: "{{RAITO_SNOWFLAKE_PASSWORD}}"
    sf-private-key: <<Path to private key>>
```

Next, replace the values of the indicated fields with your specific values:
- `<<Snowflake DataSource ID>>`: the ID of the Data source you created in the Raito Cloud UI.
- `<<Snowflake IdentityStore ID>>`: the ID of the Identity Store you created in the Raito Cloud UI.
- `<<Your Snowflake User>>`: the user that should be used to sign in to Snowflake
- `<<Your Snowflake Account>>`: the name of your Snowflake account. e.g. `kq12345.eu-central-1`


Make sure you have a system variable called `RAITO_SNOWFLAKE_PASSWORD` with the password of the Snowflake user as value or set use the 'sf-private-key' parameter to specify the path to a private key file to use public/private key authentication.

You will also need to configure the Raito CLI further to connect to your Raito Cloud account, if that's not set up yet.
A full guide on how to configure the Raito CLI can be found on (http://docs.raito.io/docs/cli/configuration).

### Trying it out

As a first step, you can check if the CLI finds this plugin correctly. In a command-line terminal, execute the following command:
```bash
$> raito info raito-io/cli-plugin-snowflake
```

This will download the latest version of the plugin (if you don't have it yet) and output the name and version of the plugin, together with all the plugin-specific parameters to configure it.

When you are ready to try out the synchronization for the first time, execute:
```bash
$> raito run
```
This will take the configuration from the `raito.yml` file (in the current working directory) and start a single synchronization.

Note: if you have multiple targets configured in your configuration file, you can run only this target by adding `--only-targets snowflake1` at the end of the command.

## Configuration
The following configuration parameters are available

| Configuration name                          | Description                                                                                                                                                                                                                                                                                                                                                                                                                                     | Mandatory | Default value        |
|---------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------|----------------------|
| `sf-account`                                | The account name of the Snowflake account to connect to. For example, xy123456.eu-central-1                                                                                                                                                                                                                                                                                                                                                     | True      |                      |
| `sf-user`                                   | The username to authenticate against the Snowflake account.                                                                                                                                                                                                                                                                                                                                                                                     | True      |                      |
| `sf-password`                               | The password to authenticate against the Snowflake account. Either this or sf-private-key must be specified.                                                                                                                                                                                                                                                                                                                                    | False     |                      |
| `sf-private-key`                            | The path of the file containing the private key to authenticate against the Snowflake account. Either this or sf-password must be specified.                                                                                                                                                                                                                                                                                                    | False     |                      |
| `sf-role`                                   | The name of the role to use for executing the necessary queries.                                                                                                                                                                                                                                                                                                                                                                                | False     | `ACCOUNTADMIN`       |
| `sf-excluded-databases`                     | A comma-separated list of databases that should be skipped.                                                                                                                                                                                                                                                                                                                                                                                     | False     |                      |
| `sf-excluded-schemas`                       | A comma-separated list of schemas that should be skipped. This can either be in a specific database (as <database>.<schema>) or a just a schema name that should be skipped in all databases.                                                                                                                                                                                                                                                   | False     | `INFORMATION_SCHEMA` |
| `sf-excluded-roles`                         | A comma-separated list of roles that should be skipped. You should not exclude roles which others (not-excluded) roles depend on as that would break the hierarchy.                                                                                                                                                                                                                                                                             | False     |                      |
| `sf-external-identity-store-owners`         | A comma-separated list of owners of SCIM integrations with external identity stores (e.g. Okta or Active Directory). Roles which are imported from groups from these identity stores will be partially or fully locked in Raito to avoid a conflict with the SCIM integration.                                                                                                                                                                  | False     |                      |
| `sf-link-to-external-identity-store-groups` | A boolean parameter can be set when the 'sf-external-identity-store-owners' parameter is set. When `true`, the 'who' of roles coming from the external access provider will refer to the group of the external access provider and the 'what' of the access provider will still be editable in Raito Cloud. When `false` the 'who' will contain the unpacked users of the group and the access provider in Raito Cloud will be locked entirely. | False     | `false`              |
| `sf-standard-edition`                       | If set, enterprise features will be disabled                                                                                                                                                                                                                                                                                                                                                                                                    | False     | `false`              |
| `sf-skip-tags`                              | If set, tags will not be fetched                                                                                                                                                                                                                                                                                                                                                                                                                | False     | `false`              |
| `sf-skip-columns`                           | If set, columns and column masking policies will not be imported.                                                                                                                                                                                                                                                                                                                                                                               | False     | `false`              |
| `sf-data-usage-window`                      | The maximum number of days of usage data to retrieve. Maximum is 90 days.                                                                                                                                                                                                                                                                                                                                                                       | False     | `90`                 |
| `sf-database-roles`                         | If set, database-roles for all databases will be fetched.                                                                                                                                                                                                                                                                                                                                                                                       | False     | `false`              |
| `sf-applications`                           | If set, application roles for all applications will be fetched.                                                                                                                                                                                                                                                                                                                                                                                 | False     | `false`              |

To get a full list of all the parameters. You can run the following command in your terminal:
```bash
$> raito info raito-io/cli-plugin-snowflake
```

## Supported features

| Feature             | Supported | Remarks                                                                                                                                                                                                                                                                |
|---------------------|-----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Row level filtering | ✅         | Only supported for enterprise editions                                                                                                                                                                                                                                 |
| Column masking      | ✅         | Only supported for enterprise editions                                                                                                                                                                                                                                 |
| Data Sharing        | ✅         |                                                                                                                                                                                                                                                                        |
| Locking             | ✅         | Support for both who and what lock                                                                                                                                                                                                                                     |
| Replay              | ✅         | Explicit deletes cannot be replayed                                                                                                                                                                                                                                    |
| Usage               | ✅         | Only supported for enterprise editions. Usage will be processed based on [QUERY_HISTORY](https://docs.snowflake.com/en/sql-reference/account-usage/query_history) and [ACCESS_HISTORY](https://docs.snowflake.com/en/sql-reference/account-usage/access_history) view. |

## Supported data objects
- Account (is represented as Raito DataSource)
- Warehouse
- Database (only STANDARD databases)
- Shared Database
- Schema
- Shared Schema
- Table
- External Table
- Shared Table
- View
- Materialized View
- Shared View
- Column
- Shared Column
- Function
- Procedure
- Integration


## Access controls
### From Target
#### Account Roles
Account roles are imported as `grant` with type `Role`.
All grants to users and other roles associated with the role are added as who-items to the grant that will be imported in Raito.
All grants to privileges on data objects associated the role are added as what-items to the grant that will be imported in Raito.
All system roles (`ORGADMIN`, `ACCOUNTADMIN`, `SECURITYADMIN`, `USERADMIN`, `SYSADMIN`, `PUBLIC`) will be annotated as non-internalizable.
If roles are managed by external identity stores in Snowflake, the following locks will be set: `name-lock`, `delete-lock`, `who-lock`, `inheritance-lock`. The parameters `sf-external-identity-store-owners` and `sf-link-to-external-identity-store-groups` will be used to indicate if roles are managed by external identities.

#### Database Roles
Database roles are imported as `grant` with type `databaseRole`.
All grants to users and other roles associated with the role are added as who-items to the grant that will be imported in Raito.
All grants to privileges on data objects associated the role are added as what-items to the grant that will be imported in Raito.
The who and what of the grants created by database roles will be locked and those grants.

#### Masking Policies
Masking policies are imported as `mask`.
Masking policies are imported as non-internalizable because most existing masking policies cannot be correctly interpreted Raito.

#### Row Access Policies
Row access policies are imported as `filters`.
The same mechanism is used as for masking policies. Therefor, row access policies are non-internalizable as the `who`-items can not be identified.

#### Shares
Shares are imported as `share`.
Access granted by the share as imported as WHAT items. Accounts set on the share are imported as WHO items.

## To Target
#### Grants
Grants will be implemented as `Account role` (type `Role`) or `Database role` (type `databaseRole`).
All associated who items will be granted access to the role.
All what items will be granted permission for the specific role.

#### Masks
Each mask will be exported as masking policy to all schemas associated with the what-items of the mask.
Within each schema a masking policy function is created for each required data type.

It is possible to define a custom decrypt masking policy. This is done using the parameters `sf-mask-decrypt-function`.
When this parameter is set, a new masking type is made available to Raito Cloud users, called `Decrypt`.
This will generate a mask that will decrypt the value of the column using the specified function when the current user or role is part of the beneficiaries of the mask in Raito Cloud.

When you need to pass an additional parameter to the decrypt function, you can use the parameter `sf-mask-decrypt-column-tag`. 
This will fetch the value of the given tag on the column to (un)mask and pass that value to your decrypt function as well. This is typically used to pass the encryption type to the decrypt function.

As an example, if you specified the following parameters in your target configuration:
```yaml
    sf-mask-decrypt-function: GLOBAL.HELPERS.DECRYPT_IT
    sf-mask-decrypt-column-tag: GLOBAL.TAGS.ENCRYPTION_TYPE
```

And you define a mask in Raito Cloud on columns in schema `MY_DATABASE.MY_SCHEMA`, a mask like this will be generated in Snowflake:
```sql
CREATE MASKING POLICY MY_DATABASE.MY_SCHEMA.DECRYPTTEST_JH3EIhVr_TEXT AS (val TEXT) RETURNS TEXT ->
  CASE
	WHEN current_user() IN ('my-user') THEN GLOBAL.HELPERS.DECRYPT_IT(val, SYSTEM$GET_TAG_ON_CURRENT_COLUMN('GLOBAL.TAGS.ENCRYPTION_TYPE'))
	ELSE val
  END;
```

Note: Make sure the role used to connect to Snowflake (as specified in `sf-role`) has USAGE permissions on the decryption method.

#### Filters
Each filter will be exported as row access policy to exactly one table.

#### Shares
Shares will be exported as [Share](https://docs.snowflake.com/en/user-guide/data-sharing-intro) in Snowflake.
Privileges on data objects associated with the share will be granted to the share.
The role that is defined with the sync should have the following permissions: `CREATE SHARE`, `MANAGE SHARE TARGET`.

## Usage
The Raito Snowflake plugin retrieves usage data from the Snowflake system views:
- `QUERY_HISTORY`: This view contains historical data on queries executed within the Snowflake account.
- `ACCESS_HISTORY`: This view contains historical data on Snowflake object access events.

The plugin extracts relevant usage information from these views based on specific columns:
- `direct_objects_accessed`: This column identifies objects directly accessed through queries (read usage).
- `base_objects_accessed`: This column identifies objects used in queries (read usage).
- `objects_modified`: This column identifies objects modified through DML statements (write usage).
- `object_modified_by_ddl`: This column identifies objects modified through DDL statements (admin usage).

Note: The maximum timeframe for retrieved usage data is configurable through the `sf-data-usage-window` parameter in the configuration file. The default value is 90 days.