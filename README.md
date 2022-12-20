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
    <a href="https://github.com/raito-io/cli-plugin-snowflake/actions/workflows/build.yml" target="_blank"><img src="https://img.shields.io/github/workflow/status/raito-io/cli-plugin-snowflake/Raito%20CLI%20-%20Snowflake%20Plugin%20-%20Build/main" alt="Build status" /></a>
    <a href="https://codecov.io/gh/raito-io/cli-plugin-snowflake" target="_blank"><img src="https://img.shields.io/codecov/c/github/raito-io/cli-plugin-snowflake" alt="Code Coverage" /></a>
</p>

<hr/>

# Raito CLI Plugin - Snowflake

This Raito CLI plugin implements the integration with Snowflake. It can
 - Synchronize the users in Snowflake to an identity store in Raito Cloud.
 - Synchronize the Snowflake meta data (data structure, known permissions, ...) to a data source in Raito Cloud.
 - Synchronize the Snowflake role, masking rules and row-level security rules with access providers in Raito Cloud.
 - Synchronize the access providers from Raito Cloud (or from a file in case of the [access-as-code flow](http://docs.raito.io/docs/guide/access)) into Snowflake roles.
 - Synchronize the data usage information to Raito Cloud.


## Prerequisites
To use this plugin, you will need

1. The Raito CLI to be correctly installed. You can check out our [documentation](http://docs.raito.io/docs/cli/installation) for help on this.
2. A Raito Cloud account to synchronize your Snowflake account with. If you don't have this yet, visit our webpage at (https://raito.io) and request a trial account.
3. At least one Snowflake account.

Note: if you are only using the [access-as-code flow](http://docs.raito.io/docs/guide/access), no Raito Cloud account is required.

A full example on how to start using Raito Cloud with Snowflake can be found as a [guide in our documentation](http://docs.raito.io/docs/guide/cloud).

## Usage
To use the plugin, add the following snippet to your Raito CLI configuration file (`raito.yml`, by default) under the `targets` section:

```json
  - name: snowflake1
    connector-name: raito-io/cli-plugin-snowflake
    data-source-id: <<Snowflake DataSource ID>>
    identity-store-id: <<Snowflake IdentityStore ID>>

    # Specifying the Snowflake specific config parameters
    sf-account: <<Your Snowflake Account>>
    sf-user: <<Your Snowflake User>>
    sf-password: "{{RAITO_SNOWFLAKE_PASSWORD}}"
```

Next, replace the values of the indicated fields with your specific values:
- `<<Snowflake DataSource ID>>`: the ID of the Data source you created in the Raito Cloud UI.
- `<<Snowflake IdentityStore ID>>`: the ID of the Identity Store you created in the Raito Cloud UI.
- `<<Your Snowflake User>>`: the user that should be used to sign in to Snowflake
- `<<Your Snowflake Account>>`: the name of your Snowflake account. e.g. `kq12345.eu-central-1`


Make sure you have a system variable called `RAITO_SNOWFLAKE_PASSWORD` with the password of the Snowflake user as value.

You will also need to configure the Raito CLI further to connect to your Raito Cloud account, if that's not set up yet.
A full guide on how to configure the Raito CLI can be found on (http://docs.raito.io/docs/cli/configuration).

## Trying it out

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
