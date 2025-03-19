provider "snowflake" {
  role                     = "ACCOUNTADMIN"
  account_name             = var.snowflake_account
  organization_name        = var.snowflake_organization
  user                     = var.snowflake_user
  authenticator            = "SNOWFLAKE_JWT"
  private_key              = file(var.snowflake_private_key_file)
  preview_features_enabled = ["snowflake_function_sql_resource", "snowflake_procedure_sql_resource", "snowflake_table_resource", "snowflake_materialized_view_resource"]
}