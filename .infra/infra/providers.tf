provider "snowflake" {
  role                     = "ACCOUNTADMIN"
  account_name             = var.snowflake_account
  organization_name        = var.snowflake_organization
  user                     = var.snowflake_user
  password                 = var.snowflake_password
  preview_features_enabled = ["snowflake_function_sql_resource", "snowflake_table_resource"]
}