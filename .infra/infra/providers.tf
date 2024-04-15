provider "snowflake" {
  role     = "ACCOUNTADMIN"
  account  = var.snowflake_account
  user     = var.snowflake_user
  password = var.snowflake_password
}