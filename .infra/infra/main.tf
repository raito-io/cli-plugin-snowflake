// SNOWFLAKE WAREHOUSE
resource "snowflake_warehouse" "warehouse" {
  name           = "RAITO_WAREHOUSE"
  warehouse_size = "xsmall"
  auto_suspend   = 60
}


module "testing" {
  count = var.testing_dataset ? 1 : 0

  source     = "./testing"
  depends_on = [snowflake_grant_account_role.role_grants]

  providers = {
    snowflake = snowflake
  }

  snowflake_warehouse      = snowflake_warehouse.warehouse.id
  existing_snowflake_roles = [snowflake_account_role.data_analyst.name, snowflake_account_role.finance.name, snowflake_account_role.human_resources.name, snowflake_account_role.marketing.name, snowflake_account_role.sales.name, snowflake_account_role.sales_analysis.name, snowflake_account_role.sales_ext.name]
}

module "demo" {
  count = var.demo_dataset ? 1 : 0

  source     = "./demo"
  depends_on = [snowflake_grant_account_role.role_grants]

  providers = {
    snowflake = snowflake
  }

  snowflake_warehouse      = snowflake_warehouse.warehouse.id
  existing_snowflake_roles = [snowflake_account_role.data_analyst.name, snowflake_account_role.finance.name, snowflake_account_role.human_resources.name, snowflake_account_role.marketing.name, snowflake_account_role.sales.name, snowflake_account_role.sales_analysis.name, snowflake_account_role.sales_ext.name]
}