output "snowflake_database_name" {
  value = snowflake_database.db.name
}

output "snowflake_warehouse_name" {
  value = snowflake_warehouse.warehouse.name
}

output "snowflake_account" {
  value     = var.snowflake_account
  sensitive = true
}

output "snowflake_tables" {
  value = [snowflake_table.ordering_supplier.qualified_name, snowflake_table.ordering_orders.qualified_name, snowflake_table.ordering_customer.qualified_name, "\"${snowflake_view.orders_limited.database}\".\"${snowflake_view.orders_limited.schema}\".\"${snowflake_view.orders_limited.name}\""]
}

output "persona_password" {
  value     = random_password.persona_password.result
  sensitive = true
}

output "personas" {
  value     = local.who_role
  sensitive = true
}