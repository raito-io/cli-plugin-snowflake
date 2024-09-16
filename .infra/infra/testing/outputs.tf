output "snowflake_database_name" {
  value = snowflake_database.db.name
}

output "snowflake_share_database_name" {
  value = snowflake_shared_database.shared_db.name
}

output "snowflake_tables" {
  value = [snowflake_table.ordering_supplier.fully_qualified_name, snowflake_table.ordering_orders.fully_qualified_name, snowflake_table.ordering_customer.fully_qualified_name, "\"${snowflake_view.orders_limited.database}\".\"${snowflake_view.orders_limited.schema}\".\"${snowflake_view.orders_limited.name}\""]
}