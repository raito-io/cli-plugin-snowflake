output "database" {
  value = snowflake_database.master_data.name
}

output "tables" {
  value = [
    // Tables
    snowflake_table.awsbuildversion.qualified_name,
    snowflake_table.state_province.qualified_name,
    snowflake_table.shopping_cart_item.qualified_name,
    snowflake_table.phone_number_type.qualified_name,
    snowflake_table.person_phone.qualified_name,
    snowflake_table.person_creditcard.qualified_name,
    snowflake_table.email_address.qualified_name,
    snowflake_table.credit_card.qualified_name,
    // This list is not complete but is only used to generate data usage

    // Views
    "${snowflake_materialized_view.customer_eu.database}.${snowflake_materialized_view.customer_eu.schema}.${snowflake_materialized_view.customer_eu.name}"
  ]
}