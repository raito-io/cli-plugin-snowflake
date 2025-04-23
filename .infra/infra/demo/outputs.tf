output "database" {
  value = snowflake_database.master_data.name
}

output "tables" {
  value = [
    // Tables
    snowflake_table.awsbuildversion.fully_qualified_name,
    snowflake_table.state_province.fully_qualified_name,
    snowflake_table.shopping_cart_item.fully_qualified_name,
    snowflake_table.phone_number_type.fully_qualified_name,
    snowflake_table.person_phone.fully_qualified_name,
    snowflake_table.person_creditcard.fully_qualified_name,
    snowflake_table.email_address.fully_qualified_name,
    snowflake_table.credit_card.fully_qualified_name,

    snowflake_table.country_region_currency.fully_qualified_name,
    snowflake_table.currency_rate.fully_qualified_name,
    snowflake_table.employee.fully_qualified_name,

    // This list is not complete but is only used to generate data usage

    // Views
    "${snowflake_materialized_view.customer_eu.database}.${snowflake_materialized_view.customer_eu.schema}.${snowflake_materialized_view.customer_eu.name}"
  ]
}