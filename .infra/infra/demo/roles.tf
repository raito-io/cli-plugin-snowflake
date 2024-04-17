resource "snowflake_grant_privileges_to_account_role" "finance_privileges_bill_of_materials" {
  account_role_name = "FINANCE"
  privileges        = ["SELECT"]

  on_schema_object {
    object_name = snowflake_table.bill_of_materials.qualified_name
    object_type = "TABLE"
  }
}

resource "snowflake_grant_privileges_to_account_role" "human_resources_privileges_department" {
  account_role_name = "HUMAN_RESOURCES"
  privileges        = ["DELETE", "INSERT", "SELECT", "UPDATE"]

  on_schema_object {
    object_name = snowflake_table.department.qualified_name
    object_type = "TABLE"
  }
}

resource "snowflake_grant_privileges_to_account_role" "human_resources_employee" {
  account_role_name = "HUMAN_RESOURCES"
  privileges        = ["DELETE", "INSERT", "SELECT", "UPDATE"]

  on_schema_object {
    object_name = snowflake_table.employee.qualified_name
    object_type = "TABLE"
  }
}

resource "snowflake_grant_privileges_to_account_role" "human_resources_employee_department" {
  account_role_name = "HUMAN_RESOURCES"
  privileges        = ["DELETE", "INSERT", "SELECT", "UPDATE"]

  on_schema_object {
    object_name = snowflake_table.empoyee_department_history.qualified_name
    object_type = "TABLE"
  }
}

resource "snowflake_grant_privileges_to_account_role" "human_resources_job_candidate" {
  account_role_name = "HUMAN_RESOURCES"
  privileges        = ["DELETE", "INSERT", "SELECT", "UPDATE"]

  on_schema_object {
    object_name = snowflake_table.job_candidate.qualified_name
    object_type = "TABLE"
  }
}

resource "snowflake_grant_privileges_to_account_role" "human_resources_shift" {
  account_role_name = "HUMAN_RESOURCES"
  privileges        = ["DELETE", "INSERT", "SELECT", "UPDATE"]

  on_schema_object {
    object_name = snowflake_table.shift.qualified_name
    object_type = "TABLE"
  }
}

resource "snowflake_grant_privileges_to_account_role" "marketing_bill_of_materials" {
  account_role_name = "MARKETING"
  privileges        = ["SELECT"]

  on_schema_object {
    object_name = snowflake_table.bill_of_materials.qualified_name
    object_type = "TABLE"
  }
}

resource "snowflake_grant_privileges_to_account_role" "sales_analysis_privileges" {
  for_each = { for v in [
    { key = "country_region_currency", name = snowflake_table.country_region_currency.qualified_name, type = "TABLE" },
    { key = "credit_card", name = snowflake_table.credit_card.qualified_name, type = "TABLE" },
    { key = "currency", name = snowflake_table.currency.qualified_name, type = "TABLE" },
    { key = "currency_rate", name = snowflake_table.currency_rate.qualified_name, type = "TABLE" },
    { key = "customer", name = snowflake_table.customer.qualified_name, type = "TABLE" },
    { key = "customer_eu", name = "${snowflake_materialized_view.customer_eu.database}.${snowflake_materialized_view.customer_eu.schema}.${snowflake_materialized_view.customer_eu.name}", type = "MATERIALIZED VIEW" },
    { key = "department", name = snowflake_table.department.qualified_name, type = "TABLE" },
    { key = "employee", name = snowflake_table.employee.qualified_name, type = "TABLE" },
    { key = "empoyee_department_history", name = snowflake_table.empoyee_department_history.qualified_name, type = "TABLE" },
    { key = "job_candidate", name = snowflake_table.job_candidate.qualified_name, type = "TABLE" },
    { key = "person_creditcard", name = snowflake_table.person_creditcard.qualified_name, type = "TABLE" },
    { key = "sales_order_detail", name = snowflake_table.sales_order_detail.qualified_name, type = "TABLE" },
    { key = "sales_order_header_sales_reason", name = snowflake_table.sales_order_header_sales_reason.qualified_name, type = "TABLE" },
    { key = "sales_person", name = snowflake_table.sales_person.qualified_name, type = "TABLE" },
    { key = "sales_person_quota_history", name = snowflake_table.sales_person_quota_history.qualified_name, type = "TABLE" },
    { key = "sales_reason", name = snowflake_table.sales_reason.qualified_name, type = "TABLE" },
    { key = "sales_tax_rate", name = snowflake_table.sales_tax_rate.qualified_name, type = "TABLE" },
    { key = "sales_territory", name = snowflake_table.sales_territory.qualified_name, type = "TABLE" },
    { key = "sales_territory_history", name = snowflake_table.sales_territory_history.qualified_name, type = "TABLE" },
    { key = "shift", name = snowflake_table.shift.qualified_name, type = "TABLE" },
    { key = "shopping_cart_item", name = snowflake_table.shopping_cart_item.qualified_name, type = "TABLE" },
    { key = "special_offer", name = snowflake_table.special_offer.qualified_name, type = "TABLE" },
    { key = "special_offer_product", name = snowflake_table.special_offer_product.qualified_name, type = "TABLE" },
    { key = "store", name = snowflake_table.store.qualified_name, type = "TABLE" }
  ] : v.key => v }

  account_role_name = "SALES_ANALYSIS"
  privileges        = ["SELECT"]

  on_schema_object {
    object_name = each.value.name
    object_type = each.value.type
  }
}

resource "snowflake_grant_privileges_to_account_role" "sales_ext_privileges" {
  for_each = { for v in [
    { key = "address", name = snowflake_table.address.qualified_name, type = "TABLE" },
    { key = "address_type", name = snowflake_table.address_type.qualified_name, type = "TABLE" },
    { key = "business_entity", name = snowflake_table.business_entity.qualified_name, type = "TABLE" },
    { key = "business_entity_address", name = snowflake_table.business_entity_address.qualified_name, type = "TABLE" },
    { key = "business_entity_contact", name = snowflake_table.business_entity_contact.qualified_name, type = "TABLE" },
    { key = "country_region", name = snowflake_table.country_region.qualified_name, type = "TABLE" },
    { key = "country_region_currency", name = snowflake_table.country_region_currency.qualified_name, type = "TABLE" },
    { key = "credit_card", name = snowflake_table.credit_card.qualified_name, type = "TABLE" },
    { key = "currency", name = snowflake_table.currency.qualified_name, type = "TABLE" },
    { key = "currency_rate", name = snowflake_table.currency_rate.qualified_name, type = "TABLE" },
    { key = "customer_eu", name = "${snowflake_materialized_view.customer_eu.database}.${snowflake_materialized_view.customer_eu.schema}.${snowflake_materialized_view.customer_eu.name}", type = "MATERIALIZED VIEW" },
    { key = "email_address", name = snowflake_table.email_address.qualified_name, type = "TABLE" },
    { key = "person_creditcard", name = snowflake_table.person_creditcard.qualified_name, type = "TABLE" },
    { key = "person_phone", name = snowflake_table.person_phone.qualified_name, type = "TABLE" },
    { key = "phone_number_type", name = snowflake_table.phone_number_type.qualified_name, type = "TABLE" },
    { key = "sales_order_detail", name = snowflake_table.sales_order_detail.qualified_name, type = "TABLE" },
    { key = "sales_order_header_sales_reason", name = snowflake_table.sales_order_header_sales_reason.qualified_name, type = "TABLE" },
    { key = "sales_person", name = snowflake_table.sales_person.qualified_name, type = "TABLE" },
    { key = "sales_person_quota_history", name = snowflake_table.sales_person_quota_history.qualified_name, type = "TABLE" },
    { key = "sales_reason", name = snowflake_table.sales_reason.qualified_name, type = "TABLE" },
    { key = "sales_tax_rate", name = snowflake_table.sales_tax_rate.qualified_name, type = "TABLE" },
    { key = "sales_territory", name = snowflake_table.sales_territory.qualified_name, type = "TABLE" },
    { key = "sales_territory_history", name = snowflake_table.sales_territory_history.qualified_name, type = "TABLE" },
    { key = "shopping_cart_item", name = snowflake_table.shopping_cart_item.qualified_name, type = "TABLE" },
    { key = "special_offer", name = snowflake_table.special_offer.qualified_name, type = "TABLE" },
    { key = "special_offer_product", name = snowflake_table.special_offer_product.qualified_name, type = "TABLE" },
    { key = "state_province", name = snowflake_table.state_province.qualified_name, type = "TABLE" },
    { key = "store", name = snowflake_table.store.qualified_name, type = "TABLE" }
  ] : v.key => v }

  account_role_name = "SALES_EXT"
  privileges        = ["SELECT"]

  on_schema_object {
    object_name = each.value.name
    object_type = each.value.type
  }
}