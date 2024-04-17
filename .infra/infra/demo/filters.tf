resource "snowflake_row_access_policy" "rls_sales_data" {
  name = "RLS_SALES_DATA"
  database = snowflake_schema.sales.database
  schema = snowflake_schema.sales.name

  signature = {
    TerritoryID = "NUMBER(38,0)"
  }

  row_access_expression = <<-EOF
    current_role() in ('SALES_EXEC') or exists (
     select 1 from ${snowflake_table.sales_person.qualified_name} WHERE
      current_role() in ('SALES') AND
      ${snowflake_table.sales_person.qualified_name}."TerritoryID" = TerritoryID AND
      ${snowflake_table.sales_person.qualified_name}.NAME = current_user()
    )
  EOF
}

// Currently we cannot assign this policy to a table using TF