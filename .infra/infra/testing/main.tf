// SNOWFLAKE SCIM INTEGRATION

resource "snowflake_account_role" "scim_role" {
  name    = "GENERIC_SCIM_PROVISIONER"
  comment = "For scim provisioning"
}

resource "snowflake_grant_privileges_to_account_role" "scim_role_assigments" {
  privileges        = ["CREATE USER", "CREATE ROLE"]
  account_role_name = snowflake_account_role.scim_role.name
  on_account        = true
}

resource "snowflake_grant_account_role" "scim_role_to_accountadmin" {
  role_name        = snowflake_account_role.scim_role.name
  parent_role_name = "ACCOUNTADMIN"
}

resource "snowflake_scim_integration" "scim_integration" {
  name          = "SCIM Integration"
  enabled       = true
  scim_client   = "GENERIC"
  sync_password = false
  run_as_role   = snowflake_account_role.scim_role.name
}

// SNOWFLAKE DATABASE
resource "snowflake_database" "db" {
  name    = "RAITO_DATABASE"
  comment = "Database for RAITO testing and demo"
}

resource "snowflake_database" "special_db" {
  name    = "DATABASE WITH SPECIAL CASES"
  comment = "Database for RAITO testing and demo"
}

resource "snowflake_schema" "ordering" {
  database = snowflake_database.db.name
  name     = "ORDERING"
  comment  = "Schema for RAITO testing and demo"
}

resource "snowflake_function_sql" "decrypt_function" {
  database = snowflake_database.db.name
  schema   = snowflake_schema.ordering.name
  name     = "decrypt"
  arguments {
    arg_data_type = "STRING"
    arg_name      = "val"
  }
  function_definition = <<EOF
    'Decrypted: ' || val
  EOF
  return_type         = "STRING"
}

resource "snowflake_procedure_sql" "my_procedure" {
  database = snowflake_database.db.name
  schema   = snowflake_schema.ordering.name
  name     = "myProcedure"
  arguments {
    arg_data_type = "VARCHAR(100)"
    arg_name      = "x"
  }
  return_type          = "VARCHAR(100)"
  procedure_definition = <<EOT
BEGIN
  RETURN message;
END;
EOT
}

resource "snowflake_schema" "special_schema" {
  database = snowflake_database.special_db.name
  name     = "SCHEMA NAME WITH S†RANGE çhars"
  comment  = "Schema for RAITO testing and demo"
}

resource "snowflake_tag" "sensitivity" {
  database       = snowflake_database.db.name
  schema         = snowflake_schema.ordering.name
  name           = "SENSITIVITY"
  allowed_values = ["PHI", "PII"]
}

// CUSTOMERS TABLE
resource "snowflake_table" "ordering_customer" {
  database = snowflake_database.db.name
  schema   = snowflake_schema.ordering.name
  name     = "CUSTOMER"

  column {
    name = "CUSTKEY"
    type = "NUMBER(38,0)"
  }

  column {
    name = "NAME"
    type = "VARCHAR"
  }

  column {
    name = "ADDRESS"
    type = "VARCHAR"
  }

  column {
    name = "NATIONKEY"
    type = "NUMBER(38,0)"
  }

  column {
    name = "PHONE"
    type = "VARCHAR"
  }

  column {
    name = "ACCTBAL"
    type = "NUMBER(38,0)"
  }

  column {
    name = "MKTSEGMENT"
    type = "VARCHAR"
  }

  column {
    name = "COMMENT"
    type = "VARCHAR"
  }
}

// ORDERS TABLE
resource "snowflake_table" "ordering_orders" {
  database = snowflake_database.db.name
  schema   = snowflake_schema.ordering.name
  name     = "ORDERS"

  column {
    name = "ORDERKEY"
    type = "NUMBER(38,0)"
  }

  column {
    name = "CUSTKEY"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ORDERSTATUS"
    type = "VARCHAR"
  }

  column {
    name = "TOTALPRICE"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ORDERDATE"
    type = "DATE"
  }

  column {
    name = "ORDERPRIORITY"
    type = "VARCHAR"
  }

  column {
    name = "CLERK"
    type = "VARCHAR"
  }

  column {
    name = "SHIPPRIORITY"
    type = "NUMBER(38,0)"
  }

  column {
    name = "COMMENT"
    type = "VARCHAR"
  }
}

// SUPPLIER
resource "snowflake_table" "ordering_supplier" {
  database = snowflake_database.db.name
  schema   = snowflake_schema.ordering.name
  name     = "SUPPLIER"

  column {
    name = "SUPPKEY"
    type = "NUMBER(38,0)"
  }

  column {
    name           = "NAME"
    type           = "VARCHAR"
    masking_policy = snowflake_masking_policy.masking_policy.fully_qualified_name
  }

  column {
    name = "ADDRESS"
    type = "VARCHAR"
  }

  column {
    name = "NATIONKEY"
    type = "NUMBER(38,0)"
  }

  column {
    name = "PHONE"
    type = "DATE"
  }

  column {
    name = "ACCTBAL"
    type = "VARCHAR"
  }

  column {
    name = "COMMENT"
    type = "VARCHAR"
  }
}

resource "snowflake_table" "special_table" {
  database = snowflake_database.special_db.name
  schema   = snowflake_schema.special_schema.name
  name     = "SPECIAL †ABLE NAME😀"

  column {
    name = "CUSTKEY"
    type = "NUMBER(38,0)"
  }

  column {
    name = "NAME"
    type = "VARCHAR"
  }

  column {
    name = "ADDRESS"
    type = "VARCHAR"
  }

  column {
    name = "NATIONKEY"
    type = "NUMBER(38,0)"
  }

  column {
    name = "PHONE"
    type = "VARCHAR"
  }

  column {
    name = "ACCTBAL"
    type = "NUMBER(38,0)"
  }

  column {
    name = "MKTSEGMENT"
    type = "VARCHAR"
  }

  column {
    name = "COMMENT"
    type = "VARCHAR"
  }
}

resource "snowflake_masking_policy" "masking_policy" {
  name                  = "ORDERING_MASKING_POLICY"
  database              = snowflake_database.db.name
  schema                = snowflake_schema.ordering.name
  exempt_other_policies = false
  argument {
    name = "val"
    type = "VARCHAR"
  }
  body = <<-EOF
    case
      when current_role() in ('ACCOUNTADMIN', 'SYSADMIN') then
        val
      else
        '******'
    end
  EOF

  return_data_type = "VARCHAR"
}

resource "snowflake_tag_association" "customer_pii" {
  object_identifiers = ["${snowflake_table.ordering_customer.fully_qualified_name}.ADDRESS", "${snowflake_table.ordering_customer.fully_qualified_name}.NAME", "${snowflake_table.ordering_customer.fully_qualified_name}.PHONE", "${snowflake_table.ordering_supplier.fully_qualified_name}.ADDRESS", "${snowflake_table.ordering_supplier.fully_qualified_name}.NAME", "${snowflake_table.ordering_supplier.fully_qualified_name}.PHONE"]

  object_type = "COLUMN"
  tag_id      = snowflake_tag.sensitivity.fully_qualified_name
  tag_value   = "PII"
}

// ORDERS VIEW
resource "snowflake_view" "orders_limited" {
  database = snowflake_database.db.name
  schema   = snowflake_schema.ordering.name
  name     = "ORDERS_LIMITED"
  comment  = "Non-materialized view with limited data"

  statement   = <<-SQL
    SELECT ORDERKEY, ORDERSTATUS, CUSTKEY FROM ${snowflake_table.ordering_orders.name};
  SQL
  copy_grants = true
}

resource "snowflake_materialized_view" "customers_limited" {
  database = snowflake_database.db.name
  schema   = snowflake_schema.ordering.name
  name     = "CUSTOMERS_LIMITED"
  comment  = "Materialized view with limited data"

  statement = <<-SQL
    SELECT CUSTKEY, ACCTBAL, MKTSEGMENT FROM ${snowflake_table.ordering_customer.name};
  SQL

  warehouse  = var.snowflake_warehouse
  or_replace = true
}

// TODO external table

// SHARE
resource "snowflake_shared_database" "shared_db" {
  name       = "SNOWFLAKE_SAMPLE_DATA"
  from_share = "SFSALESSHARED.SFC_SAMPLES_EUFRANKFURT.SAMPLE_DATA"
}

resource "snowflake_account_role" "special_account_role" {
  name    = "SπECIAL åCCOUNT RØLE"
  comment = "Account role for special cases"
}

// Role what
resource "snowflake_grant_privileges_to_account_role" "data_analyst_privileges_orders" {
  all_privileges    = true
  account_role_name = "DATA_ANALYST"
  on_schema_object {
    object_name = "\"${snowflake_view.orders_limited.database}\".\"${snowflake_view.orders_limited.schema}\".\"${snowflake_view.orders_limited.name}\""
    object_type = "VIEW"
  }
}

resource "snowflake_grant_privileges_to_account_role" "data_analyst_privileges_supplier" {
  privileges        = ["SELECT", "REFERENCES"]
  account_role_name = "DATA_ANALYST"
  on_schema_object {
    object_name = snowflake_table.ordering_supplier.fully_qualified_name
    object_type = "TABLE"
  }
}

resource "snowflake_grant_privileges_to_account_role" "data_analyst_privileges_decrypt" {
  privileges        = ["USAGE"]
  account_role_name = "DATA_ANALYST"
  on_schema_object {
    object_name = snowflake_function_sql.decrypt_function.fully_qualified_name
    object_type = "FUNCTION"
  }
}

resource "snowflake_grant_privileges_to_account_role" "data_analyst_privileges_myprocedure" {
  privileges        = ["USAGE"]
  account_role_name = "DATA_ANALYST"
  on_schema_object {
    object_name = snowflake_procedure_sql.my_procedure.fully_qualified_name
    object_type = "PROCEDURE"
  }
}

resource "snowflake_grant_privileges_to_account_role" "data_analyst_privileges_scimintegration" {
  privileges        = ["USAGE"]
  account_role_name = "DATA_ANALYST"
  on_account_object {
    object_name = snowflake_scim_integration.scim_integration.fully_qualified_name
    object_type = "INTEGRATION"
  }
}

resource "snowflake_grant_privileges_to_account_role" "sales_privileges_orders" {
  privileges        = ["SELECT", "INSERT"]
  account_role_name = "SALES"
  on_schema_object {
    object_name = snowflake_table.ordering_orders.fully_qualified_name
    object_type = "TABLE"
  }
}

resource "snowflake_grant_privileges_to_account_role" "usage_on_database" {
  privileges = ["USAGE"]

  for_each = var.existing_snowflake_roles

  account_role_name = each.value
  on_account_object {
    object_name = snowflake_database.db.name
    object_type = "DATABASE"
  }
}

resource "snowflake_grant_privileges_to_account_role" "usage_on_schema" {
  privileges = ["USAGE"]

  for_each = var.existing_snowflake_roles

  account_role_name = each.value
  on_schema {
    schema_name = "${snowflake_schema.ordering.database}.${snowflake_schema.ordering.name}"
  }
}

// Database ROLES
resource "snowflake_database_role" "database_role" {
  database = snowflake_database.db.name
  name     = "RAITO_DB_ROLE_1"
  comment  = "Database role for RAITO testing and demo"
}

resource "snowflake_grant_privileges_to_database_role" "database_role_privileges_database" {
  all_privileges     = true
  database_role_name = "\"${snowflake_database_role.database_role.database}\".\"${snowflake_database_role.database_role.name}\""
  on_database        = snowflake_database_role.database_role.database
}

resource "snowflake_grant_privileges_to_database_role" "database_role_privileges_schema" {
  all_privileges     = true
  database_role_name = "\"${snowflake_database_role.database_role.database}\".\"${snowflake_database_role.database_role.name}\""
  on_schema {
    schema_name = "\"${snowflake_database_role.database_role.database}\".\"${snowflake_schema.ordering.name}\""
  }
}

resource "snowflake_grant_privileges_to_database_role" "database_role_privileges_table" {
  privileges         = ["SELECT"]
  database_role_name = "\"${snowflake_database_role.database_role.database}\".\"${snowflake_database_role.database_role.name}\""
  on_schema_object {
    object_name = snowflake_table.ordering_orders.fully_qualified_name
    object_type = "TABLE"
  }
}

resource "snowflake_grant_database_role" "special_database_role" {
  database_role_name = "\"${snowflake_database_role.database_role.database}\".\"${snowflake_database_role.database_role.name}\""
  parent_role_name   = "HUMAN_RESOURCES"
}

resource "snowflake_database_role" "special_role" {
  database = snowflake_database.special_db.name
  name     = "SPECIAL DåTABASE RøLE"
  comment  = "Database role for RAITO testing and demo"
}