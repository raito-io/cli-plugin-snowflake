// SNOWFLAKE DATABASE
resource "snowflake_database" "db" {
  name    = "RAITO_DATABASE"
  comment = "Database for RAITO testing and demo"
}

resource "snowflake_schema" "ordering" {
  database = snowflake_database.db.name
  name     = "ORDERING"
  comment  = "Schema for RAITO testing and demo"
}

resource "snowflake_tag" "sensitivity" {
  count = var.snowflake_standard_edition ? 0 : 1

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

resource "snowflake_tag_association" "customer_pii" {
  count = var.snowflake_standard_edition ? 0 : 1

  object_identifier {
    name     = "${snowflake_table.ordering_customer.name}.ADDRESS"
    database = snowflake_database.db.name
    schema   = snowflake_schema.ordering.name
  }

  object_identifier {
    name     = "${snowflake_table.ordering_customer.name}.NAME"
    database = snowflake_database.db.name
    schema   = snowflake_schema.ordering.name
  }

  object_identifier {
    name     = "${snowflake_table.ordering_customer.name}.PHONE"
    database = snowflake_database.db.name
    schema   = snowflake_schema.ordering.name
  }

  object_type = "COLUMN"
  tag_id      = snowflake_tag.sensitivity[0].id
  tag_value   = "PII"
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
    masking_policy = var.snowflake_standard_edition ? null : snowflake_masking_policy.masking_policy[0].qualified_name
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

resource "snowflake_masking_policy" "masking_policy" {
  count = var.snowflake_standard_edition ? 0 : 1

  name     = "ORDERING_MASKING_POLICY"
  database = snowflake_database.db.name
  schema   = snowflake_schema.ordering.name
  signature {
    column {
      name = "val"
      type = "VARCHAR"
    }
  }
  masking_expression = <<-EOF
    case
      when current_role() in ('ACCOUNTADMIN', 'SYSADMIN') then
        val
      else
        '******'
    end
  EOF

  return_data_type = "VARCHAR"
}

resource "snowflake_tag_association" "supplier_pii" {
  count = var.snowflake_standard_edition ? 0 : 1

  object_identifier {
    name     = "${snowflake_table.ordering_supplier.name}.ADDRESS"
    database = snowflake_database.db.name
    schema   = snowflake_schema.ordering.name
  }

  object_identifier {
    name     = "${snowflake_table.ordering_supplier.name}.NAME"
    database = snowflake_database.db.name
    schema   = snowflake_schema.ordering.name
  }

  object_identifier {
    name     = "${snowflake_table.ordering_supplier.name}.PHONE"
    database = snowflake_database.db.name
    schema   = snowflake_schema.ordering.name
  }

  object_type = "COLUMN"
  tag_id      = snowflake_tag.sensitivity[0].id
  tag_value   = "PII"
}

// ORDERS VIEW
resource "snowflake_materialized_view" "orders_limited" {
  database  = snowflake_database.db.name
  schema    = snowflake_schema.ordering.name
  name      = "ORDERS_LIMITED"
  warehouse = snowflake_warehouse.warehouse.name
  comment   = "Materialized view with limited data"

  statement  = <<-SQL
    SELECT ORDERKEY, ORDERSTATUS, CUSTKEY FROM ORDERS;
  SQL
  or_replace = true

  depends_on = [snowflake_table.ordering_orders]
}

// SNOWFLAKE WAREHOUSE
resource "snowflake_warehouse" "warehouse" {
  name           = "RAITO_WAREHOUSE"
  warehouse_size = "xsmall"
  auto_suspend   = 60
}

