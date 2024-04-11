// RAITO PERSONAS
resource "snowflake_user" "benjamin" {
  name              = "BenjaminStewart"
  email             = "b_stewart@raito.io"
  display_name      = "Benjamin Stewart"
  first_name        = "Benjamin"
  last_name         = "Stewart"
  comment           = "Raito - Access Manager"
  password          = random_password.persona_password.result
  default_warehouse = snowflake_warehouse.warehouse.id
}

resource "snowflake_user" "nick" {
  name              = "NickNguyen"
  email             = "n_nguyen@raito.io"
  display_name      = "Nick Nguyen"
  first_name        = "Nick"
  last_name         = "Nguyen"
  comment           = "Raito - Admin"
  password          = random_password.persona_password.result
  default_warehouse = snowflake_warehouse.warehouse.id
}

resource "snowflake_user" "carla" {
  name              = "CarlaHarris"
  email             = "c_harris@raito.io"
  display_name      = "Carla Harris"
  first_name        = "Carla"
  last_name         = "Harris"
  comment           = "Raito - Observer"
  password          = random_password.persona_password.result
  default_warehouse = snowflake_warehouse.warehouse.id
}

resource "snowflake_user" "dustin" {
  name              = "DustinHayden"
  email             = "d_hayden@raito.io"
  display_name      = "Dustin Hayden"
  first_name        = "Dustin"
  last_name         = "Hayden"
  comment           = "Raito - Owner"
  password          = random_password.persona_password.result
  default_warehouse = snowflake_warehouse.warehouse.id
}

resource "snowflake_user" "mary" {
  name              = "MaryCarissa"
  email             = "m_carissa@raito.io"
  display_name      = "Mary Carissa"
  first_name        = "Mary"
  last_name         = "Carissa"
  comment           = "Raito - User"
  password          = random_password.persona_password.result
  default_warehouse = snowflake_warehouse.warehouse.id
}

locals {
  who_role = [
    {
      user : nonsensitive(snowflake_user.benjamin.name),
      email : snowflake_user.benjamin.email
      roles : [snowflake_role.human_resources.name, snowflake_role.data_analyst.name]
    },
    {
      user : nonsensitive(snowflake_user.nick.name),
      email : snowflake_user.nick.email
      roles : []
    },
    {
      user : nonsensitive(snowflake_user.carla.name),
      email : snowflake_user.carla.email
      roles : []
    },
    {
      user : nonsensitive(snowflake_user.dustin.name),
      email : snowflake_user.dustin.email
      roles : [snowflake_role.sales.name]
    },
    {
      user : nonsensitive(snowflake_user.mary.name)
      email : snowflake_user.mary.email
      roles : [snowflake_role.human_resources.name, snowflake_role.sales.name]
    }
  ]

  flatten_who_role = toset(flatten([for v in local.who_role : [for role in v.roles : { user : v.user, role : role }]]))
}

// roles
resource "snowflake_role" "human_resources" {
  name    = "RAITO_HUMAN_RESOURCES"
  comment = "Raito - Human Resources"
}

resource "snowflake_grant_account_role" "role_grants" {
  for_each = { for v in local.flatten_who_role : format("%s#%s)", v.user, v.role) => v }

  role_name = each.value.role
  user_name = each.value.user
}

resource "snowflake_role" "data_analyst" {
  name    = "RAITO_DATA_ANALYST"
  comment = "Raito - Data Analyst"
}

resource "snowflake_grant_privileges_to_account_role" "data_analyst_privileges_orders" {
  all_privileges    = true
  account_role_name = snowflake_role.data_analyst.name
  on_schema_object {
    object_name = "\"${snowflake_materialized_view.orders_limited.database}\".\"${snowflake_materialized_view.orders_limited.schema}\".\"${snowflake_materialized_view.orders_limited.name}\""
    object_type = "VIEW"
  }
}

resource "snowflake_grant_privileges_to_account_role" "data_analyst_privileges_supplier" {
  privileges        = ["SELECT", "REFERENCES"]
  account_role_name = snowflake_role.data_analyst.name
  on_schema_object {
    object_name = snowflake_table.ordering_supplier.qualified_name
    object_type = "TABLE"
  }
}

resource "snowflake_role" "marketing" {
  name    = "RAITO_MARKETING"
  comment = "Raito - Marketing"
}

resource "snowflake_role" "finance" {
  name    = "RAITO_FINANCE"
  comment = "Raito - Finance"
}

resource "snowflake_role" "sales" {
  name    = "RAITO_SALES"
  comment = "Raito - Sales"
}

resource "snowflake_grant_account_role" "sales_dustin" {
  role_name = snowflake_role.sales.name
  user_name = snowflake_user.dustin.name
}

resource "snowflake_grant_privileges_to_account_role" "sales_privileges_orders" {
  privileges        = ["SELECT", "INSERT"]
  account_role_name = snowflake_role.sales.name
  on_schema_object {
    object_name = snowflake_table.ordering_orders.qualified_name
    object_type = "TABLE"
  }
}

resource "snowflake_grant_privileges_to_account_role" "usage_on_database" {
  privileges = ["USAGE"]

  for_each = toset([snowflake_role.sales.name, snowflake_role.data_analyst.name, snowflake_role.human_resources.name, snowflake_role.finance.name, snowflake_role.marketing.name])

  account_role_name = each.value
  on_account_object {
    object_name = snowflake_database.db.name
    object_type = "DATABASE"
  }
}

resource "snowflake_grant_privileges_to_account_role" "usage_on_schema" {
  privileges = ["USAGE"]

  for_each = toset([snowflake_role.sales.name, snowflake_role.data_analyst.name, snowflake_role.human_resources.name, snowflake_role.finance.name, snowflake_role.marketing.name])

  account_role_name = each.value
  on_schema {
    all_schemas_in_database = snowflake_database.db.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "warehouse_grant" {
  privileges = ["USAGE", "OPERATE"]

  for_each = toset([snowflake_role.sales.name, snowflake_role.data_analyst.name, snowflake_role.human_resources.name, snowflake_role.finance.name, snowflake_role.marketing.name])

  account_role_name = each.value
  on_account_object {
    object_name = snowflake_warehouse.warehouse.name
    object_type = "WAREHOUSE"
  }
}

resource "random_password" "persona_password" {
  length  = 16
  special = true
}