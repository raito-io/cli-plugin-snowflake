resource "tls_private_key" "rsa-key" {
  algorithm = "RSA"
}

// RAITO PERSONAS
resource "snowflake_service_user" "benjamin" {
  name              = "BENJAMINSTEWART"
  login_name        = "BENJAMINSTEWART"
  email             = "b_stewart@raito.io"
  display_name      = "Benjamin Stewart"
  comment           = "Raito - Access Manager"
  rsa_public_key    = tls_private_key.rsa-key.public_key_pem
  default_warehouse = snowflake_warehouse.warehouse.id
  default_role      = "PUBLIC"
  disabled          = "false"
}

resource "snowflake_service_user" "nick" {
  name              = "NICKNGUYEN"
  login_name        = "NICKNGUYEN"
  email             = "n_nguyen@raito.io"
  display_name      = "Nick Nguyen"
  comment           = "Raito - Admin"
  rsa_public_key    = tls_private_key.rsa-key.public_key_pem
  default_warehouse = snowflake_warehouse.warehouse.id
  default_role      = "PUBLIC"
  disabled          = "false"
}

resource "snowflake_service_user" "carla" {
  name              = "CARLAHARRIS"
  login_name        = "CARLAHARRIS"
  email             = "c_harris@raito.io"
  display_name      = "Carla Harris"
  comment           = "Raito - Observer"
  rsa_public_key    = tls_private_key.rsa-key.public_key_pem
  default_warehouse = snowflake_warehouse.warehouse.id
  default_role      = "PUBLIC"
  disabled          = "false"
}

resource "snowflake_service_user" "dustin" {
  name              = "DUSTINHAYDEN"
  login_name        = "DUSTINHAYDEN"
  email             = "d_hayden@raito.io"
  display_name      = "Dustin Hayden"
  comment           = "Raito - Owner"
  rsa_public_key    = tls_private_key.rsa-key.public_key_pem
  default_warehouse = snowflake_warehouse.warehouse.id
  default_role      = "PUBLIC"
  disabled          = "false"
}

resource "snowflake_service_user" "mary" {
  name              = "MARYCARISSA"
  login_name        = "MARYCARISSA"
  email             = "m_carissa@raito.io"
  display_name      = "Mary Carissa"
  comment           = "Raito - User"
  rsa_public_key    = tls_private_key.rsa-key.public_key_pem
  default_warehouse = snowflake_warehouse.warehouse.id
  default_role      = "PUBLIC"
  disabled          = "false"
}

resource "snowflake_service_user" "luis_garcia_stork" {
  name              = "stork_m"
  login_name        = "STORK_M"
  email             = "lg.stork@raito.io"
  display_name      = "Luis Garcia Stork"
  rsa_public_key    = tls_private_key.rsa-key.public_key_pem
  default_warehouse = snowflake_warehouse.warehouse.id
}

resource "snowflake_service_user" "jobs_de" {
  name              = "jobs_de"
  login_name        = "JOBS_DE"
  email             = "jobs_de@raito.io"
  display_name      = "Intern Data Engineer"
  rsa_public_key    = tls_private_key.rsa-key.public_key_pem
  default_warehouse = snowflake_warehouse.warehouse.id
}

resource "snowflake_service_user" "data_engineering" {
  name              = "data_engineering"
  login_name        = "DATA_ENGINEERING"
  email             = "data_engineer@raito.io"
  display_name      = "Data Engineer service account"
  #password          = random_password.persona_password.result
  rsa_public_key    = tls_private_key.rsa-key.public_key_pem
  default_warehouse = snowflake_warehouse.warehouse.id
}

resource "snowflake_service_user" "scranton_j" {
  name         = "scranton_j"
  login_name   = "SCRANTON_J"
  email        = "jscranton123@raito.io"
  display_name = "James Robert Scranton"
  rsa_public_key    = tls_private_key.rsa-key.public_key_pem
}

resource "snowflake_service_user" "atkison_a" {
  name         = "atkison_a"
  login_name   = "ATKISON_A"
  email        = "a_abbotatkinson7576@raito.io"
  display_name = "Angelica Abbot Atkinson"
  rsa_public_key    = tls_private_key.rsa-key.public_key_pem

}

resource "snowflake_service_user" "macwilliam_j" {
  name         = "macwilliam_j"
  login_name   = "MACWILLIAM_J"
  email        = "jb_macwilliam@raito.io"
  display_name = "Juliette Berkant MacWilliam"
  rsa_public_key    = tls_private_key.rsa-key.public_key_pem
}

resource "snowflake_service_user" "keith_c" {
  name         = "keith_c"
  login_name   = "KEITH_C"
  email        = "c_keith@raito.io"
  display_name = "Claudia Seyyed Keith"
  rsa_public_key    = tls_private_key.rsa-key.public_key_pem
}

resource "snowflake_service_user" "sakamoto_w" {
  name         = "sakamoto_w"
  login_name   = "SAKAMOTO_W"
  email        = "w-sakamoto515@raito.io"
  display_name = "Wil Arya Sakamoto"
  rsa_public_key    = tls_private_key.rsa-key.public_key_pem
}

resource "snowflake_service_user" "stein_v" {
  name         = "stein_v"
  login_name   = "STEIN_V"
  email        = "v.scarletstein8246@raito.us"
  display_name = "Vladimir Scarlet Stein (US Sales)"
  rsa_public_key    = tls_private_key.rsa-key.public_key_pem
}

resource "snowflake_service_user" "kiss_k" {
  name         = "kiss_k"
  login_name   = "KISS_K"
  email        = "k.a.kiss54856@raito.io"
  display_name = "Katsuo América Kiss"
  rsa_public_key    = tls_private_key.rsa-key.public_key_pem
}

resource "snowflake_service_user" "henriksson_v" {
  name         = "henriksson_v"
  login_name   = "HENRIKSSON_V"
  email        = "vk-henriksson@raito.io"
  display_name = "Valentin Kasey Henriksson"
  rsa_public_key    = tls_private_key.rsa-key.public_key_pem

}

locals {
  who_role = [
    {
      user : snowflake_service_user.benjamin.name
      email : snowflake_service_user.benjamin.email
      roles : [snowflake_account_role.human_resources.name, snowflake_account_role.data_analyst.name]
    },
    {
      user : snowflake_service_user.nick.name
      email : snowflake_service_user.nick.email
      roles : []
    },
    {
      user : snowflake_service_user.carla.name
      email : snowflake_service_user.carla.email
      roles : []
    },
    {
      user : snowflake_service_user.dustin.name
      email : snowflake_service_user.dustin.email
      roles : [snowflake_account_role.sales.name, snowflake_account_role.sales_ext.name]
    },
    {
      user : snowflake_service_user.mary.name
      email : snowflake_service_user.mary.email
      roles : [snowflake_account_role.human_resources.name, snowflake_account_role.sales.name, snowflake_account_role.sales_analysis.name]
    },
    {
      user : snowflake_service_user.luis_garcia_stork.name
      email : snowflake_service_user.luis_garcia_stork.email
      roles : []
    },
    {
      user : snowflake_service_user.jobs_de.name
      email : snowflake_service_user.jobs_de.email
      roles : []
    },
    {
      user : snowflake_service_user.data_engineering.name
      email : snowflake_service_user.data_engineering.email
      roles : []
    },
    {
      user : snowflake_service_user.scranton_j.name
      email : snowflake_service_user.scranton_j.email
      roles : [snowflake_account_role.human_resources.name]
    },
    {
      user : snowflake_service_user.atkison_a.name
      email : snowflake_service_user.atkison_a.email
      roles : []
    },
    {
      user : snowflake_service_user.macwilliam_j.name
      email : snowflake_service_user.macwilliam_j.email
      roles : [snowflake_account_role.human_resources.name]
    },
    {
      user : snowflake_service_user.keith_c.name
      email : snowflake_service_user.keith_c.email
      roles : [snowflake_account_role.human_resources.name]
    },
    {
      user : snowflake_service_user.sakamoto_w.name
      email : snowflake_service_user.sakamoto_w.email
      roles : [snowflake_account_role.finance.name, snowflake_account_role.marketing.name]
    },
    {
      user : snowflake_service_user.stein_v.name
      email : snowflake_service_user.stein_v.email
      roles : []
    },
    {
      user : snowflake_service_user.kiss_k.name
      email : snowflake_service_user.kiss_k.email
      roles : []
    },
    {
      user : snowflake_service_user.henriksson_v.name
      email : snowflake_service_user.henriksson_v.email
      roles : []
    }
  ]

  flatten_who_role = toset(flatten([for v in local.who_role : [for role in v.roles : { user : v.user, role : role }]]))
}

// roles
resource "snowflake_account_role" "human_resources" {
  name    = "HUMAN_RESOURCES"
  comment = "Raito - Human Resources"
}

resource "snowflake_grant_account_role" "role_grants" {
  for_each = { for v in local.flatten_who_role : format("%s#%s", v.user, v.role) => v }

  role_name = each.value.role
  user_name = each.value.user
}

resource "snowflake_account_role" "data_analyst" {
  name    = "DATA_ANALYST"
  comment = "Raito - Data Analyst"
}

resource "snowflake_account_role" "marketing" {
  name    = "MARKETING"
  comment = "Raito - Marketing"
}

resource "snowflake_account_role" "finance" {
  name    = "FINANCE"
  comment = "Raito - Finance"
}

resource "snowflake_account_role" "sales" {
  name    = "SALES"
  comment = "Raito - Sales"
}

resource "snowflake_account_role" "sales_analysis" {
  name    = "SALES_ANALYSIS"
  comment = "Raito - Sales Analysis"
}

resource "snowflake_account_role" "sales_ext" {
  name    = "SALES_EXT"
  comment = "Raito - Sales Extension"
}

resource "snowflake_grant_account_role" "sales_dustin" {
  role_name = snowflake_account_role.sales.name
  user_name = snowflake_service_user.dustin.name
}

resource "snowflake_grant_privileges_to_account_role" "warehouse_grant" {
  privileges = ["USAGE", "OPERATE"]

  for_each = toset([snowflake_account_role.sales.name, snowflake_account_role.data_analyst.name, snowflake_account_role.human_resources.name, snowflake_account_role.finance.name, snowflake_account_role.marketing.name, snowflake_account_role.sales_analysis.name, snowflake_account_role.sales_ext.name])

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