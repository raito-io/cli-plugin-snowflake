output "testing_snowflake_database_name" {
  value = var.testing_dataset ? module.testing[0].snowflake_database_name : null
}

output "testing_snowflake_share_database_name" {
  value = var.testing_dataset ? module.testing[0].snowflake_database_name : null
}

output "snowflake_warehouse_name" {
  value = snowflake_warehouse.warehouse.name
}

output "snowflake_account" {
  value     = var.snowflake_account
  sensitive = true
}

output "snowflake_tables" {
  value = concat(var.testing_dataset ? module.testing[0].snowflake_tables : [], var.demo_dataset ? module.demo[0].tables : [])
}


output "persona_password" {
  value     = random_password.persona_password.result
  sensitive = true
}

output "personas" {
  value     = local.who_role
  sensitive = true
}

output "persona_rsa_private_key" {
    value     = tls_private_key.rsa-key.private_key_pem
    sensitive = true
}