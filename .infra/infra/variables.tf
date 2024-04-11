// SNOWFLAKE PROVIDER
variable "snowflake_account" {
  type        = string
  sensitive   = true
  description = "Snowflake account url"
  nullable    = false
}

variable "snowflake_user" {
  type        = string
  sensitive   = false
  description = "Snowflake username"
  nullable    = false
}

variable "snowflake_password" {
  type        = string
  sensitive   = true
  description = "Snowflake password"
  nullable    = false
}