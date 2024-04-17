variable "snowflake_warehouse" {
  type = string
  sensitive = false
  description = "Snowflake warehouse"
  nullable    = false
}

variable "existing_snowflake_roles" {
  type = set(string)
  sensitive = false
  description = "Existing Snowflake roles"
  default     = []
}