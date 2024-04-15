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

variable "snowflake_standard_edition" {
  type        = bool
  sensitive   = false
  description = "Snowflake standard edition"
  default     = true
}