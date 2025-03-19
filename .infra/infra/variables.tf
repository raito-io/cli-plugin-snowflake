// SNOWFLAKE PROVIDER
variable "snowflake_account" {
  type        = string
  sensitive   = true
  description = "Snowflake account "
  nullable    = false
}

// SNOWFLAKE PROVIDER
variable "snowflake_organization" {
  type        = string
  sensitive   = true
  description = "Snowflake organization"
  nullable    = false
}

variable "snowflake_user" {
  type        = string
  sensitive   = false
  description = "Snowflake username"
  nullable    = false
}

variable "snowflake_private_key_file" {
  type        = string
  sensitive   = true
  description = "Path to the private key file"
  nullable    = false
}

variable "testing_dataset" {
  type        = bool
  sensitive   = false
  description = "Infrastructure for testing purposes"
  default     = true
}

variable "demo_dataset" {
  type        = bool
  sensitive   = false
  description = "Infrastructure for testing purposes"
  default     = true
}