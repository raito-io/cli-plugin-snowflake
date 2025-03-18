terraform {
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "1.0.4"
    }

    tls = {
      source  = "hashicorp/tls"
      version = "4.0.6"
    }
  }
}