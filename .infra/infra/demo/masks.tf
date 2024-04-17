resource "snowflake_masking_policy" "pserson_pii" {
  database           = snowflake_schema.person.database
  schema             = snowflake_schema.person.name
  name               = "PERSON-PII_MASK"
  masking_expression = <<-EOF
    case
      when is_role_in_session('SALES') then val
      when current_role() in ('DATA_ENGINEER') then regexp_replace('.+\@', '*****@')
      else '********'
    end
  EOF
  return_data_type   = "VARCHAR"
  signature {
    column {
      name = "val"
      type = "VARCHAR"
    }
  }
}