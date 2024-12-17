// DATABASE - MASTER DATA
resource "snowflake_database" "master_data" {
  name = "MASTER_DATA"
}

// -- Schema DBO
resource "snowflake_schema" "dbo" {
  database = snowflake_database.master_data.name
  name     = "DBO"
}

// -- -- Table AWSBUILDVERSION
resource "snowflake_table" "awsbuildversion" {
  database = snowflake_schema.dbo.database
  schema   = snowflake_schema.dbo.name
  name     = "AWSBUILDVERSION"
  comment  = "Current version number of the AdventureWorks 2014 sample database."

  column {
    name = "Database Version"
    type = "VARCHAR"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "SystemInformationID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "VersionDate"
    type = "TIMESTAMP_NTZ(9)"
  }
}

// -- -- Table DATABASELOG
resource "snowflake_table" "databaselog" {
  database = snowflake_schema.dbo.database
  schema   = snowflake_schema.dbo.name
  name     = "DATABASELOG"
  comment  = "Audit table tracking all DDL changes made to the AdventureWorks database. Data is captured by the database trigger ddlDatabaseTriggerLog."

  column {
    name = "DatabaseLogID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "DatabaseUser"
    type = "VARCHAR"
  }

  column {
    name = "Event"
    type = "VARCHAR"
  }

  column {
    name = "Object"
    type = "VARCHAR"
  }

  column {
    name = "PostTime"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "Schema"
    type = "VARCHAR"
  }

  column {
    name = "TSQL"
    type = "VARCHAR"
  }

  column {
    name = "XmlEvent"
    type = "VARCHAR"
  }
}

// -- Schema HUMANRESOURCES
resource "snowflake_schema" "humanresources" {
  database = snowflake_database.master_data.name
  name     = "HUMANRESOURCES"
}

// -- -- Table DEPARTMENT
resource "snowflake_table" "department" {
  database = snowflake_schema.humanresources.database
  schema   = snowflake_schema.humanresources.name
  name     = "DEPARTMENT"
  comment  = "Lookup table containing the departments within the Adventure Works Cycles company."

  column {
    name = "DepartmentID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "GroupName"
    type = "VARCHAR"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "Name"
    type = "VARCHAR"
  }
}

// -- -- Table EMPLOYEE
resource "snowflake_table" "employee" {
  database = snowflake_schema.humanresources.database
  schema   = snowflake_schema.humanresources.name
  name     = "EMPLOYEE"
  comment  = "Employee information such as salary, department, and title."

  column {
    name = "BirthDate"
    type = "DATE"
  }

  column {
    name = "BusinessEntityID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "CurrentFlag"
    type = "VARCHAR"
  }

  column {
    name = "Gender"
    type = "VARCHAR"
  }

  column {
    name = "HireDate"
    type = "DATE"
  }

  column {
    name = "JobTitle"
    type = "VARCHAR"
  }

  column {
    name = "LoginID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "MaritalStatus"
    type = "VARCHAR"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "NationalIDNumber"
    type = "VARCHAR"
  }

  column {
    name = "OrganizationLevel"
    type = "VARCHAR"
  }

  column {
    name = "OrganizationNode"
    type = "VARCHAR"
  }

  column {
    name = "ROWGUID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "SalariedFlag"
    type = "VARCHAR"
  }

  column {
    name = "SickLeaveHours"
    type = "NUMBER(38,0)"
  }

  column {
    name = "VacationHours"
    type = "NUMBER(38,0)"
  }
}

// -- -- Table EMPLOYEEDEPARTMENTHISTORY
resource "snowflake_table" "empoyee_department_history" {
  database = snowflake_schema.humanresources.database
  schema   = snowflake_schema.humanresources.name
  name     = "EMPLOYEEDEPARTMENTHISTORY"
  comment  = "Employee department transfers."

  column {
    name = "BusinessEntityID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "DepartmentID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "EndDate"
    type = "DATE"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "ShiftID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "StartDate"
    type = "DATE"
  }
}

// -- -- Table JOBCANDIDATE
resource "snowflake_table" "job_candidate" {
  database = snowflake_schema.humanresources.database
  schema   = snowflake_schema.humanresources.name
  name     = "JOBCANDIDATE"
  comment  = "Résumés submitted to Human Resources by job applicants."

  column {
    name = "BusinessEntityID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "JobCandidateID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "Resume"
    type = "VARCHAR"
  }
}

// -- -- Table SHIFT
resource "snowflake_table" "shift" {
  database = snowflake_schema.humanresources.database
  schema   = snowflake_schema.humanresources.name
  name     = "SHIFT"
  comment  = "Work shift lookup table."

  column {
    name = "EndTime"
    type = "TIME(9)"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }


  column {
    name = "Name"
    type = "VARCHAR"
  }

  column {
    name = "ShiftID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "StartTime"
    type = "TIME(9)"
  }
}

// -- Schema PERSON
resource "snowflake_schema" "person" {
  database = snowflake_database.master_data.name
  name     = "PERSON"
}

// -- -- Table ADDRESS
resource "snowflake_table" "address" {
  database = snowflake_schema.person.database
  schema   = snowflake_schema.person.name
  name     = "ADDRESS"
  comment  = "Street address information for customers, employees, and vendors."

  column {
    name = "AddressID"
    type = "NUMBER(38,0)"
  }

  column {
    name           = "AddressLine1"
    type           = "VARCHAR"
    masking_policy = snowflake_masking_policy.pserson_pii.fully_qualified_name
  }

  column {
    name           = "AddressLine2"
    type           = "VARCHAR"
    masking_policy = snowflake_masking_policy.pserson_pii.fully_qualified_name
  }

  column {
    name           = "City"
    type           = "VARCHAR"
    masking_policy = snowflake_masking_policy.pserson_pii.fully_qualified_name
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name           = "PostalCode"
    type           = "VARCHAR"
    masking_policy = snowflake_masking_policy.pserson_pii.fully_qualified_name
  }

  column {
    name = "ROWGUID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "SpatialLocation"
    type = "VARCHAR"
  }

  column {
    name = "StateProvinceID"
    type = "NUMBER(38,0)"
  }
}

// -- -- Table ADDRESSTYPE
resource "snowflake_table" "address_type" {
  database = snowflake_schema.person.database
  schema   = snowflake_schema.person.name
  name     = "ADDRESSTYPE"
  comment  = "Types of addresses stored in the Address table."

  column {
    name = "AddressTypeID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "Name"
    type = "VARCHAR"
  }

  column {
    name = "ROWGUID"
    type = "NUMBER(38,0)"
  }
}

// -- -- Table BUSINESSENTITY
resource "snowflake_table" "business_entity" {
  database = snowflake_schema.person.database
  schema   = snowflake_schema.person.name
  name     = "BUSINESSENTITY"
  comment  = "Source of the ID that connects vendors, customers, and employees with address and contact information."

  column {
    name = "BusinessEntityID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "ROWGUID"
    type = "NUMBER(38,0)"
  }
}

// -- -- Table BUSINESSENTITYADDRESS
resource "snowflake_table" "business_entity_address" {
  database = snowflake_schema.person.database
  schema   = snowflake_schema.person.name
  name     = "BUSINESSENTITYADDRESS"
  comment  = "Cross-reference table mapping customers, vendors, and employees to their addresses."

  column {
    name = "AddressID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "AddressTypeID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "BusinessEntityID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "ROWGUID"
    type = "NUMBER(38,0)"
  }
}

// -- -- Table BUSINESSENTITYCONTACT
resource "snowflake_table" "business_entity_contact" {
  database = snowflake_schema.person.database
  schema   = snowflake_schema.person.name
  name     = "BUSINESSENTITYCONTACT"
  comment  = "Cross-reference table mapping stores, vendors, and employees to people"

  column {
    name = "BusinessEntityID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ContactTypeID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "PersonID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ROWGUID"
    type = "NUMBER(38,0)"
  }
}

// -- -- Table CONTACTTYPE
resource "snowflake_table" "contact_type" {
  database = snowflake_schema.person.database
  schema   = snowflake_schema.person.name
  name     = "CONTACTTYPE"
  comment  = "Lookup table containing the types of business entity contacts."

  column {
    name = "ContactTypeID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "Name"
    type = "VARCHAR"
  }
}

// -- -- Table COUNTRYREGION
resource "snowflake_table" "country_region" {
  database = snowflake_schema.person.database
  schema   = snowflake_schema.person.name
  name     = "COUNTRYREGION"
  comment  = "Lookup table containing the ISO standard codes for countries and regions."

  column {
    name = "CountryRegionCode"
    type = "VARCHAR(16)"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "Name"
    type = "VARCHAR"
  }
}

// -- -- Table EMAILADDRESS
resource "snowflake_table" "email_address" {
  database = snowflake_schema.person.database
  schema   = snowflake_schema.person.name
  name     = "EMAILADDRESS"
  comment  = "Where to send a person email."

  column {
    name = "BusinessEntityID"
    type = "NUMBER(38,0)"
  }

  column {
    name           = "EmailAddress"
    type           = "VARCHAR"
    masking_policy = snowflake_masking_policy.pserson_pii.fully_qualified_name
  }

  column {
    name = "EmailAddressID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "ROWGUID"
    type = "NUMBER(38,0)"
  }
}

// -- -- Table PERSONPHONE
resource "snowflake_table" "person_phone" {
  database = snowflake_schema.person.database
  schema   = snowflake_schema.person.name
  name     = "PERSONPHONE"
  comment  = "Telephone number and type of a person."

  column {
    name = "BusinessEntityID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name           = "PhoneNumber"
    type           = "VARCHAR"
    masking_policy = snowflake_masking_policy.pserson_pii.fully_qualified_name
  }

  column {
    name = "PhoneNumberTypeID"
    type = "NUMBER(38,0)"
  }
}

// -- -- Table PHONENUMBERTYPE
resource "snowflake_table" "phone_number_type" {
  database = snowflake_schema.person.database
  schema   = snowflake_schema.person.name
  name     = "PHONENUMBERTYPE"
  comment  = "Type of phone number of a person."

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "Name"
    type = "VARCHAR"
  }

  column {
    name = "PhoneNumberTypeID"
    type = "NUMBER(38,0)"
  }
}

// -- -- Table STATEPROVINCE
resource "snowflake_table" "state_province" {
  database = snowflake_schema.person.database
  schema   = snowflake_schema.person.name
  name     = "STATEPROVINCE"
  comment  = "State and province lookup table."

  column {
    name = "CountryRegionCode"
    type = "NUMBER(38,0)"
  }

  column {
    name = "IsOnlyStateProvinceFlag"
    type = "BOOLEAN"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "Name"
    type = "VARCHAR"
  }

  column {
    name = "ROWGUID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "StateProvinceID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "TerritoryID"
    type = "NUMBER(38,0)"
  }
}

// -- Schema PRODUCTION
resource "snowflake_schema" "production" {
  database = snowflake_database.master_data.name
  name     = "PRODUCTION"
}

// -- -- Table BILLOFMATERIALS
resource "snowflake_table" "bill_of_materials" {
  database = snowflake_schema.production.database
  schema   = snowflake_schema.production.name
  name     = "BILL_OF_MATERIALS"
  comment  = "Items required to make bicycles and bicycle subassemblies. It identifies the heirarchical relationship between a parent product and its components."

  column {
    name = "BillOfMaterialsID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "BOMLevel"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ComponentID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "EndDate"
    type = "DATE"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "PerAssemblyQty"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ProductAssemblyID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "StartDate"
    type = "DATE"
  }

  column {
    name = "UnitMeasureCode"
    type = "VARCHAR(16)"
  }
}

// -- -- Table CULTURE
resource "snowflake_table" "culture" {
  database = snowflake_schema.production.database
  schema   = snowflake_schema.production.name
  name     = "CULTURE"
  comment  = "Lookup table containing the languages in which some AdventureWorks data is stored."

  column {
    name = "CultureID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "Name"
    type = "VARCHAR"
  }
}

// -- -- Table DOCUMENT
resource "snowflake_table" "document" {
  database = snowflake_schema.production.database
  schema   = snowflake_schema.production.name
  name     = "DOCUMENT"
  comment  = "Product maintenance documents."

  column {
    name = "ChangeNumber"
    type = "NUMBER(38,0)"
  }

  column {
    name = "Document"
    type = "VARCHAR"
  }

  column {
    name = "DocumentLevel"
    type = "VARCHAR"
  }

  column {
    name = "DocumentNode"
    type = "VARCHAR"
  }

  column {
    name = "DocumentSummary"
    type = "VARCHAR"
  }

  column {
    name = "FileExtension"
    type = "VARCHAR"
  }

  column {
    name = "FileName"
    type = "VARCHAR"
  }

  column {
    name = "FolderFlag"
    type = "VARCHAR"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "Owner"
    type = "VARCHAR"
  }

  column {
    name = "Revision"
    type = "VARCHAR"
  }

  column {
    name = "ROWGUID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "Status"
    type = "VARCHAR"
  }

  column {
    name = "Title"
    type = "VARCHAR(32)"
  }
}

// -- -- Table ILLUSTRATION
resource "snowflake_table" "illustration" {
  database = snowflake_schema.production.database
  schema   = snowflake_schema.production.name
  name     = "ILLUSTRATION"
  comment  = "Bicycle assembly diagrams."

  column {
    name = "Diagram"
    type = "VARCHAR"
  }

  column {
    name = "IllustrationID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }
}

// -- -- Table LOCATION
resource "snowflake_table" "location" {
  database = snowflake_schema.production.database
  schema   = snowflake_schema.production.name
  name     = "LOCATION"
  comment  = "Product inventory and manufacturing locations."

  column {
    name = "Availability"
    type = "VARCHAR"
  }

  column {
    name = "CostRate"
    type = "NUMBER(38,0)"
  }

  column {
    name = "LocationID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "Name"
    type = "VARCHAR"
  }
}

// -- -- Table PRODUCT
resource "snowflake_table" "product" {
  database = snowflake_schema.production.database
  schema   = snowflake_schema.production.name
  name     = "PRODUCT"
  comment  = "Products sold or used in the manfacturing of sold products."

  column {
    name = "Class"
    type = "VARCHAR"
  }

  column {
    name = "Color"
    type = "VARCHAR"
  }

  column {
    name = "DaysToManufacture"
    type = "NUMBER(38,0)"
  }

  column {
    name = "DiscontinuedDate"
    type = "DATE"
  }

  column {
    name = "FinishedGoodsFlag"
    type = "BOOLEAN"
  }

  column {
    name = "ListPrice"
    type = "NUMBER(38,0)"
  }

  column {
    name = "MakeFlag"
    type = "BOOLEAN"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "Name"
    type = "VARCHAR"
  }

  column {
    name = "ProductID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ProductLine"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ProductModelID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ProductNumber"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ProductSubcategoryID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ReorderPoint"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ROWGUID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "SafetyStockLevel"
    type = "NUMBER(38,0)"
  }

  column {
    name = "SellEndDate"
    type = "DATE"
  }

  column {
    name = "SellStartDate"
    type = "DATE"
  }

  column {
    name = "Size"
    type = "NUMBER(38,0)"
  }

  column {
    name = "SizeUnitMeasureCode"
    type = "VARCHAR(16)"
  }

  column {
    name = "StandardCost"
    type = "NUMBER(38,0)"
  }

  column {
    name = "Style"
    type = "VARCHAR"
  }

  column {
    name = "Weight"
    type = "NUMBER(38,0)"
  }

  column {
    name = "WeightUnitMeasureCode"
    type = "VARCHAR(16)"
  }
}

// -- -- Table PRODUCTCATEGORY
resource "snowflake_table" "product_category" {
  database = snowflake_schema.production.database
  schema   = snowflake_schema.production.name
  name     = "PRODUCTCATEGORY"
  comment  = "High-level product categorization."

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "Name"
    type = "VARCHAR"
  }

  column {
    name = "ProductCategoryID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ROWGUID"
    type = "NUMBER(38,0)"
  }
}

// -- -- Table PRODUCTCOSTHISTORY
resource "snowflake_table" "product_cost_history" {
  database = snowflake_schema.production.database
  schema   = snowflake_schema.production.name
  name     = "PRODUCTCOSTHISTORY"

  column {
    name = "EndDate"
    type = "DATE"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "ProductID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "StandardCost"
    type = "NUMBER(38,0)"
  }

  column {
    name = "StartDate"
    type = "DATE"
  }
}

// -- -- Table PRODUCTDESCRIPTION
resource "snowflake_table" "product_description" {
  database = snowflake_schema.production.database
  schema   = snowflake_schema.production.name
  name     = "PRODUCTDESCRIPTION"
  comment  = "Product descriptions in several languages."

  column {
    name = "Description"
    type = "VARCHAR"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "ProductDescriptionID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ROWGUID"
    type = "NUMBER(38,0)"
  }
}

// -- -- Table PRODUCTDOCUMENT
resource "snowflake_table" "product_document" {
  database = snowflake_schema.production.database
  schema   = snowflake_schema.production.name
  name     = "PRODUCTDOCUMENT"
  comment  = "Cross-reference table mapping products to related product documents."

  column {
    name = "DocumentNode"
    type = "VARCHAR"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "ProductID"
    type = "NUMBER(38,0)"
  }
}

// -- -- Table PRODUCTINVENTORY
resource "snowflake_table" "product_inventory" {
  database = snowflake_schema.production.database
  schema   = snowflake_schema.production.name
  name     = "PRODUCTINVENTORY"
  comment  = "Product inventory information."

  column {
    name = "Bin"
    type = "VARCHAR"
  }

  column {
    name = "LocationID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "ProductID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "Quantity"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ROWGUID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "Shelf"
    type = "VARCHAR"
  }
}

// -- -- Table PRODUCTLISTPRICEHISTORY
resource "snowflake_table" "product_list_price_history" {
  database = snowflake_schema.production.database
  schema   = snowflake_schema.production.name
  name     = "PRODUCTLISTPRICEHISTORY"
  comment  = "Changes in the list price of a product over time."

  column {
    name = "EndDate"
    type = "DATE"
  }

  column {
    name = "ListPrice"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "ProductID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "StartDate"
    type = "DATE"
  }
}

// -- -- Table PRODUCTMODEL
resource "snowflake_table" "product_model" {
  database = snowflake_schema.production.database
  schema   = snowflake_schema.production.name
  name     = "PRODUCTMODEL"
  comment  = "Product model classification."

  column {
    name = "CatalogDescription"
    type = "VARCHAR"
  }

  column {
    name = "Instructions"
    type = "VARCHAR"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "Name"
    type = "VARCHAR"
  }

  column {
    name = "ProductModelID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ROWGUID"
    type = "NUMBER(38,0)"
  }
}

// -- -- Table PRODUCTMODELILLUS
resource "snowflake_table" "product_model_illus" {
  database = snowflake_schema.production.database
  schema   = snowflake_schema.production.name
  name     = "PRODUCTMODELILLUS"
  comment  = "Cross-reference table mapping product models and illustrations."

  column {
    name = "IllustrationID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "ProductModelID"
    type = "NUMBER(38,0)"
  }
}

// -- -- Table PRODUCTMODELPROD
resource "snowflake_table" "product_model_prod" {
  database = snowflake_schema.production.database
  schema   = snowflake_schema.production.name
  name     = "PRODUCTMODELPROD"
  comment  = "Cross-reference table mapping product descriptions and the language the description is written in."

  column {
    name = "CultureID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "ProductDescriptionID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ProductModelID"
    type = "NUMBER(38,0)"
  }
}

// -- -- Table PRODUCTPHOTO
resource "snowflake_table" "product_photo" {
  database = snowflake_schema.production.database
  schema   = snowflake_schema.production.name
  name     = "PRODUCTPHOTO"
  comment  = "Product images."

  column {
    name = "LargePhoto"
    type = "VARCHAR"
  }

  column {
    name = "LargePhotoFileName"
    type = "VARCHAR"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "ProductPhotoID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ThumbNailPhoto"
    type = "VARCHAR"
  }

  column {
    name = "ThumbnailPhotoFileName"
    type = "VARCHAR"
  }
}

// -- -- Table PRODUCTPRODUCTPH
resource "snowflake_table" "product_product_photo" {
  database = snowflake_schema.production.database
  schema   = snowflake_schema.production.name
  name     = "PRODUCTPRODUCTPH"
  comment  = "Cross-reference table mapping products and product photos."

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "Primary"
    type = "BOOLEAN"
  }

  column {
    name = "ProductID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ProductPhotoID"
    type = "NUMBER(38,0)"
  }
}

// -- -- Table PRODUCTREVIEW
resource "snowflake_table" "product_review" {
  database = snowflake_schema.production.database
  schema   = snowflake_schema.production.name
  name     = "PRODUCTREVIEW"
  comment  = "Customer reviews of products they have purchased."

  column {
    name = "Comments"
    type = "VARCHAR"
  }

  column {
    name = "EmailAddress"
    type = "VARCHAR"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "ProductID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ProductReviewID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "Rating"
    type = "NUMBER(4,0)"
  }

  column {
    name = "ReviewDate"
    type = "DATE"
  }

  column {
    name = "ReviewerName"
    type = "VARCHAR"
  }
}

// -- -- Table PRODUCTSUBCATEG
resource "snowflake_table" "product_sub_category" {
  database = snowflake_schema.production.database
  schema   = snowflake_schema.production.name
  name     = "PRODUCTSUBCATEG"
  comment  = "Product sub-categories."

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "Name"
    type = "VARCHAR"
  }

  column {
    name = "ProductCategoryID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ProductSubcategoryID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ROWGUID"
    type = "NUMBER(38,0)"
  }

}

// -- -- Table SCRAPREASON
resource "snowflake_table" "scrap_reason" {
  database = snowflake_schema.production.database
  schema   = snowflake_schema.production.name
  name     = "SCRAPREASON"
  comment  = "Manufacturing failure reasons lookup table."

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "Name"
    type = "VARCHAR"
  }

  column {
    name = "ScrapReasonID"
    type = "NUMBER(38,0)"
  }
}

// -- -- Table TRANSACTIONHISTORY
resource "snowflake_table" "transaction_history" {
  database = snowflake_schema.production.database
  schema   = snowflake_schema.production.name
  name     = "TRANSACTIONHISTORY"
  comment  = "Record of each purchase order, sales order, or work order transaction year to date."

  column {
    name = "ActualCost"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "ProductID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "Quantity"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ReferenceOrderID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ReferenceOrderLineID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "TransactionDate"
    type = "DATE"
  }

  column {
    name = "TransactionID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "TransactionType"
    type = "VARCHAR"
  }
}

// -- -- Table TRANSACTIONHISTORYARCHIVE
resource "snowflake_table" "transaction_history_archive" {
  database = snowflake_schema.production.database
  schema   = snowflake_schema.production.name
  name     = "TRANSACTIONHISTORYARCHIVE"
  comment  = "Transactions for previous years."

  column {
    name = "ActualCost"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "ProductID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "Quantity"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ReferenceOrderID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ReferenceOrderLineID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "TransactionDate"
    type = "DATE"
  }

  column {
    name = "TransactionID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "TransactionType"
    type = "VARCHAR"
  }
}

// -- -- Table UNITMEASURE
resource "snowflake_table" "unit_measure" {
  database = snowflake_schema.production.database
  schema   = snowflake_schema.production.name
  name     = "UNITMEASURE"
  comment  = "Unit of measure lookup table."

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "Name"
    type = "VARCHAR"
  }

  column {
    name = "UnitMeasureCode"
    type = "VARCHAR(16)"
  }
}

// -- -- Table WORKORDER
resource "snowflake_table" "work_order" {
  database = snowflake_schema.production.database
  schema   = snowflake_schema.production.name
  name     = "WORKORDER"
  comment  = "Manufacturing work orders."

  column {
    name = "DueDate"
    type = "DATE"
  }

  column {
    name = "EndDate"
    type = "DATE"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "OrderQty"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ProductID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ScrappedQty"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ScrapReasonID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "StartDate"
    type = "DATE"
  }

  column {
    name = "StockedQty"
    type = "NUMBER(38,0)"
  }

  column {
    name = "WorkOrderID"
    type = "NUMBER(38,0)"
  }
}

// -- -- Table WORKORDERROUTING
resource "snowflake_table" "work_order_routing" {
  database = snowflake_schema.production.database
  schema   = snowflake_schema.production.name
  name     = "WORKORDERROUTING"
  comment  = "Work order details."

  column {
    name = "ActualCost"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ActualEndDate"
    type = "DATE"
  }

  column {
    name = "ActualResourceHrs"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ActualStartDate"
    type = "DATE"
  }

  column {
    name = "LocationID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "OperationSequence"
    type = "NUMBER(38,0)"
  }

  column {
    name = "PlannedCost"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ProductID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ScheduledEndDate"
    type = "DATE"
  }

  column {
    name = "ScheduledStartDate"
    type = "DATE"
  }

  column {
    name = "WorkOrderID"
    type = "NUMBER(38,0)"
  }
}

// -- Schema PURCHASING
resource "snowflake_schema" "purchasing" {
  database = snowflake_schema.production.database
  name     = "PURCHASING"
}

// -- -- Table PRODUCTVENDOR
resource "snowflake_table" "product_vendor" {
  database = snowflake_schema.purchasing.database
  schema   = snowflake_schema.purchasing.name
  name     = "PRODUCTVENDOR"
  comment  = "Cross-reference table mapping vendors with the products they supply."

  column {
    name = "AverageLeadTime"
    type = "NUMBER(38,0)"
  }

  column {
    name = "BusinessEntityID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "LastReceiptCost"
    type = "NUMBER(38,0)"
  }

  column {
    name = "LastReceiptDate"
    type = "DATE"
  }

  column {
    name = "MaxOrderQty"
    type = "NUMBER(38,0)"
  }

  column {
    name = "MinOrderQty"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "OnOrderQty"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ProductID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "StandardPrice"
    type = "NUMBER(38,0)"
  }

  column {
    name = "UnitMeasureCode"
    type = "VARCHAR(16)"
  }
}

// -- -- Table PURCHASEORDERDET
resource "snowflake_table" "purchase_order_det" {
  database = snowflake_schema.purchasing.database
  schema   = snowflake_schema.purchasing.name
  name     = "PURCHASEORDERDET"
  comment  = "Individual products associated with a specific purchase order. See PurchaseOrderHeader."

  column {
    name = "DueDate"
    type = "DATE"
  }

  column {
    name = "LineTotal"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "OrderQty"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ProductID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "PurchaseOrderDetailID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "PurchaseOrderID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ReceivedQty"
    type = "NUMBER(38,0)"
  }

  column {
    name = "RejectedQty"
    type = "NUMBER(38,0)"
  }

  column {
    name = "StockedQty"
    type = "NUMBER(38,0)"
  }

  column {
    name = "UnitPrice"
    type = "NUMBER(38,0)"
  }
}

// -- -- Table PURCHASEORDERHEADER
resource "snowflake_table" "purchase_order_hea" {
  database = snowflake_schema.purchasing.database
  schema   = snowflake_schema.purchasing.name
  name     = "PURCHASEORDERHEADER"
  comment  = "General purchase order information. See PurchaseOrderDetail."

  column {
    name = "EmployeeID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "Freight"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "OrderDate"
    type = "DATE"
  }

  column {
    name = "PurchaseOrderID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "RevisionNumber"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ShipDate"
    type = "DATE"
  }

  column {
    name = "ShipMethodID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "Status"
    type = "VARCHAR"
  }

  column {
    name = "SubTotal"
    type = "NUMBER(38,0)"
  }

  column {
    name = "TaxAmt"
    type = "NUMBER(38,0)"
  }

  column {
    name = "TotalDue"
    type = "NUMBER(38,0)"
  }

  column {
    name = "VendorID"
    type = "NUMBER(38,0)"
  }
}

// -- -- Table SHIPMETHOD
resource "snowflake_table" "ship_method" {
  database = snowflake_schema.purchasing.database
  schema   = snowflake_schema.purchasing.name
  name     = "SHIPMETHOD"
  comment  = "Shipping company lookup table."

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "Name"
    type = "VARCHAR"
  }

  column {
    name = "ROWGUID"
    type = "VARCHAR"
  }

  column {
    name = "ShipBase"
    type = "VARCHAR"
  }

  column {
    name = "ShipMethodID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ShipRate"
    type = "NUMBER(38,0)"
  }
}

// -- -- Table VENDOR
resource "snowflake_table" "vendor" {
  database = snowflake_schema.purchasing.database
  schema   = snowflake_schema.purchasing.name
  name     = "VENDOR"
  comment  = "Companies from whom Adventure Works Cycles purchases parts or other goods."

  column {
    name = "AccountNumber"
    type = "VARCHAR"
  }

  column {
    name = "ActiveFlag"
    type = "BOOLEAN"
  }

  column {
    name = "BusinessEntityID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "CreditRating"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "Name"
    type = "VARCHAR"
  }

  column {
    name = "PreferredVendorStatus"
    type = "VARCHAR"
  }

  column {
    name = "PurchasingWebServiceURL"
    type = "VARCHAR"
  }
}

// -- Schema SALES
resource "snowflake_schema" "sales" {
  database = snowflake_schema.production.database
  name     = "SALES"
}

resource "snowflake_tag" "sales_sensitivity" {
  database       = snowflake_schema.sales.database
  schema         = snowflake_schema.sales.name
  name           = "SENSITIVITY"
  allowed_values = ["PCI", "PII"]
}

resource "snowflake_tag_association" "sales_sensitivity" {
  object_identifiers = [snowflake_schema.sales.fully_qualified_name]

  object_type = "SCHEMA"
  tag_id      = "${snowflake_tag.sales_sensitivity.database}.${snowflake_tag.sales_sensitivity.schema}.${snowflake_tag.sales_sensitivity.name}"
  tag_value   = "PII"
}

// -- -- Table COUNTRYREGIONCURRENCY
resource "snowflake_table" "country_region_currency" {
  database = snowflake_schema.sales.database
  schema   = snowflake_schema.sales.name
  name     = "COUNTRYREGIONCURRENCY"
  comment  = "Cross-reference table mapping ISO currency codes to a country or region."

  column {
    name = "CountryRegionCode"
    type = "VARCHAR(16)"
  }

  column {
    name = "CurrencyCode"
    type = "VARCHAR(16)"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }
}

// -- -- Table CREDITCARD
resource "snowflake_table" "credit_card" {
  database = snowflake_schema.sales.database
  schema   = snowflake_schema.sales.name
  name     = "CREDITCARD"
  comment  = "Customer credit card information."

  column {
    name = "CardNumber"
    type = "VARCHAR"
  }

  column {
    name = "CardType"
    type = "VARCHAR"
  }

  column {
    name = "CreditCardID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ExpMonth"
    type = "NUMBER(12,0)"
  }

  column {
    name = "ExpYear"
    type = "NUMBER(12,0)"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }
}

// -- -- Table CURRENCY
resource "snowflake_table" "currency" {
  database = snowflake_schema.sales.database
  schema   = snowflake_schema.sales.name
  name     = "CURRENCY"
  comment  = "Lookup table containing standard ISO currencies."

  column {
    name = "CurrencyCode"
    type = "VARCHAR(16)"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "Name"
    type = "VARCHAR"
  }
}

// -- -- Table CURRENCYRATE
resource "snowflake_table" "currency_rate" {
  database = snowflake_schema.sales.database
  schema   = snowflake_schema.sales.name
  name     = "CURRENCYRATE"
  comment  = "Currency exchange rates."

  column {
    name = "AverageRate"
    type = "NUMBER(38,0)"
  }

  column {
    name = "CurrencyRateDate"
    type = "DATE"
  }

  column {
    name = "CurrencyRateID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "EndOfDayRate"
    type = "NUMBER(38,0)"
  }

  column {
    name = "FromCurrencyCode"
    type = "VARCHAR(16)"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "ToCurrencyCode"
    type = "VARCHAR(16)"
  }
}

// -- -- Table CUSTOMER
resource "snowflake_table" "customer" {
  database = snowflake_schema.sales.database
  schema   = snowflake_schema.sales.name
  name     = "CUSTOMER"
  comment  = "Current customer information. Also see the Person and Store tables."

  column {
    name = "AccountNumber"
    type = "VARCHAR"
  }

  column {
    name = "CustomerID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "PersonID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ROWGUID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "StoreID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "TerritoryID"
    type = "NUMBER(38,0)"
  }
}

// -- -- View CUSTOMER_EU
resource "snowflake_materialized_view" "customer_eu" {
  database  = snowflake_schema.sales.database
  schema    = snowflake_schema.sales.name
  name      = "CUSTOMER_EU"
  warehouse = var.snowflake_warehouse
  statement = "SELECT * FROM ${snowflake_table.customer.fully_qualified_name} WHERE \"TerritoryID\" between 1 and 200"
}

// -- -- Table PERSONCREDITCARD
resource "snowflake_table" "person_creditcard" {
  database = snowflake_schema.sales.database
  schema   = snowflake_schema.sales.name
  name     = "PERSONCREDITCARD"
  comment  = "Cross-reference table mapping people to their credit card information in the CreditCard table."

  column {
    name = "BusinessEntityID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "CreditCardID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }
}

resource "snowflake_tag_association" "preson_creditcard_sensitivity" {
  object_identifiers = ["${snowflake_table.person_creditcard.fully_qualified_name}.CreditCardID"]

  object_type = "COLUMN"
  tag_id      = snowflake_tag.sales_sensitivity.id
  tag_value   = "PCI"
}

// -- -- Table SALESORDERDETAIL
resource "snowflake_table" "sales_order_detail" {
  database = snowflake_schema.sales.database
  schema   = snowflake_schema.sales.name
  name     = "SALESORDERDETAIL"
  comment  = "Individual products associated with a specific sales order. See SalesOrderHeader."

  column {
    name = "CarrierTrackingNumber"
    type = "VARCHAR"
  }

  column {
    name = "LineTotal"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "OrderQty"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ProductID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ROWGUID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "SalesOrderDetailID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "SalesOrderID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "SpecialOfferID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "UnitPrice"
    type = "NUMBER(38,0)"
  }

  column {
    name = "UnitPriceDiscount"
    type = "NUMBER(38,0)"
  }
}

// -- -- Table SALESORDERHEADER
resource "snowflake_table" "sales_order_header" {
  database = snowflake_schema.sales.database
  schema   = snowflake_schema.sales.name
  name     = "SALESORDERHEADER"
  comment  = "General sales order information."



  column {
    name = "AccountNumber"
    type = "VARCHAR"
  }

  column {
    name = "BillToAddressID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "Comment"
    type = "VARCHAR"
  }

  column {
    name = "CreditCardApprovalCode"
    type = "VARCHAR(16)"
  }

  column {
    name = "CreditCardID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "CurrencyRateID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "CustomerID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "DueDate"
    type = "DATE"
  }

  column {
    name = "Freight"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "OnlineOrderFlag"
    type = "BOOLEAN"
  }

  column {
    name = "OrderDate"
    type = "DATE"
  }

  column {
    name = "PurchaseOrderNumber"
    type = "VARCHAR"
  }

  column {
    name = "RevisionNumber"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ROWGUID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "SalesOrderID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "SalesOrderNumber"
    type = "VARCHAR"
  }

  column {
    name = "SalesPersonID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ShipDate"
    type = "DATE"
  }

  column {
    name = "ShipMethodID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ShipToAddressID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "Status"
    type = "VARCHAR"
  }

  column {
    name = "SubTotal"
    type = "NUMBER(38,0)"
  }

  column {
    name = "TaxAmt"
    type = "NUMBER(38,0)"
  }

  column {
    name = "TerritoryID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "TotalDue"
    type = "NUMBER(38,0)"
  }
}

// -- -- Table SALESORDERHEADERSALESREASON
resource "snowflake_table" "sales_order_header_sales_reason" {
  database = snowflake_schema.sales.database
  schema   = snowflake_schema.sales.name
  name     = "SALESORDERHEADERSALESREASON"
  comment  = "Cross-reference table mapping sales orders to sales reason codes."

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "SalesOrderID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "SalesReasonID"
    type = "NUMBER(38,0)"
  }
}

// -- -- Table SALESPERSON
resource "snowflake_table" "sales_person" {
  database = snowflake_schema.sales.database
  schema   = snowflake_schema.sales.name
  name     = "SALESPERSON"
  comment  = "Sales representative current information."

  column {
    name = "Bonus"
    type = "NUMBER(38,0)"
  }

  column {
    name = "BusinessEntityID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "CommissionPct"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "NAME"
    type = "VARCHAR"
  }

  column {
    name = "ROWGUID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "SalesLastYear"
    type = "NUMBER(38,0)"
  }

  column {
    name = "SalesQuota"
    type = "NUMBER(38,0)"
  }

  column {
    name = "SalesYTD"
    type = "NUMBER(38,0)"
  }

  column {
    name = "TerritoryID"
    type = "NUMBER(38,0)"
  }
}

// -- -- Table SALESPERSONQUOTAHISTORY
resource "snowflake_table" "sales_person_quota_history" {
  database = snowflake_schema.sales.database
  schema   = snowflake_schema.sales.name
  name     = "SALESPERSONQUOTAHISTORY"
  comment  = "Sales performance tracking."

  column {
    name = "BusinessEntityID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "QuotaDate"
    type = "DATE"
  }

  column {
    name = "ROWGUID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "SalesQuota"
    type = "NUMBER(38,0)"
  }
}

// -- -- Table SALESREASON
resource "snowflake_table" "sales_reason" {
  database = snowflake_schema.sales.database
  schema   = snowflake_schema.sales.name
  name     = "SALESREASON"
  comment  = "Lookup table of customer purchase reasons."

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "Name"
    type = "VARCHAR"
  }

  column {
    name = "ReasonType"
    type = "VARCHAR"
  }

  column {
    name = "SalesReasonID"
    type = "NUMBER(38,0)"
  }
}

// -- -- Table SALESTAXRATE
resource "snowflake_table" "sales_tax_rate" {
  database = snowflake_schema.sales.database
  schema   = snowflake_schema.sales.name
  name     = "SALESTAXRATE"
  comment  = "Tax rate lookup table."

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "Name"
    type = "VARCHAR"
  }

  column {
    name = "ROWGUID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "SalesTaxRateID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "StateProvinceID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "TaxRate"
    type = "NUMBER(38,0)"
  }

  column {
    name = "TaxType"
    type = "VARCHAR"
  }
}

// -- -- Table SALESTERRITORY
resource "snowflake_table" "sales_territory" {
  database = snowflake_schema.sales.database
  schema   = snowflake_schema.sales.name
  name     = "SALESTERRITORY"
  comment  = "Sales territory lookup table."

  column {
    name = "CostLastYear"
    type = "NUMBER(38,0)"
  }

  column {
    name = "CostYTD"
    type = "NUMBER(38,0)"
  }

  column {
    name = "CountryRegionCode"
    type = "VARCHAR(16)"
  }

  column {
    name = "Group"
    type = "VARCHAR"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "Name"
    type = "VARCHAR"
  }

  column {
    name = "ROWGUID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "SalesLastYear"
    type = "NUMBER(38,0)"
  }

  column {
    name = "SalesYTD"
    type = "NUMBER(38,0)"
  }

  column {
    name = "TerritoryID"
    type = "NUMBER(38,0)"
  }
}

// -- -- Table SALESTERRITORYHISTORY
resource "snowflake_table" "sales_territory_history" {
  database = snowflake_schema.sales.database
  schema   = snowflake_schema.sales.name
  name     = "SALESTERRITORYHISTORY"
  comment  = "Sales representative transfers to other sales territories."

  column {
    name = "BusinessEntityID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "EndDate"
    type = "DATE"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "ROWGUID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "StartDate"
    type = "DATE"
  }

  column {
    name = "TerritoryID"
    type = "NUMBER(38,0)"
  }
}

// -- -- Table SHOPPINGCARTITEM
resource "snowflake_table" "shopping_cart_item" {
  database = snowflake_schema.sales.database
  schema   = snowflake_schema.sales.name
  name     = "SHOPPINGCARTITEM"
  comment  = "Contains online customer orders until the order is submitted or cancelled."

  column {
    name = "DateCreated"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "ProductID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "Quantity"
    type = "NUMBER(38,0)"
  }


  column {
    name = "ShoppingCartID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ShoppingCartItemID"
    type = "NUMBER(38,0)"
  }
}

// -- -- Table SPECIALOFFER
resource "snowflake_table" "special_offer" {
  database = snowflake_schema.sales.database
  schema   = snowflake_schema.sales.name
  name     = "SPECIALOFFER"
  comment  = "Sale discounts lookup table."

  column {
    name = "Category"
    type = "VARCHAR"
  }

  column {
    name = "Description"
    type = "VARCHAR"
  }

  column {
    name = "DiscountPct"
    type = "NUMBER(38,0)"
  }

  column {
    name = "EndDate"
    type = "DATE"
  }

  column {
    name = "MaxQty"
    type = "NUMBER(38,0)"
  }

  column {
    name = "MinQty"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "ROWGUID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "SpecialOfferID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "StartDate"
    type = "DATE"
  }

  column {
    name = "Type"
    type = "VARCHAR"
  }
}

// -- -- Table SPECIALOFFERPRODUCT
resource "snowflake_table" "special_offer_product" {
  database = snowflake_schema.sales.database
  schema   = snowflake_schema.sales.name
  name     = "SPECIALOFFERPRODUCT"
  comment  = "Cross-reference table mapping products to special offer discounts."

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "ProductID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ROWGUID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "SpecialOfferID"
    type = "NUMBER(38,0)"
  }
}

// -- -- Table STORE
resource "snowflake_table" "store" {
  database = snowflake_schema.sales.database
  schema   = snowflake_schema.sales.name
  name     = "STORE"
  comment  = "Customers (resellers) of Adventure Works products."

  column {
    name = "BusinessEntityID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "Demographics"
    type = "VARCHAR"
  }

  column {
    name = "ModifiedDate"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "Name"
    type = "VARCHAR"
  }

  column {
    name = "ROWGUID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "SalesPersonID"
    type = "NUMBER(38,0)"
  }
}
