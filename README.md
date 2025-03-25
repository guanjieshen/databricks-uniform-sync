# **Databricks to Snowflake Table Mirroring**

This repository provides a library to **synchronize (mirror) Iceberg table metadata** from **Databricks Unity Catalog** to **Snowflake Horizon**. It automates the creation of Snowflake catalog integrations and external Iceberg tables based on the metadata stored in Unity Catalog.

_This library leverages credential vending support to access underlying storage. Snowflake External Volumes are not required to enable access to the underlying storage locations._

---

## **Table of Contents**
1. [Overview](#overview)  
2. [Snowflake Setup](#databricks-setup)  
3. [Databricks Setup](#databricks-setup)  
4. [How to Use](#how-to-use)   
5. [Example Usage](#example-usage)  
6. [Configuration](#configuration) 
7. [Limitations](#Limitations)  
8. [Troubleshooting](#troubleshooting)  

---

## **Overview**
This utility performs the following tasks:

✅ Retrieves iceberg metadata from Databricks Unity Catalog  
✅ Generates metadata tables within Databricks for mapping
✅ Automatically creates catalog integrations in Snowflake  
✅ Automatically creates iceberg tables in Snowflake


---

## **Snowflake Setup**

This library can be used to generate DDL that a user can manually run wihtin Snowflake, or it automatically create those assets from Databricks.

For automated usage of this library, a Snowflake service account is required in order to use this library. This library will be performing the following actions within Snowflake:

- Creating Catalog Integrations
- Creating External Iceberg Tables





---

## **Databricks Setup**

Install the libary using pip:

`pip install databricks_uniform_sync`


Initialize an instance of the `DatabricksToSnowflakeMirror` class. Ensure the `metadata_catalog` and `metadata_schema` exists prior to running the following code:

```python
import DatabricksToSnowflakeMirror

### Required inputs

spark_session = spark #Local spark session within Databricks
dbx_workspace_url = "dbcxyz.databricks.cloud.net" #Workspace URL to use for the mirroring
dbx_workspace_pat = "dapi..." #Databricks PAT,
metadata_catalog = "dbx_sf_mirror_catalog", #UC catalog to store the metadata.
metadata_schema = "dbx_sf_mirror_schema", #UC schema to store the metadata.

### Instantiate the DatabricksToSnowflakeMirror class
d2s = DatabricksToSnowflakeMirror(
    spark_session,
    dbx_workspace_url,
    dbx_workspace_pat,
    metadata_catalog,
    metadata_schema
)
```

---

## **How to Use**

##### 1. Create or Refresh Metadata Tables

`create_metadata_tables()` will automatically create a Delta table and view within the UC hierarhcy specified when intializing `DatabricksToSnowflakeMirror`.

```python
d2s.create_metadata_tables()
```
`refresh_metadata_tables()` will automatically search for Iceberg tables within the specified catalog location and add them to the metadata tables. 

```python
d2s.refresh_metadata_tables(catalog = "example_catalog")
```

If the metadata tables are not created, then `refresh_metadata_tables()` will also create the metadata tables.

_These methods are idempotent, and will not resulted in duplicates when rerun._

##### 2. Add UC Discovery Tags
`refresh_uc_metadata_tags()` will automatically add pre-defined tags to tables identified by `refresh_metadata_tables`. By setting the values within these tags, they can be used to enable/disable syncornization to Snowflake.

```python
d2s.refresh_uc_metadata_tags()
```
_Please do not remove these tags, as they are used to map the tables into Snowflake._

##### 3. Create Snowflake Catalog Integrations
`generate_create_sf_catalog_integrations_sql()` will only print the SQL DDLs for the Catalog Ingegrations.

`create_sf_catalog_integrations_sql()` will automatically create the catalog integrations within Snowflake.

```python 

### Required Inputs
sf_account_id = "xyz-123"  # Snowflake Account ID,
sf_user: "svc_name"  # Snowflake Service User,
sf_private_key_file = "rsa/rsa_key.p8"  # Snowflake Private Certificate
sf_private_key_file_pwd = ""  # Snowflake Private Certificate Password
oauth_client_id: "svc_name"  # Databricks SP Client ID,
oauth_client_secret: "svc_name"  # Databricks SP Client Secret,

# This will print out the SQL DDL to create the Catalog Integrations.
# Use this as a dry run to validate the DDLs.
d2s.generate_create_sf_catalog_integrations_sql(
    oauth_client_id,
    oauth_client_secret,
)

# This will run the DDLs automatically within Snowflake.
d2s.create_sf_catalog_integrations(
    sf_account_id,
    sf_user,
    sf_private_key_file,
    sf_private_key_file_pwd,
    oauth_client_id,
    oauth_client_secret,
)

```
_The Catalog Integration name is automatically generated by this library as hash of the UC catalog and schema names. Tables within a given UC schema will reuse the same Snowflake Catalog Integration_

##### 4. Create Snowflake Iceberg Tables
`generate_create_sf_iceberg_tables_sql()` will only print the SQL DDLs for the Iceberg Tables.

`create_sf_iceberg_tables_sql()` will automatically create the Iceberg tables within Snowflake.

```python 

### Required Inputs
sf_account_id = "xyz-123"  # Snowflake Account ID,
sf_user: "svc_name"  # Snowflake Service User,
sf_private_key_file = "rsa/rsa_key.p8"  # Snowflake Private Certificate
sf_private_key_file_pwd = ""  # Snowflake Private Certificate Password


# This will print out the SQL DDL to create the Iceberg Tables Integrations.
# Use this as a dry run to validate the DDLs.
d2s.generate_create_sf_iceberg_tables_sql()

# This will run the table creation DDLs automatically within Snowflake.
d2s.create_sf_iceberg_tables_sql(
    sf_account_id,
    sf_user,
    sf_private_key_file,
    sf_private_key_file_pwd
)

```

_It's possible to reuse the same Snowflake credentials used to create Catalog Integrations to also create the Iceberg tables._

---


## **Configuration**

There are a few optional parameters for more advanced configuration.

##### Custom Metadata Table Name

This allows a custom table name to be used for the metadata assets. The view creation is automatic and suffixes the table name with `_vw`.

```python

### Instantiate the DatabricksToSnowflakeMirror class
d2s = DatabricksToSnowflakeMirror(
    spark_session,
    dbx_workspace_url,
    dbx_workspace_pat,
    metadata_catalog,
    metadata_schema,
    "custom_table_name"
    
)
```

##### Set Catalog Integration Refresh Interval
This allows a custom refresh interval to be used for the Catalog Integration. The default is 3600 seconds.

```python

d2s.generate_create_sf_catalog_integrations_sql(
    oauth_client_id,
    oauth_client_secret,
    refresh_interval_seconds = 120
)

d2s.create_sf_catalog_integrations(
    sf_account_id,
    sf_user,
    sf_private_key_file,
    sf_private_key_file_pwd,
    oauth_client_id,
    oauth_client_secret,
    refresh_interval_seconds = 120
)
```

##### Set Iceberg Table Auto Refresh
This allows the ablility to default auto-refresh for Iceberg Tables. The default is true.

```python

d2s.generate_create_sf_iceberg_tables_sql(auto_refresh = False)

d2s.create_sf_iceberg_tables_sql(
    sf_account_id,
    sf_user,
    sf_private_key_file,
    sf_private_key_file_pwd,
    auto_refresh = False
)
)
```




---
## **Example Usage**


---

## **Limitations**

- Only supports Iceberg tables within S3. Pending Snowflake adding credential vending support for other cloud storage options. ADLS is not supported.
- Does not support table drops i.e. deleting a table in Unity Catalog, will not remove the table in Snowflake. 
- RSA Certficate is the only supported way to authenticate to Snowflake. (this is due to changes in Snowflake to enforce MFA for all logins.)