#  Databricks to Snowflake Table Mirroring

This repository provides a utility to **synchronize (mirror) Iceberg table metadata** from **Databricks Unity Catalog** to **Snowflake Horizon**. It automates the creation of **Snowflake Catalog Integrations** and **External Iceberg Tables**, based on metadata managed within Unity Catalog.

> ⚠️ **Note:** This library uses **credential vending**, so there's no need to configure Snowflake External Volumes for storage access.

---

## Table of Contents

1. [Overview](#overview)  
2. [Snowflake Setup](#snowflake-setup)  
3. [Databricks Setup](#databricks-setup)  
4. [How to Use](#how-to-use)  
5. [Example Usage](#example-usage)  
6. [Configuration](#configuration-options)  
7. [Limitations](#limitations)  

---

## Overview

This tool automates the following:

- ✅ Retrieves Iceberg table metadata from Unity Catalog  
- ✅ Manages Delta-based metadata tables in Databricks  
- ✅ Creates Snowflake Catalog Integrations  
- ✅ Creates External Iceberg Tables in Snowflake  

---

## ❄️ **Snowflake Setup**

This tool supports two usage patterns:

- **Manual:** Generate SQL DDLs for execution in Snowflake  
- **Automated:** Create Snowflake objects directly from Databricks  

For automated, configure a **Snowflake service account** with the appropriate privileges to perform the following:

- Catalog Integration creation  
- External Iceberg Table creation  

---

## Databricks Setup

Install the package:

```bash
pip install databricks_uniform_sync
```

Initialize the class (ensure the specified catalog & schema exist in Unity Catalog):

```python
from databricks_uniform_sync import DatabricksToSnowflakeMirror

d2s = DatabricksToSnowflakeMirror(
    spark_session=spark,
    dbx_workspace_url="https://dbcxyz.databricks.cloud.net",
    dbx_workspace_pat="dapi...",  # Personal Access Token
    metadata_catalog="dbx_sf_mirror_catalog",
    metadata_schema="dbx_sf_mirror_schema"
)
```

---

## How to Use

### 1. Create or Refresh Metadata Tables

```python
d2s.create_metadata_tables()
d2s.refresh_metadata_tables(catalog="your_catalog")
```

> These methods are **idempotent** — re-running them will not create duplicates.

---

### 2. Add Unity Catalog Discovery Tags

```python
d2s.refresh_uc_metadata_tags()
```

> **Important:** These internal tags are required for sync. Do **not remove** them. This method is also **idempotent**.

---

### 3. Create Snowflake Catalog Integrations

**Dry run (generate SQL):**

```python
d2s.generate_create_sf_catalog_integrations_sql(
    oauth_client_id="client-id",
    oauth_client_secret="client-secret"
)
```

**Execute directly:**

```python
d2s.create_sf_catalog_integrations(
    sf_account_id="xyz-123",
    sf_user="svc_name",
    sf_private_key_file="rsa/rsa_key.p8",
    sf_private_key_file_pwd="your-password",
    oauth_client_id="client-id",
    oauth_client_secret="client-secret"
)
```

> Integration names are auto-generated using a hash of the Unity Catalog & schema.

---

### 4. Create Iceberg Tables in Snowflake

**Dry run (generate SQL):**

```python
d2s.generate_create_sf_iceberg_tables_sql()
```

**Execute directly:**

```python
d2s.create_sf_iceberg_tables_sql(
    sf_account_id="xyz-123",
    sf_user="svc_name",
    sf_private_key_file="rsa/rsa_key.p8",
    sf_private_key_file_pwd="your-password"
)
```

> 🔐 Credentials can be reused from the Catalog Integration step.

---

## Configuration Options

### Custom Metadata Table Name

```python
d2s = DatabricksToSnowflakeMirror(
    spark,
    dbx_workspace_url,
    dbx_workspace_pat,
    metadata_catalog,
    metadata_schema,
    metadata_table_name="custom_metadata_table"
)
```

> A corresponding view will be auto-created with a `_vw` suffix.

---

### Custom Refresh Interval (Catalog Integration)

```python
d2s.generate_create_sf_catalog_integrations_sql(
    oauth_client_id,
    oauth_client_secret,
    refresh_interval_seconds=120
)

d2s.create_sf_catalog_integrations(
    sf_account_id,
    sf_user,
    sf_private_key_file,
    sf_private_key_file_pwd,
    oauth_client_id,
    oauth_client_secret,
    refresh_interval_seconds=120
)
```

---

### Disable Auto-Refresh on Iceberg Tables

```python
d2s.generate_create_sf_iceberg_tables_sql(auto_refresh=False)

d2s.create_sf_iceberg_tables_sql(
    sf_account_id,
    sf_user,
    sf_private_key_file,
    sf_private_key_file_pwd,
    auto_refresh=False
)
```

---

## Example Usage

_Coming soon._  
Add a sample script, demo notebook, or walkthrough here.

---

## **Limitations**

- ❌ Only supports **Iceberg tables on S3**  
  (Support for other storage types like ADLS depends on Snowflake updates)  
- ❌ **Deletes are not mirrored** — Dropping tables in Unity Catalog does **not** delete them in Snowflake  
- ❌ Only supports **RSA key pair authentication** — User/password logins aren't supported due to MFA  

---
