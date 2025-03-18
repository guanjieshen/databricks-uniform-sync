from typing import List, Optional
from pydantic import BaseModel, Field

# ----------------------------------------
# Project Data Models
# ----------------------------------------

class Table(BaseModel):
    """
    Represents a table with metadata information.
    """
    uc_id: str           # Unity Catalog identifier for the table.
    uc_name: str         # Unity Catalog name for the table.
    sf_name: str         # Snowflake name for the table.
    location: str        # Physical or logical location of the table.
    table_type: str      # Type/category of the table (e.g., fact, dimension).

class Schema(BaseModel):
    """
    Represents a database schema containing multiple tables.
    """
    uc_id: str           # Unity Catalog identifier for the schema.
    uc_name: str         # Unity Catalog name for the schema.
    sf_name: str         # Snowflake name for the schema.
    tables: List[Table]  # List of tables contained within the schema.

class Catalog(BaseModel):
    """
    Represents a catalog that groups together various schemas.
    """
    uc_id: str           # Unity Catalog identifier for the catalog.
    uc_name: str         # Unity Catalog name for the catalog.
    sf_name: str         # Snowflake name for the catalog.
    schemas: List[Schema]  # List of schemas within this catalog.

class SnowflakeIcebergTableConfig(BaseModel):
    """
    Configuration settings for an Iceberg table within Snowflake.
    """
    sf_database_name: str          # Snowflake database name.
    sf_schema_name: str            # Snowflake schema name.
    sf_table_name: str             # Snowflake table name.
    sf_external_volume: str        # Identifier for the Snowflake external volume.
    sf_catalog_integration_name: str  # Name of the Snowflake catalog integration.
    db_table_name: str             # Database table name (could be used as an alias).

# ----------------------------------------
# Iceberg Catalog Models
# ----------------------------------------

class TableIdentifier(BaseModel):
    """
    Represents the identifier for a table in an Iceberg catalog.
    """
    namespace: List[str]  # List representing the namespace components.
    name: str             # The actual name of the table.

class UnityCatalogIcebergTables(BaseModel):
    """
    Represents a paginated response containing Iceberg table identifiers from Unity Catalog.
    """
    identifiers: List[TableIdentifier]  # List of table identifiers.
    next_page_token: Optional[str] = Field(
        None, alias="next-page-token"
    )  # Token for fetching the next page of results, if available.

class UnityCatalogIcebergSchema(BaseModel):
    """
    Represents a paginated response containing namespaces (schemas) from an Iceberg catalog.
    """
    namespaces: List[List[str]]  # A list where each element is a list of strings representing a namespace.
    next_page_token: Optional[str] = Field(
        None, alias="next-page-token"
    )  # Token for fetching the next page of namespaces, if available.

class SnowflakeCatIntlDTO(BaseModel):
    """
    Data Transfer Object (DTO) for Snowflake catalog integrations.
    """
    catalog_integration_name: str          # Name of the external volume.
    uc_catalog_name: str  # Storage name associated with the external volume.
    uc_schema_name: str                  # Account name related to the external volume.
    uc_endpoint: str                # Container name used in the external volume.
    refresh_interval_seconds: int                     # Identifier for the tenant.
    oauth_client_id: str                # Container name used in the external volume.
    oauth_client_secret: str                     # Identifier for the tenant.

class SnowflakeIcebergTableDTO(BaseModel):
    """
    Data Transfer Object (DTO) for Snowflake Iceberg table.
    """
    catalog_integration_name: str          # Name of the external volume.
    uc_table_name: str  # Storage name associated with the external volume.
    snowflake_database: str                     # Identifier for the tenant.
    snowflake_schema: str                # Container name used in the external volume.
    snowflake_table: str                     # Identifier for the tenant.
    auto_refresh: bool                     # Should the table automatically refresh