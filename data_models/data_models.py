from typing import List, Optional
from pydantic import BaseModel, Field


## Project Data Mdoels
class Table(BaseModel):
    uc_id: str
    uc_name: str
    sf_name: str
    location: str


class Schema(BaseModel):
    uc_id: str
    uc_name: str
    sf_name: str
    tables: List[Table]


class Catalog(BaseModel):
    uc_id: str
    uc_name: str
    sf_name: str
    schemas: List[Schema]

class SnowflakeIcebergTableConfig(BaseModel):
    sf_database_name: str
    sf_schema_name: str
    sf_table_name: str
    sf_external_volume: str
    sf_catalog_integration_name: str
    db_table_name: str

### Iceberg Catalog Models


class TableIdentifier(BaseModel):
    namespace: List[str]
    name: str


class UnityCatalogIcebergTables(BaseModel):
    identifiers: List[TableIdentifier]
    next_page_token: Optional[str] = Field(None, alias="next-page-token")


class UnityCatalogIcebergSchema(BaseModel):
    namespaces: List[List[str]]  # A list of lists of strings
    next_page_token: Optional[str] = Field(None, alias="next-page-token")
