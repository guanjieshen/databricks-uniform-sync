from repository.databricks.repository_iceberg_catalog import (
    IcebergCatalogRepository,
)
from repository.databricks.repository_unity_catalog import UnityCatalogRepository

from data_models.data_models import (
    Catalog,
    Schema,
    Table,
    UnityCatalogIcebergSchema,
    UnityCatalogIcebergTables,
)
from typing import List
from databricks.sdk.service.catalog import CatalogInfo, SchemaInfo, TableInfo
from pyspark.sql import SparkSession

# Systems Schemas to exclude
excluded_schemas = ["information_schema"]


class UCMappingLogic:
    def __init__(self, spark_session: SparkSession, workspace_url, bearer_token):
        self.workspace_url = workspace_url
        self.bearer_token = bearer_token

        self.iceberg_catalog_repo = IcebergCatalogRepository(
            workspace_url, bearer_token
        )
        self.unity_catalog_repo: UnityCatalogRepository = UnityCatalogRepository(
            spark_session, workspace_url, bearer_token
        )

    def build_hierarchy_for_catalog(
        self,
        catalog_name: str,
        schemas_include: List[str] = None,
        include_empty_schemas=True,
    ) -> Catalog:
        # Get the schemas
        schemas_object: UnityCatalogIcebergSchema = (
            self.iceberg_catalog_repo.list_schemas(catalog_name=catalog_name)
        )

        schemas: List[List[str]] = schemas_object.namespaces

        # Get the catalog info
        catalog_info: CatalogInfo = self.unity_catalog_repo.get_catalog(catalog_name)

        # Build the catalog object
        catalog: Catalog = Catalog(
            uc_id=catalog_info.id,
            uc_name=catalog_name,
            sf_name=catalog_name,
            schemas=[],
        )

        if schemas_include is None:
            # Loop through the schemas and build the hierarchy
            data_schemas: List[Schema] = []
            for schema in schemas:
                if schema[0] not in excluded_schemas:
                    data_schema: Schema = self.build_hierarchy_for_schema(
                        catalog_name=catalog_name, schema_name=schema[0]
                    )

                    if len(data_schema.tables) != 0 or include_empty_schemas:

                        data_schemas.append(data_schema)

            catalog.schemas = data_schemas
        else:
            for schema in schemas_include:
                data_schema: Schema = self.build_hierarchy_for_schema(
                    catalog_name=catalog_name, schema_name=schema
                )

                if len(data_schema.tables) != 0 or include_empty_schemas:
                    catalog.schemas.append(data_schema)

        return catalog

    def build_hierarchy_for_schema(self, catalog_name: str, schema_name: str) -> Schema:
        # Get the Schema info
        schemaInfo: SchemaInfo = self.unity_catalog_repo.get_schema(
            catalog_name=catalog_name, schema_name=schema_name
        )

        # Get the tables for the schema
        tables: UnityCatalogIcebergTables = self.iceberg_catalog_repo.list_tables(
            catalog_name=catalog_name, schema_name=schema_name
        )

        # Build the schema object
        data_schema: Schema = Schema(
            uc_id=schemaInfo.schema_id,
            uc_name=schema_name,
            sf_name=schema_name,
            tables=[],
        )

        if len(tables.identifiers) != 0:
            # Loop through the tables and add them to the data schema
            for table in tables.identifiers:
                # Get the table info
                table_info: TableInfo = self.unity_catalog_repo.get_table(
                    catalog_name=catalog_name,
                    schema_name=schema_name,
                    table_name=table.name,
                )

                # Add the table to the schema
                data_schema.tables.append(
                    Table(
                        uc_id=table_info.table_id,
                        uc_name=table.name,
                        sf_name=table.name,
                        location=table_info.storage_location,
                        table_type=table_info.table_type,
                    )
                )
        return data_schema
