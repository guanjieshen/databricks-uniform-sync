from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    CatalogInfo,
    SchemaInfo,
    TableInfo,
    EffectivePermissionsList,
)
import re
from databricks.sdk.service import catalog
from pyspark.sql import SparkSession


class UnityCatalogRepository:
    def __init__(self, spark_session: SparkSession, workspace_url, bearer_token):
        self.workspace_url = workspace_url
        self.bearer_token = bearer_token
        self.spark_session = spark_session

        self.workspace_client = WorkspaceClient(token=bearer_token, host=workspace_url)

    def get_catalog(self, catalog_name: str) -> CatalogInfo:
        catalog: CatalogInfo = self.workspace_client.catalogs.get(catalog_name)

        # Extract the ID from the catalog storage location
        # Regular expression to extract the UUID (36 characters)
        uuid_pattern = r"([a-f0-9-]{36})"
        try:
            # Find all matches of the pattern in the URL
            matches = re.findall(uuid_pattern, catalog.storage_location)

            # Print the first match if it exists
            if matches:
                catalog_id = matches[0]
                catalog.id = catalog_id
            else:
                catalog_id = "No valid UUID found"

            return catalog
        except Exception as e:
            print(f"Error extracting catalog ID: {catalog}")

    def get_schema(self, catalog_name: str, schema_name: str) -> SchemaInfo:
        schema: SchemaInfo = self.workspace_client.schemas.get(
            f"{catalog_name}.{schema_name}"
        )
        return schema

    def get_table(
        self, catalog_name: str, schema_name: str, table_name: str
    ) -> TableInfo:
        table: TableInfo = self.workspace_client.tables.get(
            f"{catalog_name}.{schema_name}.{table_name}"
        )
        return table

    def get_grants_effective(
        self, catalog_name: str, schema_name: str, table_name: str
    ) -> EffectivePermissionsList:
        return self.workspace_client.grants.get_effective(
            securable_type=catalog.SecurableType.TABLE,
            full_name=f"{catalog_name}.{schema_name}.{table_name}",
        )

    def set_tags(self, catalog_name: str, schema_name: str, table_name: str):
        self.spark_session.sql(
            f"""
            ALTER TABLE
            {catalog_name}.{schema_name}.{table_name}
            SET
            TAGS (
                'snowflake_database' = '{catalog_name}',
                'snowflake_schema' = '{schema_name}',
                'snowflake_table' = '{table_name}',
                'snowflake_external_volume= '',
                'snowflake_uniform_sync' = 'true'
            );
            """
        )
