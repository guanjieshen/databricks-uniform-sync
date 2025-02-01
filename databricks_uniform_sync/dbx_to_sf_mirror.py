from typing import List
from databricks_unity_catalog.logic_uc_mapping import UCMappingLogic
from databricks_unity_catalog.logic_uc_tags import UCTagsLogic
from data_models.data_models import (
    Catalog,
    SnowflakeExtVolDTO,
    SnowflakeIcebergTableConfig,
)
from metadata_mapping.metadata_mapping_logic import MetadataMappingLogic
from pyspark.sql import SparkSession, DataFrame
from databricks_uniform_sync.dbx_to_sf_helpers import DatabricksToSnowflakeHelpers
import logging

# Configure the logging system
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class DatabricksToSnowflakeMirror:
    def __init__(
        self,
        spark_session: SparkSession,
        dbx_workspace_url: str,
        dbx_workspace_pat: str,
        metadata_catalog: str,
        metadata_schema: str,
        metadata_table: str = "dbx_sf_uniform_metadata",
    ):
        self.dbx_workspace_url: str = dbx_workspace_url
        self.dbx_workspace_pat: str = dbx_workspace_pat
        self.spark_session: SparkSession = spark_session
        self.metadata_catalog: str = metadata_catalog
        self.metadata_schema: str = metadata_schema
        self.metadata_table: str = metadata_table

        # Instantiate all of the required repository and logic classes
        self.metadata_mapping_logic: MetadataMappingLogic = MetadataMappingLogic(
            spark_session=self.spark_session,
            catalog=self.metadata_catalog,
            schema=self.metadata_schema,
            table=self.metadata_table,
        )
        self.uc_mapping_logic: UCMappingLogic = UCMappingLogic(
            spark_session=spark_session,
            workspace_url=self.dbx_workspace_url,
            bearer_token=self.dbx_workspace_pat,
        )
        self.uc_tags_logic: UCTagsLogic = UCTagsLogic(
            spark_session=spark_session,
            workspace_url=dbx_workspace_url,
            bearer_token=dbx_workspace_pat,
        )
        self.dbx_to_sf_helpers: DatabricksToSnowflakeHelpers = (
            DatabricksToSnowflakeHelpers(
                spark_session=self.spark_session,
                metadata_catalog=self.metadata_catalog,
                metadata_schema=self.metadata_schema,
                metadata_table=self.metadata_table,
            )
        )

    def create_metadata_tables(self):
        # Create metadata tables
        self.metadata_mapping_logic.create_metadata_tables()

    def refresh_uc_metadata(self, catalog, schema=None, table=None):
        # Ensure the metadata table is created
        self.create_metadata_tables()

        # Get the catalog hierarchy
        catalog: Catalog = self.uc_mapping_logic.build_hierarchy_for_catalog(
            catalog_name=catalog, schemas_include=schema, include_empty_schemas=False
        )

        # Refresh the metadata table
        self.metadata_mapping_logic.refresh_metadata_table(catalog=catalog)
        logging.info(f"Metadata refresh completed for catalog: {catalog}")

    def refresh_uc_metadata_tags(self):
        # Get the metadata table
        metadata_table: DataFrame = self.metadata_mapping_logic.get_metadata_view()

        # Convert metadata table to a list and filter out tables that already have metadata tags
        # TODO: Move this into a helper class
        metadata_table_results = (
            metadata_table.filter(
                (metadata_table.snowflake_database.isNull())
                & (metadata_table.snowflake_schema.isNull())
                & (metadata_table.snowflake_table.isNull())
                & (metadata_table.snowflake_uniform_sync.isNull())
            ).select(
                metadata_table.uc_catalog_name,
                metadata_table.uc_schema_name,
                metadata_table.uc_table_name,
                metadata_table.dbx_sf_uniform_metadata_id,
            )
        ).collect()

        # Add tags to the tables
        for table in metadata_table_results:
            try:
                self.uc_tags_logic.add_uc_metadata_tags(
                    table.uc_catalog_name, table.uc_schema_name, table.uc_table_name
                )
                logging.info(
                    f"Adding UC discovery tags to table: {table.uc_catalog_name}.{table.uc_schema_name}.{table.uc_table_name}"
                )
            except Exception as e:
                print(f"Error adding tags to table: {e}")

    def sf_create_external_volumes(
        self, generate_sql_only: bool = True, azure_tenant_id: str = None
    ):
        if generate_sql_only:
            external_locations: List[SnowflakeExtVolDTO] = (
                self.dbx_to_sf_helpers.fetch_uc_storage_locations(
                    tenant_id=azure_tenant_id
                )
            )
            return self.dbx_to_sf_helpers.create_sf_external_volume_ddls(
                external_locations
            )

    def sf_create_catalog_integrations(
        self,
        oauth_client_id: str,
        oauth_client_secret: str,
        generate_sql_only: bool = True,
        refresh_interval_seconds: str = 3600,
    ):
        catalog_integrations: List[SnowflakeIcebergTableConfig] = (
            self.dbx_to_sf_helpers.fetch_uc_catalog_integration(
                uc_endpoint=self.dbx_workspace_url,
                refresh_interval_seconds=refresh_interval_seconds,
                oauth_client_id=oauth_client_id,
                oauth_client_secret=oauth_client_secret,
            )
        )
        return self.dbx_to_sf_helpers.create_sf_cat_int_ddls(
            catalog_integrations
        )
        # Get uc metadata for catalog integrations.
        pass
