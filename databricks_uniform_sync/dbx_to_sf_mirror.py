from databricks_unity_catalog.logic_uc_mapping import UCMappingLogic
from databricks_unity_catalog.logic_uc_tags import UCTagsLogic
from data_models.data_models import Catalog
from metadata_mapping.metadata_mapping_logic import MetadataMappingLogic
from pyspark.sql import SparkSession, DataFrame
import logging

# Configure the logging system
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

        # Create an instance of the MappingLogic class and YamlLogic class
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
        logging.info("Metadata refresh completed.")

    def refresh_uc_metadata_tags(self):
        # Get the metadata table
        metadata_table: DataFrame = self.metadata_mapping_logic.get_metadata_view()

        # Convert metadata table to a list and filter out tables that already have metadata tags
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
                print(f"Metadata Tags to Table: {table.uc_catalog_name}.{table.uc_schema_name}.{table.uc_table_name}")
            except Exception as e:
                print(f"Error adding tags to table: {e}")

    def sf_create_external_volumes(dry_run: bool = False):
        pass

    def sf_create_catalog_integrations(
        self, refresh_interval: int = 120, workspace_url=None, dry_run: bool = False
    ):
        workspace_url: str = workspace_url or self.dbx_workspace_url
        #TODO: This code should be moved into the logic class.
        # catalog_url: str = f"{workspace_url}/api/2.1/unity-catalog/iceberg"

        # Get the metadata table

        # Create catalog integrations.
        pass

    def sync_catalogs(dry_run: bool = False):
        pass
