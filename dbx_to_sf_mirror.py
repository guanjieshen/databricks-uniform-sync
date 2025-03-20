from typing import List, Optional
from logic.databricks.logic_uc_mapping import UCMappingLogic
from logic.databricks.logic_uc_tags import UCTagsLogic
from data_models.data_models import (
    SnowflakeCatIntlDTO,
)
from logic.metadata.metadata_mapping_logic import MetadataMappingLogic
from pyspark.sql import SparkSession
from utils.dbx_to_sf_helpers import DatabricksToSnowflakeHelpers
from config.logging_config import setup_logging

# Initialize logging
setup_logging()
import logging
logger = logging.getLogger('dbx_to_sf_mirror')

class DatabricksToSnowflakeMirror:
    """
    Class to handle synchronization between Databricks Unity Catalog and Snowflake.

    This class handles metadata retrieval, tagging, and SQL generation for creating
    Snowflake catalogs and tables based on Unity Catalog data.
    """

    def __init__(
        self,
        spark_session: SparkSession,
        dbx_workspace_url: str,
        dbx_workspace_pat: str,
        metadata_catalog: str,
        metadata_schema: str,
        metadata_table: str = "dbx_sf_uniform_metadata",
    ):
        """
        Initialize the DatabricksToSnowflakeMirror class.

        Args:
            spark_session (SparkSession): The active Spark session.
            dbx_workspace_url (str): The Databricks workspace URL.
            dbx_workspace_pat (str): The personal access token for Databricks.
            metadata_catalog (str): The catalog where metadata is stored.
            metadata_schema (str): The schema where metadata is stored.
            metadata_table (str): The table where metadata is stored. Defaults to "dbx_sf_uniform_metadata".
        """
        self.spark_session = spark_session
        self.dbx_workspace_url = dbx_workspace_url
        self.dbx_workspace_pat = dbx_workspace_pat
        self.metadata_catalog = metadata_catalog
        self.metadata_schema = metadata_schema
        self.metadata_table = metadata_table

        # Instantiate required repository and logic classes for handling data flow
        self.metadata_mapping_logic = MetadataMappingLogic(
            spark_session, metadata_catalog, metadata_schema, metadata_table
        )
        self.uc_mapping_logic = UCMappingLogic(
            spark_session, dbx_workspace_url, dbx_workspace_pat
        )
        self.uc_tags_logic = UCTagsLogic(
            spark_session, dbx_workspace_url, dbx_workspace_pat
        )
        self.dbx_to_sf_helpers = DatabricksToSnowflakeHelpers(
            spark_session, metadata_catalog, metadata_schema, metadata_table
        )

    def create_metadata_tables(self) -> None:
        """
        Create metadata tables if they do not exist.

        This method ensures that the metadata tables required for syncing
        between Databricks and Snowflake are available.
        """
        self.metadata_mapping_logic.create_metadata_tables()
        logging.info("Metadata tables have been created or already exist.")

    def refresh_uc_metadata(
        self, catalog: str, schema: Optional[str] = None, table: Optional[str] = None
    ) -> None:
        """
        Refresh Unity Catalog metadata and store it in the metadata table.

        Args:
            catalog (str): The catalog name to refresh.
            schema (Optional[str]): The schema name to refresh. If None, all schemas are refreshed.
            table (Optional[str]): The table name to refresh. If None, all tables are refreshed.
        """
        # Ensure metadata table is created before refreshing
        self.create_metadata_tables()

        logging.info(
            f"Refreshing metadata for catalog: {catalog}, schema: {schema}, table: {table}"
        )

        # Build the catalog hierarchy from Unity Catalog
        catalog_data = self.uc_mapping_logic.build_hierarchy_for_catalog(
            catalog_name=catalog, schemas_include=schema, include_empty_schemas=False
        )

        # Write catalog data into the metadata table
        self.metadata_mapping_logic.refresh_metadata_table(catalog=catalog_data)
        logging.info(f"Metadata refresh completed for catalog: {catalog}")

    def refresh_uc_metadata_tags(self) -> None:
        """
        Refresh metadata tags in Unity Catalog.

        This method retrieves records from the metadata table that have missing
        Snowflake mappings and updates them with appropriate metadata tags.
        """
        logging.info("Refreshing metadata tags in Unity Catalog...")

        # Get metadata records that lack Snowflake mappings
        metadata_table = self.metadata_mapping_logic.get_metadata_view()
        metadata_table_results = (
            metadata_table.filter(
                metadata_table.snowflake_database.isNull()
                & metadata_table.snowflake_schema.isNull()
                & metadata_table.snowflake_table.isNull()
                & metadata_table.snowflake_uniform_sync.isNull()
            )
            .select(
                metadata_table.uc_catalog_name,
                metadata_table.uc_schema_name,
                metadata_table.uc_table_name,
                metadata_table.dbx_sf_uniform_metadata_id,
            )
            .collect()
        )

        # Apply metadata tags to each table
        for table in metadata_table_results:
            try:
                self.uc_tags_logic.add_uc_metadata_tags(
                    table.uc_catalog_name, table.uc_schema_name, table.uc_table_name
                )
                logging.info(
                    f"Added UC discovery tags to table: {table.uc_catalog_name}.{table.uc_schema_name}.{table.uc_table_name}"
                )
            except Exception as e:
                logging.error(
                    f"Failed to add tags to table {table.uc_catalog_name}.{table.uc_schema_name}.{table.uc_table_name}: {e}"
                )

    def generate_sf_create_catalog_integrations_sql(
        self,
        oauth_client_id: str,
        oauth_client_secret: str,
        refresh_interval_seconds: int = 3600,
    ) -> List[str]:
        """
        Generate SQL statements for creating Snowflake catalog integrations.

        Args:
            oauth_client_id (str): OAuth client ID for Snowflake.
            oauth_client_secret (str): OAuth client secret for Snowflake.
            refresh_interval_seconds (int): Interval for refreshing the integration. Defaults to 3600 seconds.

        Returns:
            List[str]: A list of SQL statements to create catalog integrations.
        """
        logging.info("Generating SQL for creating Snowflake catalog integrations...")

        # Fetch catalog integration details from Databricks
        catalog_integrations: List[SnowflakeCatIntlDTO] = (
            self.dbx_to_sf_helpers.fetch_uc_catalog_integration(
                uc_endpoint=self.dbx_workspace_url,
                refresh_interval_seconds=refresh_interval_seconds,
                oauth_client_id=oauth_client_id,
                oauth_client_secret=oauth_client_secret,
            )
        )

        # Generate SQL for catalog integration creation
        sql_statements = self.dbx_to_sf_helpers.create_sf_cat_int_ddls(
            catalog_integrations
        )
        logging.info("Generated catalog integration SQL statements.")
        return sql_statements

    def create_sf_create_catalog_integrations(
        self,
        sf_account_id: str,
        sf_user: str,
        sf_private_key_file: str,
        sf_private_key_file_pwd: str,
        dbx_oauth_client_id: str,
        dbx_oauth_client_secret: str,
        refresh_interval_seconds: int = 3600,
    ):
        logging.info("Creating Snowflake catalog integrations...")

        # TODO: Test the connection to the IRC using the credentials provided.

        # Fetch catalog integration details from Databricks
        catalog_integrations: List[SnowflakeCatIntlDTO] = (
            self.dbx_to_sf_helpers.fetch_uc_catalog_integration(
                uc_endpoint=self.dbx_workspace_url,
                refresh_interval_seconds=refresh_interval_seconds,
                oauth_client_id=dbx_oauth_client_id,
                oauth_client_secret=dbx_oauth_client_secret,
            )
        )

        # Generate SQL for catalog integration creation
        sql_statements = self.dbx_to_sf_helpers.create_sf_cat_int(
            sf_account_id=sf_account_id,
            sf_user=sf_user,
            sf_private_key_file=sf_private_key_file,
            sf_private_key_file_pwd=sf_private_key_file_pwd,
            sf_cat_int_dtos=catalog_integrations,
        )
        logging.info("Creating Snowflake catalog integrations completed.")
        return sql_statements

    def generate_sf_create_tables_sql(self, auto_refresh: bool = True) -> List[str]:
        """
        Generate SQL statements for creating Snowflake tables.

        Args:
            auto_refresh (bool): Whether to automatically refresh tables. Defaults to True.

        Returns:
            List[str]: A list of SQL statements to create Snowflake tables.
        """
        logging.info("Generating SQL for creating Snowflake tables...")

        # Fetch Unity Catalog tables for Snowflake
        tables = self.dbx_to_sf_helpers.fetch_uc_tables(auto_refresh)

        # Generate SQL for table creation
        sql_statements = self.dbx_to_sf_helpers.create_sf_table_ddls(tables)
        logging.info("Generated Snowflake table creation SQL statements.")
        return sql_statements
