from typing import List, Optional
from logic.databricks.logic_uc_mapping import UCMappingLogic
from logic.databricks.logic_uc_tags import UCTagsLogic
from data_models.data_models import (
    SnowflakeCatIntlDTO,
    SnowflakeIcebergTableDTO,
)
from logic.metadata.metadata_mapping_logic import MetadataMappingLogic
from pyspark.sql import SparkSession
from utils.dbx_to_sf_helpers import DatabricksToSnowflakeHelpers
from config.logging_config import setup_logging

# Initialize logging
setup_logging()
import logging

logger = logging.getLogger("dbx_to_sf_mirror")


class DatabricksToSnowflakeMirror:
    """
    Synchronizes Databricks Unity Catalog with Snowflake.

    Handles metadata retrieval, tagging, and SQL generation
    for Snowflake catalog and table creation.
    """

    def __init__(
        self,
        spark_session: SparkSession,
        dbx_workspace_url: str,
        dbx_workspace_pat: str,
        metadata_catalog: str,
        metadata_schema: str,
        metadata_table: str = "dbx_sf_uniform_metadata",
    ) -> None:
        """
        Initialize DatabricksToSnowflakeMirror.

        Args:
            spark_session: Active Spark session.
            dbx_workspace_url: Databricks workspace URL.
            dbx_workspace_pat: Personal access token for Databricks.
            metadata_catalog: Catalog where metadata is stored.
            metadata_schema: Schema where metadata is stored.
            metadata_table: Table where metadata is stored (default: "dbx_sf_uniform_metadata").
        """
        self.spark_session = spark_session
        self.dbx_workspace_url = dbx_workspace_url
        self.dbx_workspace_pat = dbx_workspace_pat

        self.metadata_mapping_logic = MetadataMappingLogic(
            spark_session, metadata_catalog, metadata_schema, metadata_table
        )
        self.uc_mapping_logic = UCMappingLogic(
            spark_session, dbx_workspace_url, dbx_workspace_pat
        )
        self.uc_tags_logic = UCTagsLogic(
            spark_session, dbx_workspace_url, dbx_workspace_pat
        )
        self.helpers = DatabricksToSnowflakeHelpers(
            spark_session, metadata_catalog, metadata_schema, metadata_table
        )

    def create_metadata_tables(self) -> None:
        """
        Create metadata tables if they don't exist.

        Ensures that the metadata tables required for syncing between
        Databricks and Snowflake are available.
        """
        self.metadata_mapping_logic.create_metadata_tables()
        logger.info("Metadata tables created or already exist.")

    def refresh_uc_metadata(
        self, catalog: str, schema: Optional[str] = None, table: Optional[str] = None
    ) -> None:
        """
        Refresh Unity Catalog metadata and store in metadata table.

        Args:
            catalog: Name of the catalog to refresh.
            schema: Name of the schema to refresh (if None, refreshes all schemas).
            table: Name of the table to refresh (if None, refreshes all tables).
        """
        self.create_metadata_tables()
        logger.info(
            f"Refreshing metadata for catalog={catalog}, schema={schema}, table={table}"
        )

        catalog_data = self.uc_mapping_logic.build_hierarchy_for_catalog(
            catalog_name=catalog,
            schemas_include=schema,
            include_empty_schemas=False,
        )

        self.metadata_mapping_logic.refresh_metadata_table(catalog=catalog_data)
        logger.info(f"Metadata refresh completed for catalog={catalog}")

    def refresh_uc_metadata_tags(self) -> None:
        """
        Update missing Snowflake mappings with metadata tags.

        This method retrieves records from the metadata table that lack
        Snowflake mappings and updates them with metadata tags.
        """
        logger.info("Refreshing Unity Catalog metadata tags...")

        metadata_results = (
            self.metadata_mapping_logic.get_metadata_view()
            .filter(
                "snowflake_database IS NULL AND snowflake_schema IS NULL "
                "AND snowflake_table IS NULL AND snowflake_uniform_sync IS NULL"
            )
            .select(
                "uc_catalog_name",
                "uc_schema_name",
                "uc_table_name",
                "dbx_sf_uniform_metadata_id",
            )
            .collect()
        )

        for record in metadata_results:
            try:
                self.uc_tags_logic.add_uc_metadata_tags(
                    record.uc_catalog_name, record.uc_schema_name, record.uc_table_name
                )
                logger.info(
                    f"Added tags to {record.uc_catalog_name}.{record.uc_schema_name}.{record.uc_table_name}"
                )
            except Exception as e:
                logger.error(
                    f"Failed to add tags to {record.uc_catalog_name}.{record.uc_schema_name}.{record.uc_table_name}: {e}"
                )

    def generate_create_sf_catalog_integrations_sql(
        self,
        oauth_client_id: str,
        oauth_client_secret: str,
        refresh_interval_seconds: int = 3600,
    ) -> List[str]:
        """
        Generate SQL for creating Snowflake catalog integrations.

        Args:
            oauth_client_id: OAuth client ID for Snowflake.
            oauth_client_secret: OAuth client secret for Snowflake.
            refresh_interval_seconds: Refresh interval in seconds (default: 3600).

        Returns:
            List[str]: List of generated SQL statements.
        """
        logger.info("Generating catalog integration SQL...")

        catalog_integrations: List[SnowflakeCatIntlDTO] = (
            self.helpers.fetch_uc_catalog_integration(
                uc_endpoint=self.dbx_workspace_url,
                refresh_interval_seconds=refresh_interval_seconds,
                oauth_client_id=oauth_client_id,
                oauth_client_secret=oauth_client_secret,
            )
        )
        return self.helpers.create_sf_cat_int_ddls(catalog_integrations)

    def create_sf_catalog_integrations(
        self,
        sf_account_id: str,
        sf_user: str,
        sf_private_key_file: str,
        sf_private_key_file_pwd: str,
        oauth_client_id: str,
        oauth_client_secret: str,
        refresh_interval_seconds: int = 3600,
    ) -> None:
        """
        Create catalog integrations in Snowflake.

        Args:
            sf_account_id: Snowflake account ID.
            sf_user: Snowflake user.
            sf_private_key_file: Path to private key file.
            sf_private_key_file_pwd: Password for private key file.
            oauth_client_id: OAuth client ID.
            oauth_client_secret: OAuth client secret.
            refresh_interval_seconds: Refresh interval in seconds (default: 3600).
        """
        logger.info("Creating Snowflake catalog integrations...")

        catalog_integrations: List[SnowflakeCatIntlDTO] = (
            self.helpers.fetch_uc_catalog_integration(
                uc_endpoint=self.dbx_workspace_url,
                refresh_interval_seconds=refresh_interval_seconds,
                oauth_client_id=oauth_client_id,
                oauth_client_secret=oauth_client_secret,
            )
        )

        self.helpers.create_sf_cat_int(
            sf_account_id,
            sf_user,
            sf_private_key_file,
            sf_private_key_file_pwd,
            catalog_integrations,
        )
        logger.info("All Catalog Integrations created...")

    def generate_create_sf_iceberg_tables_sql(self, auto_refresh: bool = True) -> List[str]:
        """
        Generate SQL for creating Snowflake tables.

        Args:
            auto_refresh: Enable table auto-refresh (default: True).

        Returns:
            List[str]: List of generated SQL statements.
        """
        logger.info("Generating table creation SQL...")
        tables = self.helpers.fetch_uc_tables(auto_refresh)
        return self.helpers.create_sf_table_ddls(tables)

    def create_sf_iceberg_tables(
        self,
        sf_account_id: str,
        sf_user: str,
        sf_private_key_file: str,
        sf_private_key_file_pwd: str,
        auto_refresh: bool = True,
    ) -> None:
        """
        Create Iceberg tables in Snowflake.

        Args:
            sf_account_id: Snowflake account ID.
            sf_user: Snowflake user.
            sf_private_key_file: Path to private key file.
            sf_private_key_file_pwd: Password for private key file.
            auto_refresh: Enable table auto-refresh (default: True).
        """
        logger.info("Creating Snowflake Iceberg tables...")
        tables = self.helpers.fetch_uc_tables(auto_refresh)
        self.helpers.create_sf_tables(
            sf_account_id,
            sf_user,
            sf_private_key_file,
            sf_private_key_file_pwd,
            tables,
        )
        logger.info("Iceberg table   creation process completed.")
        #TODO: Update metadata table with updated dates
