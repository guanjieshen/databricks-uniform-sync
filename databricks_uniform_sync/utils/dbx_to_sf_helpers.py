from typing import List

from pyspark.sql import Row, SparkSession

from databricks_uniform_sync.data_models.data_models import (
    SnowflakeCatIntlDTO,
    SnowflakeIcebergTableDTO,
)

from databricks_uniform_sync.logic.metadata.metadata_mapping_logic import MetadataMappingLogic
from databricks_uniform_sync.logic.snowflake.logic_snowflake_catalog_integration import SnowflakeCatalogIntegrationLogic
from databricks_uniform_sync.logic.snowflake.logic_snowflake_database import SnowflakeDatabaseLogic
from databricks_uniform_sync.logic.snowflake.logic_snowflake_schema import SnowflakeSchemaLogic
from databricks_uniform_sync.logic.snowflake.logic_snowflake_table import SnowflakeTableLogic

from databricks_uniform_sync.repository.snowflake.repository_snowflake import SnowflakeRepository
from databricks_uniform_sync.repository.snowflake.repository_snowflake_database import SnowflakeDatabaseRepository
from databricks_uniform_sync.repository.snowflake.repository_snowflake_schema import SnowflakeSchemaRepository



class DatabricksToSnowflakeHelpers:
    """
    Helper class for integrating Databricks with Snowflake.
    """

    def __init__(
        self,
        spark_session: SparkSession,
        metadata_catalog: str,
        metadata_schema: str,
        metadata_table: str,
    ):
        """
        Initializes the helper class.

        Args:
            spark_session (SparkSession): The active Spark session.
            metadata_catalog (str): The catalog name where metadata is stored.
            metadata_schema (str): The schema name within the catalog.
            metadata_table (str): The table name within the schema.
        """
        self.spark_session = spark_session
        self.metadata_mapping_logic = MetadataMappingLogic(
            spark_session=spark_session,
            catalog=metadata_catalog,
            schema=metadata_schema,
            table=metadata_table,
        )
        self.catalog_integration_logic = SnowflakeCatalogIntegrationLogic()
        self.table_logic = SnowflakeTableLogic()
        self.database_logic = SnowflakeDatabaseLogic()
        self.schema_logic = SnowflakeSchemaLogic()

    def _initialize_snowflake_repository(
        self,
        account_id: str,
        user: str,
        private_key_file: str,
        private_key_file_pwd: str,
    ) -> SnowflakeRepository:
        """
        Initializes and returns a Snowflake repository instance.

        Args:
            account_id (str): Snowflake account ID.
            user (str): Snowflake user.
            private_key_file (str): Path to the private key file.
            private_key_file_pwd (str): Password for the private key file.

        Returns:
            SnowflakeRepository: Initialized Snowflake repository instance.
        """

        return SnowflakeRepository(
            account_id=account_id,
            user=user,
            private_key_file=private_key_file,
            private_key_file_pwd=private_key_file_pwd,
        )

    def fetch_uc_catalog_integration(
        self,
        uc_endpoint: str,
        refresh_interval_seconds: int,
        oauth_client_id: str,
        oauth_client_secret: str,
    ) -> List[SnowflakeCatIntlDTO]:
        """
        Retrieves catalog integration metadata from Unity Catalog.

        Args:
            uc_endpoint (str): Unity Catalog endpoint.
            refresh_interval_seconds (int): Refresh interval in seconds.
            oauth_client_id (str): OAuth client ID.
            oauth_client_secret (str): OAuth client secret.

        Returns:
            List[SnowflakeCatIntlDTO]: List of catalog integration DTOs.
        """
        metadata_rows: List[Row] = (
            self.metadata_mapping_logic.get_metadata_sf_catalog_integration()
        )
        return [
            SnowflakeCatIntlDTO(
                catalog_integration_name=row["snowflake_catalog_integration"],
                uc_catalog_name=row["uc_catalog_name"],
                uc_schema_name=row["uc_schema_name"],
                uc_endpoint=uc_endpoint,
                refresh_interval_seconds=refresh_interval_seconds,
                oauth_client_id=oauth_client_id,
                oauth_client_secret=oauth_client_secret,
            )
            for row in metadata_rows
        ]

    def fetch_uc_tables(
        self,
        auto_refresh: bool,
    ) -> List[SnowflakeIcebergTableDTO]:
        """
        Retrieves table metadata from Unity Catalog.

        Args:
            auto_refresh (bool): Whether to enable auto-refresh.

        Returns:
            List[SnowflakeIcebergTableDTO]: List of table DTOs.
        """
        metadata_rows: List[Row] = self.metadata_mapping_logic.get_metadata_tables()
        return [
            SnowflakeIcebergTableDTO(
                dbx_sf_uniform_metadata_id=row["dbx_sf_uniform_metadata_id"],
                catalog_integration_name=row["snowflake_catalog_integration"],
                uc_table_name=row["uc_table_name"],
                snowflake_database=row["snowflake_database"],
                snowflake_schema=row["snowflake_schema"],
                snowflake_table=row["snowflake_table"],
                auto_refresh=auto_refresh,
            )
            for row in metadata_rows
        ]

    def create_sf_cat_int_ddls(
        self, sf_cat_int_dtos: List[SnowflakeCatIntlDTO]
    ) -> List[str]:
        """
        Generates DDL statements for creating catalog integrations.

        Args:
            sf_cat_int_dtos (List[SnowflakeCatIntlDTO]): List of catalog integration DTOs.

        Returns:
            List[str]: List of generated DDL statements.
        """
        return [
            self.catalog_integration_logic.generate_ddl(
                catalog_integration_name=item.catalog_integration_name,
                uc_catalog_name=item.uc_catalog_name,
                uc_schema_name=item.uc_schema_name,
                uc_endpoint=item.uc_endpoint,
                oauth_client_id=item.oauth_client_id,
                oauth_client_secret=item.oauth_client_secret,
                refresh_interval_seconds=item.refresh_interval_seconds,
            )
            for item in sf_cat_int_dtos
        ]

    def create_sf_cat_int(
        self,
        sf_account_id: str,
        sf_user: str,
        sf_private_key_file: str,
        sf_private_key_file_pwd: str,
        sf_cat_int_dtos: List[SnowflakeCatIntlDTO],
    ) -> None:
        """
        Creates catalog integrations in Snowflake.

        Args:
            sf_account_id (str): Snowflake account ID.
            sf_user (str): Snowflake user.
            sf_private_key_file (str): Path to private key file.
            sf_private_key_file_pwd (str): Password for private key file.
            sf_cat_int_dtos (List[SnowflakeCatIntlDTO]): Catalog integration DTOs.
        """
        repository = self._initialize_snowflake_repository(
            sf_account_id, sf_user, sf_private_key_file, sf_private_key_file_pwd
        )

        for item in sf_cat_int_dtos:
            self.catalog_integration_logic.create_catalog_integration(
                repository, **vars(item)  # Unpacks DTO attributes into arguments
            )

    def create_sf_table_ddls(
        self,
        sf_table_dtos: List[SnowflakeIcebergTableDTO],
    ) -> List[str]:
        """
        Generates DDL statements for creating tables in Snowflake.

        Args:
            sf_table_dtos (List[SnowflakeIcebergTableDTO]): Table DTOs.

        Returns:
            List[str]: List of generated DDL statements.
        """
        return [
            self.table_logic.generate_ddl(
                sf_database_name=item.snowflake_database,
                sf_schema_name=item.snowflake_schema,
                sf_table_name=item.snowflake_table,
                sf_catalog_integration_name=item.catalog_integration_name,
                db_table_name=item.uc_table_name,
                auto_refresh=item.auto_refresh,
            )
            for item in sf_table_dtos
        ]

    def create_sf_tables(
        self,
        sf_account_id: str,
        sf_user: str,
        sf_private_key_file: str,
        sf_private_key_file_pwd: str,
        sf_table_dtos: List[SnowflakeIcebergTableDTO],
    ) -> None:
        """
        Creates tables in Snowflake.

        Args:
            sf_account_id (str): Snowflake account ID.
            sf_user (str): Snowflake user.
            sf_private_key_file (str): Path to private key file.
            sf_private_key_file_pwd (str): Password for private key file.
            sf_table_dtos (List[SnowflakeIcebergTableDTO]): Table DTOs.
        """
        repository = self._initialize_snowflake_repository(
            sf_account_id, sf_user, sf_private_key_file, sf_private_key_file_pwd
        )

        sucessful_metadata_ids:List[str] = []

        for item in sf_table_dtos:
            try:
                self.table_logic.create_iceberg_table(
                    repository,
                    sf_database_name=item.snowflake_database,
                    sf_schema_name=item.snowflake_schema,
                    sf_table_name=item.snowflake_table,
                    sf_catalog_integration_name=item.catalog_integration_name,
                    db_table_name=item.uc_table_name,
                    auto_refresh=item.auto_refresh,
                )
                sucessful_metadata_ids.append(item.dbx_sf_uniform_metadata_id)
            except Exception as e:
                pass
        self.metadata_mapping_logic.update_metadata_last_sync_date(sucessful_metadata_ids)
        
        # Update the metadata table with the successful table IDs

    def create_sf_databases(
        self,
        sf_account_id: str,
        sf_user: str,
        sf_private_key_file: str,
        sf_private_key_file_pwd: str,
        sf_table_dtos: List[SnowflakeIcebergTableDTO],
    ):
        """
        Creates Snowflake databases based on metadata.
        """
        databases: list[str] = list(
            {getattr(obj, "snowflake_database", None) for obj in sf_table_dtos}
        )

        repository = SnowflakeDatabaseRepository(
            sf_account_id, sf_user, sf_private_key_file, sf_private_key_file_pwd
        )
        for database in databases:
            self.database_logic.create_database(repository, database)

    def create_sf_schemas(
        self,
        sf_account_id: str,
        sf_user: str,
        sf_private_key_file: str,
        sf_private_key_file_pwd: str,
        sf_table_dtos: List[SnowflakeIcebergTableDTO],
    ):
        """
        Creates Snowflake databases based on metadata.
        """
        schemas: list[str] = list({(obj.snowflake_database, obj.snowflake_schema) for obj in sf_table_dtos})

        repository = SnowflakeSchemaRepository(
            sf_account_id, sf_user, sf_private_key_file, sf_private_key_file_pwd
        )
        for database, schema in schemas:
            self.schema_logic.create_schema(repository, database, schema)
            


