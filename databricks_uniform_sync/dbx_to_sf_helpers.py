from typing import List
from snowflake_iceberg_catalog.logic_snowflake_table import SnowflakeTableLogic
from snowflake_iceberg_catalog.repository_snowflake import SnowflakeRepository
from snowflake_iceberg_catalog.logic_snowflake_catalog_integration import (
    SnowflakeCatalogIntegrationLogic,
)
from metadata_mapping.metadata_mapping_logic import MetadataMappingLogic
from pyspark.sql import SparkSession
from data_models.data_models import SnowflakeCatIntlDTO, SnowflakeIcebergTableDTO
from pyspark.sql import Row

from snowflake_iceberg_catalog.repository_snowflake_table import (
    SnowflakeTableRepository,
)


class DatabricksToSnowflakeHelpers:
    """
    Helper class for integrating Databricks with Snowflake.

    Provides methods to retrieve Azure storage locations from metadata and
    generate Snowflake External Volume DDLs.
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
        """
        self.spark_session = spark_session
        self.metadata_mapping_logic = MetadataMappingLogic(
            spark_session=spark_session,
            catalog=metadata_catalog,
            schema=metadata_schema,
            table=metadata_table,
        )
        self.sf_cat_int_logic = SnowflakeCatalogIntegrationLogic()
        self.sf_table_logic = SnowflakeTableLogic(SnowflakeTableRepository())

    def fetch_uc_catalog_integration(
        self,
        uc_endpoint: str,
        refresh_interval_seconds: int,
        oauth_client_id: str,
        oauth_client_secret: str,
    ) -> List[SnowflakeCatIntlDTO]:

        metadata_view_df: List[Row] = (
            self.metadata_mapping_logic.get_metadata_az_sf_catalog_integration()
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
            for row in metadata_view_df
        ]

    def fetch_uc_tables(
        self,
        auto_refresh: bool,
    ) -> List[SnowflakeIcebergTableDTO]:

        metadata_view_df: List[Row] = (
            self.metadata_mapping_logic.get_metadata_az_tables()
        )
        return [
            SnowflakeIcebergTableDTO(
                catalog_integration_name=row["snowflake_catalog_integration"],
                uc_table_name=row["uc_table_name"],
                snowflake_database=row["snowflake_database"],
                snowflake_schema=row["snowflake_schema"],
                snowflake_table=row["snowflake_table"],
                auto_refresh=auto_refresh,
            )
            for row in metadata_view_df
        ]

    def create_sf_cat_int_ddls(
        self, sf_cat_int_dtos: List[SnowflakeCatIntlDTO]
    ) -> List[str]:
        return [
            self.sf_cat_int_logic.generate_ddl_catalog_integration(
                sf_catalog_integration_name=item.catalog_integration_name,
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
        
        snowflake_repository:SnowflakeRepository = SnowflakeRepository(
            account_id=sf_account_id,
            user=sf_user,
            private_key_file=sf_private_key_file,
            private_key_file_pwd=sf_private_key_file_pwd,
        )

        return [
            self.sf_cat_int_logic.create_catalog_integration(
                snowflake_repository=snowflake_repository,
                sf_catalog_integration_name=item.catalog_integration_name,
                uc_catalog_name=item.uc_catalog_name,
                uc_schema_name=item.uc_schema_name,
                uc_endpoint=item.uc_endpoint,
                oauth_client_id=item.oauth_client_id,
                oauth_client_secret=item.oauth_client_secret,
                refresh_interval_seconds=item.refresh_interval_seconds,
            )
            for item in sf_cat_int_dtos
        ]

    def create_sf_table_ddls(
        self,
        sf_table_dtos: List[SnowflakeIcebergTableDTO],
    ) -> List[str]:

        return [
            self.sf_table_logic.create_iceberg_table(
                only_generate_sql=True,
                sf_catalog_integration_name=item.catalog_integration_name,
                sf_database_name=item.snowflake_database,
                sf_schema_name=item.snowflake_schema,
                sf_table_name=item.snowflake_table,
                db_table_name=item.uc_table_name,
                auto_refresh=item.auto_refresh,
            )
            for item in sf_table_dtos
        ]
