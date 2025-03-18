from typing import List
from snowflake_iceberg_catalog.repository_snowflake_catalog_integration import (
    SnowflakeCatalogIntegrationRepository,
)
from snowflake_iceberg_catalog.logic_snowflake_catalog_integration import (
    SnowflakeCatalogIntegrationLogic,
)
from metadata_mapping.metadata_mapping_logic import MetadataMappingLogic
from pyspark.sql import SparkSession
from data_models.data_models import SnowflakeCatIntlDTO
from pyspark.sql import Row


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
        self.sf_cat_int_logic = SnowflakeCatalogIntegrationLogic(
            SnowflakeCatalogIntegrationRepository()
        )

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
        pass


    def create_sf_cat_int_ddls(
        self, sf_cat_int_dtos: List[SnowflakeCatIntlDTO]
    ) -> List[str]:
        return [
            self.sf_cat_int_logic.create_catalog_integration(
                only_generate_sql=True,
                sf_integration_name=item.catalog_integration_name,
                uc_catalog_name=item.uc_catalog_name,
                uc_schema_name=item.uc_schema_name,
                uc_endpoint=item.uc_endpoint,
                oauth_client_id=item.oauth_client_id,
                oauth_client_secret=item.oauth_client_secret,
                refresh_interval_seconds=item.refresh_interval_seconds,
            )
            for item in sf_cat_int_dtos
        ]
