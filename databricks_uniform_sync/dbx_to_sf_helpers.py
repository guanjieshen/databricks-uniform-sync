from typing import List
from snowflake_iceberg_catalog.repository_snowflake_ext_vol import (
    SnowflakeExternalVolumeRepository,
)
from snowflake_iceberg_catalog.logic_snowflake_ext_vol import (
    SnowflakeExternalVolumeLogic,
)
from metadata_mapping.metadata_mapping_logic import MetadataMappingLogic
from pyspark.sql import SparkSession
from data_models.data_models import SnowflakeExtVolDTO
from pyspark.sql import Row

class DatabricksToSnowflakeHelpers:
    """
    Helper class for integrating Databricks with Snowflake.

    Provides methods to retrieve Azure storage locations from metadata and 
    generate Snowflake External Volume DDLs.
    """

    def __init__(
        self, spark_session: SparkSession, metadata_catalog: str, metadata_schema: str, metadata_table:str
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
            table = metadata_table
        )
        self.sf_ext_vol_repo = SnowflakeExternalVolumeRepository()
        self.sf_ext_vol_logic = SnowflakeExternalVolumeLogic(self.sf_ext_vol_repo)

    def fetch_uc_storage_locations(self, tenant_id: str) -> List[SnowflakeExtVolDTO]:
        """
        Fetches Azure storage locations from the metadata catalog.

        Args:
            tenant_id (str): The Azure tenant ID.

        Returns:
            List[SnowflakeExtVolDTO]: A list of storage account and container details.

        Example:
            >>> helper.fetch_uc_storage_locations("tenant123")
            [SnowflakeExtVolDTO(account_name="storage1", container_name="container1", tenant_id="tenant123")]
        """
        metadata_view_df: List[Row] = self.metadata_mapping_logic.get_metadata_az_sf_external_volume()

        # Extract required fields and convert to a list of AzureStorageDetails objects
        return self._convert_df_to_ext_vol_dto(metadata_view_df, tenant_id)

    @staticmethod
    def _convert_df_to_ext_vol_dto(
        metadata_df: List[Row], tenant_id: str
    ) -> List[SnowflakeExtVolDTO]:
        """
        Converts a Spark DataFrame containing Snowfla storage metadata into a list of SnowflakeExtVolDTO.

        Args:
            metadata_df (DataFrame): The DataFrame containing metadata.
            tenant_id (str): The Azure tenant ID.

        Returns:
            List[AzureStorageDetails]: A list of storage account details.
        """
        return [
            SnowflakeExtVolDTO(
                external_volume_name=row["snowflake_external_volume"],
                external_volume_storage_name=row["snowflake_external_volume_storage"],
                account_name=row["az_storage_account"],
                container_name=row["az_container_name"],
                tenant_id=tenant_id,
            )
            for row in metadata_df
        ]

    def create_sf_external_volume_ddls(
        self, snowflakeExtVolDTOs: List[SnowflakeExtVolDTO]
    ) -> List[str]:
        """
        Generates Snowflake External Volume DDLs based on Azure storage details.

        Args:
            storage_details_list (List[AzureStorageDetails]): A list of AzureStorageDetails objects.

        Returns:
            List[str]: A list of generated DDL statements.

        Example:
            >>> storage_details = [AzureStorageDetails("storage1", "container1", "tenant123")]
            >>> helper.create_sf_external_volume_ddls(storage_details)
            ['CREATE EXTERNAL VOLUME ...']
        """
        return [
            self.sf_ext_vol_logic.az_create_external_volume(
                only_generate_sql=True,
                external_location_name=item.external_volume_name,
                external_location_storage_name=item.external_volume_storage_name,
                az_tenant_id=item.tenant_id,
                az_storage_account_name=item.account_name,
                az_container_name=item.container_name,
            )
            for item in snowflakeExtVolDTOs
        ]
