from typing import Optional
from snowflake_iceberg_catalog.repository_snowflake_ext_vol import (
    SnowflakeExternalVolumeRepository,
)

class SnowflakeExternalVolumeLogic:
    """
    This class encapsulates the logic for managing external volumes in Snowflake,
    specifically for Azure storage integration.
    """

    def __init__(self, snowflake_ext_vol_repo: SnowflakeExternalVolumeRepository):
        """
        Initializes the SnowflakeExternalVolumeLogic with a repository instance.

        :param snowflake_ext_vol_repo: Instance of SnowflakeExternalVolumeRepository
                                       to interact with Snowflake.
        """
        self.snowflake_ext_vol_repo: SnowflakeExternalVolumeRepository = (
            snowflake_ext_vol_repo
        )

    def az_create_external_volume(
        self,
        only_generate_sql: bool = True,
        external_location_name: str = None,
        external_location_storage_name: str = None,
        az_tenant_id: Optional[str] = None,
        az_storage_account_name: Optional[str] = None,
        az_container_name: Optional[str] = None,
    ) -> Optional[str]:
        """
        Generates the SQL DDL statement for creating an Azure-based external volume in Snowflake.

        :param only_generate_sql: If True, returns the SQL statement instead of executing it.
        :param az_tenant_id: The Azure tenant ID.
        :param az_storage_account_name: The name of the Azure storage account.
        :param az_container_name: The name of the Azure container.
        :return: The generated SQL DDL statement for creating the external volume, or None if inputs are invalid.
        """

        # Ensure required parameters are provided
        if not all(
            [
                external_location_name,
                external_location_storage_name,
                az_tenant_id,
                az_storage_account_name,
                az_container_name,
            ]
        ):
            raise ValueError(
                "external_location_name, external_location_storage_name, az_tenant_id, az_storage_account_name, and az_container_name are required."
            )
        
        ddl_query = self.snowflake_ext_vol_repo.generate_ddl_azure_ext_vol(
            ext_vol_name=external_location_name,
            storage_name=external_location_storage_name,
            az_tenant_id=az_tenant_id,
            az_storage_account_name=az_storage_account_name,
            az_container_name=az_container_name,
        )
        
        if only_generate_sql:
            return ddl_query

        return None  # Explicitly return None if no SQL is generated
