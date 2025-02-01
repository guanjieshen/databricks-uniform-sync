from typing import Optional
from snowflake_iceberg_catalog.repository_snowflake_ext_vol import (
    SnowflakeExternalVolumeRepository,
)

# Define a constant prefix for generated names
AZURE_RESOURCE_PREFIX = "az_dbx_uc_"


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
        self.snowflake_ext_vol_repo: SnowflakeExternalVolumeRepository = snowflake_ext_vol_repo

    def __az_generate_name(
        self,
        resource_type: str,
        az_storage_account_name: str,
        az_container_name: str,
    ) -> str:
        """
        Generates a deterministic and unique name for Azure-related objects 
        in Snowflake by hashing key Azure parameters.

        :param resource_type: The type of resource (e.g., 'extvol' or 'storage').
        :param az_storage_account_name: The name of the Azure storage account.
        :param az_container_name: The name of the Azure container.
        :return: A generated unique name for the Snowflake resource.
        """
        unique_identifier = abs(hash(f"{az_storage_account_name}_{az_container_name}"))
        return f"{AZURE_RESOURCE_PREFIX}{resource_type}_{unique_identifier}"

    def az_create_external_volume(
        self,
        only_generate_sql: bool = True,
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
        if not all([az_tenant_id, az_storage_account_name, az_container_name]):
            raise ValueError("az_tenant_id, az_storage_account_name, and az_container_name are required.")

        if only_generate_sql:
            ddl_query = self.snowflake_ext_vol_repo.generate_ddl_azure_ext_vol(
                ext_vol_name=self.__az_generate_name(
                    "extvol", az_storage_account_name, az_container_name
                ),
                storage_name=self.__az_generate_name(
                    "storage",  az_storage_account_name, az_container_name
                ),
                az_tenant_id=az_tenant_id,
                az_storage_account_name=az_storage_account_name,
                az_container_name=az_container_name,
            )

            return ddl_query

        return None  # Explicitly return None if no SQL is generated
