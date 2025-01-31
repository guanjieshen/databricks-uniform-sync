from snowflake_iceberg_catalog.repository_snowflake_ext_vol import (
    SnowflakeExternalVolumeRepository,
)


class SnowflakeExternalVolumeLogic:
    def __init__(self, snowflake_ext_vol_repo: SnowflakeExternalVolumeRepository):
        self.snowflake_ext_vol_repo: SnowflakeExternalVolumeRepository = (
            snowflake_ext_vol_repo
        )

    def __az_generate_name(
        self,
        type: str,
        az_tenant_id: str,
        az_storage_account_name: str,
        az_container_name: str,
    ):
        return f"az_dbx_uc_{type}" + str(
            abs(hash(az_tenant_id + az_storage_account_name + az_container_name))
        )

    def az_create_external_volume(
        self,
        only_generate_sql: bool = True,
        az_tenant_id: str = None,
        az_storage_account_name: str = None,
        az_container_name: str = None,
    ) -> str:

        if only_generate_sql:
            ddl_query = self.snowflake_ext_vol_repo.generate_ddl_azure_ext_vol(
                ext_vol_name=self.__az_generate_name(
                    "extvol", az_tenant_id, az_storage_account_name, az_container_name
                ),
                storage_name=self.__az_generate_name(
                    "storage",
                    az_tenant_id + az_storage_account_name + az_container_name,
                ),
                az_tenant_id=az_tenant_id,
                az_storage_account_name=az_storage_account_name,
                az_container_name=az_container_name,
            )

        return ddl_query
