from typing import Optional
from snowflake_iceberg_catalog.repository_snowflake_catalog_integration import (
    SnowflakeCatalogIntegrationRepository,
)


class SnowflakeCatalogIntegrationLogic:

    def __init__(self, snowflake_cat_int_repo: SnowflakeCatalogIntegrationRepository):

        self.snowflake_cat_int_repo: SnowflakeCatalogIntegrationRepository = (
            snowflake_cat_int_repo
        )

    def create_catalog_integration(
        self,
        only_generate_sql: bool = True,
        sf_integration_name: str = None,
        uc_catalog_name: str = None,
        uc_schema_name: str = None,
        uc_endpoint: str = None,
        oauth_client_id: str = None,
        oauth_client_secret: str = None,
        refresh_interval_seconds: int = 3600,
    ) -> Optional[str]:

        oidc_endpoint = f"{uc_endpoint}/oidc/v1/token"

        ddl_query = self.snowflake_cat_int_repo.generate_ddl_catalog_integration(
            sf_integration_name=sf_integration_name,
            uc_catalog_name=uc_catalog_name,
            uc_schema_name=uc_schema_name,
            uc_endpoint=uc_endpoint,
            oidc_endpoint=oidc_endpoint,
            oauth_client_id=oauth_client_id,
            oauth_client_secret=oauth_client_secret,
            refresh_interval_seconds=refresh_interval_seconds,
        )
        if only_generate_sql:
            return ddl_query

        return None  # Explicitly return None if no SQL is generated
