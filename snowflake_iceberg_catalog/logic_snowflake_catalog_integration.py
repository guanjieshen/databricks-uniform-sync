from typing import Optional
from snowflake_iceberg_catalog.repository_snowflake import SnowflakeRepository
from snowflake.connector import ProgrammingError
import logging

# Configure logger
logging.basicConfig(
    level=logging.INFO,  # Set log level
    format="%(asctime)s - %(levelname)s - %(message)s"  # Define log format
)

logger = logging.getLogger(__name__)

class SnowflakeCatalogIntegrationLogic:

    def __init__(self):
        
        pass 
    def generate_ddl_catalog_integration(
        self,
        sf_catalog_integration_name: str,
        uc_catalog_name: str,
        uc_schema_name: str,
        uc_endpoint: str,
        oauth_client_id: str,
        oauth_client_secret: str,
        refresh_interval_seconds: int = 3600,
    ) -> str:
        """
        Generates a DDL statement for creating or modifying a Snowflake catalog integration.

        :param sf_catalog_integration_name: Name of the catalog integration in Snowflake.
        :param uc_catalog_name: Name of the Unity Catalog in Snowflake.
        :param uc_schema_name: Schema name under the Unity Catalog.
        :param uc_endpoint: Endpoint for the Unity Catalog.
        :param oidc_endpoint: OIDC endpoint for authentication.
        :param oauth_client_id: OAuth client ID for the catalog integration.
        :param oauth_client_secret: OAuth client secret for the catalog integration.
        :param refresh_interval_seconds: Frequency (in seconds) for refreshing OAuth tokens.
        :return: A formatted DDL statement as a string.
        """
        oidc_endpoint:str = f"{uc_endpoint}oidc/v1/token"
        return f"""
CREATE CATALOG INTEGRATION {sf_catalog_integration_name} 
CATALOG_SOURCE = ICEBERG_REST
TABLE_FORMAT = ICEBERG
CATALOG_NAMESPACE = '{uc_schema_name}'
REST_CONFIG = (
    CATALOG_URI = '{uc_endpoint}',
    WAREHOUSE = '{uc_catalog_name}',
    ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS
)
REST_AUTHENTICATION = (
    TYPE = OAUTH,
    OAUTH_TOKEN_URI = '{oidc_endpoint}',
    OAUTH_CLIENT_ID = '{oauth_client_id}',
    OAUTH_CLIENT_SECRET = '{oauth_client_secret}',
    OAUTH_ALLOWED_SCOPES = ('all-apis', 'sql')
)
ENABLED = TRUE
REFRESH_INTERVAL_SECONDS = {refresh_interval_seconds};
        """

    def create_catalog_integration(
        self,
        snowflake_repository: SnowflakeRepository,
        sf_catalog_integration_name: str,
        uc_catalog_name: str,
        uc_schema_name: str,
        uc_endpoint: str,
        oauth_client_id: str,
        oauth_client_secret: str,
        refresh_interval_seconds: int = 3600,
    ) -> Optional[str]:
        """
        Creates a Snowflake catalog integration.

        :param sf_catalog_integration_name: Name of the catalog integration in Snowflake.
        :param uc_catalog_name: Name of the Unity Catalog in Snowflake.
        :param uc_schema_name: Schema name under the Unity Catalog.
        :param uc_endpoint: Endpoint for the Unity Catalog.
        :param oidc_endpoint: OIDC endpoint for authentication.
        :param oauth_client_id: OAuth client ID for the catalog integration.
        :param oauth_client_secret: OAuth client secret for the catalog integration.
        :param refresh_interval_seconds: Frequency (in seconds) for refreshing OAuth tokens.
        :return: None if successful, error message if failed.
        """
        ddl = self.generate_ddl_catalog_integration(
            sf_catalog_integration_name,
            uc_catalog_name,
            uc_schema_name,
            uc_endpoint,
            oauth_client_id,
            oauth_client_secret,
            refresh_interval_seconds,
        )

        try:
            snowflake_repository.run_query(ddl)
            logger.info(f"'{sf_catalog_integration_name}' created successfully.")
        except ProgrammingError as e:
            if "already exists" in str(e):
                logger.warning(f"Catalog integration '{sf_catalog_integration_name}' already exists. Skipping creation.")
            else:
                logger.error(f"SQL compilation error: {e}")
        except Exception as e:
            logger.exception(f"Error executing DDL: {e}")