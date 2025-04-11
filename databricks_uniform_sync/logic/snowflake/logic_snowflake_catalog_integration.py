import logging
from typing import Optional

from snowflake.connector import ProgrammingError

from databricks_uniform_sync.config.logging_config import setup_logging  # Project logging setup
from databricks_uniform_sync.repository.snowflake.repository_snowflake import (
    SnowflakeRepository,
)  # Custom repository for Snowflake operations


# Initialize logging using the configured settings
setup_logging()

# Create a logger for this module
logger = logging.getLogger("dbx_to_sf_mirror")


# Define a class to handle Snowflake catalog integration logic
class SnowflakeCatalogIntegrationLogic:
    def __init__(self):
        # Constructor – no initialization required at the moment
        pass

    # Method to generate a DDL (Data Definition Language) statement for creating a catalog integration
    def generate_ddl(
        self,
        catalog_integration_name: str,  # Name of the catalog integration in Snowflake
        uc_catalog_name: str,  # Unity Catalog name
        uc_schema_name: str,  # Schema name under the Unity Catalog
        uc_endpoint: str,  # Endpoint for the Unity Catalog
        oauth_client_id: str,  # OAuth client ID for authentication
        oauth_client_secret: str,  # OAuth client secret for authentication
        refresh_interval_seconds: int = 3600,  # Token refresh interval in seconds (default: 1 hour)
        access_delegation_mode: str = "VENDED_CREDENTIALS",  # Access delegation mode (default: VENDED_CREDENTIALS)
    ) -> str:
        # Construct the OIDC endpoint based on the provided endpoint
        oidc_endpoint = f"{uc_endpoint}/oidc/v1/token"

        # Return a formatted DDL statement for creating a catalog integration
        return f"""
        CREATE CATALOG INTEGRATION {catalog_integration_name} 
        CATALOG_SOURCE = ICEBERG_REST
        TABLE_FORMAT = ICEBERG
        CATALOG_NAMESPACE = '{uc_schema_name}'
        REST_CONFIG = (
            CATALOG_URI = '{uc_endpoint}/api/2.1/unity-catalog/iceberg',
            WAREHOUSE = '{uc_catalog_name}',
            ACCESS_DELEGATION_MODE = {access_delegation_mode}
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

    # Method to create a catalog integration in Snowflake using the generated DDL statement
    def create_catalog_integration(
        self,
        repository: SnowflakeRepository,  # Instance of SnowflakeRepository to execute queries
        catalog_integration_name: str,  # Name of the catalog integration in Snowflake
        uc_catalog_name: str,  # Unity Catalog name
        uc_schema_name: str,  # Schema name under the Unity Catalog
        uc_endpoint: str,  # Endpoint for the Unity Catalog
        oauth_client_id: str,  # OAuth client ID for authentication
        oauth_client_secret: str,  # OAuth client secret for authentication
        refresh_interval_seconds: int = 3600,  # Token refresh interval in seconds (default: 1 hour)
        access_delegation_mode: str = "VENDED_CREDENTIALS",  # Access delegation mode (default: VENDED_CREDENTIALS)
    ) -> Optional[str]:
        # Generate the DDL statement using the provided parameters
        ddl = self.generate_ddl(
            catalog_integration_name,
            uc_catalog_name,
            uc_schema_name,
            uc_endpoint,
            oauth_client_id,
            oauth_client_secret,
            refresh_interval_seconds,
            access_delegation_mode
        )

        try:
            # Log the creation attempt for tracking and debugging
            logger.info(f"Creating Catalog Integration: '{catalog_integration_name}'")

            # Execute the DDL statement using the Snowflake repository
            repository.run_query(ddl)

            # Log successful creation
            logger.info(
                f"Catalog Integration '{catalog_integration_name}' created successfully."
            )

        except ProgrammingError as e:
            error_message = str(e)
            if "already exists" in error_message:
                logger.info(
                    f"Catalog '{catalog_integration_name}' already exists, skipping creation."
                )
            else:
                logger.error(
                    f"SQL error creating catalog '{catalog_integration_name}': {error_message}"
                )
            return error_message

        except Exception as e:
            logger.exception(
                f"Error executing DDL for catalog '{catalog_integration_name}': {e}"
            )
            return str(e)

        return None
