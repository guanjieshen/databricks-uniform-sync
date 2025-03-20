from typing import Optional
from repository.snowflake.repository_snowflake import SnowflakeRepository  # Custom repository for Snowflake operations
from snowflake.connector import ProgrammingError  # Exception handling for Snowflake errors
import logging
from config.logging_config import setup_logging  # Import logging setup configuration

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
        integration_name: str,  # Name of the catalog integration in Snowflake
        catalog_name: str,  # Unity Catalog name
        schema_name: str,  # Schema name under the Unity Catalog
        endpoint: str,  # Endpoint for the Unity Catalog
        client_id: str,  # OAuth client ID for authentication
        client_secret: str,  # OAuth client secret for authentication
        refresh_interval: int = 3600,  # Token refresh interval in seconds (default: 1 hour)
    ) -> str:
        # Construct the OIDC endpoint based on the provided endpoint
        oidc_endpoint = f"{endpoint}oidc/v1/token"

        # Return a formatted DDL statement for creating a catalog integration
        return f"""
        CREATE CATALOG INTEGRATION {integration_name} 
        CATALOG_SOURCE = ICEBERG_REST
        TABLE_FORMAT = ICEBERG
        CATALOG_NAMESPACE = '{schema_name}'
        REST_CONFIG = (
            CATALOG_URI = '{endpoint}/oidc/v1/token',
            WAREHOUSE = '{catalog_name}',
            ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS
        )
        REST_AUTHENTICATION = (
            TYPE = OAUTH,
            OAUTH_TOKEN_URI = '{oidc_endpoint}',
            OAUTH_CLIENT_ID = '{client_id}',
            OAUTH_CLIENT_SECRET = '{client_secret}',
            OAUTH_ALLOWED_SCOPES = ('all-apis', 'sql')
        )
        ENABLED = TRUE
        REFRESH_INTERVAL_SECONDS = {refresh_interval};
        """

    # Method to create a catalog integration in Snowflake using the generated DDL statement
    def create_catalog_integration(
        self,
        repository: SnowflakeRepository,  # Instance of SnowflakeRepository to execute queries
        integration_name: str,  # Name of the catalog integration in Snowflake
        catalog_name: str,  # Unity Catalog name
        schema_name: str,  # Schema name under the Unity Catalog
        endpoint: str,  # Endpoint for the Unity Catalog
        client_id: str,  # OAuth client ID for authentication
        client_secret: str,  # OAuth client secret for authentication
        refresh_interval: int = 3600,  # Token refresh interval in seconds (default: 1 hour)
    ) -> Optional[str]:
        # Generate the DDL statement using the provided parameters
        ddl = self.generate_ddl(
            integration_name,
            catalog_name,
            schema_name,
            endpoint,
            client_id,
            client_secret,
            refresh_interval,
        )

        try:
            # Log the creation attempt for tracking and debugging
            logger.info(f"Creating Catalog Integration: '{integration_name}'")

            # Execute the DDL statement using the Snowflake repository
            repository.run_query(ddl)

            # Log successful creation
            logger.info(f"Catalog Integration '{integration_name}' created successfully.")

        except ProgrammingError as e:
            # Handle SQL compilation errors specific to Snowflake
            logger.error(f"SQL error creating catalog '{integration_name}': {e}")
            return str(e)
        except Exception as e:
            # Handle any other unexpected exceptions
            logger.exception(f"Error executing DDL for catalog '{integration_name}': {e}")
            return str(e)

        return None
