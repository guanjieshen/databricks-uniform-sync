# Import necessary modules
from typing import List
from repository.snowflake.repository_snowflake import SnowflakeRepository  # Custom repository for Snowflake operations
from concurrent.futures import ThreadPoolExecutor  # For concurrent execution of tasks
import logging
from config.logging_config import setup_logging  # Import logging setup configuration
from snowflake.connector import ProgrammingError  # Exception handling for Snowflake errors

# Initialize logging using the configured settings
setup_logging()

# Create a logger for this module
logger = logging.getLogger("dbx_to_sf_mirror")

# Define a class to handle Snowflake table logic
class SnowflakeTableLogic:
    def __init__(self):
        # Constructor – no initialization required at the moment
        pass

    # Method to generate a DDL (Data Definition Language) statement for creating an Iceberg table
    def generate_ddl(
        self,
        sf_database_name: str,  # Snowflake database name
        sf_schema_name: str,  # Snowflake schema name
        sf_table_name: str,  # Snowflake table name
        sf_catalog_integration_name: str,  # Catalog integration name
        db_table_name: str,  # External table name in catalog
        auto_refresh: bool,  # Whether to enable auto-refresh of the table
    ) -> str:
        # Return a formatted DDL string for creating an Iceberg table
        return f"""
CREATE OR REPLACE ICEBERG TABLE {sf_database_name}.{sf_schema_name}.{sf_table_name}
CATALOG = '{sf_catalog_integration_name}'
CATALOG_TABLE_NAME = '{db_table_name}'
AUTO_REFRESH = {"TRUE" if auto_refresh else "FALSE"};
        """

    # Method to create an Iceberg table in Snowflake using the generated DDL statement
    def create_iceberg_table(
        self,
        snowflake_repository: SnowflakeRepository,  # Instance of SnowflakeRepository to execute queries
        sf_database_name: str,  # Snowflake database name
        sf_schema_name: str,  # Snowflake schema name
        sf_table_name: str,  # Snowflake table name
        sf_catalog_integration_name: str,  # Catalog integration name
        db_table_name: str,  # External table name in catalog
        auto_refresh: bool,  # Whether to enable auto-refresh of the table
    ):
        # Generate the DDL statement using the provided parameters
        ddl = self.generate_ddl(
            sf_database_name=sf_database_name,
            sf_schema_name=sf_schema_name,
            sf_table_name=sf_table_name,
            sf_catalog_integration_name=sf_catalog_integration_name,
            db_table_name=db_table_name,
            auto_refresh=auto_refresh,
        )

        try:
            # Log the creation attempt for tracking and debugging
            logger.info(
                f"Creating Iceberg Table: '{sf_database_name}.{sf_schema_name}.{sf_table_name}'"
            )

            # Execute the DDL statement using the Snowflake repository
            snowflake_repository.run_query(ddl)

        except ProgrammingError as e:
            # Handle SQL compilation errors specific to Snowflake
            logger.error(f"SQL compilation error: {e}")
        except Exception as e:
            # Handle any other unexpected exceptions
            logger.exception(f"Error executing DDL: {e}")
