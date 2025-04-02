from repository.snowflake.repository_snowflake_schema import (
    SnowflakeSchemaRepository,
)
import logging
from config.logging_config import setup_logging  # Import logging setup configuration

# Initialize logging using the configured settings
setup_logging()

# Create a logger for this module
logger = logging.getLogger("dbx_to_sf_mirror")

class SnowflakeSchemaLogic:
    def __init__(self):
        # Constructor – no initialization required at the moment
        pass

    def create_schema(
        self,
        snowflake_schema_repository: SnowflakeSchemaRepository,
        database_name:str,
        schema_name: str,
    ):
        snowflake_schema_repository.create_schema(database_name,schema_name)
