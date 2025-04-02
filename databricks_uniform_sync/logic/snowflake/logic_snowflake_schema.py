import logging

from databricks_uniform_sync.config.logging_config import setup_logging  # Project logging setup
from databricks_uniform_sync.repository.snowflake.repository_snowflake_schema import (
    SnowflakeSchemaRepository,
)

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
