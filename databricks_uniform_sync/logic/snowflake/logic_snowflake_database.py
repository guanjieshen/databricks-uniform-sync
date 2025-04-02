from repository.snowflake.repository_snowflake_database import (
    SnowflakeDatabaseRepository,
)
import logging
from config.logging_config import setup_logging  # Import logging setup configuration

# Initialize logging using the configured settings
setup_logging()

# Create a logger for this module
logger = logging.getLogger("dbx_to_sf_mirror")


class SnowflakeDatabaseLogic:
    def __init__(self):
        # Constructor – no initialization required at the moment
        pass

    def create_database(
        self,
        snowflake_database_repository: SnowflakeDatabaseRepository,
        database_name: str,
    ):
        snowflake_database_repository.create_database(database_name)
