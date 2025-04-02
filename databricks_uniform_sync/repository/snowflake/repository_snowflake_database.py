import logging

import snowflake.connector
from snowflake.core import Root
from snowflake.core.database import Database
from snowflake.core.database._generated.models.database import DatabaseModel
from snowflake.core.exceptions import ConflictError
from snowflake.snowpark import Session

from databricks_uniform_sync.config.logging_config import setup_logging  # Project logging config

# Initialize logging using the configured settings
setup_logging()

# Create a logger for this module
logger = logging.getLogger("dbx_to_sf_mirror")

class SnowflakeDatabaseRepository:
    def __init__(      self,
        account_id: str = None,
        user: str = None,
        private_key_file: str = None,
        private_key_file_pwd: str = None,):
        """
        Initializes the repository with Snowflake connection credentials.

        :param account_id: Snowflake account ID.
        :param user: Snowflake username.
        :param private_key_file: Path to the private key file for authentication.
        :param private_key_file_pwd: Password for the private key file.
        """
        self.account_id = account_id
        self.user = user
        self.private_key_file = private_key_file
        self.private_key_file_pwd = private_key_file_pwd

        # Connection parameters used for establishing a Snowflake session or direct connection.
        self.connection_parameters = {
            "account": account_id,
            "user": user,
            "private_key_file": private_key_file,
            "private_key_file_pwd": private_key_file_pwd,
        }

        # Establish a direct connection to Snowflake using snowflake.connector.
        self.connection = snowflake.connector.connect(**self.connection_parameters)

        # Establish a Snowpark session for executing DataFrame-style operations.
        self.session: Session = Session.builder.configs(
            self.connection_parameters
        ).create()

        # Root object from Snowflake core API (useful for low-level operations).
        self.root: Root = Root(self.session)

    #### Database Commands

    def create_database(self, database_name: str)->DatabaseModel:
        my_db = Database(name=database_name)
        try:
            self.root.databases.create(my_db)
            logger.info(f"Database: {database_name} created")
        except ConflictError as e:
            # Print the error type
            logger.info(f"Database: {database_name} already exists")
        except Exception as e:
            # Handle any other exceptions that occur
            logger.error(f"Caught a different error: {e}")
