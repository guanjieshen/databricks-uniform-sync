import snowflake.connector
from snowflake.core import Root
from snowflake.snowpark import Session
import logging
from config.logging_config import setup_logging  # Import logging setup configuration

# Initialize logging using the configured settings
setup_logging()

# Create a logger for this module
logger = logging.getLogger("dbx_to_sf_mirror")


class SnowflakeRepository:
    """
    A repository class for managing catalog integrations in Snowflake.
    Provides methods to create and configure catalog integrations using Snowflake SQL.
    """

    def __init__(
        self,
        account_id: str = None,
        user: str = None,
        private_key_file: str = None,
        private_key_file_pwd: str = None,
    ):
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

    def run_query(self, query: str) -> list:
        """
        Runs a SQL query using the Snowflake connection.

        :param query: SQL query string.
        :return: List of tuples representing the query result.
        """
        if not query.strip():
            raise ValueError("Query cannot be empty.")

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                return result
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            raise