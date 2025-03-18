import snowflake.connector as snow
from snowflake.core import Root
from snowflake.snowpark import Session
from snowflake.core.database._generated.models.database import DatabaseModel
from snowflake.core.schema import Schema, SchemaResource
from snowflake.core.exceptions import ConflictError
from snowflake.core.table import Table, TableResource


class SnowflakeTableRepository:
    def __init__(self, account_id: str=None, username: str = None, password: str = None):
        self.account_id = account_id
        self.username = username
        self.password = password
        if all([self.account_id, self.username, self.password]):
            # Define the connection parameters
            connection_parameters = {
                "account": account_id,
                "user": username,
                "password": password,
            }
            # Create a connection to Snowflake for SQL
            self.connection = snow.connect(
                account=connection_parameters["account"],
                user=connection_parameters["user"],
                password=connection_parameters["password"],
            )
            # Create a session to Snowflake for Snowpark
            session: Session = Session.builder.configs(connection_parameters).create()
            self.root: Root = Root(session)
        else:
            self.connection = None
            self.root = None

    def generate_ddl_iceberg_table(
        sf_database_name: str,
        sf_schema_name: str,
        sf_table_name: str,
        sf_catalog_integration_name: str,
        db_table_name: str,
        auto_refresh: str,
    ):
        return f"""CREATE OR REPLACE ICEBERG TABLE {sf_database_name}.{sf_schema_name}.{sf_table_name}
                    CATALOG = '{sf_catalog_integration_name}'
                    CATALOG_TABLE_NAME = '{db_table_name}'
                    AUTO_REFRESH = {auto_refresh};"""

    def create_iceberg_table(
        self,
        sf_database_name: str,
        sf_schema_name: str,
        sf_table_name: str,
        sf_catalog_integration_name: str,
        db_table_name: str,
    ):
        try:
            # Initialize cursor to None to avoid UnboundLocalError
            cursor = None

            # Execute a SQL query against Snowflake to get the current_version
            cursor = self.connection.cursor()
            result = cursor.execute(
                f"""CREATE OR REPLACE ICEBERG TABLE {sf_database_name}.{sf_schema_name}.{sf_table_name}
                    CATALOG = '{sf_catalog_integration_name}'
                    CATALOG_TABLE_NAME = '{db_table_name}';"""
            )

            # Fetch one row (if applicable)
            one_row = result.fetchone()

            # Optionally print the row fetched
            print(one_row)
        except Exception as e:
            # Handle any other exceptions that occur
            print(f"Caught an error: {e}")
        finally:
            # Safely close cursor if it was created
            if cursor is not None:
                cursor.close()
