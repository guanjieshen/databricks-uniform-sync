import snowflake.connector as snow
from snowflake.core import Root
from snowflake.snowpark import Session
from snowflake.core.database._generated.models.database import DatabaseModel
from snowflake.core.schema import Schema, SchemaResource
from snowflake.core.exceptions import ConflictError
from snowflake.core.table import Table, TableResource


class SnowflakeExternalVolumeRepository:
    def __init__(self, account_id: str = None, username: str = None, password: str = None):
        self.account_id = account_id
        self.username = username
        self.password = password

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

    def generate_ddl_azure_ext_vol(
        self, ext_vol_name:str, storage_name:str, az_tenant_id:str, az_storage_account_name:str, az_container_name:str
    ):
        ddl_query = f""" 
        CREATE EXTERNAL VOLUME {ext_vol_name}
            STORAGE_LOCATIONS =
                (
                    (
                        NAME = '{storage_name}'
                        STORAGE_PROVIDER = 'AZURE'
                        STORAGE_BASE_URL = 'azure://{az_storage_account_name}.blob.core.windows.net/{az_container_name}/'
                        AZURE_TENANT_ID = '{az_tenant_id}'
                    )
                )
            ALLOW_WRITES = FALSE;
        """