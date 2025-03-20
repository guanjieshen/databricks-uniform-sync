import snowflake.connector as snow
from snowflake.core import Root
from snowflake.snowpark import Session
from snowflake.core.database._generated.models.database import DatabaseModel
from snowflake.core.schema import Schema, SchemaResource
from snowflake.core.exceptions import ConflictError

class SnowflakeSchemaRepository:
    def __init__(self, account_id:str, username:str = None, password:str = None):
        self.account_id = account_id
        self.username = username
        self.password = password

        # Define the connection parameters
        connection_parameters = {
            'account': account_id,
            'user': username,
            'password': password
        }
        # Create a connection to Snowflake for SQL
        self.connection = snow.connect(
            account=connection_parameters['account'],
            user=connection_parameters['user'],
            password=connection_parameters['password'],

        )
        # Create a session to Snowflake for Snowpark
        session:Session = Session.builder.configs(connection_parameters).create()
        self.root:Root = Root(session)

    def create_schema(self, database_name:str, schema_name: str)->SchemaResource:
        schema:Schema = Schema(name=schema_name)
        try:
            schema_resource:SchemaResource = self.root.databases[database_name].schemas.create(schema)
            print(f"Schema: {database_name}.{schema_name} created")
            return schema_resource
        except ConflictError as e:
            # Print the error type
            print(f"Schema: {database_name}.{schema_name} already exists")
        except Exception as e:
            # Handle any other exceptions that occur
            print(f"Caught a different error: {e}")
