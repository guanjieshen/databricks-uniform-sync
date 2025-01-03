import snowflake.connector as snow
from snowflake.core import Root
from snowflake.snowpark import Session
from snowflake.core.database._generated.models.database import DatabaseModel
from snowflake.core.database import Database
from snowflake.core.exceptions import ConflictError

class SnowflakeDatabaseRepository:
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

    #### Database Commands
    # Snowflake Database is the equivalent of a Catalog in Unity Catalog
    def get_database(self, database_name: str)->DatabaseModel:
        #  The Python SDK does not return the database_id (does no incur compute costs)
        my_db:DatabaseModel = self.root.databases[database_name].fetch()
        print(my_db.name)

    def get_database_with_id(self, database_name: str)->DatabaseModel:
        try:
            # Execute a SQL query against Snowflake to get the current_version
            cursor = self.connection.cursor()
            result = cursor.execute("SELECT * FROM unity_catalog.flights.airports")
            one_row = result.fetchone()

            # Fetch column names from the cursor's description attribute
            column_names = [desc[0] for desc in cursor.description]

            # Fetch one row of data
            one_row = result.fetchone()

            # Print the column names
            print("Column Names:", column_names)

            # Optionally print the row fetched
            print("One Row:", one_row)
        finally:
            cursor.close()
            self.connection.close()

    def create_database(self, database_name: str)->DatabaseModel:
        my_db = Database(name=database_name)
        try:
            self.root.databases.create(my_db)
            print(f"Database: {database_name} created")
        except ConflictError as e:
            # Print the error type
            print(f"Database: {database_name} already exists")
        except Exception as e:
            # Handle any other exceptions that occur
            print(f"Caught a different error: {e}")
