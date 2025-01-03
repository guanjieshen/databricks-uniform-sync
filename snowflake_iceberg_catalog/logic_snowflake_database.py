from snowflake_iceberg_catalog.repository_snowflake_database import SnowflakeDatabaseRepository


class SnowflakeDatabaseLogic:
    def __init__(self,snowflake_database_repository: SnowflakeDatabaseRepository):
        self.snowflake_database_repository = snowflake_database_repository

    def create_database(self, database_name: str):
        self.snowflake_database_repository.create_database(database_name)

    

