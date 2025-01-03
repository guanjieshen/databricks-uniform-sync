from snowflake_iceberg_catalog.repository_snowflake_schema import (
    SnowflakeSchemaRepository,
)


class SnowflakeSchemaLogic:
    def __init__(self, snowflake_database_repository: SnowflakeSchemaRepository):
        self.snowflake_schema_repository: SnowflakeSchemaRepository = (
            snowflake_database_repository
        )

    def create_schema(self, database_name: str, schema_name: str):
        self.snowflake_schema_repository.create_schema(
            database_name=database_name, schema_name=schema_name
        )
