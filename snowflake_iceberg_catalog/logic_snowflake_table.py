from typing import List
from snowflake_iceberg_catalog.repository_snowflake_table import (
    SnowflakeTableRepository,
)
from concurrent.futures import ThreadPoolExecutor
from data_models.data_models import SnowflakeIcebergTableDTO


class SnowflakeTableLogic:
    def __init__(self, snowflake_table_repository: SnowflakeTableRepository):
        self.snowflake_table_repository: SnowflakeTableRepository = (
            snowflake_table_repository
        )


    def create_iceberg_table(
        self,
        sf_database_name: str,
        sf_schema_name: str,
        sf_table_name: str,
        sf_catalog_integration_name: str,
        db_table_name: str,
        auto_refresh: bool,
        only_generate_sql: bool = True,
    ):
        ddl_query = self.snowflake_table_repository.generate_ddl_iceberg_table(
            sf_database_name=sf_database_name,
            sf_schema_name=sf_schema_name,
            sf_table_name=sf_table_name,
            sf_catalog_integration_name=sf_catalog_integration_name,
            db_table_name=db_table_name,
            auto_refresh="TRUE" if auto_refresh else "FALSE"
        )
        if only_generate_sql:
            return ddl_query

    # def create_iceberg_tables_in_parallel(
    #     self, table_configs: List[SnowflakeIcebergTableConfig]
    # ):
    #     with ThreadPoolExecutor(max_workers=3) as executor:
    #         executor.map(self.create_iceberg_table, table_configs)
