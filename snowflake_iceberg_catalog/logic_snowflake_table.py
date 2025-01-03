from typing import List
from snowflake_iceberg_catalog.repository_snowflake_table import (
    SnowflakeTableRepository,
)
from concurrent.futures import ThreadPoolExecutor
from data_models.data_models import SnowflakeIcebergTableConfig


class SnowflakeTableLogic:
    def __init__(self, snowflake_table_repository: SnowflakeTableRepository):
        self.snowflake_table_repository: SnowflakeTableRepository = (
            snowflake_table_repository
        )

    def create_iceberg_table(self, table_config: SnowflakeIcebergTableConfig):
        self.snowflake_table_repository.create_iceberg_table(
            sf_database_name=table_config.sf_database_name,
            sf_schema_name=table_config.sf_schema_name,
            sf_table_name=table_config.sf_table_name,
            sf_external_volume=table_config.sf_external_volume,
            sf_catalog_integration_name=table_config.sf_catalog_integration_name,
            db_table_name=table_config.db_table_name,
        )

    def create_iceberg_tables_in_parallel(
        self, table_configs: List[SnowflakeIcebergTableConfig]
    ):
        with ThreadPoolExecutor(max_workers=3) as executor:
            executor.map(self.create_iceberg_table, table_configs)
