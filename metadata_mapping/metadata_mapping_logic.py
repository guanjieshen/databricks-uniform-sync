from typing import List
from pyspark.sql import SparkSession, DataFrame
from data_models.data_models import Catalog
from metadata_mapping.metadata_mapping_repository import MetadataMappingRepository
from pyspark.sql.functions import xxhash64, lit
from pyspark.sql.functions import collect_list, struct
from pyspark.sql import Row
from pyspark.sql.functions import (
    xxhash64,
    lit,
    abs as ps_abs,
    regexp_extract,
    col,
    concat,
)


class MetadataMappingLogic:

    def __init__(
        self, spark_session: SparkSession, catalog: str, schema: str, table: str
    ):
        self.metadata_mapping_repository: MetadataMappingRepository = (
            MetadataMappingRepository(
                spark_session=spark_session, catalog=catalog, schema=schema, table=table
            )
        )
        self.spark_session = spark_session

    def create_metadata_tables(self):
        try:
            self.metadata_mapping_repository.create_metadata_table()
            self.metadata_mapping_repository.create_metadata_joined_view()
        except Exception as e:
            print(f"Error creating metadata table: {e}")

    def get_metadata_table(self) -> DataFrame:
        return self.metadata_mapping_repository.get_metadata_table()

    def get_metadata_view(self) -> DataFrame:
        return self.metadata_mapping_repository.get_metadata_view()

    def get_metadata_az_storage(self) -> List[Row]:
        return (
            self.get_metadata_view()
            .select(
                collect_list(struct("az_storage_account", "az_container_name")).alias(
                    "combinations"
                )
            )
            .collect()[0]["combinations"]
        )

    def refresh_metadata_table(self, catalog: Catalog):
        # Flatten the nested structure
        rows = [
            {
                "uc_catalog_id": catalog.uc_id,
                "uc_schema_id": schema.uc_id,
                "uc_table_id": table.uc_id,
                "uc_catalog_name": catalog.uc_name,
                "uc_schema_name": schema.uc_name,
                "uc_table_name": table.uc_name,
                "table_location": table.location,
                "table_type": table.table_type,
            }
            for schema in catalog.schemas
            for table in schema.tables
        ]

        # Create Spark DataFrame
        df_updates: DataFrame = (
            self.spark_session.createDataFrame(rows)
            .withColumn(
                "dbx_sf_uniform_metadata_id",
                ps_abs(
                    xxhash64(
                        col("uc_catalog_id"), col("uc_schema_id"), col("uc_table_id")
                    )
                ),
            )
            .withColumn(
                "az_storage_account",
                regexp_extract(col("table_location"), r"@([^\.]+)", 1),
            )
            .withColumn(
                "az_container_name",
                regexp_extract(col("table_location"), r"abfss://([^@]+)", 1),
            )
            .withColumn(
                "snowflake_external_volume",
                concat(
                    lit("az_dbx_uc_extvol_"),
                    ps_abs(xxhash64(col("az_storage_account"), col("az_container_name"))),
                ),
            )
            .withColumn(
                "snowflake_catalog_integration",
                concat(
                    lit("az_dbx_uc_catint_"),
                    ps_abs(xxhash64(col("uc_catalog_id"), col("uc_schema_id"))),
                ),
            )
            .withColumn("last_sync_dated", lit(None))
        )

        try:
            self.metadata_mapping_repository.upsert_metadata_table(df_updates)
        except Exception as e:
            print(f"Error updating metadata table: {e}")
