from pyspark.sql import SparkSession, DataFrame
from data_models.data_models import Catalog
from metadata_mapping.metadata_mapping_repository import MetadataMappingRepository
from pyspark.sql.functions import xxhash64, lit, current_timestamp


class MetadataMappingLogic:

    def __init__(
        self, spark_session: SparkSession, catalog: str, schema: str, table: str
    ):
        self.metadata_mapping_repository:MetadataMappingRepository = MetadataMappingRepository(
            spark_session=spark_session, catalog=catalog, schema=schema, table=table
        )
        self.spark_session = spark_session

    def create_metadata_tables(self):
        try:
            self.metadata_mapping_repository.create_metadata_table()
            self.metadata_mapping_repository.create_metadata_joined_view()
        except Exception as e:
            print(f"Error creating metadata table: {e}")

    def get_metadata_table(self)->DataFrame:
        return self.metadata_mapping_repository.get_metadata_table()
    
    def get_metadata_view(self)->DataFrame:
        return self.metadata_mapping_repository.get_metadata_view()


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
                "table_location": table.location
            }
            for schema in catalog.schemas
            for table in schema.tables
        ]

        # Create Spark DataFrame
        df_updates = (
            self.spark_session.createDataFrame(rows)
            .withColumn(
                "dbx_sf_uniform_metadata_id",
                xxhash64("uc_catalog_id", "uc_schema_id", "uc_table_id"),
            )
            .withColumn("last_sync_dated", lit(None))
        )

        try:
            self.metadata_mapping_repository.upsert_metadata_table(df_updates)
        except Exception as e:
            print(f"Error updating metadata table: {e}")
