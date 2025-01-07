from pyspark.sql import SparkSession
from metadata_mapping.metadata_mapping_repository import MetadataMappingRepository


class MetadataMappingLogic:

    def __init__(self, spark_session: SparkSession):
        self.metadata_mapping_repository = MetadataMappingRepository(
            spark_session=spark_session
        )

    def create_metadata_table(self, catalog: str, schema: str, table: str):
        try:
            self.metadata_mapping_repository.create_metadata_table(catalog, schema, table)
        except Exception as e:
            print(f"Error creating metadata table: {e}")
