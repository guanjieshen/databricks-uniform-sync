from pyspark.sql import SparkSession
from metadata_mapping.metadata_mapping_repository import MetadataMappingRepository


class MetadataMappingLogic:

    def __init__(
        self, spark_session: SparkSession, catalog: str, schema: str, table: str
    ):
        self.metadata_mapping_repository = MetadataMappingRepository(
            spark_session=spark_session, catalog=catalog, schema=schema, table=table
        )

    def create_metadata_table(self):
        try:
            self.metadata_mapping_repository.create_metadata_table()
        except Exception as e:
            print(f"Error creating metadata table: {e}")
