from pyspark.sql import SparkSession as spark
from metadata_mapping.metadata_mapping_repository import MetadataMappingRepository
class MetadataMappingLogic:

    def __init__(self):
        self.metadata_mapping_repository = MetadataMappingRepository()

    def create_metadata_table(self,catalog: str, schema: str):
        try:
            self.metadata_mapping_repository.create_metadata_table(catalog, schema)
        except Exception as e:
            print(f"Error creating metadata table: {e}")


