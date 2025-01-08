from databricks_unity_catalog.repository_unity_catalog import UnityCatalogRepository
from pyspark.sql import SparkSession


class UCTagsLogic:
    def __init__(
        self, spark_session: SparkSession, workspace_url: str, bearer_token: str
    ):
        self.repository_unity_catalog = UnityCatalogRepository(
            spark_session=spark_session,
            workspace_url=workspace_url,
            bearer_token=bearer_token,
        )

    def add_uc_metadata_tags(self, catalog: str, schema: str, table: str):
        self.repository_unity_catalog.set_tags(
            catalog_name=catalog, schema_name=schema, table_name=table
        )
