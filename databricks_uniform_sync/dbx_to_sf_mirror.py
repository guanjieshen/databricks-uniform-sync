from typing import List
from databricks_unity_catalog.logic_uc_mapping import UCMappingLogic
from data_models.data_models import Catalog, Schema
from databricks_unity_catalog.logic_yaml import YamlLogic
from metadata_mapping.metdata_mapping_logic import MetadataMappingLogic
from pyspark.sql import SparkSession


class DatabricksToSnowflakeMirror:
    def __init__(
        self,
        spark_session: SparkSession,
        dbx_workspace_url: str,
        dbx_workspace_pat: str,
    ):
        self.dbx_workspace_url = dbx_workspace_url
        self.dbx_workspace_pat = dbx_workspace_pat
        self.spark_session = spark_session

        # Create an instance of the MappingLogic class and YamlLogic class
        self.metadata_mapping_logic = MetadataMappingLogic(spark_session=self.spark_session)

    def create_metadata_tables(self, catalog_name: str, schema_name: str):
        # Create metadata tables
        self.metadata_mapping_logic.create_metadata_table(
            catalog=catalog_name, schema=schema_name
        )
