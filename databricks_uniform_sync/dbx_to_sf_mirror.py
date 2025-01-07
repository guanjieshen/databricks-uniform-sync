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
        metadata_catalog: str,
        metadata_schema: str,
        metadata_table: str = "dbx_sf_uniform_metadata",
    ):
        self.dbx_workspace_url: str = dbx_workspace_url
        self.dbx_workspace_pat: str = dbx_workspace_pat
        self.spark_session: SparkSession = spark_session
        self.metadata_catalog: str = metadata_catalog
        self.metadata_schema: str = metadata_schema
        self.metadata_table: str = metadata_table

        # Create an instance of the MappingLogic class and YamlLogic class
        self.metadata_mapping_logic: MetadataMappingLogic = MetadataMappingLogic(
            spark_session=self.spark_session
        )
        self.uc_mapping_logic: UCMappingLogic = UCMappingLogic(
            workspace_url=self.dbx_workspace_url, bearer_token=self.dbx_workspace_pat
        )

    def create_metadata_tables(self):
        # Create metadata tables
        self.metadata_mapping_logic.create_metadata_table(
            catalog=self.metadata_catalog,
            schema=self.metadata_schema,
            table=self.metadata_table,
        )

    def refresh_uc_metadata(self, catalog, schema=None, table=None):
        # Ensure the metadata table is created
        self.create_metadata_tables()

        catalog_hierarchy: Catalog = self.uc_mapping_logic.build_hierarchy_for_catalog(
            catalog_name=catalog, schemas_include=schema, include_empty_schemas=False
        )
