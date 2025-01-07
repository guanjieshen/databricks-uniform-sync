from typing import List
from databricks_unity_catalog.logic_uc_mapping import UCMappingLogic
from data_models.data_models import Catalog, Schema
from databricks_unity_catalog.logic_yaml import YamlLogic
import getpass


class DatabricksToSnowflakeMirror:
    def __init__(self, dbx_workspace_url: str, dbx_workspace_pat: str):
        self.dbx_workspace_url = dbx_workspace_url
        self.dbx_workspace_pat = dbx_workspace_pat

        # Create an instance of the MappingLogic class and YamlLogic class
        self.mapping_logic: UCMappingLogic = UCMappingLogic(
            workspace_url=self.dbx_workspace_url, bearer_token=self.dbx_workspace_pat
        )
        self.yaml_logic = YamlLogic()

    def export_uc_schemas(self, uc_catalog_name: str, uc_schema_names: List[str]):
        catalog: Catalog = self.mapping_logic.build_hierarchy_for_catalog(
            catalog_name=uc_catalog_name, schemas_include=uc_schema_names
        )
        print(catalog)
        # self.yaml_logic.generate_yaml_file(
        #     catalog=catalog,
        #     catalog_name=uc_catalog_name,
        #     schema_name=uc_schema_names[0],
        # )
