import requests

from databricks_uniform_sync.data_models.data_models import (
    UnityCatalogIcebergSchema,
    UnityCatalogIcebergTables,
)

class IcebergCatalogRepository:
    def __init__(self, workspace_url, bearer_token):
        self.catalog_url = workspace_url + "/api/2.1/unity-catalog/iceberg/v1"
        self.bearer_token = bearer_token
        self.headers = {
            "Authorization": f"Bearer {self.bearer_token}",
            "Accept": "application/json",
        }

    def __generate_api_uri(self, api_endpoint: str) -> str:
        return f"{self.catalog_url}{api_endpoint}"

    def __submit_get_request(self, uri: str):
        resp = requests.get(uri, headers=self.headers)
        return resp

    def list_tables(self, catalog_name, schema_name):
        list_tables_endpoint = (
            f"/catalogs/{catalog_name}/namespaces/{schema_name}/tables"
        )
        url = self.__generate_api_uri(list_tables_endpoint)
        response = self.__submit_get_request(url)

        return UnityCatalogIcebergTables.model_validate_json(response.content)

    def list_schemas(self, catalog_name):
        list_tables_endpoint = f"/catalogs/{catalog_name}/namespaces"
        url = self.__generate_api_uri(list_tables_endpoint)
        response = self.__submit_get_request(url)

        return UnityCatalogIcebergSchema.model_validate_json(response.content)
