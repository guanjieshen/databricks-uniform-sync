import yaml
from data_models.data_models import Schema, Catalog


class YamlLogic:
    def __init__(self):
        self.path = "mapping_configs"

    def __save_yaml(self, data: dict, filename: str):
        with open(f"{self.path}/{filename}.yaml", "w") as file:
            yaml.dump(data, file, sort_keys=False, default_flow_style=False)

        print("YAML file generated successfully!")

    def __catalog_to_dict(self, catalog: Catalog) -> dict:
        return {
            "catalog": {
                "uc_id": catalog.uc_id,
                "uc_name": catalog.uc_name,
                "sf_name": catalog.sf_name,
                "schemas": [
                    {
                        "uc_id": schema.uc_id,
                        "uc_name": schema.uc_name,
                        "sf_name": schema.sf_name,
                        "tables": [
                            {
                                "uc_id": table.uc_id,
                                "uc_name": table.uc_name,
                                "sf_name": table.sf_name,
                                "location": table.location
                            }
                            for table in schema.tables
                        ],
                    }
                    for schema in catalog.schemas
                ],
            }
        }

    def __schema_to_dict(self, schema: Schema) -> dict:
        return {
            "schema": {
                "uc_id": schema.uc_id,
                "uc_name": schema.uc_name,
                "sf_name": schema.sf_name,
                "tables": [
                    {
                        "uc_id": table.uc_id,
                        "uc_name": table.uc_name,
                        "sf_name": table.sf_name,
                        "location": table.location
                    }
                    for table in schema.tables
                ],
            }
        }

    def generate_yaml_file(self, catalog: Catalog, catalog_name:str, schema_name:str = None,file_name: str = None) -> None:
        # Convert the Catalog object to a dictionary
        catalog_dict = self.__catalog_to_dict(catalog)

        if schema_name:
            file_name = f"catalog_{catalog_name}_schema_{schema_name}"
        else:
            file_name = f"catalog_{catalog_name}"
        # Generate and save the YAML file
        self.__save_yaml(catalog_dict, file_name)

    def generate_catalog_yaml_file(self, catalog: Catalog, file_name: str) -> None:
        # Convert the Catalog object to a dictionary
        catalog_dict = self.__catalog_to_dict(catalog)

        catalog_file_name = f"catalog_{file_name}"

        # Generate and save the YAML file
        self.__save_yaml(catalog_dict, catalog_file_name)

    def generate_schema_yaml_file(self, schema: Schema, file_name: str) -> None:
        # Convert the Catalog object to a dictionary
        catalog_dict = self.__schema_to_dict(schema)

        schema_file_name = f"schema_{file_name}"

        # Generate and save the YAML file
        self.__save_yaml(catalog_dict, schema_file_name)
