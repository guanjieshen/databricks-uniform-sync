import yaml
from data_models.data_models import Catalog, Schema


# Function to read YAML and map to Pydantic model
def read_catalog_yaml_to_pydantic(file_path: str):
    # Read the YAML file
    with open(file_path, "r") as file:
        yaml_data = yaml.safe_load(file)

    # # Map the YAML data to Pydantic model
    catalog_model:Catalog = Catalog(**yaml_data['catalog'])

    return catalog_model  # Now returning the Pydantic object


# Function to read YAML and map to Pydantic model
def read_schema_yaml_to_pydantic(file_path: str):
    # Read the YAML file
    with open(file_path, "r") as file:
        yaml_data = yaml.safe_load(file)

    # # Map the YAML data to Pydantic model
    schema_model:Schema = Schema(**yaml_data['schema'])

    return schema_model  # Now returning the Pydantic object
