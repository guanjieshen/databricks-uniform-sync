from pyspark.sql import SparkSession


class MetadataMappingRepository:

    def __init__(self, spark_session: SparkSession):
        self.spark_session: SparkSession = spark_session

    def create_metadata_table(self, catalog: str, schema: str, table: str):
        try:
            sql_text = f"""
                        CREATE TABLE IF NOT EXISTS  `{catalog}`.`{schema}`.`{table}` (
                        dbx_sf_uniform_metadata_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 0 INCREMENT BY 1),
                        uc_catalog_id STRING,
                        uc_catalog_name STRING,
                        uc_schema_id STRING,
                        uc_schema_name STRING,
                        uc_table_id STRING,
                        uc_table_name STRING,
                        table_location STRING,
                        sf_database_name STRING,
                        sf_schema_name STRING,
                        sf_table_name STRING,
                        active BOOLEAN,
                        created_date TIMESTAMP,
                        updated_date TIMESTAMP,
                        created_by STRING,
                        updated_by STRING)
                        USING delta
                        COMMENT 'The `dbx_sf_uniform_metadata` table contains metadata information about how tables within Unity Catalog are mirrored within the Snowflake catalog. 

                        This table is managed by the `DatabricksToSnowflakeMirror` library.'
                    """
            self.spark_session.sql(sqlQuery=sql_text)
        except Exception as e:
            print(f"Error creating metadata table: {e}")
