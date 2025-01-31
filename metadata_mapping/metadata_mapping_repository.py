from pyspark.sql import SparkSession, DataFrame
from delta.tables import *
import logging


class MetadataMappingRepository:

    def __init__(
        self, spark_session: SparkSession, catalog: str, schema: str, table: str
    ):
        self.spark_session: SparkSession = spark_session
        self.catalog = catalog
        self.schema = schema
        self.table = table

    def create_metadata_table(self):
        try:
            sql_text = f"""
                        CREATE TABLE IF NOT EXISTS  `{self.catalog}`.`{self.schema}`.`{self.table}` (
                        dbx_sf_uniform_metadata_id LONG,
                        uc_catalog_id STRING,
                        uc_catalog_name STRING,
                        uc_schema_id STRING,
                        uc_schema_name STRING,
                        uc_table_id STRING,
                        uc_table_name STRING,
                        table_location STRING,
                        table_type STRING,
                        last_sync_dated TIMESTAMP)
                        USING delta
                        COMMENT 'The`dbx_sf_uniform_metadata` table contains metadata information. 

                        This table is managed by the `DatabricksToSnowflakeMirror` library.'
                    """
            self.spark_session.sql(sqlQuery=sql_text)
            logging.info(f"Metadata table `{self.catalog}`.`{self.schema}`.`{self.table}` confirmed.")
        except Exception as e:
            print(f"Error creating metadata table: {e}")

    def create_metadata_joined_view(self):
        try:
            sql_text = f"""
                        CREATE VIEW IF NOT EXISTS `{self.catalog}`.`{self.schema}`.`{self.table}_vw`
                        COMMENT 'The `dbx_sf_uniform_metadata` table contains metadata information. This table is managed by the `DatabricksToSnowflakeMirror` library.' 
                        AS(
                        SELECT
                            a.*,
                            REGEXP_EXTRACT(a.table_location, '@([^\\.]+)', 1) AS az_storage_account,
                            REGEXP_EXTRACT(a.table_location, 'abfss://([^@]+)', 1) AS az_container_name,
                            p.snowflake_external_volume,
                            p.snowflake_catalog_integration,
                            p.snowflake_database,
                            p.snowflake_schema,
                            p.snowflake_table,
                            p.snowflake_uniform_sync
                            FROM
                            `{self.catalog}`.`{self.schema}`.`{self.table}` a
                            LEFT JOIN (
                                SELECT
                                catalog_name,
                                schema_name,
                                table_name,
                                MAX(CASE WHEN tag_name = 'snowflake_external_volume' THEN tag_value END) AS snowflake_external_volume,
                                MAX(CASE WHEN tag_name = 'snowflake_catalog_integration' THEN tag_value END) AS snowflake_catalog_integration,
                                MAX(CASE WHEN tag_name = 'snowflake_database' THEN tag_value END) AS snowflake_database,
                                MAX(CASE WHEN tag_name = 'snowflake_schema' THEN tag_value END) AS snowflake_schema,
                                MAX(CASE WHEN tag_name = 'snowflake_table' THEN tag_value END) AS snowflake_table,
                                MAX(CASE WHEN tag_name = 'snowflake_uniform_sync' THEN tag_value END) AS snowflake_uniform_sync
                                FROM
                                system.information_schema.table_tags
                                GROUP BY
                                catalog_name,
                                schema_name,
                                table_name
                            ) p
                            ON
                            a.uc_catalog_name = p.catalog_name
                            AND a.uc_schema_name = p.schema_name
                            AND a.uc_table_name = p.table_name
                        )
                    """
            self.spark_session.sql(sqlQuery=sql_text)
            logging.info(f"Metadata view `{self.catalog}`.`{self.schema}`.`{self.table}` confirmed.")
        except Exception as e:
            print(f"Error creating metadata table: {e}")

    def get_metadata_table(self) -> DataFrame:
        return self.spark_session.sql(
            f"SELECT * FROM `{self.catalog}`.`{self.schema}`.`{self.table}`"
        )
    
    def get_metadata_view(self) -> DataFrame:
            return self.spark_session.sql(
                f"SELECT * FROM `{self.catalog}`.`{self.schema}`.`{self.table}_vw`"
            )

    def upsert_metadata_table(self, df_updates: DataFrame):
        metadata_table = DeltaTable.forName(
            self.spark_session, f"`{self.catalog}`.`{self.schema}`.`{self.table}`"
        )
        (
            metadata_table.alias("target")
            .merge(
                df_updates.alias("updates"),
                "target.dbx_sf_uniform_metadata_id = updates.dbx_sf_uniform_metadata_id",
            )
            .whenMatchedUpdate(
                set={
                    "uc_catalog_name": "updates.uc_catalog_name",
                    "uc_schema_name": "updates.uc_schema_name",
                    "uc_table_name": "updates.uc_table_name",
                    "table_location": "updates.table_location",
                    "table_type": "updates.table_type"
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
