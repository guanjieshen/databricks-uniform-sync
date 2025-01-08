from pyspark.sql import SparkSession, DataFrame
from delta.tables import *


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
                        last_sync_dated TIMESTAMP)
                        USING delta
                        COMMENT 'The`dbx_sf_uniform_metadata` table contains metadata information. 

                        This table is managed by the `DatabricksToSnowflakeMirror` library.'
                    """
            self.spark_session.sql(sqlQuery=sql_text)
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
                            b.tags
                        FROM
                            `{self.catalog}`.`{self.schema}`.`{self.table}` a
                            LEFT JOIN (
                            SELECT
                                catalog_name,
                                schema_name,
                                table_name,
                                COLLECT_LIST(
                                NAMED_STRUCT('tag_name', tag_name, 'tag_value', tag_value) 
                                ) as tags
                            FROM
                                system.information_schema.table_tags
                            GROUP BY
                                catalog_name,
                                schema_name,
                                table_name
                            ) b on a.uc_catalog_name = b.catalog_name
                            and a.uc_schema_name = b.schema_name
                            and a.uc_table_name = b.table_name
                        )
                    """
            self.spark_session.sql(sqlQuery=sql_text)
        except Exception as e:
            print(f"Error creating metadata table: {e}")

    def get_metadata_table(self) -> DataFrame:
        return self.spark_session.sql(
            f"SELECT * FROM `{self.catalog}`.`{self.schema}`.`{self.table}`"
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
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
