import logging
from typing import List
from pyspark.sql import Row, SparkSession, DataFrame
from pyspark.sql.functions import (
    abs as ps_abs,
    col,
    concat,
    current_timestamp,
    lit,
    struct,
    xxhash64,
    collect_list,
)
from config.logging_config import setup_logging  # Import logging setup configuration
from data_models.data_models import Catalog
from repository.metadata.metadata_mapping_repository import MetadataMappingRepository

# Initialize logging using the configured settings
setup_logging()

# Create a logger for this module
logger = logging.getLogger("dbx_to_sf_mirror")


class MetadataMappingLogic:
    """
    Logic class for handling metadata mapping between Databricks and Snowflake.
    This class provides methods to create, refresh, and query metadata tables.
    """

    def __init__(
        self, spark_session: SparkSession, catalog: str, schema: str, table: str
    ):
        """
        Initialize MetadataMappingLogic.

        Args:
            spark_session (SparkSession): Spark session object.
            catalog (str): Catalog name.
            schema (str): Schema name.
            table (str): Table name.
        """
        self.metadata_mapping_repository: MetadataMappingRepository = (
            MetadataMappingRepository(
                spark_session=spark_session, catalog=catalog, schema=schema, table=table
            )
        )
        self.spark_session = spark_session

    def create_metadata_tables(self):
        """
        Create metadata table and joined view in the repository.
        Catches any exception and logs it.
        """
        try:
            self.metadata_mapping_repository.create_metadata_table()
            self.metadata_mapping_repository.create_metadata_joined_view()
        except Exception as e:
            logger.error(f"Error creating metadata table: {e}")

    def get_metadata_table(self) -> DataFrame:
        """
        Get the metadata table from the repository.

        Returns:
            DataFrame: Metadata table as a Spark DataFrame.
        """
        return self.metadata_mapping_repository.get_metadata_table()

    def get_metadata_view(self) -> DataFrame:
        """
        Get the metadata view from the repository.

        Returns:
            DataFrame: Metadata view as a Spark DataFrame.
        """
        return self.metadata_mapping_repository.get_metadata_view()

    def get_metadata_sf_catalog_integration(self) -> List[Row]:
        """
        Get a list of catalog and schema combinations where the values are not null.

        Filters:
            - snowflake_catalog_integration, uc_catalog_name, and uc_schema_name should not be null.

        Returns:
            List[Row]: A list of unique catalog and schema combinations.
        """
        return (
            self.get_metadata_view()
            .filter(
                col("snowflake_catalog_integration").isNotNull()
                & col("uc_catalog_name").isNotNull()
                & col("uc_schema_name").isNotNull()
            )
            .select(
                col("snowflake_catalog_integration"),
                col("uc_catalog_name"),
                col("uc_schema_name"),
            )
            .distinct()  # Ensure distinct rows BEFORE aggregation
            .select(
                collect_list(
                    struct(
                        "snowflake_catalog_integration",
                        "uc_catalog_name",
                        "uc_schema_name",
                    )
                ).alias("combinations")
            )
            .collect()[0]["combinations"]
        )

    def get_metadata_tables(self) -> List[Row]:
        """
        Get a list of table combinations where the metadata is valid.

        Filters:
            - snowflake_uniform_sync must be True.
            - None of the required fields should be null.

        Returns:
            List[Row]: A list of unique table combinations.
        """
        return (
            self.get_metadata_view()
            .filter(
                (col("snowflake_uniform_sync") == True)
                & col("snowflake_catalog_integration").isNotNull()
                & col("uc_table_name").isNotNull()
                & col("snowflake_database").isNotNull()
                & col("snowflake_schema").isNotNull()
                & col("snowflake_table").isNotNull()
            )
            .distinct()  # Ensure distinct combinations
            .select(
                collect_list(
                    struct(
                        "dbx_sf_uniform_metadata_id",
                        "snowflake_catalog_integration",
                        "uc_table_name",
                        "snowflake_database",
                        "snowflake_schema",
                        "snowflake_table",
                    )
                ).alias("combinations")
            )
            .collect()[0]["combinations"]
        )

    def refresh_metadata_table(self, catalog: Catalog):
        """
        Refresh the metadata table by flattening and updating with new data.

        Args:
            catalog (Catalog): Catalog object containing schema and table details.
        """
        # Flatten the nested structure of the catalog object into rows
        rows = [
            {
                "uc_catalog_id": catalog.uc_id,
                "uc_schema_id": schema.uc_id,
                "uc_table_id": table.uc_id,
                "uc_catalog_name": catalog.uc_name,
                "uc_schema_name": schema.uc_name,
                "uc_table_name": table.uc_name,
                "table_location": table.location,
                "table_type": table.table_type,
            }
            for schema in catalog.schemas
            for table in schema.tables
        ]

        # Create Spark DataFrame from the flattened rows
        df_updates: DataFrame = (
            self.spark_session.createDataFrame(rows)
            # Generate a unique hash for identifying records
            .withColumn(
                "dbx_sf_uniform_metadata_id",
                ps_abs(
                    xxhash64(
                        col("uc_catalog_id"), col("uc_schema_id"), col("uc_table_id")
                    )
                ),
            )
            # Create a Snowflake catalog integration string based on hash values
            .withColumn(
                "snowflake_catalog_integration",
                concat(
                    lit("dbx_uc_catint_"),
                    ps_abs(xxhash64(col("uc_catalog_id"), col("uc_schema_id"))),
                ),
            )
            # Add a column for the last sync date (initially set to None)
            .withColumn("last_sync_dated", lit(None))
        )

        try:
            # Upsert the metadata table with the new DataFrame
            self.metadata_mapping_repository.upsert_metadata_table(df_updates)
        except Exception as e:
            logger.error(f"Error updating metadata table: {e}")

    def update_metadata_last_sync_date(self, metadata_ids: List[str]):
        """
        Update the last sync date in the metadata table

        Args:
            metadata_ids ( List[str]): List of IDs to update
        """
        rows = [
            {
                "dbx_sf_uniform_metadata_id": metadata_id,
            }
            for metadata_id in metadata_ids
        ]

        # Create Spark DataFrame from the flattened rows
        df_updates: DataFrame = self.spark_session.createDataFrame(rows).withColumn(
            "last_sync_dated", current_timestamp()
        )
        try:
            logger.info(f"Updating last sync date...")
            # Upsert the metadata table with the new DataFrame
            self.metadata_mapping_repository.update_last_sync_dated(df_updates)
            logger.info(f"Updating last sync date completed...")
        except Exception as e:
            logger.error(f"Error updating last sync date: {e}")
