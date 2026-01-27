# Databricks notebook to setup the MDLH catalog
# MAGIC %pip install pyiceberg
import os
import logging
import sys
from typing import List, Tuple
from pyiceberg.catalog import load_catalog
from pyiceberg.table import TableIdentifier
from pyspark.sql import SparkSession

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# COMMAND ----------

# Databricks notebook source
# -----------------------------
# Configuration
# -----------------------------
CLIENT_ID = os.getenv("CLIENT_ID", "<polaris-reader-client-id>")
CLIENT_SECRET = os.getenv("CLIENT_SECRET", "<polaris-reader-client-secret>")
POLARIS_CATALOG_URI = os.getenv("POLARIS_CATALOG_URI", "https://<tenant-domain>.atlan.com/api/polaris/api/catalog")
CATALOG_NAME = os.getenv("CATALOG_NAME", "<polaris-catalog-name>")
WAREHOUSE_NAME = os.getenv("WAREHOUSE_NAME", "<polaris-warehouse-name>")
DBX_CATALOG_NAME = os.getenv("DBX_CATALOG_NAME", "<target-dbx-catalog-name>")

# Flag to control history sync
# default is Skipping atlan-history. please set to true to enable history tables sync
HISTORY_NAMESPACE_SYNC = os.getenv("HISTORY_NAMESPACE_SYNC", "false").lower() == "true"

# COMMAND ----------

# Databricks notebook source
# -----------------------------
# Polaris SQL Reader
# -----------------------------
class PolarisSQLReader:
    def __init__(self):
        self.catalog = None
        self.catalog_name = None
        self.warehouse_name = None

    def detect_catalog(self) -> Tuple[str, str]:
        """
        Detect catalog and warehouse dynamically.
        Prefer 'atlan-wh', fallback to 'context_store'.
        """
        logger.info("Detecting Polaris catalog...")

        for candidate in ["atlan-wh", "context_store"]:
            try:
                test_catalog = load_catalog(
                    candidate,
                    uri=POLARIS_CATALOG_URI,
                    warehouse=candidate,
                    credential=f"{CLIENT_ID}:{CLIENT_SECRET}",
                    scope="PRINCIPAL_ROLE:ALL"
                )
                namespaces = test_catalog.list_namespaces()
                if namespaces:
                    logger.info(f"Using catalog: {candidate}")
                    return candidate, candidate
            except Exception:
                logger.info(f"Catalog not accessible: {candidate}")

        raise RuntimeError("No valid Polaris catalog found")

    def connect_to_catalog(self):
        if not self.catalog:
            self.catalog_name, self.warehouse_name = self.detect_catalog()
            self.catalog = load_catalog(
                self.catalog_name,
                uri=POLARIS_CATALOG_URI,
                warehouse=self.warehouse_name,
                credential=f"{CLIENT_ID}:{CLIENT_SECRET}",
                scope="PRINCIPAL_ROLE:ALL"
            )
            logger.info(f"Connected to Polaris catalog: {self.catalog_name}")

    def list_namespaces(self) -> List[str]:
        self.connect_to_catalog()
        namespaces = self.catalog.list_namespaces()
        logger.info(f"Found {len(namespaces)} namespaces: {namespaces}")
        return [ns[0] if isinstance(ns, tuple) else ns for ns in namespaces]

    def list_tables(self, namespace: str) -> List[TableIdentifier]:
        self.connect_to_catalog()
        return self.catalog.list_tables(namespace)

# COMMAND ----------

# Databricks notebook source
# -----------------------------
# Helpers
# -----------------------------
def get_metadata_path(table) -> str:
    if not table.metadata_location:
        raise ValueError(f"No metadata location for {table.identifier}")
    return table.metadata_location

# COMMAND ----------

# Databricks notebook source
# -----------------------------
# Main
# -----------------------------
def main():
    logger.info("Starting Polaris → Unity Catalog sync")

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.databricks.delta.uniform.readIcebergEnabled", "true")

    reader = PolarisSQLReader()
    namespaces = reader.list_namespaces()

    for namespace in namespaces:
        if namespace == "atlan-history" and not HISTORY_NAMESPACE_SYNC:
            logger.info("Skipping atlan-history (flag disabled)")
            continue

        logger.info(f"Processing namespace: {namespace}")

        # 1. Create schema
        try:
            spark.sql(
                f"CREATE SCHEMA IF NOT EXISTS {DBX_CATALOG_NAME}.`{namespace}`"
            )
        except Exception as e:
            logger.error(f"❌ Failed creating schema {namespace}: {e}")
            continue

        # 2. Create tables
        try:
            tables = reader.list_tables(namespace)
        except Exception as e:
            logger.error(f"❌ Failed listing tables for {namespace}: {e}")
            continue

        for table_identifier in tables:
            try:
                table = reader.catalog.load_table(table_identifier)
                metadata_path = get_metadata_path(table)
                table_name = table_identifier[-1]

                full_table = f"{DBX_CATALOG_NAME}.`{namespace}`.`{table_name}`"

                sql = f"""
                CREATE TABLE IF NOT EXISTS {full_table}
                UNIFORM ICEBERG
                METADATA_PATH '{metadata_path}'
                """

                logger.info(f"Creating table:\n{sql}")
                spark.sql(sql)
                logger.info(f"✅ Created table: {full_table}")

            except Exception as e:
                logger.error(
                    f"❌ Failed table {namespace}.{table_identifier[-1]}: {e}"
                )
                continue
    logger.info("✅ Sync completed for all namespaces")

# COMMAND ----------

# Databricks notebook source
# -----------------------------
# Run
# -----------------------------
if __name__ == "__main__":
    main()
