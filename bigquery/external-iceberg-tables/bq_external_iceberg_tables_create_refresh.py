import os
import sys
import logging
from typing import List, Tuple

from pyiceberg.catalog import load_catalog
from google.cloud import bigquery

# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------
PROJECT_ID = os.getenv("BQ_PROJECT_ID", "<gcp-project-id>")
BQ_LOCATION = os.getenv("BQ_LOCATION", "<bq-location>")
CONNECTION_ID = os.getenv("BQ_CONNECTION_ID", "<bq-connection-id>")

POLARIS_CATALOG_URI = os.getenv(
    "POLARIS_CATALOG_URI",
    "https://<your-atlan-endpoint>/api/polaris/api/catalog"
)

CLIENT_ID = os.getenv("CLIENT_ID", "polaris_client_id")
CLIENT_SECRET = os.getenv("CLIENT_SECRET", "polaris_client_secret")

# History namespace flag
ENABLE_HISTORY_NAMESPACE_SYNC = (
    os.getenv("ENABLE_HISTORY_NAMESPACE_SYNC", "false").lower() == "true"
)

# --------------------------------------------------
# LOGGING
# --------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# --------------------------------------------------
# HELPERS
# --------------------------------------------------
def bq_safe_dataset(namespace: str) -> str:
    """
    Convert Polaris namespace → BigQuery-safe dataset
    BigQuery does NOT allow hyphens.
    """
    return namespace.replace("-", "_")


def get_metadata_path(table) -> str:
    if not table.metadata_location:
        raise ValueError(f"Table {table.identifier} has no metadata location")
    return table.metadata_location


# --------------------------------------------------
# POLARIS READER
# --------------------------------------------------
class PolarisReader:
    def __init__(self):
        self.catalog = None
        self.catalog_name = None
        self.warehouse_name = None

    def detect_catalog(self):
        """
        Try atlan-wh first, fall back to context_store
        """
        candidates = ["atlan-wh", "context_store"]

        for name in candidates:
            try:
                logger.info(f"Trying Polaris catalog: {name}")
                catalog = load_catalog(
                    name,
                    uri=POLARIS_CATALOG_URI,
                    warehouse=name,
                    credential=f"{CLIENT_ID}:{CLIENT_SECRET}",
                    scope="PRINCIPAL_ROLE:ALL"
                )
                catalog.list_namespaces()
                self.catalog = catalog
                self.catalog_name = name
                self.warehouse_name = name
                logger.info(f"✅ Using Polaris catalog: {name}")
                return
            except Exception as e:
                logger.warning(f"Failed catalog {name}: {str(e)}")

        raise RuntimeError("❌ No valid Polaris catalog found")

    def list_namespaces(self) -> List[str]:
        namespaces = self.catalog.list_namespaces()
        return [ns[0] if isinstance(ns, tuple) else ns for ns in namespaces]

    def list_tables(self, namespace: str):
        return self.catalog.list_tables(namespace)


# --------------------------------------------------
# BIGQUERY OPERATIONS
# --------------------------------------------------
def ensure_dataset(client: bigquery.Client, namespace: str):
    dataset_id = bq_safe_dataset(namespace)
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{dataset_id}")
    dataset_ref.location = BQ_LOCATION

    client.create_dataset(dataset_ref, exists_ok=True)
    logger.info(f"✅ Dataset ready: {dataset_id}")

    return dataset_id


def create_external_iceberg_table(
    client: bigquery.Client,
    dataset_id: str,
    table_name: str,
    metadata_path: str
):
    sql = f"""
    CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.{dataset_id}.{table_name}`
    WITH CONNECTION `projects/{PROJECT_ID}/locations/{BQ_LOCATION}/connections/{CONNECTION_ID}`
    OPTIONS (
      format = "ICEBERG",
      uris = ["{metadata_path}"]
    )
    """

    logger.info(f"Creating table {dataset_id}.{table_name}")
    job = client.query(sql)
    job.result()


# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    logger.info("Starting BigQuery MDLH sync")

    bq_client = bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)

    reader = PolarisReader()
    reader.detect_catalog()

    namespaces = reader.list_namespaces()
    logger.info(f"Found namespaces: {namespaces}")

    for namespace in namespaces:
        if namespace == "atlan-history" and not ENABLE_HISTORY_NAMESPACE_SYNC:
            logger.info("Skipping atlan-history (flag disabled)")
            continue

        try:
            logger.info(f"Processing namespace: {namespace}")
            dataset_id = ensure_dataset(bq_client, namespace)

            tables = reader.list_tables(namespace)

            for identifier in tables:
                try:
                    table = reader.catalog.load_table(identifier)
                    metadata_path = get_metadata_path(table)
                    table_name = identifier[-1]

                    create_external_iceberg_table(
                        bq_client,
                        dataset_id,
                        table_name,
                        metadata_path
                    )

                    logger.info(
                        f"✅ Created {dataset_id}.{table_name}"
                    )

                except Exception as table_err:
                    logger.error(
                        f"❌ Failed table {identifier}: {table_err}"
                    )
                    continue

        except Exception as ns_err:
            logger.error(
                f"❌ Failed namespace {namespace}: {ns_err}"
            )
            continue

    logger.info("✅ BigQuery MDLH sync completed")


# --------------------------------------------------
# ENTRY POINT
# --------------------------------------------------
if __name__ == "__main__":
    main()
