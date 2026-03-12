"""
PySpark + Iceberg connection verification script for AWS S3.

Connects to the Polaris REST catalog, then:
  1. Lists all namespaces
  2. Counts the number of tables in each namespace

── pip installs ──────────────────────────────────────────────────────────────
    pip install pyspark==3.5.5
    pip install "pyarrow>=15.0.0"

    Java 11 or 17 must be installed and JAVA_HOME must be set:
        brew install openjdk@17
        export JAVA_HOME=$(brew --prefix openjdk@17)

    The Iceberg / Hadoop JARs are downloaded automatically by Spark on
    first run via spark.jars.packages (requires internet access, cached in
    ~/.ivy2 afterwards)

── usage ─────────────────────────────────────────────────────────────────────
    python pyspark_lakehouse_s3.py

── environment variables ─────────────────────────────────────────────────────
    TENANT_DOMAIN          e.g. https://example.atlan.com
    POLARIS_READER_ID
    POLARIS_READER_SECRET
    READER_ROLE_NAME       default: ALL
    WAREHOUSE_NAME         default: context_store
    CATALOG_NAME           default: context_store
    S3_REGION              default: us-east-1

S3 access is granted via Polaris vended credentials — no AWS keys required.
"""

import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# Silence noisy Java / PySpark loggers
for name in ("py4j", "py4j.java_gateway", "pyspark"):
    logging.getLogger(name).setLevel(logging.WARNING)

# ── Settings (inlined so the script is self-contained) ────────────────────────

ATLAN_DOMAIN        = os.getenv("TENANT_DOMAIN", os.getenv("ATLAN_DOMAIN", ""))
POLARIS_CATALOG_URI = f"{ATLAN_DOMAIN}/api/polaris/api/catalog"
POLARIS_AUTH_URI    = os.getenv(
    "POLARIS_CATALOG_AUTH_URI",
    f"{ATLAN_DOMAIN}/api/polaris/api/catalog/v1/oauth/tokens",
)

POLARIS_READER_ID     = os.getenv("POLARIS_READER_ID", "")
POLARIS_READER_SECRET = os.getenv("POLARIS_READER_SECRET", "")
POLARIS_READER_CREDS  = f"{POLARIS_READER_ID}:{POLARIS_READER_SECRET}"
READER_ROLE_NAME      = os.getenv("READER_ROLE_NAME", "ALL")

CATALOG_NAME  = os.getenv("CATALOG_NAME",  "context_store")
WAREHOUSE     = os.getenv("WAREHOUSE_NAME","context_store")
AWS_REGION    = os.getenv("S3_REGION",     "us-east-1")

SPARK_DRIVER_MEMORY          = os.getenv("SPARK_DRIVER_MEMORY",          "4G")
SPARK_DRIVER_MEMORY_OVERHEAD = os.getenv("SPARK_DRIVER_MEMORY_OVERHEAD", "1G")
SPARK_EXECUTOR_MEMORY        = os.getenv("SPARK_EXECUTOR_MEMORY",        "4G")
SPARK_EXECUTOR_MEMORY_OVERHEAD = os.getenv("SPARK_EXECUTOR_MEMORY_OVERHEAD", "1G")
LOGICAL_CORES_COUNT          = int(os.getenv("LOGICAL_CORES_COUNT", "4"))

NUM_OF_COMMIT_RETRY       = int(os.getenv("NUM_OF_COMMIT_RETRY",        "5"))
COMMIT_RETRY_MIN_WAIT_MS  = int(os.getenv("COMMIT_RETRY_MIN_WAIT_MS",  "100"))
COMMIT_RETRY_MAX_WAIT_MS  = int(os.getenv("COMMIT_RETRY_MAX_WAIT_MS", "5000"))


# ── Spark session ─────────────────────────────────────────────────────────────

def create_spark_session():
    """Create a SparkSession wired to the Polaris REST catalog on AWS S3."""
    from pyspark.sql import SparkSession

    if not ATLAN_DOMAIN:
        raise ValueError("TENANT_DOMAIN (or ATLAN_DOMAIN) environment variable must be set")
    if not POLARIS_READER_ID or not POLARIS_READER_SECRET:
        raise ValueError("POLARIS_READER_ID and POLARIS_READER_SECRET must be set")

    os.environ["AWS_REGION"] = AWS_REGION

    logger.info("Building SparkSession")
    logger.info("  Catalog URI : %s", POLARIS_CATALOG_URI)
    logger.info("  Auth URI    : %s", POLARIS_AUTH_URI)
    logger.info("  Catalog     : %s", CATALOG_NAME)
    logger.info("  Warehouse   : %s", WAREHOUSE)
    logger.info("  AWS region  : %s", AWS_REGION)

    builder = (
        SparkSession.builder
        .appName("pyspark-lakehouse-s3-verify")
        .master("local[*]")

        # ── Memory ────────────────────────────────────────────────────────────
        .config("spark.driver.memory",            SPARK_DRIVER_MEMORY)
        .config("spark.driver.memoryOverhead",    SPARK_DRIVER_MEMORY_OVERHEAD)
        .config("spark.executor.memory",          SPARK_EXECUTOR_MEMORY)
        .config("spark.executor.memoryOverhead",  SPARK_EXECUTOR_MEMORY_OVERHEAD)
        .config("spark.sql.shuffle.partitions",   LOGICAL_CORES_COUNT)
        .config("spark.default.parallelism",      LOGICAL_CORES_COUNT)

        # ── Iceberg extensions ─────────────────────────────────────────────────
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.iceberg.spark.SparkSessionCatalog")

        # ── JARs (S3 only — no Azure / GCP bundles) ───────────────────────────
        .config("spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,"
                "org.apache.hadoop:hadoop-common:3.4.0,"
                "org.apache.iceberg:iceberg-aws-bundle:1.9.2")
        .config("spark.jars.excludes",
                "com.sun.jersey:jersey-server,"
                "com.sun.jersey:jersey-core,"
                "com.sun.jersey:jersey-servlet")

        # ── S3A filesystem ─────────────────────────────────────────────────────
        .config("spark.hadoop.fs.s3.impl",  "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3n.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "15000")
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.region",   AWS_REGION)

        # ── Polaris REST catalog ───────────────────────────────────────────────
        .config(f"spark.sql.catalog.{CATALOG_NAME}",
                "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.type",             "rest")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.uri",              POLARIS_CATALOG_URI)
        .config(f"spark.sql.catalog.{CATALOG_NAME}.oauth2-server-uri",POLARIS_AUTH_URI)
        .config(f"spark.sql.catalog.{CATALOG_NAME}.io-impl",
                "org.apache.iceberg.io.ResolvingFileIO")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.credential",       POLARIS_READER_CREDS)
        .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse",        WAREHOUSE)
        .config(f"spark.sql.catalog.{CATALOG_NAME}.scope",
                f"PRINCIPAL_ROLE:{READER_ROLE_NAME}")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.token-refresh-enabled", "true")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.header.X-Iceberg-Access-Delegation",
                "vended-credentials")

        # ── Retry ─────────────────────────────────────────────────────────────
        .config(f"spark.sql.catalog.{CATALOG_NAME}.commit.retry.num-retries",
                NUM_OF_COMMIT_RETRY)
        .config(f"spark.sql.catalog.{CATALOG_NAME}.commit.retry.min-wait-ms",
                COMMIT_RETRY_MIN_WAIT_MS)
        .config(f"spark.sql.catalog.{CATALOG_NAME}.commit.retry.max-wait-ms",
                COMMIT_RETRY_MAX_WAIT_MS)

        .config("spark.sql.session.timeZone", "UTC")
    )

    # S3 credentials are handled entirely by Polaris vended-credentials.
    # No static AWS keys or web-identity token assumption needed.

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created")
    return spark


# ── Verification queries ──────────────────────────────────────────────────────

def list_namespaces_and_table_counts(spark):
    """List all namespaces and the number of Iceberg tables in each."""
    sep = "=" * 65

    # ── 1. List namespaces ────────────────────────────────────────────────────
    logger.info("\n%s", sep)
    logger.info("NAMESPACES IN CATALOG: %s", CATALOG_NAME)
    logger.info(sep)

    ns_df = spark.sql(f"SHOW NAMESPACES IN `{CATALOG_NAME}`")
    namespaces = [row[0] for row in ns_df.collect()]

    if not namespaces:
        logger.warning("No namespaces found — check credentials and warehouse name")
        return

    logger.info("Found %d namespace(s):", len(namespaces))
    for ns in namespaces:
        logger.info("  - %s", ns)

    # ── 2. Count tables per namespace ─────────────────────────────────────────
    logger.info("\n%s", sep)
    logger.info("TABLE COUNTS PER NAMESPACE")
    logger.info(sep)
    logger.info("%-40s  %s", "Namespace", "Tables")
    logger.info("%-40s  %s", "-" * 40,    "------")

    total = 0
    for ns in namespaces:
        try:
            tables_df = spark.sql(f"SHOW TABLES IN `{CATALOG_NAME}`.`{ns}`")
            count = tables_df.count()
            total += count
            logger.info("%-40s  %d", ns, count)
        except Exception as e:
            logger.warning("%-40s  ERROR: %s", ns, e)

    logger.info("%-40s  %s", "-" * 40, "------")
    logger.info("%-40s  %d", "TOTAL", total)
    logger.info(sep)


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 65)
    logger.info("PySpark Lakehouse S3 — Connection Verification")
    logger.info("=" * 65)
    logger.info("  TENANT_DOMAIN : %s", ATLAN_DOMAIN)
    logger.info("  CATALOG_NAME  : %s", CATALOG_NAME)
    logger.info("  WAREHOUSE     : %s", WAREHOUSE)
    logger.info("  AWS_REGION    : %s", AWS_REGION)
    logger.info("  READER_ID     : %s", POLARIS_READER_ID[:6] + "..." if POLARIS_READER_ID else "NOT SET")

    try:
        spark = create_spark_session()
        list_namespaces_and_table_counts(spark)
        logger.info("\nVerification complete")
    except Exception:
        logger.exception("Verification failed")
        sys.exit(1)
    finally:
        try:
            spark.stop()
        except Exception:
            pass


if __name__ == "__main__":
    main()
