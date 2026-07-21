"""
Snowflake Native Streamlit App - MDLH Object Store Sync
Integrates Snowflake with the Atlan Lakehouse WITHOUT the Iceberg REST
catalog: Iceberg tables are created from an object-store external volume
and their metadata pointers are kept current from the pointer files that
Atlan publishes at <catalog-root>/_latest/ (see mdlh PR #704 / LH-1940).

Handles the full lifecycle: bootstrap (external volume, catalog
integration, stage, scheduled sync task), sync (plan + apply), and
teardown. S3 only for now; GCS and Azure ADLS are planned.
"""

import json
import re

import streamlit as st
import pandas as pd
from typing import Dict, List

# ============================================================================
# Sync core
#
# Shared between the Streamlit app and the scheduled stored procedure: the
# string below is exec'd here AND embedded verbatim in the procedure body,
# so the interactive sync and the scheduled sync can never drift apart.
# ============================================================================

SYNC_CORE = r'''
import json

BASE_PREFIX = "atlan_mdlh"
DB_BASE_NAME = "atlan_context_store"

def set_environment(env=""):
    """Bind every resource name for one environment (empty = default).

    The database is atlan_context_store; every other resource keeps the
    atlan_mdlh_ prefix. An optional environment suffix (e.g. 'prod' ->
    atlan_context_store_prod / atlan_mdlh_prod_*) allows several independent
    setups in one Snowflake account. The app rediscovers environments from
    the marker comment on the database, so nothing has to be re-typed.
    """
    global ENV, PREFIX, DB, ADMIN_SCHEMA, CONFIG_TABLE, STAGE, FILE_FORMAT
    global SYNC_PROCEDURE, SYNC_TASK, EXTERNAL_VOLUME, CATALOG_INTEGRATION
    global STORAGE_INTEGRATION
    ENV = env or ""
    PREFIX = BASE_PREFIX + (f"_{ENV}" if ENV else "")
    DB = DB_BASE_NAME + (f"_{ENV}" if ENV else "")
    ADMIN_SCHEMA = f"{PREFIX}_admin"
    CONFIG_TABLE = f"{DB}.{ADMIN_SCHEMA}.{PREFIX}_config"
    STAGE = f"{DB}.{ADMIN_SCHEMA}.{PREFIX}_pointer_stage"
    FILE_FORMAT = f"{DB}.{ADMIN_SCHEMA}.{PREFIX}_json_format"
    SYNC_PROCEDURE = f"{DB}.{ADMIN_SCHEMA}.{PREFIX}_sync"
    SYNC_TASK = f"{DB}.{ADMIN_SCHEMA}.{PREFIX}_sync_task"
    EXTERNAL_VOLUME = f"{PREFIX}_external_volume"
    CATALOG_INTEGRATION = f"{PREFIX}_catalog_integration"
    STORAGE_INTEGRATION = f"{PREFIX}_storage_integration"

set_environment("")

INDEX_PATH = "_latest/_index.json"
INDEX_TYPE = "atlan-lh-object-store-metadata-index"
POINTER_TYPE = "atlan-lh-object-store-metadata-pointer"
FORMAT_VERSION = 1

def quote_ident(name):
    """Quote a Snowflake identifier, escaping embedded double quotes."""
    return '"' + str(name).replace('"', '""') + '"'

def sql_literal(value):
    """Escape a value for use inside a single-quoted SQL string literal."""
    return str(value).replace("'", "''")

def schema_for_namespace(namespace):
    """Map a (possibly dotted) Iceberg namespace to a Snowflake schema name."""
    return namespace.replace(".", "__")

def table_fqn(namespace, table):
    return f"{DB}.{quote_ident(schema_for_namespace(namespace))}.{quote_ident(table)}"

def read_stage_json(session, relative_path):
    """Read one JSON document from the pointer stage."""
    path = relative_path.lstrip("/")
    rows = session.sql(f"SELECT $1 FROM @{STAGE}/{sql_literal(path)}").collect()
    if not rows:
        raise ValueError(f"missing or empty file on stage: {path}")
    doc = rows[0][0]
    return json.loads(doc) if isinstance(doc, str) else doc

def load_config(session):
    rows = session.sql(f"SELECT key, value FROM {CONFIG_TABLE}").collect()
    return {row[0]: row[1] for row in rows}

def current_metadata_location(session, fqn):
    """metadataLocation the Snowflake table currently points at."""
    rows = session.sql(
        f"SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('{sql_literal(fqn)}')"
    ).collect()
    return json.loads(rows[0][0]).get("metadataLocation")

def relative_metadata_path(metadata_location, base_uri):
    """Path of metadata_location relative to the external volume base URI."""
    if not metadata_location.startswith(base_uri):
        raise ValueError(
            f"metadataLocation {metadata_location} is outside the configured "
            f"base URI {base_uri} — check that the base URI matches the "
            f"scheme, bucket, and path in the pointer files exactly"
        )
    return metadata_location[len(base_uri):]

def list_existing_tables(session, namespace):
    """Names of Iceberg tables already registered in the namespace schema."""
    schema = schema_for_namespace(namespace)
    try:
        rows = session.sql(
            f"SHOW ICEBERG TABLES IN SCHEMA {DB}.{quote_ident(schema)}"
        ).collect()
    except Exception as e:
        # Only a missing schema means "no tables yet". Anything else must
        # fail the plan: treating it as empty would turn every REFRESH into
        # a CREATE IF NOT EXISTS, which no-ops on existing tables and lets
        # them silently go stale while the sync reports success.
        if "does not exist" in str(e).lower():
            return set()
        raise
    return {row["name"] for row in rows}

def plan_sync(session, config):
    """Compare the pointer files against Snowflake and build an action list.

    Read-only. Returns dicts with keys: action (CREATE | REFRESH | UP_TO_DATE
    | DROP_ORPHAN | ERROR), namespace, table, detail, sql.
    """
    base_uri = config["base_uri"]
    actions = []

    index = read_stage_json(session, INDEX_PATH)
    if index.get("type") != INDEX_TYPE:
        raise ValueError(f"unexpected index file type: {index.get('type')}")
    if int(index.get("formatVersion", 0)) > FORMAT_VERSION:
        raise ValueError(
            f"pointer format version {index.get('formatVersion')} is newer than "
            f"this app supports ({FORMAT_VERSION}); please update the app"
        )

    for ns_entry in index.get("namespaces", []):
        namespace = ns_entry["namespace"]
        try:
            pointer = read_stage_json(session, ns_entry["pointerFile"])
        except Exception as e:
            actions.append({"action": "ERROR", "namespace": namespace, "table": "",
                            "detail": f"could not read pointer file: {e}", "sql": ""})
            continue
        if pointer.get("type") != POINTER_TYPE:
            actions.append({"action": "ERROR", "namespace": namespace, "table": "",
                            "detail": f"unexpected pointer file type: {pointer.get('type')}",
                            "sql": ""})
            continue

        existing = list_existing_tables(session, namespace)
        seen = set()
        for tp in pointer.get("tables", []):
            name = tp["table"]
            seen.add(name)
            fqn = table_fqn(namespace, name)
            try:
                rel = relative_metadata_path(tp["metadataLocation"], base_uri)
            except ValueError as e:
                actions.append({"action": "ERROR", "namespace": namespace,
                                "table": name, "detail": str(e), "sql": ""})
                continue
            if name not in existing:
                actions.append({
                    "action": "CREATE", "namespace": namespace, "table": name,
                    "detail": f"register table at metadata version {tp.get('metadataVersion')}",
                    "sql": (
                        f"CREATE ICEBERG TABLE IF NOT EXISTS {fqn} "
                        f"EXTERNAL_VOLUME = '{EXTERNAL_VOLUME}' "
                        f"CATALOG = '{CATALOG_INTEGRATION}' "
                        f"METADATA_FILE_PATH = '{sql_literal(rel)}'"
                    ),
                })
            else:
                try:
                    current = current_metadata_location(session, fqn)
                except Exception as e:
                    actions.append({
                        "action": "ERROR", "namespace": namespace, "table": name,
                        "detail": f"could not read current metadata location: {e}",
                        "sql": "",
                    })
                    continue
                if current == tp["metadataLocation"]:
                    actions.append({"action": "UP_TO_DATE", "namespace": namespace,
                                    "table": name, "detail": "", "sql": ""})
                else:
                    actions.append({
                        "action": "REFRESH", "namespace": namespace, "table": name,
                        "detail": f"advance pointer to metadata version {tp.get('metadataVersion')}",
                        "sql": f"ALTER ICEBERG TABLE {fqn} REFRESH '{sql_literal(rel)}'",
                    })

        for orphan in sorted(existing - seen):
            actions.append({
                "action": "DROP_ORPHAN", "namespace": namespace, "table": orphan,
                "detail": "registered in Snowflake but absent from the pointer file",
                "sql": f"DROP ICEBERG TABLE IF EXISTS {table_fqn(namespace, orphan)}",
            })

    return actions

def apply_actions(session, config, actions, drop_orphans=None, on_progress=None):
    """Execute a plan. Returns dicts with keys: action, namespace, table,
    status (APPLIED | FAILED | SKIPPED), message."""
    if drop_orphans is None:
        drop_orphans = str(config.get("drop_orphans", "false")).lower() == "true"

    executable = [a for a in actions if a["action"] in ("CREATE", "REFRESH", "DROP_ORPHAN")]
    results = []
    ensured_schemas = set()
    for idx, action in enumerate(executable):
        if on_progress:
            on_progress(idx + 1, len(executable), action["table"])
        if action["action"] == "DROP_ORPHAN" and not drop_orphans:
            results.append({**_result_key(action), "status": "SKIPPED",
                            "message": "orphan cleanup is disabled"})
            continue
        try:
            if action["action"] == "CREATE":
                schema = schema_for_namespace(action["namespace"])
                if schema not in ensured_schemas:
                    session.sql(
                        f"CREATE SCHEMA IF NOT EXISTS {DB}.{quote_ident(schema)}"
                    ).collect()
                    ensured_schemas.add(schema)
            session.sql(action["sql"]).collect()
            results.append({**_result_key(action), "status": "APPLIED", "message": ""})
        except Exception as e:
            results.append({**_result_key(action), "status": "FAILED", "message": str(e)})
    return results

def _result_key(action):
    return {"action": action["action"], "namespace": action["namespace"],
            "table": action["table"]}
'''

exec(SYNC_CORE, globals())

# ============================================================================
# Configuration
# ============================================================================

st.set_page_config(
    page_title="MDLH Object Store Sync",
    layout="wide"
)

AWS_ROLE_ARN_PATTERN = re.compile(r"^arn:aws(-[a-z]+)?:iam::\d{12}:role/.+$")
ENV_NAME_PATTERN = re.compile(r"^[a-z0-9_]{1,20}$")

# Marker comment on the environment database; used to rediscover environments
DB_MARKER_COMMENT = "atlan-mdlh-object-store-sync"

# Provider registry — S3 implemented; add builders here for GCS / Azure ADLS
PROVIDERS = {
    "S3 (AWS)": "s3",
    "GCS (Google Cloud) - coming soon": None,
    "Azure ADLS - coming soon": None,
}

# ============================================================================
# Connection helpers
# ============================================================================

def get_snowflake_connection():
    """Get the active Snowpark session (native Streamlit app)."""
    try:
        from snowflake.snowpark.context import get_active_session
        session = get_active_session()
        if session is not None:
            return session
    except Exception:
        pass

    try:
        if hasattr(st, 'connection'):
            return st.connection("snowflake").session()
    except Exception:
        pass

    st.error("Could not establish Snowflake connection.")
    st.info("This app must be deployed as a native Snowflake Streamlit app to work properly.")
    return None

def execute_statements(conn, statements: List[str], on_progress=None,
                       stop_on_error: bool = True) -> List[Dict]:
    """Run DDL statements in order; stop at the first failure by default."""
    results = []
    for idx, statement in enumerate(statements):
        if on_progress:
            on_progress(idx + 1, len(statements), statement)
        try:
            conn.sql(statement).collect()
            results.append({"statement": statement, "status": "OK", "message": ""})
        except Exception as e:
            results.append({"statement": statement, "status": "FAILED", "message": str(e)})
            if stop_on_error:
                break
    return results

def try_load_config(conn) -> Dict:
    try:
        return load_config(conn)
    except Exception:
        return {}

def discover_environments(conn) -> List[str]:
    """Environment suffixes of every bootstrap in this account, found via the
    marker comment on the environment database ('' = the default one)."""
    try:
        rows = conn.sql("SHOW DATABASES").collect()
    except Exception:
        return []
    envs = []
    for row in rows:
        if row["comment"] != DB_MARKER_COMMENT:
            continue
        name = str(row["name"]).lower()
        if name == DB_BASE_NAME:
            envs.append("")
        elif name.startswith(DB_BASE_NAME + "_"):
            envs.append(name[len(DB_BASE_NAME) + 1:])
    return sorted(set(envs))

def list_warehouses(conn) -> List[str]:
    try:
        return [row["name"] for row in conn.sql("SHOW WAREHOUSES").collect()]
    except Exception:
        return []

# ============================================================================
# Bootstrap SQL builders (S3)
# ============================================================================

def s3_external_volume_sql(base_uri: str, role_arn: str, external_id: str) -> str:
    external_id_line = (
        f"        STORAGE_AWS_EXTERNAL_ID = '{sql_literal(external_id)}'\n"
        if external_id else ""
    )
    return (
        f"CREATE EXTERNAL VOLUME IF NOT EXISTS {EXTERNAL_VOLUME}\n"
        f"    STORAGE_LOCATIONS = ((\n"
        f"        NAME = '{PREFIX}_s3_location'\n"
        f"        STORAGE_PROVIDER = 'S3'\n"
        f"        STORAGE_BASE_URL = '{sql_literal(base_uri)}'\n"
        f"        STORAGE_AWS_ROLE_ARN = '{sql_literal(role_arn)}'\n"
        f"{external_id_line}"
        f"    ))\n"
        f"    ALLOW_WRITES = FALSE"
    )

def s3_storage_integration_sql(base_uri: str, role_arn: str, external_id: str) -> str:
    external_id_line = (
        f"    STORAGE_AWS_EXTERNAL_ID = '{sql_literal(external_id)}'\n"
        if external_id else ""
    )
    return (
        f"CREATE STORAGE INTEGRATION IF NOT EXISTS {STORAGE_INTEGRATION}\n"
        f"    TYPE = EXTERNAL_STAGE\n"
        f"    STORAGE_PROVIDER = 'S3'\n"
        f"    ENABLED = TRUE\n"
        f"    STORAGE_AWS_ROLE_ARN = '{sql_literal(role_arn)}'\n"
        f"{external_id_line}"
        f"    STORAGE_ALLOWED_LOCATIONS = ('{sql_literal(base_uri)}')"
    )

def sync_procedure_sql() -> str:
    # Pin the procedure to its environment (ENV is validated to [a-z0-9_])
    body = SYNC_CORE + (
        f"\nset_environment('{ENV}')\n"
        "\n"
        "def run(session):\n"
        "    config = load_config(session)\n"
        "    actions = plan_sync(session, config)\n"
        "    results = apply_actions(session, config, actions)\n"
        "    counts = {}\n"
        "    for a in actions:\n"
        "        counts[a['action']] = counts.get(a['action'], 0) + 1\n"
        "    for r in results:\n"
        "        counts[r['status']] = counts.get(r['status'], 0) + 1\n"
        "    failed = [r for r in results if r['status'] == 'FAILED']\n"
        "    return json.dumps({'counts': counts, 'failed': failed[:20]})\n"
    )
    return (
        f"CREATE OR REPLACE PROCEDURE {SYNC_PROCEDURE}()\n"
        f"RETURNS VARCHAR\n"
        f"LANGUAGE PYTHON\n"
        f"RUNTIME_VERSION = '3.11'\n"
        f"PACKAGES = ('snowflake-snowpark-python')\n"
        f"HANDLER = 'run'\n"
        f"EXECUTE AS OWNER\n"
        f"AS\n$$\n{body}$$"
    )

def sync_task_sql(warehouse: str, schedule_minutes: int) -> str:
    """The sync task; serverless when no warehouse is given (the refresh is a
    cloud-services metadata operation, so serverless or XS is sufficient)."""
    if warehouse:
        compute_line = f"    WAREHOUSE = {quote_ident(warehouse)}\n"
    else:
        compute_line = "    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'\n"
    # OR REPLACE so a re-bootstrap picks up a changed schedule or warehouse;
    # the replaced task comes back suspended, which the success message covers
    return (
        f"CREATE OR REPLACE TASK {SYNC_TASK}\n"
        f"{compute_line}"
        f"    SCHEDULE = '{int(schedule_minutes)} MINUTE'\n"
        f"AS CALL {SYNC_PROCEDURE}()"
    )

def bootstrap_statements(base_uri: str, role_arn: str, external_id: str,
                         warehouse: str, schedule_minutes: int,
                         drop_orphans: bool) -> List[str]:
    """Full bootstrap, in dependency order. Every statement is idempotent."""
    return [
        s3_external_volume_sql(base_uri, role_arn, external_id),
        f"CREATE CATALOG INTEGRATION IF NOT EXISTS {CATALOG_INTEGRATION}\n"
        f"    CATALOG_SOURCE = OBJECT_STORE\n"
        f"    TABLE_FORMAT = ICEBERG\n"
        f"    ENABLED = TRUE",
        s3_storage_integration_sql(base_uri, role_arn, external_id),
        f"CREATE DATABASE IF NOT EXISTS {DB} COMMENT = '{DB_MARKER_COMMENT}'",
        f"CREATE SCHEMA IF NOT EXISTS {DB}.{ADMIN_SCHEMA}",
        f"CREATE FILE FORMAT IF NOT EXISTS {FILE_FORMAT} TYPE = JSON",
        f"CREATE STAGE IF NOT EXISTS {STAGE}\n"
        f"    URL = '{sql_literal(base_uri)}'\n"
        f"    STORAGE_INTEGRATION = {STORAGE_INTEGRATION}\n"
        f"    FILE_FORMAT = {FILE_FORMAT}",
        f"CREATE TABLE IF NOT EXISTS {CONFIG_TABLE} (key VARCHAR, value VARCHAR)",
        f"DELETE FROM {CONFIG_TABLE}",
        f"INSERT INTO {CONFIG_TABLE} (key, value) VALUES\n"
        f"    ('base_uri', '{sql_literal(base_uri)}'),\n"
        f"    ('provider', 'S3'),\n"
        f"    ('drop_orphans', '{'true' if drop_orphans else 'false'}')",
        sync_procedure_sql(),
        sync_task_sql(warehouse, schedule_minutes),
    ]

def fetch_trust_info(conn) -> List[Dict]:
    """IAM values the customer shares with Atlan for the role trust policy."""
    info = []
    try:
        rows = conn.sql(f"DESC EXTERNAL VOLUME {EXTERNAL_VOLUME}").collect()
        for row in rows:
            if row["property"].startswith("STORAGE_LOCATION_"):
                loc = json.loads(row["property_value"])
                info.append({
                    "resource": EXTERNAL_VOLUME,
                    "iam_user_arn": loc.get("STORAGE_AWS_IAM_USER_ARN", ""),
                    "external_id": loc.get("STORAGE_AWS_EXTERNAL_ID", ""),
                })
    except Exception as e:
        info.append({"resource": EXTERNAL_VOLUME, "iam_user_arn": f"error: {e}",
                     "external_id": ""})
    try:
        rows = conn.sql(f"DESC STORAGE INTEGRATION {STORAGE_INTEGRATION}").collect()
        props = {row["property"]: row["property_value"] for row in rows}
        info.append({
            "resource": STORAGE_INTEGRATION,
            "iam_user_arn": props.get("STORAGE_AWS_IAM_USER_ARN", ""),
            "external_id": props.get("STORAGE_AWS_EXTERNAL_ID", ""),
        })
    except Exception as e:
        info.append({"resource": STORAGE_INTEGRATION, "iam_user_arn": f"error: {e}",
                     "external_id": ""})
    return info

# ============================================================================
# UI: Bootstrap tab
# ============================================================================

def render_bootstrap_tab(conn):
    st.header("Bootstrap")
    st.markdown(
        "Creates every Snowflake resource the integration needs — the "
        "`atlan_context_store` database plus `atlan_mdlh_`-prefixed resources: "
        "an external volume and object-store catalog integration for the "
        "Iceberg tables, a storage integration and stage for reading the "
        "pointer files, and a stored procedure plus scheduled task for the "
        "periodic sync. The base URI and IAM role are provided by Atlan."
    )

    provider_label = st.selectbox("Storage provider", options=list(PROVIDERS.keys()),
                                  key="provider")
    if PROVIDERS[provider_label] != "s3":
        st.info("Only S3 is supported at the moment. GCS and Azure ADLS are planned.")
        return

    env = st.text_input(
        "Resource name suffix (optional)",
        key="bootstrap_env",
        help="Leave blank for the default names (atlan_context_store database, "
             "atlan_mdlh_* resources). A suffix like 'prod' names every "
             "resource with it (atlan_context_store_prod, atlan_mdlh_prod_*), "
             "so several independent setups can coexist in one account.",
    ).strip().lower()
    if env and not ENV_NAME_PATTERN.match(env):
        st.warning("Suffix must be 1-20 characters: a-z, 0-9, underscore.")
        return
    set_environment(env)

    col1, col2 = st.columns(2)
    with col1:
        base_uri = st.text_input(
            "S3 base URI (catalog root, provided by Atlan)",
            placeholder="s3://<bucket>/<catalog-root>/",
            key="base_uri",
        ).strip()
        role_arn = st.text_input(
            "IAM role ARN (provided by Atlan)",
            placeholder="arn:aws:iam::123456789012:role/atlan-mdlh-reader",
            key="role_arn",
        ).strip()
        external_id = st.text_input(
            "External ID (optional, if agreed with Atlan)",
            key="external_id",
            help="Set only if Atlan pre-configured the role trust policy with a "
                 "fixed external ID; otherwise Snowflake generates one and you "
                 "share it with Atlan after bootstrap.",
        ).strip()
    with col2:
        serverless_label = "Serverless (recommended)"
        warehouse_choice = st.selectbox(
            "Warehouse for the scheduled sync task",
            options=[serverless_label] + list_warehouses(conn),
            key="task_warehouse",
            help="The refresh is a metadata-only operation, so a serverless "
                 "task (requires the EXECUTE MANAGED TASK privilege) or an "
                 "XS warehouse is sufficient.",
        )
        warehouse = "" if warehouse_choice == serverless_label else warehouse_choice
        schedule_minutes = st.number_input(
            "Sync interval (minutes)", min_value=5, max_value=1440, value=120,
            key="schedule_minutes",
            help="Atlan publishes pointer files every 2 hours by default. "
                 "Syncing more often is safe (a refresh to the current "
                 "metadata file is a no-op) but adds no freshness.",
        )
        drop_orphans = st.checkbox(
            "Scheduled sync drops orphaned tables",
            value=False,
            key="bootstrap_drop_orphans",
            help="When enabled, the scheduled sync drops Snowflake tables that "
                 "no longer appear in the pointer files. Interactive sync asks "
                 "separately.",
        )

    problems = []
    if base_uri and (not base_uri.startswith("s3://") or not base_uri.endswith("/")):
        problems.append("Base URI must start with s3:// and end with a trailing slash.")
    if role_arn and not AWS_ROLE_ARN_PATTERN.match(role_arn):
        problems.append("IAM role ARN does not look like arn:aws:iam::<account>:role/<name>.")
    for problem in problems:
        st.warning(problem)

    ready = bool(base_uri and role_arn) and not problems
    if not ready:
        st.info("Fill in the base URI and IAM role ARN to continue.")
        return

    statements = bootstrap_statements(base_uri, role_arn, external_id,
                                      warehouse, schedule_minutes, drop_orphans)

    with st.expander("Preview SQL", expanded=False):
        for statement in statements:
            st.code(statement + ";", language="sql")

    if st.button("Run Bootstrap", type="primary", key="bootstrap_btn"):
        progress_bar = st.progress(0)
        progress_text = st.empty()

        def _on_progress(done, total, statement):
            progress_text.text(f"Running statement {done}/{total}...")
            progress_bar.progress(done / total)

        results = execute_statements(conn, statements, _on_progress)
        progress_bar.empty()
        progress_text.empty()
        # Keyed by suffix so results (and the trust panel) never render
        # against a different setup than the one that was bootstrapped
        st.session_state.bootstrap_results = {"env": env, "results": results}

    saved = st.session_state.get("bootstrap_results")
    if saved and saved.get("env") == env:
        results = saved["results"]
        failed = [r for r in results if r["status"] == "FAILED"]
        if failed:
            st.error(f"Bootstrap stopped at statement {len(results)} of {len(statements)}.")
            st.code(failed[0]["statement"], language="sql")
            st.error(failed[0]["message"])
        else:
            st.success(
                f"Bootstrap complete: {len(results)} statements executed. The sync "
                f"task is created suspended; enable it from the Scheduled Sync tab "
                f"once access is verified."
            )
            render_trust_section(conn)

def render_trust_section(conn):
    st.subheader("Share with Atlan")
    st.markdown(
        "Atlan must add these Snowflake-generated values to the IAM role's trust "
        "policy before Snowflake can read the bucket. Send both rows to your "
        "Atlan contact, then use **Verify Access**."
    )
    trust = pd.DataFrame(fetch_trust_info(conn))
    trust.columns = ["Resource", "Snowflake IAM User ARN", "External ID"]
    st.table(trust.reset_index(drop=True))

    if st.button("Verify Access", key="verify_btn"):
        volume_ok, stage_ok = True, True
        try:
            row = conn.sql(
                f"SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('{EXTERNAL_VOLUME}')"
            ).collect()[0]
            st.text(f"External volume: {row[0]}")
        except Exception as e:
            volume_ok = False
            st.error(f"External volume verification failed: {e}")
        try:
            files = conn.sql(f"LIST @{STAGE}/_latest/").collect()
            st.text(f"Pointer stage: found {len(files)} file(s) under _latest/")
            if not files:
                stage_ok = False
                st.warning(
                    "No pointer files found. Confirm with Atlan that the "
                    "metadata pointer workflow is enabled for your tenant."
                )
        except Exception as e:
            stage_ok = False
            st.error(f"Pointer stage verification failed: {e}")
        if volume_ok and stage_ok:
            st.success("Access verified. You can run a sync from the Sync tab.")

# ============================================================================
# UI: Sync tab
# ============================================================================

def render_sync_tab(conn, env):
    set_environment(env)
    st.header("Sync")
    config = try_load_config(conn)
    if not config.get("base_uri"):
        st.info("No configuration found. Run Bootstrap first.")
        return

    st.markdown(
        f"Compares the pointer files under `{config['base_uri']}_latest/` with "
        f"the tables registered in `{DB}` and builds a plan: **CREATE** missing "
        f"tables, **REFRESH** tables whose metadata pointer moved, and flag "
        f"**orphans** that disappeared from the pointer files."
    )

    if st.button("Plan Sync", type="primary", key="plan_btn"):
        with st.spinner("Reading pointer files and comparing table state..."):
            try:
                st.session_state.sync_plan = plan_sync(conn, config)
                st.session_state.sync_results = []
            except Exception as e:
                st.session_state.sync_plan = None
                st.error(f"Planning failed: {e}")

    plan = st.session_state.get("sync_plan")
    if plan is not None:
        counts = {}
        for action in plan:
            counts[action["action"]] = counts.get(action["action"], 0) + 1
        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("Create", counts.get("CREATE", 0))
        col2.metric("Refresh", counts.get("REFRESH", 0))
        col3.metric("Up to date", counts.get("UP_TO_DATE", 0))
        col4.metric("Orphans", counts.get("DROP_ORPHAN", 0))
        col5.metric("Errors", counts.get("ERROR", 0))

        pending = [a for a in plan if a["action"] != "UP_TO_DATE"]
        if pending:
            df = pd.DataFrame(pending)[["action", "namespace", "table", "detail"]]
            df.columns = ["Action", "Namespace", "Table", "Detail"]
            st.table(df.reset_index(drop=True))
            with st.expander("Preview SQL", expanded=False):
                for action in pending:
                    if action["sql"]:
                        st.code(action["sql"] + ";", language="sql")
        else:
            st.success("Everything is up to date; nothing to apply.")

        actionable = [a for a in plan if a["action"] in ("CREATE", "REFRESH", "DROP_ORPHAN")]
        if actionable:
            drop_orphans = st.checkbox(
                "Also drop orphaned tables",
                value=False,
                key="sync_drop_orphans",
            ) if counts.get("DROP_ORPHAN") else False

            if st.button("Apply Plan", type="primary", key="apply_btn"):
                progress_bar = st.progress(0)
                progress_text = st.empty()

                def _on_progress(done, total, table):
                    progress_text.text(f"Applying {done}/{total}: {table}...")
                    progress_bar.progress(done / total)

                st.session_state.sync_results = apply_actions(
                    conn, config, plan,
                    drop_orphans=drop_orphans,
                    on_progress=_on_progress,
                )
                progress_bar.empty()
                progress_text.empty()

    results = st.session_state.get("sync_results")
    if results:
        st.divider()
        st.subheader("Results")
        df = pd.DataFrame(results)[["action", "namespace", "table", "status", "message"]]
        df.columns = ["Action", "Namespace", "Table", "Status", "Message"]
        st.table(df.reset_index(drop=True))
        failed = sum(1 for r in results if r["status"] == "FAILED")
        if failed:
            st.error(f"{failed} action(s) failed; see messages above.")
        else:
            st.success("All actions applied successfully.")

# ============================================================================
# UI: Scheduled sync tab
# ============================================================================

def render_scheduled_tab(conn, env):
    set_environment(env)
    st.header("Scheduled Sync")
    st.markdown(
        f"A Snowflake task (`{SYNC_TASK}`) calls the sync procedure on a fixed "
        f"schedule so table pointers stay current without anyone opening this "
        f"app. The task is created suspended; resume it once access is verified."
    )

    try:
        tasks = conn.sql(
            f"SHOW TASKS LIKE '{PREFIX}_sync_task' IN SCHEMA {DB}.{ADMIN_SCHEMA}"
        ).collect()
    except Exception:
        tasks = []
    if not tasks:
        st.info("Sync task not found. Run Bootstrap first.")
        return

    task = tasks[0]
    col1, col2, col3 = st.columns(3)
    col1.metric("State", task["state"])
    col2.metric("Schedule", task["schedule"])
    col3.metric("Warehouse", task["warehouse"] or "serverless")

    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("Resume Task", key="resume_task_btn"):
            conn.sql(f"ALTER TASK {SYNC_TASK} RESUME").collect()
            st.rerun()
    with col2:
        if st.button("Suspend Task", key="suspend_task_btn"):
            conn.sql(f"ALTER TASK {SYNC_TASK} SUSPEND").collect()
            st.rerun()
    with col3:
        if st.button("Run Sync Now", key="run_now_btn"):
            with st.spinner("Running sync procedure..."):
                try:
                    row = conn.sql(f"CALL {SYNC_PROCEDURE}()").collect()[0]
                    st.json(json.loads(row[0]))
                except Exception as e:
                    st.error(f"Sync failed: {e}")

    st.subheader("Recent Runs")
    try:
        history = conn.sql(
            f"SELECT scheduled_time, state, completed_time, return_value, error_message "
            f"FROM TABLE({DB}.INFORMATION_SCHEMA.TASK_HISTORY("
            f"TASK_NAME => '{PREFIX.upper()}_SYNC_TASK')) "
            f"ORDER BY scheduled_time DESC LIMIT 20"
        ).collect()
        if history:
            df = pd.DataFrame([row.as_dict() for row in history])
            st.table(df.reset_index(drop=True))
        else:
            st.info("No task runs yet.")
    except Exception as e:
        st.warning(f"Could not read task history: {e}")

# ============================================================================
# UI: Teardown tab
# ============================================================================

def render_teardown_tab(conn, env):
    set_environment(env)
    st.header("Teardown")
    st.markdown(
        f"Removes everything the app created: the `{DB}` database (including all "
        f"registered Iceberg tables, the stage, the sync procedure and task), the "
        f"catalog integration, the external volume, and the storage integration."
    )
    st.warning(
        "Iceberg tables here are external: dropping them removes the Snowflake "
        "registrations only — no data or metadata in the object store is touched. "
        "This action cannot be undone on the Snowflake side."
    )

    confirmation = st.text_input(
        f"Type {DB} to confirm", key="teardown_confirm"
    ).strip()
    if st.button("Drop All Resources", type="primary", key="teardown_btn",
                 disabled=confirmation != DB):
        statements = [
            f"ALTER TASK IF EXISTS {SYNC_TASK} SUSPEND",
            f"DROP DATABASE IF EXISTS {DB}",
            f"DROP CATALOG INTEGRATION IF EXISTS {CATALOG_INTEGRATION}",
            f"DROP EXTERNAL VOLUME IF EXISTS {EXTERNAL_VOLUME}",
            f"DROP STORAGE INTEGRATION IF EXISTS {STORAGE_INTEGRATION}",
        ]
        # Keep going past failures so a partially-torn-down setup (e.g. the
        # database already dropped) still releases the account-level
        # integrations; "does not exist" just means already gone
        results = execute_statements(conn, statements, stop_on_error=False)
        failed = [r for r in results if r["status"] == "FAILED"
                  and "does not exist" not in r["message"].lower()]
        if failed:
            st.error(f"{len(failed)} statement(s) failed:")
            for failure in failed:
                st.code(failure["statement"], language="sql")
                st.error(failure["message"])
        else:
            st.success("All resources dropped.")

# ============================================================================
# Main App
# ============================================================================

def main():
    if 'sync_plan' not in st.session_state:
        st.session_state.sync_plan = None
    if 'sync_results' not in st.session_state:
        st.session_state.sync_results = []

    st.title("MDLH Object Store Sync")
    st.markdown(
        "**Query Atlan Lakehouse Iceberg tables from Snowflake via object "
        "storage — no REST catalog required**"
    )

    conn = get_snowflake_connection()
    if conn is None:
        return

    # Environments are rediscovered from the marker comment on their database,
    # so a custom-named setup is found again without re-typing anything
    environments = discover_environments(conn)
    if environments:
        labels = ["default" if e == "" else e for e in environments]
        choice = st.selectbox(
            "Setup", options=labels, key="environment_choice",
            help="Setups are distinguished by their resource name suffix.",
        )
        selected_env = "" if choice == "default" else choice
    else:
        selected_env = ""

    if st.session_state.get("active_env") != selected_env:
        st.session_state.active_env = selected_env
        st.session_state.sync_plan = None
        st.session_state.sync_results = []

    bootstrap_tab, sync_tab, scheduled_tab, teardown_tab = st.tabs(
        ["Bootstrap", "Sync", "Scheduled Sync", "Teardown"]
    )
    with bootstrap_tab:
        render_bootstrap_tab(conn)
    with sync_tab:
        render_sync_tab(conn, selected_env)
    with scheduled_tab:
        render_scheduled_tab(conn, selected_env)
    with teardown_tab:
        render_teardown_tab(conn, selected_env)


if __name__ == "__main__":
    main()
