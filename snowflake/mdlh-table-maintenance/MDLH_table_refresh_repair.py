"""
Snowflake Native Streamlit App - MDLH Table Refresh Repair
Identifies MDLH Iceberg tables whose auto-refresh has stopped working
(via SYSTEM$AUTO_REFRESH_STATUS) and provides option to repair them
"""

import json
import re

import streamlit as st
import pandas as pd
from typing import List, Dict, Tuple

# ============================================================================
# Configuration
# ============================================================================

st.set_page_config(
    page_title="MDLH Table Refresh Repair",
    page_icon="🔧",
    layout="wide"
)

# ============================================================================
# Helper Functions
# ============================================================================

def initialize_session_state():
    """Initialize session state variables."""
    if 'stale_tables' not in st.session_state:
        st.session_state.stale_tables = []
    if 'repair_results' not in st.session_state:
        st.session_state.repair_results = []

def quote_ident(name: str) -> str:
    """Quote a Snowflake identifier, escaping embedded double quotes."""
    return '"' + name.replace('"', '""') + '"'

def get_snowflake_connection():
    """Get the active Snowpark session (native Streamlit app)."""
    # Canonical method for native Snowflake Streamlit apps
    try:
        from snowflake.snowpark.context import get_active_session
        session = get_active_session()
        if session is not None:
            return session
    except Exception:
        pass

    # Fallback: st.connection wraps the Snowpark session in Streamlit-in-Snowflake
    try:
        if hasattr(st, 'connection'):
            return st.connection("snowflake").session()
    except Exception:
        pass

    st.error("❌ Could not establish Snowflake connection.")
    st.info("💡 This app must be deployed as a native Snowflake Streamlit app to work properly.")
    return None

def execute_query(conn, query: str) -> List[Tuple]:
    """Execute a SQL query and return results."""
    try:
        result = conn.sql(query).collect()
        return [tuple(row) for row in result]
    except Exception as e:
        st.error(f"❌ Query execution error: {str(e)}")
        raise

def list_schemas(conn, database: str) -> List[str]:
    """List all schemas in a database."""
    try:
        # Use INFORMATION_SCHEMA to get exact schema names (case-sensitive)
        query = f"""
        SELECT DISTINCT SCHEMA_NAME
        FROM {quote_ident(database)}.INFORMATION_SCHEMA.SCHEMATA
        WHERE SCHEMA_NAME <> 'INFORMATION_SCHEMA'
        ORDER BY SCHEMA_NAME
        """
        results = execute_query(conn, query)
        
        # Extract schema names from results
        schemas = []
        for row in results:
            schemas.append(str(row[0]))  # SCHEMA_NAME column
        
        return schemas
    except Exception as e:
        st.error(f"❌ Error listing schemas: {str(e)}")
        return []

def list_iceberg_tables(conn, database: str, schema: str, days_threshold: int) -> List[Dict]:
    """List every Iceberg table in a schema, with a LAST_ALTERED staleness flag."""
    try:
        # Use exact schema name match (case-sensitive) as in direct SQL query
        # Escape single quotes in schema name if present
        schema_escaped = schema.replace("'", "''")

        # Only Iceberg tables can be repaired with ALTER ICEBERG TABLE; an empty
        # stale table is still worth repairing, so no ROW_COUNT filter
        query = f"""
        SELECT
            TABLE_NAME,
            LAST_ALTERED,
            ROW_COUNT,
            LAST_ALTERED < DATEADD(day, -{int(days_threshold)}, CURRENT_TIMESTAMP()) AS TIME_STALE
        FROM {quote_ident(database)}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema_escaped}'
          AND IS_ICEBERG = 'YES'
        ORDER BY LAST_ALTERED ASC
        """

        results = execute_query(conn, query)

        return [
            {
                'table_name': row[0],
                'last_altered': row[1],
                'row_count': row[2],
                'time_stale': bool(row[3]),
                'database': database,
                'schema': schema,
            }
            for row in results
        ]
    except Exception as e:
        st.error(f"❌ Error listing Iceberg tables: {str(e)}")
        return []

def check_auto_refresh_status(conn, database: str, schema: str, table_name: str) -> Dict:
    """Check SYSTEM$AUTO_REFRESH_STATUS for a table — the authoritative signal.

    Auto-refresh is broken when executionState is anything other than RUNNING
    or when a failure/error/invalid field is populated. A table whose status
    can't be read is reported as broken so it isn't silently skipped.
    """
    fqn = f'{quote_ident(database)}.{quote_ident(schema)}.{quote_ident(table_name)}'
    fqn_literal = fqn.replace("'", "''")
    query = f"SELECT SYSTEM$AUTO_REFRESH_STATUS('{fqn_literal}')"
    try:
        # Quiet path (no st.error): a per-table failure belongs in the results
        # table, not as a page-level error banner for each of N tables
        rows = conn.sql(query).collect()
        raw = str(rows[0][0])
    except Exception as e:
        return {'state': 'CHECK_FAILED', 'broken': True, 'detail': str(e)}
    try:
        status = json.loads(raw)
    except (TypeError, ValueError):
        return {'state': 'UNPARSEABLE', 'broken': True, 'detail': raw}
    state = str(status.get('executionState', 'UNKNOWN'))
    errors = {
        k: v for k, v in status.items()
        if v not in (None, '', 0, False)
        and any(marker in k.lower() for marker in ('fail', 'error', 'invalid'))
    }
    detail = '; '.join(f'{k}: {v}' for k, v in sorted(errors.items()))
    return {'state': state, 'broken': state != 'RUNNING' or bool(errors), 'detail': detail}

def find_problem_tables(conn, database: str, schema: str, days_threshold: int,
                        use_threshold: bool, on_progress=None) -> List[Dict]:
    """Scan a schema and flag tables whose auto-refresh is broken.

    SYSTEM$AUTO_REFRESH_STATUS is authoritative. When use_threshold is set,
    tables that are merely stale by LAST_ALTERED are merged in and labelled,
    so healthy-but-quiet tables are distinguishable from broken ones.
    """
    tables = list_iceberg_tables(conn, database, schema, days_threshold)
    flagged = []
    total = len(tables)
    for idx, table in enumerate(tables):
        if on_progress:
            on_progress(idx + 1, total, table['table_name'])
        status = check_auto_refresh_status(conn, database, schema, table['table_name'])
        time_stale = use_threshold and table['time_stale']
        if status['broken'] and time_stale:
            flagged_by = 'Status + threshold'
        elif status['broken']:
            flagged_by = 'Auto-refresh status'
        elif time_stale:
            flagged_by = 'Threshold only'
        else:
            continue
        flagged.append({
            **table,
            'execution_state': status['state'],
            'status_detail': status['detail'],
            'broken': status['broken'],
            'flagged_by': flagged_by,
        })
    return flagged

def repair_statements(database: str, schema: str, table_name: str) -> Tuple[str, str, str]:
    """Build the repair statements for a table.

    Manual REFRESH is rejected while AUTO_REFRESH = TRUE, so auto-refresh
    must be disabled first, then re-enabled after the manual refresh.
    """
    fqn = f'{quote_ident(database)}.{quote_ident(schema)}.{quote_ident(table_name)}'
    return (
        f'ALTER ICEBERG TABLE {fqn} SET AUTO_REFRESH = FALSE',
        f'ALTER ICEBERG TABLE {fqn} REFRESH',
        f'ALTER ICEBERG TABLE {fqn} SET AUTO_REFRESH = TRUE',
    )

def repair_table(conn, database: str, schema: str, table_name: str) -> Tuple[bool, str]:
    """Repair a single table by refreshing it and enabling auto-refresh."""
    disable_stmt, refresh_stmt, enable_stmt = repair_statements(database, schema, table_name)
    try:
        execute_query(conn, disable_stmt)
        execute_query(conn, refresh_stmt)
    except Exception as e:
        # Don't leave the table with auto-refresh explicitly disabled — before
        # the repair it was merely suspended, which is a less-broken state
        try:
            execute_query(conn, enable_stmt)
        except Exception:
            return False, (
                f"{e} (auto-refresh could not be re-enabled — run "
                f"{enable_stmt} manually)"
            )
        return False, str(e)
    try:
        execute_query(conn, enable_stmt)
    except Exception as e:
        return False, (
            f"Refresh succeeded but auto-refresh could not be re-enabled: {e} "
            f"(run {enable_stmt} manually)"
        )
    return True, "Success"

# ============================================================================
# Main App
# ============================================================================

def main():
    initialize_session_state()
    
    st.title("🔧 MDLH Table Refresh Repair")
    st.markdown("**Identify and repair MDLH Iceberg tables whose auto-refresh has stopped working**")
    
    # Get Snowflake connection (native app)
    conn = get_snowflake_connection()
    
    if conn is None:
        st.error("❌ Unable to connect to Snowflake. Please check your app configuration.")
        with st.expander("🔍 Troubleshooting", expanded=True):
            st.markdown("""
            ### This app requires a native Snowflake Streamlit environment
            
            **To fix this issue:**
            
            1. **Ensure you're running this as a native Snowflake Streamlit app**
               - This app must be deployed in Snowflake's Native App framework
               - It cannot run as a standalone Streamlit app
            
            2. **Check your deployment:**
               - Verify the app is properly configured in Snowflake
               - Ensure the app has access to Snowpark session
            
            3. **Required imports:**
               - The app needs `snowflake.snowpark.Session` to be available
               - This is automatically available in native Snowflake apps
            
            **If you're testing locally:**
            - This app is designed for native Snowflake deployment only
            - For local testing, you would need to modify the connection method
            """)
        return
    
    # Database and Schema Selection
    st.header("📊 Select Database & Schema")
    
    col1, col2 = st.columns(2)
    
    with col1:
        database_name = st.text_input(
            "Database Name",
            value="ATLAN_CONTEXT_STORE",
            help="Enter the database name to check for stale tables",
            key="database_name"
        ).strip()
        # Mirror Snowflake identifier resolution: a pre-quoted name ("my_db")
        # is matched exactly; a plain name resolves case-insensitively, i.e.
        # it is stored uppercase, so uppercase it before we quote it
        if len(database_name) >= 2 and database_name.startswith('"') and database_name.endswith('"'):
            database_name = database_name[1:-1].replace('""', '"')
        elif re.fullmatch(r'[A-Za-z_][A-Za-z0-9_$]*', database_name):
            database_name = database_name.upper()
    
    with col2:
        # Load schemas when database is provided
        if database_name:
            try:
                with st.spinner("Loading schemas..."):
                    schemas = list_schemas(conn, database_name)
                
                if schemas:
                    selected_schema = st.selectbox(
                        "Select Schema",
                        options=schemas,
                        help="Select the schema to check for stale tables",
                        key="selected_schema"
                    )
                else:
                    st.warning("⚠️ No schemas found in this database")
                    selected_schema = None
            except Exception as e:
                st.error(f"❌ Error loading schemas: {str(e)}")
                selected_schema = None
        else:
            selected_schema = None
            st.info("👈 Enter a database name first")
    
    st.divider()
    
    # Configuration
    if database_name and selected_schema:
        st.subheader("⚙️ Configuration")

        st.markdown(
            "The scan checks `SYSTEM$AUTO_REFRESH_STATUS` on every Iceberg table in the "
            "schema — the authoritative signal for whether auto-refresh is working."
        )
        col1, col2 = st.columns(2)
        with col1:
            use_threshold = st.checkbox(
                "Also flag tables by staleness threshold (LAST_ALTERED)",
                value=False,
                help="Additionally flag tables whose LAST_ALTERED is older than N days. "
                     "These can be healthy tables that simply had no new data — "
                     "check the 'Flagged By' column in the results.",
                key="use_threshold"
            )
        with col2:
            days_threshold = st.number_input(
                "Days Threshold",
                min_value=1,
                max_value=365,
                value=1,
                help="Used only when the staleness-threshold option is enabled",
                key="days_threshold",
                disabled=not use_threshold
            )

        # Scan tables
        if st.button("🔍 Scan Tables", type="primary", key="find_stale_btn"):
            progress_bar = st.progress(0)
            progress_text = st.empty()

            def _on_progress(done, total, name):
                progress_text.text(f"Checking auto-refresh status {done}/{total}: {name}...")
                progress_bar.progress(done / total)

            flagged_tables = find_problem_tables(
                conn,
                database_name,
                selected_schema,
                days_threshold,
                use_threshold,
                _on_progress
            )
            progress_bar.empty()
            progress_text.empty()

            st.session_state.stale_tables = flagged_tables
            # New scan: preselect the genuinely broken tables and drop old results
            st.session_state.tables_to_repair = [
                t['table_name'] for t in flagged_tables if t['broken']
            ]
            st.session_state.repair_results = []

            broken_count = sum(1 for t in flagged_tables if t['broken'])
            threshold_only_count = len(flagged_tables) - broken_count
            if broken_count:
                message = f"⚠️ Found {broken_count} table(s) with broken auto-refresh"
                if threshold_only_count:
                    message += f", plus {threshold_only_count} past the staleness threshold but reporting healthy"
                st.warning(message)
            elif flagged_tables:
                st.info(
                    f"ℹ️ Auto-refresh is healthy on every table; {threshold_only_count} table(s) "
                    f"exceed the staleness threshold — likely just no new data"
                )
            else:
                st.success(f"✅ Auto-refresh is healthy on all Iceberg tables in {database_name}.{selected_schema}")
        
        # Display flagged tables
        if st.session_state.stale_tables:
            st.divider()
            st.header("📋 Flagged Tables")
            
            # Create DataFrame for display
            df_stale = pd.DataFrame(st.session_state.stale_tables)
            df_stale['last_altered'] = pd.to_datetime(df_stale['last_altered'])
            
            # Handle timezone-aware vs timezone-naive datetime
            # Convert timezone-aware datetimes to UTC, then remove timezone for consistent calculation
            if df_stale['last_altered'].dt.tz is not None:
                # Convert to UTC and remove timezone info
                df_stale['last_altered'] = df_stale['last_altered'].dt.tz_convert('UTC').dt.tz_localize(None)
            
            # Use pd.Timestamp.now() in UTC, then remove timezone for calculation
            now = pd.Timestamp.now(tz='UTC').tz_localize(None)
            
            # Calculate days since refresh
            df_stale['days_since_refresh'] = (now - df_stale['last_altered']).dt.days
            df_stale['last_altered_formatted'] = df_stale['last_altered'].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # Display table
            st.markdown(f"""
            **⚠️ {len(df_stale)} table(s) flagged — the 'Flagged By' column shows why each one is listed:**
            """)

            # Broken tables first, then by how long since the last alteration
            df_stale = df_stale.sort_values(
                ['broken', 'days_since_refresh'], ascending=[False, False]
            )

            # Format for display
            display_df = df_stale[[
                'table_name',
                'flagged_by',
                'execution_state',
                'status_detail',
                'last_altered_formatted',
                'days_since_refresh',
                'row_count'
            ]].copy()
            display_df['row_count'] = display_df['row_count'].fillna(0).astype(int)
            display_df.columns = [
                'Table Name', 'Flagged By', 'Refresh State', 'Status Detail',
                'Last Altered', 'Days Since Altered', 'Row Count'
            ]
            display_df = display_df.reset_index(drop=True)  # Remove index column

            # Use st.table which doesn't show index
            st.table(display_df)

            # Summary statistics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Flagged", len(df_stale))
            with col2:
                broken_total = int(df_stale['broken'].sum())
                st.metric("Broken Auto-Refresh", broken_total)
            with col3:
                st.metric("Threshold Only", len(df_stale) - broken_total)
            with col4:
                total_rows = int(df_stale['row_count'].fillna(0).sum())
                st.metric("Total Rows", f"{total_rows:,}")
            
            st.divider()
            
            # Repair Options
            st.header("🔧 Repair Options")
            
            # Select tables to repair
            st.subheader("Select Tables to Repair")
            
            all_table_names = df_stale['table_name'].tolist()
            broken_table_names = df_stale[df_stale['broken']]['table_name'].tolist()

            # Selection lives in session state (no default= — combining both
            # triggers a Streamlit API warning); keep it within current options
            if 'tables_to_repair' not in st.session_state:
                st.session_state.tables_to_repair = broken_table_names
            else:
                st.session_state.tables_to_repair = [
                    t for t in st.session_state.tables_to_repair if t in all_table_names
                ]

            def _select_all_tables():
                st.session_state.tables_to_repair = all_table_names

            def _clear_all_tables():
                st.session_state.tables_to_repair = []

            col1, col2 = st.columns([3, 1])
            with col1:
                selected_tables = st.multiselect(
                    "Choose tables to repair:",
                    options=all_table_names,
                    help="Select which tables to refresh and enable auto-refresh",
                    key="tables_to_repair"
                )
            with col2:
                st.button("Select All", key="select_all_btn", on_click=_select_all_tables)
                st.button("Clear All", key="clear_all_btn", on_click=_clear_all_tables)
            
            if selected_tables:
                st.info(f"💡 {len(selected_tables)} table(s) selected for repair")
                
                # Show what will be executed (same statements repair_table runs)
                with st.expander("📝 Preview SQL Commands", expanded=False):
                    st.markdown("**The following commands will be executed for each selected table:**")
                    for table_name in selected_tables:
                        statements = ";\n".join(
                            repair_statements(database_name, selected_schema, table_name)
                        )
                        st.code(f"""
-- For table: {table_name}
{statements};
                        """)
                
                # Repair button
                if st.button("🔧 Repair Selected Tables", type="primary", key="repair_btn"):
                    repair_results = []
                    progress_bar = st.progress(0)
                    status_text = st.empty()

                    total_tables = len(selected_tables)

                    for idx, table_name in enumerate(selected_tables):
                        status_text.text(f"Processing {idx + 1}/{total_tables}: {table_name}...")

                        success, message = repair_table(
                            conn,
                            database_name,
                            selected_schema,
                            table_name
                        )

                        repair_results.append({
                            'table_name': table_name,
                            'success': success,
                            'message': message
                        })

                        progress_bar.progress((idx + 1) / total_tables)

                    progress_bar.empty()
                    status_text.empty()

                    st.session_state.repair_results = repair_results

                    if all(r['success'] for r in repair_results):
                        st.balloons()
            else:
                st.info("👈 Select tables from the list above to repair them")

            # Rendered from session state so results survive widget interactions
            if st.session_state.repair_results:
                st.divider()
                st.header("✅ Repair Results")

                df_results = pd.DataFrame(st.session_state.repair_results)
                success_count = df_results['success'].sum()
                failure_count = len(df_results) - success_count

                if success_count > 0:
                    st.success(f"✅ Successfully repaired {success_count} table(s)")
                if failure_count > 0:
                    st.error(f"❌ Failed to repair {failure_count} table(s)")

                # Results table
                results_display = df_results.copy()
                results_display['status'] = results_display['success'].apply(
                    lambda x: '✅ Success' if x else '❌ Failed'
                )
                results_display = results_display[['table_name', 'status', 'message']]
                results_display.columns = ['Table Name', 'Status', 'Message']
                results_display = results_display.reset_index(drop=True)  # Remove index column

                # Use st.table instead of st.dataframe to avoid showing index
                st.table(results_display)

                # Show failed tables details
                failed_tables = df_results[~df_results['success']]
                if len(failed_tables) > 0:
                    st.warning("⚠️ Some tables failed to repair. Details:")
                    for _, row in failed_tables.iterrows():
                        with st.expander(f"❌ {row['table_name']}", expanded=False):
                            st.error(f"Error: {row['message']}")

                # Success message
                if failure_count == 0:
                    st.success("""
                    🎉 **All selected tables have been successfully repaired!**

                    - ✅ Tables have been refreshed
                    - ✅ Auto-refresh has been enabled

                    The tables should now stay up-to-date automatically.
                    """)
    
    # Instructions
    st.divider()
    with st.expander("ℹ️ How This Works", expanded=False):
        st.markdown("""
        ### What This App Does

        1. **Finds Broken Tables**:
           - Queries `INFORMATION_SCHEMA.TABLES` to find every Iceberg table in the schema
           - Checks `SYSTEM$AUTO_REFRESH_STATUS` on each one — the authoritative signal;
             any `executionState` other than `RUNNING` (or a populated failure field)
             means auto-refresh is broken
           - Optionally also flags tables whose `LAST_ALTERED` is older than N days;
             these can be healthy tables that simply had no new data, so they are
             labelled separately in the 'Flagged By' column and not preselected

        2. **Repairs Tables**:
           - Disables auto-refresh (manual refresh is rejected while it is enabled)
           - Runs `ALTER ICEBERG TABLE <db>.<schema>.<table> REFRESH` to refresh metadata
           - Runs `ALTER ICEBERG TABLE <db>.<schema>.<table> SET AUTO_REFRESH = TRUE` to re-enable auto-refresh
           - Processes tables one by one and shows progress
        
        3. **Results**:
           - Shows success/failure status for each table
           - Displays error messages for any failures
           - Provides summary statistics
        
        ### SQL Queries Used

        **List Iceberg Tables:**
        ```sql
        SELECT
            TABLE_NAME,
            LAST_ALTERED,
            ROW_COUNT
        FROM <database>.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '<schema>'
          AND IS_ICEBERG = 'YES'
        ORDER BY LAST_ALTERED ASC
        ```

        **Check Auto-Refresh Status (per table):**
        ```sql
        SELECT SYSTEM$AUTO_REFRESH_STATUS('<database>."<schema>"."<table>"');
        ```
        
        **Repair Each Table:**
        ```sql
        ALTER ICEBERG TABLE <database>.<schema>.<table> SET AUTO_REFRESH = FALSE;
        ALTER ICEBERG TABLE <database>.<schema>.<table> REFRESH;
        ALTER ICEBERG TABLE <database>.<schema>.<table> SET AUTO_REFRESH = TRUE;
        ```
        
        ### Best Practices
        
        - Run this check regularly (e.g., daily)
        - Monitor tables that frequently become stale
        - Consider adjusting refresh schedules if many tables are stale
        - Enable auto-refresh on all Iceberg tables to prevent this issue
        """)
    

if __name__ == "__main__":
    main()
