"""
Snowflake Native Streamlit App - MDLH Table Refresh Repair
Identifies stale MDLH Iceberg tables and provides option to refresh them
"""

import streamlit as st
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional, Tuple

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
    if 'selected_tables' not in st.session_state:
        st.session_state.selected_tables = []
    if 'repair_results' not in st.session_state:
        st.session_state.repair_results = []

def get_snowflake_connection():
    """Get Snowflake connection (native app)."""
    try:
        # Native Snowflake Streamlit apps use snowflake.snowpark.Session
        # Method 1: Try getActive() - most common for native apps
        try:
            from snowflake.snowpark import Session
            session = Session.builder.getActive()
            if session is not None:
                return session
        except Exception as e1:
            pass
        
        # Method 2: Try get_active_session() from context
        try:
            from snowflake.snowpark.context import get_active_session
            session = get_active_session()
            if session is not None:
                return session
        except Exception as e2:
            pass
        
        # Method 3: Try st.connection (if available in newer versions)
        try:
            if hasattr(st, 'connection'):
                return st.connection("snowflake")
        except Exception as e3:
            pass
        
        # If all methods fail, raise an error
        raise Exception(
            "Could not establish Snowflake connection. "
            "Please ensure this is running as a native Snowflake Streamlit app. "
            "The app needs access to an active Snowpark session."
        )
    except Exception as e:
        st.error(f"❌ Error getting Snowflake connection: {str(e)}")
        st.info("💡 This app must be deployed as a native Snowflake Streamlit app to work properly.")
        return None

def execute_query(conn, query: str) -> List[Tuple]:
    """Execute a SQL query and return results."""
    try:
        # Native Streamlit connection uses .sql() method
        if hasattr(conn, 'sql'):
            result = conn.sql(query).collect()
            return [tuple(row) for row in result]
        else:
            # Fallback: try direct execution
            result = conn.sql(query).collect()
            return [tuple(row) for row in result]
    except Exception as e:
        st.error(f"❌ Query execution error: {str(e)}")
        raise

def list_schemas(conn, database: str) -> List[str]:
    """List all schemas in a database."""
    try:
        # Quote identifier to handle special characters
        database_quoted = f'"{database}"' if not database.startswith('"') else database
        
        # Use INFORMATION_SCHEMA to get exact schema names (case-sensitive)
        query = f"""
        SELECT DISTINCT SCHEMA_NAME 
        FROM {database_quoted}.INFORMATION_SCHEMA.SCHEMATA
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

def find_stale_tables(conn, database: str, schema: str, days_threshold: int = 1) -> List[Dict]:
    """Find tables that haven't been refreshed in the specified number of days."""
    try:
        # Quote identifiers for database
        database_quoted = f'"{database}"' if not database.startswith('"') else database
        
        # Use exact schema name match (case-sensitive) as in direct SQL query
        # Escape single quotes in schema name if present
        schema_escaped = schema.replace("'", "''")
        
        # Query to find stale tables - matching the exact query format
        query = f"""
        SELECT
            TABLE_NAME,
            LAST_ALTERED,
            ROW_COUNT
        FROM {database_quoted}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema_escaped}'
          AND ROW_COUNT > 0
          AND LAST_ALTERED < DATEADD(day, -{days_threshold}, CURRENT_TIMESTAMP())
        ORDER BY LAST_ALTERED DESC
        """
        
        results = execute_query(conn, query)
        
        stale_tables = []
        for row in results:
            stale_tables.append({
                'table_name': row[0],
                'last_altered': row[1],
                'row_count': row[2] if len(row) > 2 else 0,
                'database': database,
                'schema': schema
            })
        
        return stale_tables
    except Exception as e:
        st.error(f"❌ Error finding stale tables: {str(e)}")
        return []

def repair_table(conn, database: str, schema: str, table_name: str) -> Tuple[bool, str]:
    """Repair a single table by refreshing it and enabling auto-refresh."""
    try:
        # Quote identifiers
        database_quoted = f'"{database}"' if not database.startswith('"') else database
        schema_quoted = f'"{schema}"' if not schema.startswith('"') else schema
        table_quoted = f'"{table_name}"' if not table_name.startswith('"') else table_name
        
        # Refresh the table
        refresh_query = f'ALTER ICEBERG TABLE {database_quoted}.{schema_quoted}.{table_quoted} REFRESH'
        execute_query(conn, refresh_query)
        
        # Enable auto-refresh
        auto_refresh_query = f'ALTER ICEBERG TABLE {database_quoted}.{schema_quoted}.{table_quoted} SET AUTO_REFRESH = TRUE'
        execute_query(conn, auto_refresh_query)
        
        return True, "Success"
    except Exception as e:
        return False, str(e)

# ============================================================================
# Main App
# ============================================================================

def main():
    initialize_session_state()
    
    st.title("🔧 MDLH Table Refresh Repair")
    st.markdown("**Identify and repair stale MDLH Iceberg tables that haven't been refreshed recently**")
    
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
        )
    
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
        
        col1, col2 = st.columns(2)
        with col1:
            days_threshold = st.number_input(
                "Days Threshold",
                min_value=1,
                max_value=365,
                value=1,
                help="Find tables not refreshed in the last N days",
                key="days_threshold"
            )
        with col2:
            st.info(f"💡 Will find tables not refreshed in the last **{days_threshold} day(s)**")
        
        # Find Stale Tables
        if st.button("🔍 Find Stale Tables", type="primary", key="find_stale_btn"):
            with st.spinner(f"Searching for stale tables in {database_name}.{selected_schema}..."):
                stale_tables = find_stale_tables(
                    conn,
                    database_name,
                    selected_schema,
                    days_threshold
                )
                st.session_state.stale_tables = stale_tables
                
                if stale_tables:
                    st.warning(f"⚠️ Found {len(stale_tables)} table(s) that haven't been refreshed in the last {days_threshold} day(s)")
                else:
                    st.success(f"✅ All tables in {database_name}.{selected_schema} have been refreshed recently!")
        
        # Display Stale Tables
        if st.session_state.stale_tables:
            st.divider()
            st.header("📋 Stale Tables Found")
            
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
            **⚠️ The following {len(df_stale)} table(s) have not been refreshed in the last {days_threshold} day(s):**
            """)
            
            # Format for display
            display_df = df_stale[[
                'table_name',
                'last_altered_formatted',
                'days_since_refresh',
                'row_count'
            ]].copy()
            display_df.columns = ['Table Name', 'Last Refreshed', 'Days Since Refresh', 'Row Count']
            display_df = display_df.sort_values('Days Since Refresh', ascending=False)
            display_df = display_df.reset_index(drop=True)  # Remove index column
            
            # Use st.table which doesn't show index
            st.table(display_df)
            
            # Summary statistics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Stale Tables", len(df_stale))
            with col2:
                avg_days = df_stale['days_since_refresh'].mean()
                st.metric("Avg Days Since Refresh", f"{avg_days:.1f}")
            with col3:
                max_days = df_stale['days_since_refresh'].max()
                st.metric("Max Days Since Refresh", f"{max_days:.0f}")
            with col4:
                total_rows = df_stale['row_count'].sum()
                st.metric("Total Rows", f"{total_rows:,}")
            
            st.divider()
            
            # Repair Options
            st.header("🔧 Repair Options")
            
            # Select tables to repair
            st.subheader("Select Tables to Repair")
            
            all_table_names = df_stale['table_name'].tolist()
            
            col1, col2 = st.columns([3, 1])
            with col1:
                selected_tables = st.multiselect(
                    "Choose tables to repair:",
                    options=all_table_names,
                    default=all_table_names,
                    help="Select which tables to refresh and enable auto-refresh",
                    key="tables_to_repair"
                )
            with col2:
                if st.button("Select All", key="select_all_btn"):
                    st.session_state.selected_tables = all_table_names
                    st.rerun()
                if st.button("Clear All", key="clear_all_btn"):
                    st.session_state.selected_tables = []
                    st.rerun()
            
            if selected_tables:
                st.info(f"💡 {len(selected_tables)} table(s) selected for repair")
                
                # Show what will be executed
                with st.expander("📝 Preview SQL Commands", expanded=False):
                    st.markdown("**The following commands will be executed for each selected table:**")
                    for table_name in selected_tables:
                        st.code(f"""
-- For table: {table_name}
ALTER ICEBERG TABLE {database_name}.{selected_schema}."{table_name}" REFRESH;
ALTER ICEBERG TABLE {database_name}.{selected_schema}."{table_name}" SET AUTO_REFRESH = TRUE;
                        """)
                
                # Repair button
                if st.button("🔧 Repair Selected Tables", type="primary", key="repair_btn"):
                    if selected_tables:
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
                        
                        # Display results
                        st.divider()
                        st.header("✅ Repair Results")
                        
                        df_results = pd.DataFrame(repair_results)
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
                            st.balloons()
                            st.success("""
                            🎉 **All selected tables have been successfully repaired!**
                            
                            - ✅ Tables have been refreshed
                            - ✅ Auto-refresh has been enabled
                            
                            The tables should now stay up-to-date automatically.
                            """)
                    else:
                        st.warning("⚠️ Please select at least one table to repair")
            else:
                st.info("👈 Select tables from the list above to repair them")
    
    # Instructions
    st.divider()
    with st.expander("ℹ️ How This Works", expanded=False):
        st.markdown("""
        ### What This App Does
        
        1. **Finds Stale Tables**: 
           - Queries `INFORMATION_SCHEMA.TABLES` to find Iceberg tables
           - Filters tables that haven't been refreshed in the last N days
           - Shows table name, last refresh timestamp, and row count
        
        2. **Repairs Tables**:
           - Runs `ALTER ICEBERG TABLE <db>.<schema>.<table> REFRESH` to refresh metadata
           - Runs `ALTER ICEBERG TABLE <db>.<schema>.<table> SET AUTO_REFRESH = TRUE` to enable auto-refresh
           - Processes tables one by one and shows progress
        
        3. **Results**:
           - Shows success/failure status for each table
           - Displays error messages for any failures
           - Provides summary statistics
        
        ### SQL Queries Used
        
        **Find Stale Tables:**
        ```sql
        SELECT
            TABLE_NAME,
            LAST_ALTERED,
            ROW_COUNT
        FROM <database>.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '<schema>'
          AND ROW_COUNT > 0
          AND LAST_ALTERED < DATEADD(day, -<days_threshold>, CURRENT_TIMESTAMP())
        ORDER BY LAST_ALTERED DESC
        ```
        
        **Repair Each Table:**
        ```sql
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
