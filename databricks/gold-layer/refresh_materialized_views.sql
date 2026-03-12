-- Refresh Materialized Views for ATLAN Gold Layer
-- This script refreshes the ASSETS and BASE_EDGES materialized views
-- Use this file in Databricks SQL Jobs for scheduled refresh

USE CATALOG ATLAN;

-- Refresh ASSETS materialized view
REFRESH MATERIALIZED VIEW ATLAN.ATLAN_GOLD.ASSETS;

-- Refresh BASE_EDGES materialized view
REFRESH MATERIALIZED VIEW ATLAN.ATLAN_GOLD.BASE_EDGES;

