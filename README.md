# Lakehouse Solutions

## Overview

This repository provides a collection of solutions for the Atlan Metadata Lakehouse (MDLH), enabling Atlan customers to deploy production-ready tools on their own compute environments. Solutions include analytical models (Gold Layer), maintenance utilities, and more.

## Supported Platforms

- **Snowflake** ❄️
- **Databricks** 🔷
- **BigQuery** ☁️
- **DuckDB** 🦆 *(coming soon)*
- **Trino** 🚀 *(coming soon)*

## Solutions

### Gold Layer

The Gold Layer provides curated, analytics-ready metadata views that serve as the single entry point for both human and AI consumption of lakehouse metadata.

**What it delivers:**

- **Unified Asset Registry**: Centralized view of all assets across SQL, BI, pipelines, data quality, and object domains
- **Relational Asset Details**: Consolidated metadata for databases, schemas, tables, views, columns, queries, and procedures
- **Governance & Classification**: Tag and custom metadata views for data governance
- **Lineage**: Complete multi-hop upstream and downstream lineage relationships
- **Data Quality**: Views for Atlan-native and third-party data quality rules
- **Pipeline Details**: Orchestration and pipeline asset metadata
- **Glossary**: Business glossary terms, categories, and hierarchies

### MDLH Table Maintenance *(Snowflake only)*

A native Snowflake Streamlit app that identifies stale Iceberg tables and provides an option to repair them by refreshing metadata and enabling auto-refresh.

## Getting Started

### Prerequisites

- Access to one of the supported compute environments
- Appropriate permissions to create databases, schemas, views, and tables
- Connection to your Atlan metadata catalog

### Platform Guides

Navigate to the platform-specific folder for available solutions and setup instructions:

- [Snowflake](./snowflake/)
- [Databricks](./databricks/)
- [BigQuery](./bigquery/)

## Use Cases

- **Analytics & Reporting**: Query-ready metadata for business intelligence and analytics
- **AI/ML Consumption**: Structured metadata for AI agents and machine learning models
- **Data Governance**: Standardized views for compliance and governance reporting
- **Lineage Analysis**: Complete lineage visualization and impact analysis
- **Asset Discovery**: Unified search and discovery across all metadata types

## Repository Structure

```
lakehouse-solutions/
├── README.md                              # This file
├── snowflake/
│   ├── README.md                          # Snowflake solutions overview
│   ├── gold-layer/
│   │   ├── README.md                      # Gold Layer setup guide
│   │   └── MDLH_Gold_layer.sql            # Gold Layer deployment script
│   └── mdlh-table-maintenance/
│       ├── README.md                      # Table maintenance setup guide
│       └── MDLH_table_refresh_repair.py   # Streamlit app
├── databricks/
│   ├── README.md                          # Databricks solutions overview
│   └── gold-layer/
│       ├── README.md                      # Gold Layer setup guide
│       ├── MDLH_Gold_layer.sql            # Gold Layer deployment script
│       └── refresh_materialized_views.sql # Scheduled refresh script
├── bigquery/
│   ├── README.md                          # BigQuery solutions overview
│   └── gold-layer/
│       ├── README.md                      # Gold Layer setup guide
│       └── MDLH_Gold_layer.sql            # Gold Layer deployment script
├── duckdb/                                # Coming soon
└── trino/                                 # Coming soon
```

## Contributing

This repository is maintained by the Atlan team. For issues, questions, or contributions, please contact the Atlan engineering team.

## License

[Specify license here]

## Support

For support and questions:
- Documentation: See platform-specific README files
- Atlan Support: [Contact information]

---

**Note**: This repository contains deployment scripts for customer-managed infrastructure. All scripts are designed to be idempotent and production-ready.
