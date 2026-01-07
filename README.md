# Lakehouse Solutions - Gold Layer

## Overview

This repository provides the **Gold layer** implementation for the Metadata Lakehouse architecture, enabling Atlan customers to deploy standardized, production-ready analytical models on their own compute environments.

The Gold layer serves as the single entry point for both human and AI consumption of metadata, representing the curated, analytics-ready tier in our medallion architecture (Bronze → Silver → Gold).

## Architecture

The Metadata Lakehouse follows a medallion architecture pattern:

- **Bronze Layer**: Raw metadata ingestion
- **Silver Layer**: Cleaned and validated metadata
- **Gold Layer**: Curated, production-ready analytical models for consumption

This repository focuses exclusively on the **Gold Layer**, providing standardized views and models that transform metadata into analytics-ready formats.

## Supported Platforms

The Gold layer deployment scripts are available for the following compute environments:

- **Snowflake** ❄️
- **Databricks** 🔷
- **BigQuery** ☁️
- **DuckDB** 🦆
- **Trino** 🚀

## Repository Contents

This repository contains:

- **Documentation**: Comprehensive README files with setup guides and usage instructions
- **SQL Deployment Scripts**: Production-ready SQL scripts for deploying the Gold layer in customer-managed infrastructure

## Getting Started

### Prerequisites

- Access to one of the supported compute environments
- Appropriate permissions to create databases, schemas, views, and tables
- Connection to your Atlan metadata catalog

### Platform-Specific Setup

Navigate to the platform-specific folder for detailed setup instructions:

- [Snowflake Setup Guide](./snowflake/README.md)

*Additional platform guides coming soon for Databricks, BigQuery, DuckDB, and Trino.*

## What the Gold Layer Provides

The Gold layer delivers:

- **Unified Asset Registry**: Centralized view of all metadata assets across SQL, BI, pipelines, data quality, and object domains
- **Relational Asset Details**: Consolidated metadata for databases, schemas, tables, views, columns, queries, and procedures
- **Governance & Classification**: Tag and custom metadata views for data governance
- **Lineage**: Complete multi-hop upstream and downstream lineage relationships
- **Data Quality**: Views for Atlan-native and third-party data quality rules
- **Pipeline Details**: Orchestration and pipeline asset metadata
- **Glossary**: Business glossary terms, categories, and hierarchies

## Use Cases

The Gold layer enables:

- **Analytics & Reporting**: Query-ready metadata for business intelligence and analytics
- **AI/ML Consumption**: Structured metadata for AI agents and machine learning models
- **Data Governance**: Standardized views for compliance and governance reporting
- **Lineage Analysis**: Complete lineage visualization and impact analysis
- **Asset Discovery**: Unified search and discovery across all metadata types

## Repository Structure

```
lakehouse-solutions/
├── README.md (this file)
├── snowflake/
│   ├── README.md              # Snowflake-specific setup guide
│   └── MDLH_Gold_layer.sql    # Snowflake deployment script
├── databricks/                # Coming soon
├── bigquery/                  # Coming soon
├── duckdb/                    # Coming soon
└── trino/                     # Coming soon
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
