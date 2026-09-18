# Azure Fabric OneLake

Solutions for consuming the Atlan Lakehouse from Microsoft Fabric.

Fabric does not federate an Iceberg REST catalog. Instead, OneLake reads Iceberg tables directly from storage through shortcuts and virtualizes them as Delta Lake tables, which every Fabric workload can query.

## Solutions

### [OneLake Iceberg Shortcuts](./onelake-iceberg-shortcuts/)

A script that creates OneLake shortcuts to the Atlan Lakehouse Iceberg tables hosted on Atlan's ADLS Gen2 storage account, inside a lakehouse in the customer's own Fabric tenant. Handles the one-time setup (lakehouse data item, ADLS Gen2 connection with the Atlan-provided service principal, pointer-file shortcut) and the repeatable sync, which maps each Iceberg namespace to a lakehouse schema and each table to a shortcut. Namespaces can be synced selectively and re-synced at any time.
