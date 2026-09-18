# OneLake Iceberg Shortcuts

Creates Microsoft Fabric OneLake shortcuts to the Atlan Lakehouse Iceberg tables. The tables stay on the Atlan-hosted ADLS Gen2 storage account; the shortcuts live in a lakehouse in your own Fabric tenant. OneLake virtualizes each Iceberg table folder as a Delta Lake table, so the tables are queryable from every Fabric workload without copying any data.

No Iceberg REST catalog is involved. Fabric reads the metadata and data files straight from storage using a service principal that Atlan provides.

## How It Works

Atlan publishes pointer files at the catalog storage root:

```
<catalog-root>/_latest/
  _index.json            # tracked namespaces -> pointer file
  <namespace>.json       # per namespace: each table's current metadata location,
                         # table folder, and record count
```

`setup` creates a shortcut over the `_latest` folder itself, so `sync` reads the pointer files through OneLake with your own Fabric identity. The Atlan service principal secret is therefore needed only once, when the connection is first created.

`sync` then reads the index, and for every table in each selected namespace creates a shortcut at `Tables/<schema>/<table>`, where `<schema>` is the Iceberg namespace. The shortcut targets the Iceberg table folder, which is what OneLake needs in order to generate the Delta log.

Shortcuts do not need refreshing. They point at the table folder rather than at a specific `metadata.json`, so OneLake picks up new snapshots on its own. Re-running `sync` adds tables that have appeared since the last run.

## Prerequisites

From Atlan:

- The shortcut connector URL: the ADLS Gen2 catalog root, for example `abfss://<container>@<account>.dfs.core.windows.net/<catalog-root>`
- Service principal tenant id, client id, and client secret

The service principal needs Storage Blob Data Reader on the Atlan storage account, or the Delegator role plus directory ACLs. Atlan configures this.

In your Fabric tenant:

- A workspace on a Fabric capacity, with at least the Contributor role for the identity running the script
- Admin portal, Tenant settings, OneLake settings: **Users can access data stored in OneLake with apps external to Fabric** enabled
- If you run the script as your own service principal, also enable **Service principals can use Fabric APIs** and **Service principals can create workspaces, connections, and deployment pipelines**

Local:

- Python 3.8 or later
- The [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli), logged in to your Fabric tenant with `az login`

```bash
pip install requests azure-identity
```

## Setup

Run once per lakehouse. Everything it creates is reused on later runs.

```bash
python fabric_onelake_shortcuts.py setup \
  --workspace "Analytics" \
  --adls-url "abfss://<container>@<account>.dfs.core.windows.net/<catalog-root>" \
  --atlan-tenant-id <tenant-id-from-atlan> \
  --atlan-client-id <client-id-from-atlan>
```

The secret is read from a console prompt, or from `ATLAN_SP_CLIENT_SECRET` if it is set. It is never written to disk and never logged.

This creates, in order:

| Object | Default name |
|---|---|
| Lakehouse data item (schema-enabled) | `atlan_context_store` |
| ADLS Gen2 cloud connection | `atlan_context_store_adls_connection` |
| Shortcut over the pointer files | `Files/_latest` |

Override the names with `--lakehouse-name` and `--connection-name`. Existing objects with those names are reused rather than recreated, so an object you made yourself in the Fabric portal works too.

Setup finishes by reading the index through the new shortcut, which proves the connection credentials work, and prints the namespaces Atlan publishes.

Non-secret settings are saved to `~/.atlan/fabric-onelake-shortcuts/default.json`. Later runs need no arguments. Use `--profile <name>` to keep several setups side by side.

The profile is a convenience, not the source of truth. Setup finds the lakehouse, connection, and pointer shortcut by name in Fabric, so if the file is lost you can re-run setup with `--workspace` and `--adls-url` and everything is rediscovered without touching the connection or asking for the secret. If you used `--lakehouse-name` or `--connection-name` originally, pass the same names again; otherwise setup will not find your objects and will create a second set under the default names.

Connections are visible only to identities they are shared with. If a colleague ran setup, share the connection with yourself in the Fabric portal (Manage connections and gateways) before running setup, or setup fails with `DuplicateConnectionName` rather than creating a duplicate.

## Sync

```bash
# every table in the default namespaces
python fabric_onelake_shortcuts.py sync

# preview without changing anything
python fabric_onelake_shortcuts.py sync --plan

# only tables that hold data
python fabric_onelake_shortcuts.py sync --non-empty-only

# one namespace at a time
python fabric_onelake_shortcuts.py sync --namespaces gold
```

Default namespaces are `entity_metadata`, `gold`, `atlan-ns`. Namespaces Atlan does not publish are reported and skipped, not treated as an error, so the default list works whatever your tenant has enabled.

Run `sync` as often as you like. It is idempotent: tables that already have a shortcut are left alone, so a second run only picks up what is new. Syncing a namespace you synced before is safe, and so is adding a namespace you skipped the first time.

`--non-empty-only` uses the `totalRecords` field in the pointer file, which is the record count of each table's current snapshot. A table whose count is missing is included rather than assumed empty.

Other flags:

- `--overwrite` repoints a shortcut whose target no longer matches the pointer file. Without it, such tables are reported and left alone.
- `--prune` deletes table shortcuts that are gone from the pointer files. Only shortcuts bound to the Atlan connection are candidates; anything else under the schema is left alone.
- `--verbose` lists up-to-date and skipped tables as well as changes.

## Status

```bash
python fabric_onelake_shortcuts.py status
```

Prints the profile, the table shortcuts that exist, and the namespaces Atlan currently publishes.

## Authentication

Two identities are involved, and they are not the same:

- **Your Fabric identity** calls the Fabric APIs and reads the pointer files. By default this is your `az login` identity; if you belong to several tenants, log in with `az login --tenant <id>` or set `FABRIC_TENANT_ID`. For unattended runs, pass `--fabric-auth service_principal` and set `FABRIC_TENANT_ID`, `FABRIC_CLIENT_ID`, and `FABRIC_CLIENT_SECRET` for a service principal in your own tenant.
- **The Atlan service principal** is only ever stored inside the Fabric connection, as the delegated credential Fabric uses to read the Atlan storage account. The script never authenticates to anything with it.

Cross-tenant ADLS shortcuts require a service principal or SAS token; organizational account and workspace identity authentication do not work across tenants. That is why Atlan issues a service principal.

## Naming

Iceberg namespaces and table names are mapped to names Fabric accepts: characters outside `A-Z a-z 0-9 _` become underscores, and a dotted namespace uses a double underscore.

| Iceberg | Fabric |
|---|---|
| `entity_metadata` | schema `entity_metadata` |
| `gold` | schema `gold` |
| `atlan-ns` | schema `atlan_ns` |
| `gold.lineage-edges` | table `lineage_edges` in schema `gold` |

## Notes

- Shortcuts are read-only references. Deleting one, or the whole lakehouse, removes the reference only; nothing in Atlan's storage is affected.
- OneLake takes between 5 seconds and 2 minutes to virtualize an Iceberg table as Delta. If a table does not appear, open its **View files** menu and read `_delta_log/latest_conversion_log.txt`.
- Conversion requires Iceberg V2 tables with fewer than 5,000 commits, and does not support the `bucket[N]`, `truncate[W]`, or `void` partition transforms.
- The feature is not available on tenants or workspaces with private links enabled, or in the Qatar Central and Norway West regions.
- If a storage firewall protects the Atlan account, it needs [trusted workspace access](https://learn.microsoft.com/en-us/fabric/security/security-trusted-workspace-access) for your workspace.

## Troubleshooting

**`setup` fails with `IncorrectCredentials`** — the service principal cannot read the storage account. Check with Atlan that it has Storage Blob Data Reader and that the secret has not expired.

**`sync` reports that `_index.json` was not found** — either `setup` has not run, or Atlan has not enabled the metadata pointer workflow for your tenant. Ask Atlan to enable it.

**Shortcuts appear as folders, not tables** — the Iceberg to Delta conversion did not run or did not succeed. Read `_delta_log/latest_conversion_log.txt` under the table. If the folder has no `_delta_log`, conversion was never attempted.

Microsoft's Iceberg documentation contradicts itself on schema-enabled lakehouses: the walkthrough tells you to create Iceberg shortcuts under a schema, while the troubleshooting section says conversion only happens for shortcuts placed directly under `Tables` in a lakehouse without schemas. This script uses schemas, because namespaces map to schemas. If conversion turns out not to fire under a schema in your tenant, raise it with Atlan.

**A table shows a Parquet type conversion error in the lakehouse preview** — a known Fabric issue with `INT64`, `double`, and `decimal(p,s)` where p >= 10. Preview the table from the SQL Endpoint view instead, or set `spark.sql.parquet.enableVectorizedReader` to `false` in a notebook.

**Conversion fails with more than one set of metadata files** — the Iceberg table folder holds metadata from a dropped and recreated table. Atlan needs to clean up the stale metadata files.

## Reference

- [Use Iceberg tables with OneLake](https://learn.microsoft.com/en-us/fabric/onelake/onelake-iceberg-tables)
- [Create an ADLS Gen2 shortcut](https://learn.microsoft.com/en-us/fabric/onelake/create-adls-shortcut)
- [OneLake shortcuts REST API](https://learn.microsoft.com/en-us/rest/api/fabric/core/onelake-shortcuts)
