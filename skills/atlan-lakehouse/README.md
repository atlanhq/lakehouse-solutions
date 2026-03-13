# Atlan Lakehouse Skill

An [Agent Skills](https://agentskills.io/specification) skill that teaches AI coding agents how to connect to and query the Atlan Metadata Lakehouse (MDLH). Works across Snowflake (Cortex Code), Databricks (Genie Code), and Python (PyIceberg).

## What's included

- **Cross-platform connection** — automatic platform detection with native SQL for Snowflake/Databricks and PyIceberg for Python environments
- **Entity Metadata templates** — metadata completeness, lineage analysis (orphans, circular dependencies, coverage, hubs), glossary term export
- **Usage Analytics templates** — active users (DAU/WAU/MAU), feature adoption, engagement depth, retention cohorts, customer health scoring
- **Key conventions** — domain handling, noise event filtering, session derivation, timezone conversion, feature area mapping

## Installation

### Claude Code

Copy the skill directory into your Claude Code skills folder:

```bash
# Global installation (available in all projects)
mkdir -p ~/.claude/skills/atlan-lakehouse
cp SKILL.md ~/.claude/skills/atlan-lakehouse/SKILL.md
```

Or symlink it if you want to stay in sync with this repo:

```bash
mkdir -p ~/.claude/skills
ln -s "$(pwd)" ~/.claude/skills/atlan-lakehouse
```

### Any Agent Skills-compatible tool

This skill follows the [Agent Skills Spec](https://agentskills.io/specification). Place the `SKILL.md` file where your agent framework discovers skills.

## Prerequisites

- **Snowflake / Cortex Code**: Access to the Snowflake database containing your Atlan Lakehouse data
- **Databricks / Genie Code**: Access to the Unity Catalog catalog containing your Atlan Lakehouse data
- **Python / PyIceberg**: MDLH OAuth credentials (Client ID + Secret) from the Atlan Marketplace. See [Enable Lakehouse](https://docs.atlan.com/platform/lakehouse/how-tos/enable-lakehouse#next-steps) for setup.

## Usage

Once installed, the skill activates automatically when you ask questions like:

- "What's our metadata completeness coverage?"
- "Show me MAU trends for the last 6 months"
- "Which assets have no lineage?"
- "Export all glossary terms with their categories"
- "Who are the most active users?"
- "What's the retention rate for new users?"

The skill detects your platform and uses the appropriate connection method — no manual configuration needed.

## SQL Dialect

All SQL templates use Snowflake as the canonical dialect. Agents adapt syntax automatically for other engines (e.g., `CONVERT_TIMEZONE` to `FROM_UTC_TIMESTAMP` for Databricks).
