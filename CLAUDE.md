# CLAUDE.md — Atlan AI Agent Guidelines

> **Applies To:** `lakehouse-solutions`
> **Full security policy:** See `AGENTS.md`

---

## Security

`lakehouse-solutions` contains lakehouse architecture solution templates. Key surfaces: cloud credentials and SQL template security.

### Security Contact
Security questions → `#bu-security-and-it` on Slack.

### General Invariants
- **[MUST]** No cloud credentials in solution templates — use placeholder values.
- **[MUST]** SQL templates accepting user input must use parameterised queries.
- **[SHOULD]** Each solution template documents minimum required IAM permissions.
