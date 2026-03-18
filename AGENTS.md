# AGENTS.md — Atlan AI Agent Guidelines

> **Applies To:** `lakehouse-solutions`
> **Companion file:** See `CLAUDE.md` for the lean summary.

---

## Security

`lakehouse-solutions` contains lakehouse architecture solution templates.

### Security Contact
`#bu-security-and-it` on Slack.

---

### General Invariants

- **[MUST]** No cloud credentials in committed templates — placeholder values only.
- **[MUST]** SQL templates: parameterised queries only, no string concatenation.
- **[SHOULD]** Minimum-required-permissions section in each solution template.
