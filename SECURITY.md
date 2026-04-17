# Security Policy

## Reporting a vulnerability

If you believe you've found a security vulnerability in Watchdog, please
report it privately. **Do not open a public GitHub issue or pull request**
for security-sensitive findings.

- Email the maintainers at the address listed on the repository profile.
- Include a clear description, reproduction steps, and — if possible — a
  proof-of-concept. Please give us a reasonable window to respond before
  public disclosure.

We aim to acknowledge reports within two business days and to provide a
remediation plan within ten business days.

## Scope

The following are in scope:

- Code in `engine/`, `guardrails/`, `mcp/`, and `ontos-adapter/`.
- DABs job definitions and deploy scripts under `engine/resources/` and
  `scripts/`.

The following are out of scope:

- Issues that require a compromised operator account or Databricks
  workspace.
- Vulnerabilities in Databricks itself — please report those to Databricks
  directly.

## Handling sensitive data

Watchdog reads metadata (tags, owners, grants) — it never reads table
contents. If you discover a case where Watchdog does read row data, that
is a bug and should be reported here.

## Dependency advisories

We track advisories for direct dependencies listed in `engine/setup.py`,
`guardrails/pyproject.toml` (if present), `mcp/`, and
`ontos-adapter/pyproject.toml`. If you see a CVE we've missed, please
report it through the channel above.
