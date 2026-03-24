# Maintenance scripts

| Script | Purpose |
|--------|--------|
| `migrate_dedupe_findings.py` | One-off migration over `db/runs/*/report.json` (dedupe / score filter). Prefer the UI **POST /api/migrate-dedupe-findings** when possible. |
| `install_presidio.sh` | Install spaCy 3.7.x + Presidio + `en_core_web_sm` into `.venv`. |

Run from **repository root**:

```bash
python scripts/migrate_dedupe_findings.py
./scripts/install_presidio.sh
```
