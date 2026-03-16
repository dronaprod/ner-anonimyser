# ARMOR — Intelligent Contextual Anonymiser

PII anonymisation pipeline: Presidio + GLiNER + **Qwen (Ollama)** for NER; **Qwen 9B via Ollama** for contextual anonymisation.

## Storage (`db/`)

- **`db/reports/`** — Final report JSONs from each run.
- **`db/uploads/`** — Uploaded files (via ARMOR UI).
- **`db/runs/`** — Per-run data: `run_id/run_meta.json` (file list, chunk logs), `run_id/report.json` (copy of report).

## ARMOR UI

1. Start the app:
   ```bash
   .venv/bin/python run_armor_app.py
   ```
2. Open **http://127.0.0.1:8765/** in the browser.
3. Upload one or more files (PDF, DOCX, XLSX, TXT).
4. Click **Run anonymisation** (runs pipeline on `db/uploads/`, writes to `db/reports/` and `db/runs/`).
5. View **Processed files** (name, entities found, types, chunks) and **Chunk processing details** (per-chunk size, Presidio/GLiNER/Qwen/agreed/replacements counts).
6. Click **View report** to open the detailed diff-style report (original vs anonymised, PII table, replacements).

Port: set `ARMOR_PORT` to override 8765. Python: set `ARMOR_PYTHON` to the interpreter that has the pipeline deps.

## Pipeline (CLI)

- **NER**: Presidio, GLiNER, Qwen NER (Ollama). `OLLAMA_NER_MODEL` (default: `qwen3.5:9b`).
- **Anonymisation**: `run_qwen_ollama.py` via Ollama. `OLLAMA_MODEL` (default: `qwen3.5:9b`).
- **Reports**: Default `--report-dir` is `db/reports`. Each run also writes `db/runs/<timestamp>/run_meta.json` and `report.json`.
- Ensure Ollama is running and the Qwen model is pulled (e.g. `ollama pull qwen3.5:9b`).
