# Armor Data Anonymizer

PII detection and anonymisation: batch pipeline (Presidio + GLiNER + Qwen/Ollama) plus a Flask web UI.

## Layout

| Path | Purpose |
|------|---------|
| **`app/`** | Single Python package: Flask app (`create_app`), pipeline (`pipeline.py`), NER services, models, routers, config loaders |
| **`config/`** | Versioned YAML (`default.yaml`); optional **`local.yaml`** (gitignored) for overrides |
| **`instance/`** | Runtime state (`state.yaml` — `latest_report`, `updated_at`) |
| **`deployment/`** | Docker + Gunicorn |
| **`log/`** | Rotating log files (see `config/default.yaml`) |
| **`scripts/`** | Maintenance utilities |
| **`tests/`** | Manual / smoke scripts |
| **`ui/`** | Static front-end assets |
| **`.env`** | Secrets and environment overrides (copy from `.env.example`; not committed) |
| **`main.py`** | **Entry point**: web server by default, or `main.py pipeline …` for the CLI |
| **`wsgi.py`** | Gunicorn / WSGI hosting |

Rename this repository directory to `armor_data_anonymizer` if you want the folder name to match the product name; paths in code are resolved from the project root automatically.

## Configuration

1. **`config/default.yaml`** — `mode`, `qwen`, `paths`, `pipeline`, `flask`, `logging`, `security`.
2. **`config/local.yaml`** — optional overrides (create from `config/local.example.yaml`).
3. **`.env`** — `ARMOR_SECRET_KEY`, `ARMOR_PORT`, `OLLAMA_*`, `LITELLM_*`, etc.

Pipeline subprocess runs **`python -m app.pipeline`** (see `pipeline.module` in YAML) so imports stay correct.

**Python API:** `from app.config import load_armor_config, apply_qwen_runtime_settings`, `from app.models import PiiDetection`, NER from `app.services.ner`.

## Run

```bash
cd /path/to/project
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env   # then edit
.venv/bin/python main.py
```

Open **http://127.0.0.1:8765/** (port from `ARMOR_PORT`).

**Pipeline only (CLI):**

```bash
.venv/bin/python main.py pipeline --input-dir ./db/uploads --num-files 1 --report-dir ./db/reports
```

**Production:**

```bash
gunicorn -c deployment/gunicorn.conf.py 'wsgi:app'
```

Docker: see `deployment/README.md`.

## Anonymisation worker

Ollama chat script: **`app/anonymize/ollama.py`** (default `--qwen-script`). Alternatives: `app/anonymize/huggingface.py`, `app/anonymize/stub.py`.

## GLiNER / Ollama

- Default GLiNER: `knowledgator/gliner-x-large`. Lower memory: `ARMOR_GLINER_MODEL=urchade/gliner_medium-v2.1`.
- Ollama: `OLLAMA_HOST`, `OLLAMA_MODEL` / `OLLAMA_NER_MODEL`; **`mode: cpu`** in YAML sets CPU-friendly defaults via `apply_qwen_runtime_settings`.
