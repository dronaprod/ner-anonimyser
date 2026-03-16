# NER Anonymisation Report UI

Lightweight diff-style viewer for the NER anonymisation pipeline.

## Usage

1. **Run the pipeline** (without `--no-report`) so it writes a report JSON and updates `config.json`:
   ```bash
   python pii_anonymization_pipeline.py --input-dir ... --num-files 1
   ```

2. **Start the UI** and open the printed link:
   ```bash
   python run_ui.py
   ```
   Then open **http://127.0.0.1:8765/** in your browser.

## What you see

- **Left**: Original extracted text with PII spans highlighted by type (colour).
- **Right**: Anonymised text with the same highlights.
- **Center**: Table of PII found (value, type, confidence, stage: presidio / gliner / qwen / audit); table of contextual anonymised replacements (original → anonymised, type).
- **Top**: Summary counts (chunks anonymised, chunks unchanged, files).

Port can be overridden with `NER_UI_PORT=9000 python run_ui.py`.
