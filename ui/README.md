# UI

Static assets served by Flask from the repo **`ui/`** directory (`paths.ui_dir` in YAML).

## Typical flow

1. Run the pipeline (writes reports under `db/reports/` and `db/runs/`, updates `instance/state.yaml`):

   ```bash
   python main.py pipeline --input-dir ./db/uploads --num-files 1
   ```

2. Start the web app:

   ```bash
   python main.py
   ```

3. Open the app URL (default **http://127.0.0.1:8765/**).
