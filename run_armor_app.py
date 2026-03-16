#!/usr/bin/env python3
"""
ARMOR - Intelligent Contextual Anonymiser.
Flask app: upload files, run pipeline, list runs, view file details and chunk logs, view report.
"""
from __future__ import annotations

import json
import os
import subprocess
import threading
import uuid
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory

SCRIPT_DIR = Path(__file__).resolve().parent
DB = SCRIPT_DIR / "db"
UPLOADS = DB / "uploads"
REPORTS = DB / "reports"
RUNS = DB / "runs"
PROGRESS_FILE = DB / "progress.json"
UI_DIR = SCRIPT_DIR / "ui"
PIPELINE = SCRIPT_DIR / "pii_anonymization_pipeline.py"
_venv_py = SCRIPT_DIR / ".venv" / "bin" / "python"
PYTHON = os.environ.get("ARMOR_PYTHON", str(_venv_py if _venv_py.is_file() else "python3"))

app = Flask(__name__, static_folder=str(UI_DIR), static_url_path="")
app.config["MAX_CONTENT_LENGTH"] = 200 * 1024 * 1024  # 200 MB max upload

UPLOADS.mkdir(parents=True, exist_ok=True)
RUNS.mkdir(parents=True, exist_ok=True)
REPORTS.mkdir(parents=True, exist_ok=True)

SUPPORTED_EXT = {".pdf", ".docx", ".xlsx", ".txt"}
_run_lock = threading.Lock()
_running = False


def _write_progress(obj):
    try:
        PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = PROGRESS_FILE.with_suffix(PROGRESS_FILE.suffix + ".tmp")
        tmp.write_text(json.dumps(obj, ensure_ascii=True), encoding="utf-8")
        tmp.replace(PROGRESS_FILE)
    except Exception:
        pass


def _run_pipeline_background(file_list=None):
    """Run pipeline. file_list: optional list of filenames to process (only these); None = all in uploads."""
    global _running
    with _run_lock:
        if _running:
            return
        _running = True
    _write_progress({"running": True, "stage": "starting"})
    try:
        if file_list:
            existing = {p.name for p in UPLOADS.iterdir() if p.is_file() and p.suffix.lower() in SUPPORTED_EXT}
            to_run = [f for f in file_list if f in existing]
            missing = [f for f in file_list if f not in existing]
            if missing:
                _write_progress({"running": False, "error": "File(s) not in uploads: " + ", ".join(missing[:5])})
                return
            if not to_run:
                _write_progress({"running": False, "error": "No requested files found in uploads"})
                return
            count = len(to_run)
            files_arg = ",".join(to_run)
        else:
            count = len([p for p in UPLOADS.iterdir() if p.is_file() and p.suffix.lower() in SUPPORTED_EXT])
            if count == 0:
                _write_progress({"running": False, "error": "No files in uploads"})
                return
            files_arg = None
        cmd = [
            PYTHON,
            str(PIPELINE),
            "--input-dir", str(UPLOADS),
            "--report-dir", str(REPORTS),
            "--num-files", str(count),
            "--progress-file", str(PROGRESS_FILE),
        ]
        if files_arg:
            cmd.extend(["--files", files_arg])
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600, cwd=str(SCRIPT_DIR))
        if result.returncode != 0:
            _write_progress({
                "running": False,
                "error": "Pipeline failed",
                "stderr": (result.stderr or "")[-1000:],
            })
            return
        run_id = None
        for d in sorted(RUNS.iterdir(), reverse=True):
            if d.is_dir() and (d / "run_meta.json").is_file():
                run_id = d.name
                break
        _write_progress({"running": False, "run_id": run_id})
    except subprocess.TimeoutExpired:
        _write_progress({"running": False, "error": "Pipeline timed out"})
    except Exception as e:
        _write_progress({"running": False, "error": str(e)})
    finally:
        with _run_lock:
            _running = False


@app.route("/api/progress")
def get_progress():
    """Live progress (stage, file, chunk; no content). Poll while run is active."""
    if not PROGRESS_FILE.is_file():
        return jsonify({"running": False})
    try:
        data = json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
        return jsonify(data)
    except Exception:
        return jsonify({"running": False})


@app.route("/")
def index():
    return send_from_directory(UI_DIR, "armor.html")


@app.route("/report-viewer.html")
def report_viewer():
    return send_from_directory(UI_DIR, "report-viewer.html")


@app.route("/api/upload", methods=["POST"])
def upload():
    if "files" not in request.files and "file" not in request.files:
        return jsonify({"error": "No file(s)"}), 400
    files = request.files.getlist("files") or [request.files.get("file")]
    saved = []
    for f in files:
        if not f or not f.filename:
            continue
        ext = Path(f.filename).suffix.lower()
        if ext not in SUPPORTED_EXT:
            continue
        safe_name = f.filename
        path = UPLOADS / safe_name
        f.save(str(path))
        saved.append(safe_name)
    return jsonify({"uploaded": saved})


@app.route("/api/files")
def list_uploads():
    names = [p.name for p in UPLOADS.iterdir() if p.is_file() and p.suffix.lower() in SUPPORTED_EXT]
    return jsonify({"files": sorted(names)})


@app.route("/api/runs")
def list_runs():
    runs = []
    for d in sorted(RUNS.iterdir(), reverse=True):
        if d.is_dir():
            meta = d / "run_meta.json"
            if meta.is_file():
                try:
                    data = json.loads(meta.read_text(encoding="utf-8"))
                    runs.append({
                        "run_id": data.get("run_id", d.name),
                        "created_at": data.get("created_at"),
                        "file_count": len(data.get("files", [])),
                        "total_chunks_anonymized": data.get("total_chunks_anonymized"),
                    })
                except Exception:
                    runs.append({"run_id": d.name, "created_at": None})
    return jsonify({"runs": runs})


@app.route("/api/runs/<run_id>")
def get_run(run_id):
    run_dir = RUNS / run_id
    meta_file = run_dir / "run_meta.json"
    if not meta_file.is_file():
        return jsonify({"error": "Run not found"}), 404
    try:
        data = json.loads(meta_file.read_text(encoding="utf-8"))
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/report/<run_id>")
def get_report(run_id):
    run_dir = RUNS / run_id
    report_file = run_dir / "report.json"
    if not report_file.is_file():
        return jsonify({"error": "Report not found"}), 404
    try:
        data = json.loads(report_file.read_text(encoding="utf-8"))
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/run", methods=["POST"])
def run_pipeline():
    if not PIPELINE.is_file():
        return jsonify({"error": "Pipeline script not found"}), 500
    file_list = None
    if request.is_json:
        body = request.get_json(silent=True) or {}
        if body.get("files"):
            file_list = [str(f).strip() for f in body["files"] if f]
    if file_list is not None and len(file_list) == 0:
        return jsonify({"error": "No files specified."}), 400
    if file_list is None:
        count = len([p for p in UPLOADS.iterdir() if p.is_file() and p.suffix.lower() in SUPPORTED_EXT])
        if count == 0:
            return jsonify({"error": "No files in uploads. Upload files first."}), 400
    with _run_lock:
        if _running:
            return jsonify({"error": "A run is already in progress"}), 409
        t = threading.Thread(target=_run_pipeline_background, kwargs={"file_list": file_list}, daemon=True)
        t.start()
    return jsonify({"started": True})


@app.route("/report.json")
def report_json_legacy():
    """Legacy: serve latest report for report-viewer when opened with ?run_id=."""
    run_id = request.args.get("run_id")
    if run_id:
        report_file = RUNS / run_id / "report.json"
        if report_file.is_file():
            try:
                data = json.loads(report_file.read_text(encoding="utf-8"))
                return jsonify(data)
            except Exception:
                pass
    config_file = SCRIPT_DIR / "config.json"
    if config_file.is_file():
        try:
            config = json.loads(config_file.read_text(encoding="utf-8"))
            rel = config.get("latest_report")
            if rel:
                path = (SCRIPT_DIR / rel).resolve()
                if path.is_file():
                    data = json.loads(path.read_text(encoding="utf-8"))
                    return jsonify(data)
        except Exception:
            pass
    return jsonify({"error": "No report found"}), 404


if __name__ == "__main__":
    port = int(os.environ.get("ARMOR_PORT", 8765))
    print("ARMOR - Intelligent Contextual Anonymiser")
    print("Open in browser: http://127.0.0.1:{}/".format(port))
    app.run(host="0.0.0.0", port=port, threaded=True)
