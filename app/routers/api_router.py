"""JSON API routes (REST-style): uploads, runs, pipeline, LLM comparison."""
from __future__ import annotations

import json
import logging
import threading
from pathlib import Path

from flask import Blueprint, current_app, jsonify, request

from app import runtime_state as rs
from app.exceptions import ArmorApiError
from app.services.core import (
    SUPPORTED_EXT,
    build_scanned_files_payload,
    load_deleted_scanned_files,
    load_llm_analysis,
    migrate_dedupe_findings,
    run_llm_ner_for_files,
    run_pipeline_background,
    save_deleted_scanned_files,
    save_llm_analysis,
    scanned_file_names,
)
from app.services.json_io import read_json
from app.config import load_armor_config, normalize_mode, qwen_public_display_name

log = logging.getLogger(__name__)

bp = Blueprint("armor_api", __name__, url_prefix="/api")


def _paths():
    return current_app.config["ARMOR_PATHS"]


def _settings():
    return current_app.config["ARMOR_SETTINGS"]


@bp.route("/settings")
def api_armor_settings():
    cfg = load_armor_config()
    mode = normalize_mode(cfg.get("mode", "gpu"))
    return jsonify({
        "mode": mode,
        "qwen_ner_tab_label": "NER Qwen CPU" if mode == "cpu" else "NER Qwen",
        "qwen_progress_label": qwen_public_display_name(mode),
    })


@bp.route("/progress")
def get_progress():
    paths = _paths()
    if not paths.progress_file.is_file():
        return jsonify({"running": False})
    data = read_json(paths.progress_file, {"running": False})
    return jsonify(data if isinstance(data, dict) else {"running": False})


@bp.route("/upload", methods=["POST"])
def upload():
    paths = _paths()
    if "files" not in request.files and "file" not in request.files:
        raise ArmorApiError("No file(s)", 400)
    files = request.files.getlist("files") or [request.files.get("file")]
    saved = []
    paths.uploads.mkdir(parents=True, exist_ok=True)
    for f in files:
        if not f or not f.filename:
            continue
        ext = Path(f.filename).suffix.lower()
        if ext not in SUPPORTED_EXT:
            continue
        safe_name = f.filename
        f.save(str(paths.uploads / safe_name))
        saved.append(safe_name)
    return jsonify({"uploaded": saved})


@bp.route("/files")
def list_uploads():
    paths = _paths()
    names = [p.name for p in paths.uploads.iterdir() if p.is_file() and p.suffix.lower() in SUPPORTED_EXT]
    return jsonify({"files": sorted(names)})


@bp.route("/runs")
def list_runs():
    paths = _paths()
    runs = []
    if not paths.runs.is_dir():
        return jsonify({"runs": runs})
    for d in sorted(paths.runs.iterdir(), reverse=True):
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


@bp.route("/scanned-files")
def list_scanned_files():
    return jsonify(build_scanned_files_payload(_paths()))


@bp.route("/run-llm-ner", methods=["POST"])
def run_llm_ner():
    with rs.llm_ner_lock:
        if rs.llm_ner_running:
            return jsonify({"error": "LLM NER already in progress"}), 409
        rs.llm_ner_running = True
    try:
        body = request.get_json(silent=True) or {}
        file_list = body.get("files")
        paths = _paths()
        settings = _settings()
        if not file_list:
            all_scanned = scanned_file_names(paths)
            scope = (body.get("scope") or "all").strip().lower()
            if scope == "pending":
                llm_analysis = load_llm_analysis(paths.llm_analysis)
                file_list = [
                    name for name in all_scanned
                    if name not in llm_analysis or llm_analysis.get(name, {}).get("llm_error")
                ]
            else:
                file_list = all_scanned
        if not file_list:
            return jsonify({"error": "No scanned files to run LLM NER on"}), 400
        results = run_llm_ner_for_files(paths, paths.project_root, settings, file_list)
        if isinstance(results, dict) and results.get("error"):
            return jsonify(results), 500
        return jsonify({"ok": True, "results": results})
    finally:
        with rs.llm_ner_lock:
            rs.llm_ner_running = False


@bp.route("/migrate-dedupe-findings", methods=["POST"])
def api_migrate_dedupe_findings():
    paths = _paths()
    result = migrate_dedupe_findings(paths, paths.project_root)
    if result.get("error"):
        return jsonify(result), 500
    return jsonify(result)


@bp.route("/restore-scanned-files", methods=["POST"])
def restore_scanned_files():
    save_deleted_scanned_files(_paths().deleted_scanned, set())
    return jsonify({"ok": True, "message": "All previously scanned files will show again."})


@bp.route("/scanned-file/<path:filename>", methods=["DELETE"])
def delete_scanned_file(filename):
    paths = _paths()
    safe_name = Path(filename).name
    if not safe_name or ".." in safe_name:
        raise ArmorApiError("Invalid filename", 400)
    upload_path = paths.uploads / safe_name
    if upload_path.is_file():
        try:
            upload_path.unlink()
        except OSError as e:
            log.warning("delete upload failed: %s", e)
            return jsonify({"error": str(e)}), 500
    analysis = load_llm_analysis(paths.llm_analysis)
    if safe_name in analysis:
        del analysis[safe_name]
        save_llm_analysis(paths.llm_analysis, analysis)
    deleted = load_deleted_scanned_files(paths.deleted_scanned)
    deleted.add(safe_name)
    save_deleted_scanned_files(paths.deleted_scanned, deleted)
    return jsonify({"deleted": safe_name})


@bp.route("/runs/<run_id>")
def get_run(run_id):
    paths = _paths()
    meta_file = paths.runs / run_id / "run_meta.json"
    if not meta_file.is_file():
        return jsonify({"error": "Run not found"}), 404
    try:
        data = json.loads(meta_file.read_text(encoding="utf-8"))
        return jsonify(data)
    except Exception as e:
        log.warning("get_run failed: %s", e)
        return jsonify({"error": str(e)}), 500


@bp.route("/report/<run_id>")
def get_report(run_id):
    paths = _paths()
    report_file = paths.runs / run_id / "report.json"
    if not report_file.is_file():
        return jsonify({"error": "Report not found"}), 404
    try:
        data = json.loads(report_file.read_text(encoding="utf-8"))
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/file-llm-entities/<path:filename>")
def get_file_llm_entities(filename):
    safe_name = Path(filename).name
    if not safe_name or ".." in safe_name:
        raise ArmorApiError("Invalid filename", 400)
    analysis = load_llm_analysis(_paths().llm_analysis)
    if safe_name not in analysis:
        return jsonify({
            "file_name": safe_name,
            "llm_entities_list": [],
            "armor_entities": 0,
            "llm_entities": 0,
            "same": 0,
            "different_llm": 0,
            "recall_pct": 0,
            "precision_pct": 0,
        })
    entry = analysis[safe_name]
    return jsonify({
        "file_name": safe_name,
        "llm_entities_list": entry.get("llm_entities_list", []),
        "armor_entities": entry.get("armor_entities", 0),
        "llm_entities": entry.get("llm_entities", 0),
        "same": entry.get("same", 0),
        "different_llm": entry.get("different_llm", 0),
        "recall_pct": entry.get("recall_pct", 0),
        "precision_pct": entry.get("precision_pct", 0),
        "llm_error": entry.get("llm_error"),
    })


@bp.route("/run", methods=["POST"])
def run_pipeline():
    paths = _paths()
    settings = _settings()
    if not paths.pipeline_script.is_file():
        return jsonify({"error": "Pipeline script not found"}), 500
    file_list = None
    if request.is_json:
        body = request.get_json(silent=True) or {}
        if body.get("files"):
            file_list = [str(f).strip() for f in body["files"] if f]
    if file_list is not None and len(file_list) == 0:
        return jsonify({"error": "No files specified."}), 400
    if file_list is None:
        count = len([p for p in paths.uploads.iterdir() if p.is_file() and p.suffix.lower() in SUPPORTED_EXT])
        if count == 0:
            return jsonify({"error": "No files in uploads. Upload files first."}), 400
    with rs.run_lock:
        if rs.running:
            return jsonify({"error": "A run is already in progress"}), 409
    threading.Thread(
        target=run_pipeline_background,
        args=(paths, settings, file_list),
        daemon=True,
    ).start()
    return jsonify({"started": True})
