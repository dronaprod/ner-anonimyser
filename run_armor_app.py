#!/usr/bin/env python3
"""
ARMOR - Intelligent Contextual Anonymiser.
Flask app: upload files, run pipeline, list runs, view file details and chunk logs, view report.
LLM NER: send first chunk to LiteLLM (GPT OSS 20B) for NER; compare with armor entities; store recall/precision.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import uuid
from pathlib import Path

# Ensure pipeline and litellm_ner_module can be imported when app is run from any cwd
if (SCRIPT_DIR := Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from flask import Flask, jsonify, request, send_from_directory

SCRIPT_DIR = Path(__file__).resolve().parent
DB = SCRIPT_DIR / "db"
UPLOADS = DB / "uploads"
REPORTS = DB / "reports"
RUNS = DB / "runs"
PROGRESS_FILE = DB / "progress.json"
LLM_ANALYSIS_FILE = DB / "llm_analysis.json"
DELETED_SCANNED_FILES = DB / "deleted_scanned_files.json"
UI_DIR = SCRIPT_DIR / "ui"
PIPELINE = SCRIPT_DIR / "pii_anonymization_pipeline.py"
_venv_py = SCRIPT_DIR / ".venv" / "bin" / "python"
PYTHON = os.environ.get("ARMOR_PYTHON", str(_venv_py if _venv_py.is_file() else "python3"))

# Chunk size/overlap for LLM NER (same as pipeline default)
CHUNK_SIZE = 1600
CHUNK_OVERLAP = 200

app = Flask(__name__, static_folder=str(UI_DIR), static_url_path="")
app.config["MAX_CONTENT_LENGTH"] = 200 * 1024 * 1024  # 200 MB max upload

UPLOADS.mkdir(parents=True, exist_ok=True)
RUNS.mkdir(parents=True, exist_ok=True)
REPORTS.mkdir(parents=True, exist_ok=True)

SUPPORTED_EXT = {".pdf", ".docx", ".xlsx", ".txt"}
_run_lock = threading.Lock()
_running = False
_llm_ner_lock = threading.Lock()
_llm_ner_running = False


def _load_llm_analysis() -> dict:
    """Load { file_name: { armor_entities, llm_entities, same, different_llm, recall_pct, precision_pct, last_run_id } }."""
    if not LLM_ANALYSIS_FILE.is_file():
        return {}
    try:
        return json.loads(LLM_ANALYSIS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_llm_analysis(data: dict) -> None:
    LLM_ANALYSIS_FILE.parent.mkdir(parents=True, exist_ok=True)
    LLM_ANALYSIS_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_deleted_scanned_files() -> set[str]:
    """Load set of file names that user has deleted from the scanned-files list."""
    if not DELETED_SCANNED_FILES.is_file():
        return set()
    try:
        data = json.loads(DELETED_SCANNED_FILES.read_text(encoding="utf-8"))
        return set(data) if isinstance(data, list) else set(data.keys()) if isinstance(data, dict) else set()
    except Exception:
        return set()


def _save_deleted_scanned_files(names: set[str]) -> None:
    DELETED_SCANNED_FILES.parent.mkdir(parents=True, exist_ok=True)
    DELETED_SCANNED_FILES.write_text(json.dumps(sorted(names)), encoding="utf-8")


def _canonical_label(l: str) -> str:
    l = (l or "").strip().lower()
    if l in ("aadhaar", "aadhaar number"):
        return "aadhaar number"
    if l in ("pan", "pan number"):
        return "pan number"
    if l in ("gst_number", "gst number"):
        return "gst number"
    if l in ("date_of_birth", "date of birth"):
        return "date of birth"
    return l


def _label_agreement(l1: str, l2: str) -> bool:
    """True if two labels are compatible for matching (e.g. date + date of birth, location + city)."""
    a, b = (l1 or "").strip().lower(), (l2 or "").strip().lower()
    if a == b:
        return True
    if _canonical_label(a) == _canonical_label(b):
        return True
    if a in ("person", "name") and b in ("person", "name"):
        return True
    if "address" in a or "address" in b:
        if a in ("state", "location", "city", "street address") or "address" in a:
            if b in ("state", "location", "city", "street address") or "address" in b:
                return True
    if a in ("state", "location", "city", "street address") and b in ("state", "location", "city", "address", "street address"):
        return True
    if a in ("date", "date of birth", "date_of_birth") and b in ("date", "date of birth", "date_of_birth"):
        return True
    return False


def _count_same_with_containment(
    armor_entities: list[tuple[str, str]],
    llm_entities: list[tuple[str, str]],
) -> int:
    """Count pairs (armor, llm) that match: labels compatible and one value contains the other (case-insensitive)."""
    armor_used: set[int] = set()
    same = 0
    for v_l, label_l in llm_entities:
        for i, (v_a, label_a) in enumerate(armor_entities):
            if i in armor_used:
                continue
            if not _label_agreement(label_a, label_l):
                continue
            if v_a in v_l or v_l in v_a:
                armor_used.add(i)
                same += 1
                break
    return same


def _dedupe_entity_pairs(pairs: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """Group duplicate/semi-duplicate (value, label) pairs: same label and one value contains the other; return one representative per group (longest value)."""
    if not pairs:
        return []
    # Group by: same canonical label and (v1 in v2 or v2 in v1)
    groups: list[list[tuple[str, str]]] = []
    for val_lower, label in pairs:
        merged = False
        for g in groups:
            if g[0][1] != label:
                continue
            for v, _ in g:
                if val_lower in v or v in val_lower:
                    g.append((val_lower, label))
                    merged = True
                    break
            if merged:
                break
        if not merged:
            groups.append([(val_lower, label)])
    # One representative per group: longest value
    return [max(g, key=lambda x: len(x[0])) for g in groups]


def _get_first_chunk(file_path: Path) -> str | None:
    """Extract text and return first chunk (same size as pipeline)."""
    try:
        from pii_anonymization_pipeline import extract_text, chunk_text
    except ImportError:
        return None
    try:
        text = extract_text(file_path)
        chunks = chunk_text(text, chunk_size=CHUNK_SIZE, chunk_overlap=CHUNK_OVERLAP)
        return chunks[0] if chunks else None
    except Exception:
        return None


def _get_armor_entities_for_file(file_name: str) -> tuple[list[tuple[str, str]], str | None]:
    """Get (value, pii_type) list for first chunk of file from last run that contains it. Returns (entities, run_id)."""
    for d in sorted(RUNS.iterdir(), reverse=True):
        if not d.is_dir():
            continue
        report_file = d / "report.json"
        if not report_file.is_file():
            continue
        try:
            data = json.loads(report_file.read_text(encoding="utf-8"))
            for f in data.get("files", []):
                if (f.get("file_name") or f.get("file")) != file_name:
                    continue
                chunks = f.get("chunks", [])
                if not chunks:
                    continue
                first = chunks[0]
                findings = first.get("findings", [])
                entities = []
                for x in findings:
                    v = (x.get("value") or "").strip()
                    t = (x.get("pii_type") or "").strip()
                    if v and t:
                        entities.append((v.lower(), _canonical_label(t)))
                # Dedupe duplicate/semi-duplicate entities (report findings are already deduped; this handles old reports)
                entities = _dedupe_entity_pairs(entities)
                return (entities, d.name)
        except Exception:
            continue
    return ([], None)


def _run_llm_ner_for_files(file_list: list[str]) -> dict:
    """For each file: get first chunk, get armor entities from last run, call LLM, compare, save. Returns { file_name: {...} }."""
    try:
        from litellm_ner_module import detect_pii_with_litellm
    except ImportError:
        return {"error": "litellm_ner_module not available"}
    api_key = os.environ.get("LITELLM_OPENAI_API_KEY", "").strip()
    base_url = os.environ.get("LITELLM_OPENAI_BASE_URL", "").strip()
    if not api_key:
        return {"error": "LITELLM_OPENAI_API_KEY not set"}
    results = {}
    analysis = _load_llm_analysis()
    for file_name in file_list:
        path = UPLOADS / file_name
        if not path.is_file() or path.suffix.lower() not in SUPPORTED_EXT:
            continue
        chunk = _get_first_chunk(path)
        if not chunk:
            results[file_name] = {"error": "Could not extract first chunk"}
            continue
        armor_entities, run_id = _get_armor_entities_for_file(file_name)
        llm_error = None
        try:
            llm_raw = detect_pii_with_litellm(chunk, api_key=api_key, base_url=base_url or None)
        except Exception as e:
            llm_error = str(e)
            llm_raw = []
        llm_entities = [(str(x.get("text", "")).strip().lower(), _canonical_label(str(x.get("label", "")))) for x in llm_raw if x.get("text") and x.get("label")]
        llm_entities = _dedupe_entity_pairs(llm_entities)
        # Same = match when labels compatible and (armor value in LLM value or LLM value in armor value), case-insensitive
        same = _count_same_with_containment(armor_entities, llm_entities)
        armor_count = len(armor_entities)
        llm_count = len(llm_entities)
        different_llm = llm_count - same
        # Recall = Same/LLM (of what LLM found, how many Armor also had). Precision = Same/Armor (of what Armor found, how many LLM also found).
        recall_pct = round(100.0 * same / llm_count, 1) if llm_count else 0.0
        precision_pct = round(100.0 * same / armor_count, 1) if armor_count else 0.0
        # Store raw LLM entity list for report viewer (value, label)
        llm_entities_list = [{"text": t, "label": l} for t, l in llm_entities]
        analysis[file_name] = {
            "armor_entities": armor_count,
            "llm_entities": llm_count,
            "same": same,
            "different_llm": different_llm,
            "recall_pct": recall_pct,
            "precision_pct": precision_pct,
            "last_run_id": run_id,
            "llm_entities_list": llm_entities_list,
        }
        if llm_error:
            analysis[file_name]["llm_error"] = llm_error
        results[file_name] = analysis[file_name]
    _save_llm_analysis(analysis)
    return results


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
        # Inherit env so OLLAMA_HOST, OLLAMA_NER_MODEL, ARMOR_GLINER_MODEL etc. work when run from app
        env = os.environ.copy()
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600, cwd=str(SCRIPT_DIR), env=env)
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


def _run_dirs_newest_first():
    """Yield run directories (newest first) by name for deterministic order."""
    if not RUNS.is_dir():
        return []
    try:
        dirs = [d for d in RUNS.iterdir() if d.is_dir()]
    except OSError:
        return []
    dirs.sort(key=lambda d: d.name, reverse=True)
    return dirs


@app.route("/api/scanned-files")
def list_scanned_files():
    """List all files that have been scanned; include LLM analysis metrics if present. Exclude deleted files."""
    deleted = _load_deleted_scanned_files()
    by_name = {}
    for d in _run_dirs_newest_first():
        run_id = d.name
        created = None
        file_names_in_run = []
        meta = d / "run_meta.json"
        if meta.is_file():
            try:
                data = json.loads(meta.read_text(encoding="utf-8"))
                run_id = data.get("run_id", run_id)
                created = data.get("created_at")
                for f in data.get("files", []):
                    name = f.get("file_name") or f.get("file")
                    if name and name not in file_names_in_run:
                        file_names_in_run.append(name)
            except Exception:
                pass
        report_file = d / "report.json"
        if report_file.is_file():
            try:
                report = json.loads(report_file.read_text(encoding="utf-8"))
                created = created or report.get("created_at")
                for file_rec in report.get("files", []):
                    name = file_rec.get("file_name") or file_rec.get("file")
                    if name and name not in file_names_in_run:
                        file_names_in_run.append(name)
            except Exception:
                pass
        for name in file_names_in_run:
            if name in deleted:
                continue
            if name not in by_name:
                by_name[name] = {"file_name": name, "last_run_id": run_id, "last_scanned_at": created}
    # Armor counts from report (deduped) so table shows current deduped entity count
    run_ids_done: set[str] = set()
    for rec in by_name.values():
        run_id = rec.get("last_run_id")
        if not run_id or run_id in run_ids_done:
            continue
        run_ids_done.add(run_id)
        report_file = RUNS / run_id / "report.json"
        if not report_file.is_file():
            continue
        try:
            report = json.loads(report_file.read_text(encoding="utf-8"))
            for file_rec in report.get("files", []):
                fname = file_rec.get("file_name") or file_rec.get("file")
                if fname and fname in by_name:
                    by_name[fname]["armor_entities"] = len(file_rec.get("all_findings", []))
        except Exception:
            continue

    llm_analysis = _load_llm_analysis()
    total_armor = total_llm = total_same = total_diff_llm = 0
    for rec in by_name.values():
        name = rec["file_name"]
        if name in llm_analysis:
            rec["llm_entities"] = llm_analysis[name].get("llm_entities")
            rec["same"] = llm_analysis[name].get("same")
            rec["different_llm"] = llm_analysis[name].get("different_llm")
            rec["recall_pct"] = llm_analysis[name].get("recall_pct")
            rec["precision_pct"] = llm_analysis[name].get("precision_pct")
            rec["llm_error"] = llm_analysis[name].get("llm_error")
            total_armor += rec.get("armor_entities") or 0
            total_llm += rec.get("llm_entities") or 0
            total_same += rec.get("same") or 0
            total_diff_llm += rec.get("different_llm") or 0
        else:
            total_armor += rec.get("armor_entities") or 0
    combined = {
        "armor_entities": total_armor,
        "llm_entities": total_llm,
        "same": total_same,
        "different_llm": total_diff_llm,
        "recall_pct": round(100.0 * total_same / total_llm, 1) if total_llm else 0.0,
        "precision_pct": round(100.0 * total_same / total_armor, 1) if total_armor else 0.0,
    }
    sorted_list = sorted(by_name.values(), key=lambda x: (x.get("last_scanned_at") or ""), reverse=True)
    latest_run_id = None
    for d in _run_dirs_newest_first():
        if (d / "run_meta.json").is_file() or (d / "report.json").is_file():
            latest_run_id = d.name
            break
    return jsonify({"scanned_files": sorted_list, "combined": combined, "latest_run_id": latest_run_id})


def _scanned_file_names() -> list[str]:
    """Return list of all scanned file names (from run_meta), excluding deleted."""
    deleted = _load_deleted_scanned_files()
    by_name = {}
    for d in sorted(RUNS.iterdir(), reverse=True):
        if not d.is_dir():
            continue
        meta = d / "run_meta.json"
        if not meta.is_file():
            continue
        try:
            data = json.loads(meta.read_text(encoding="utf-8"))
            for f in data.get("files", []):
                name = f.get("file_name") or f.get("file")
                if name and name not in deleted:
                    by_name[name] = True
        except Exception:
            continue
    return list(by_name.keys())


@app.route("/api/run-llm-ner", methods=["POST"])
def run_llm_ner():
    """Run LLM NER on scanned files. Body: { scope: 'pending' | 'all' } or { files: [...] }. pending = no LLM result yet or had error."""
    global _llm_ner_running
    with _llm_ner_lock:
        if _llm_ner_running:
            return jsonify({"error": "LLM NER already in progress"}), 409
        _llm_ner_running = True
    try:
        body = request.get_json(silent=True) or {}
        file_list = body.get("files")
        if not file_list:
            all_scanned = _scanned_file_names()
            scope = (body.get("scope") or "all").strip().lower()
            if scope == "pending":
                llm_analysis = _load_llm_analysis()
                file_list = [
                    name for name in all_scanned
                    if name not in llm_analysis or llm_analysis.get(name, {}).get("llm_error")
                ]
            else:
                file_list = all_scanned
        if not file_list:
            return jsonify({"error": "No scanned files to run LLM NER on"}), 400
        results = _run_llm_ner_for_files(file_list)
        if isinstance(results, dict) and results.get("error"):
            return jsonify(results), 500
        return jsonify({"ok": True, "results": results})
    finally:
        with _llm_ner_lock:
            _llm_ner_running = False


def _migrate_dedupe_findings() -> dict:
    """Apply dedupe to all existing report.json and run_meta.json; return { migrated: int, error?: str }."""
    try:
        from pii_anonymization_pipeline import _dedupe_findings
    except ImportError as e:
        return {"migrated": 0, "error": str(e)}
    migrated = 0
    for run_dir in sorted(RUNS.iterdir()):
        if not run_dir.is_dir():
            continue
        report_file = run_dir / "report.json"
        meta_file = run_dir / "run_meta.json"
        if not report_file.is_file():
            continue
        try:
            report = json.loads(report_file.read_text(encoding="utf-8"))
        except Exception as e:
            continue
        for file_rec in report.get("files", []):
            chunks = file_rec.get("chunks", [])
            all_findings_raw = []
            all_dropped = []
            for chunk in chunks:
                chunk["findings"] = _dedupe_findings(chunk.get("findings", []))
                all_findings_raw.extend(chunk["findings"])
                dropped = chunk.get("dropped_findings", [])
                if dropped:
                    chunk["dropped_findings"] = _dedupe_findings(dropped)
                    all_dropped.extend(chunk["dropped_findings"])
            # Apply same file-level dedupe as pipeline: merge same value across chunks
            file_rec["all_findings"] = _dedupe_findings(all_findings_raw)
            if all_dropped:
                file_rec["all_dropped_findings"] = _dedupe_findings(all_dropped)
            elif file_rec.get("all_dropped_findings"):
                file_rec["all_dropped_findings"] = _dedupe_findings(file_rec["all_dropped_findings"])
        report_file.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        migrated += 1
        if meta_file.is_file():
            try:
                meta = json.loads(meta_file.read_text(encoding="utf-8"))
            except Exception:
                continue
            for i, file_rec in enumerate(report.get("files", [])):
                all_f = file_rec.get("all_findings", [])
                if i < len(meta.get("files", [])):
                    meta["files"][i]["entity_count"] = len(all_f)
                    meta["files"][i]["entity_types"] = sorted(set(f.get("pii_type", "") for f in all_f if f.get("pii_type")))
                    for j, chunk in enumerate(file_rec.get("chunks", [])):
                        if j < len(meta["files"][i].get("chunk_logs", [])):
                            meta["files"][i]["chunk_logs"][j]["agreed_count"] = len(chunk.get("findings", []))
            meta_file.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"migrated": migrated}


@app.route("/api/migrate-dedupe-findings", methods=["POST"])
def api_migrate_dedupe_findings():
    """One-time: apply duplicate/semi-duplicate grouping to all existing reports and run_meta."""
    result = _migrate_dedupe_findings()
    if result.get("error"):
        return jsonify(result), 500
    return jsonify(result)


@app.route("/api/restore-scanned-files", methods=["POST"])
def restore_scanned_files():
    """Clear the deleted-files list so all previously scanned files show again."""
    _save_deleted_scanned_files(set())
    return jsonify({"ok": True, "message": "All previously scanned files will show again."})


@app.route("/api/scanned-file/<path:filename>", methods=["DELETE"])
def delete_scanned_file(filename):
    """Remove file from uploads, LLM analysis, and hide from scanned-files list."""
    safe_name = Path(filename).name
    if not safe_name or ".." in safe_name:
        return jsonify({"error": "Invalid filename"}), 400
    path = UPLOADS / safe_name
    if path.is_file():
        try:
            path.unlink()
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    analysis = _load_llm_analysis()
    if safe_name in analysis:
        del analysis[safe_name]
        _save_llm_analysis(analysis)
    deleted = _load_deleted_scanned_files()
    deleted.add(safe_name)
    _save_deleted_scanned_files(deleted)
    return jsonify({"deleted": safe_name})


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


@app.route("/api/file-llm-entities/<path:filename>")
def get_file_llm_entities(filename):
    """Return LLM analysis for a file (for report viewer: entities by Armor vs LLM)."""
    safe_name = Path(filename).name
    if not safe_name or ".." in safe_name:
        return jsonify({"error": "Invalid filename"}), 400
    analysis = _load_llm_analysis()
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
    gliner_env = os.environ.get("ARMOR_GLINER_MODEL")
    if gliner_env:
        print("GLiNER model (ARMOR_GLINER_MODEL):", gliner_env)
    else:
        print("GLiNER: knowledgator/gliner-x-large (if OOM, run: ARMOR_GLINER_MODEL=urchade/gliner_medium-v2.1 .venv/bin/python run_armor_app.py)")
    print("Open in browser: http://127.0.0.1:{}/".format(port))
    app.run(host="0.0.0.0", port=port, threaded=True)
