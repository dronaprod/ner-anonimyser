"""Business logic: LLM analysis, scanned-file index, pipeline subprocess, report migration."""
from __future__ import annotations

import json
import logging
import os
import subprocess
import threading
from pathlib import Path
from typing import Any

from app.services.json_io import read_json, write_json_atomic, write_progress
from app.config import ArmorPaths, chunk_params, pipeline_python_executable, pipeline_timeout_seconds
from app.config import load_armor_config, normalize_mode

log = logging.getLogger(__name__)

SUPPORTED_EXT = {".pdf", ".docx", ".xlsx", ".txt"}

_NON_AUDIT_SOURCES = frozenset({
    "presidio", "gliner", "qwen", "qwen_cpu",
    "gliner_xlarge", "gliner_gretelai", "gliner_urchade", "gliner_arabic",
    "gretelai_gliner_large", "urchade_gliner_large_2.1_og",
})


def load_llm_analysis(path: Path) -> dict:
    data = read_json(path, {})
    return data if isinstance(data, dict) else {}


def save_llm_analysis(path: Path, data: dict) -> None:
    write_json_atomic(path, data)


def load_deleted_scanned_files(path: Path) -> set[str]:
    data = read_json(path, [])
    if isinstance(data, list):
        return set(data)
    if isinstance(data, dict):
        return set(data.keys())
    return set()


def save_deleted_scanned_files(path: Path, names: set[str]) -> None:
    write_json_atomic(path, sorted(names))


def canonical_label(l: str) -> str:
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


def label_agreement(l1: str, l2: str) -> bool:
    a, b = (l1 or "").strip().lower(), (l2 or "").strip().lower()
    if a == b:
        return True
    if canonical_label(a) == canonical_label(b):
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


def count_same_with_containment(
    armor_entities: list[tuple[str, str]],
    llm_entities: list[tuple[str, str]],
) -> int:
    armor_used: set[int] = set()
    same = 0
    for v_l, label_l in llm_entities:
        for i, (v_a, label_a) in enumerate(armor_entities):
            if i in armor_used:
                continue
            if not label_agreement(label_a, label_l):
                continue
            if v_a in v_l or v_l in v_a:
                armor_used.add(i)
                same += 1
                break
    return same


def dedupe_entity_pairs(pairs: list[tuple[str, str]]) -> list[tuple[str, str]]:
    if not pairs:
        return []
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
    return [max(g, key=lambda x: len(x[0])) for g in groups]


def get_first_chunk(file_path: Path, project_root: Path, settings: dict) -> str | None:
    try:
        import sys
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
        from app.utils.text import chunk_text
        from app.pipeline import extract_text
    except ImportError:
        return None
    chunk_size, chunk_overlap = chunk_params(settings)
    try:
        text = extract_text(file_path)
        chunks = chunk_text(text, chunk_size=chunk_size, chunk_overlap=chunk_overlap)
        return chunks[0] if chunks else None
    except Exception as exc:
        log.warning("get_first_chunk failed %s: %s", file_path, exc)
        return None


def get_armor_entities_for_file(paths: ArmorPaths, file_name: str) -> tuple[list[tuple[str, str]], str | None]:
    for d in sorted(paths.runs.iterdir(), reverse=True):
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
                        entities.append((v.lower(), canonical_label(t)))
                entities = dedupe_entity_pairs(entities)
                return (entities, d.name)
        except Exception:
            continue
    return ([], None)


def run_llm_ner_for_files(
    paths: ArmorPaths,
    project_root: Path,
    settings: dict,
    file_list: list[str],
) -> dict:
    try:
        from app.services.ner.litellm_ner import detect_pii_with_litellm
    except ImportError:
        return {"error": "litellm NER module not available"}
    api_key = os.environ.get("LITELLM_OPENAI_API_KEY", "").strip()
    base_url = os.environ.get("LITELLM_OPENAI_BASE_URL", "").strip()
    if not api_key:
        return {"error": "LITELLM_OPENAI_API_KEY not set"}
    results: dict = {}
    analysis = load_llm_analysis(paths.llm_analysis)
    for file_name in file_list:
        path = paths.uploads / file_name
        if not path.is_file() or path.suffix.lower() not in SUPPORTED_EXT:
            continue
        chunk = get_first_chunk(path, project_root, settings)
        if not chunk:
            results[file_name] = {"error": "Could not extract first chunk"}
            continue
        armor_entities, run_id = get_armor_entities_for_file(paths, file_name)
        llm_error = None
        try:
            llm_raw = detect_pii_with_litellm(chunk, api_key=api_key, base_url=base_url or None)
        except Exception as e:
            llm_error = str(e)
            llm_raw = []
        llm_entities = [
            (str(x.get("text", "")).strip().lower(), canonical_label(str(x.get("label", ""))))
            for x in llm_raw
            if x.get("text") and x.get("label")
        ]
        llm_entities = dedupe_entity_pairs(llm_entities)
        same = count_same_with_containment(armor_entities, llm_entities)
        armor_count = len(armor_entities)
        llm_count = len(llm_entities)
        different_llm = llm_count - same
        recall_pct = round(100.0 * same / llm_count, 1) if llm_count else 0.0
        precision_pct = round(100.0 * same / armor_count, 1) if armor_count else 0.0
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
    save_llm_analysis(paths.llm_analysis, analysis)
    return results


def run_dirs_newest_first(runs_dir: Path) -> list[Path]:
    if not runs_dir.is_dir():
        return []
    try:
        dirs = [d for d in runs_dir.iterdir() if d.is_dir()]
    except OSError:
        return []
    dirs.sort(key=lambda d: d.name, reverse=True)
    return dirs


def build_scanned_files_payload(paths: ArmorPaths) -> dict:
    deleted = load_deleted_scanned_files(paths.deleted_scanned)
    by_name: dict[str, dict] = {}
    for d in run_dirs_newest_first(paths.runs):
        run_id = d.name
        created = None
        file_names_in_run: list[str] = []
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

    run_ids_done: set[str] = set()
    for rec in by_name.values():
        run_id = rec.get("last_run_id")
        if not run_id or run_id in run_ids_done:
            continue
        run_ids_done.add(run_id)
        report_file = paths.runs / run_id / "report.json"
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

    llm_analysis = load_llm_analysis(paths.llm_analysis)
    total_armor = total_llm = total_same = total_diff_llm = 0
    for rec in by_name.values():
        name = rec["file_name"]
        if name in llm_analysis:
            la = llm_analysis[name]
            rec["llm_entities"] = la.get("llm_entities")
            rec["same"] = la.get("same")
            rec["different_llm"] = la.get("different_llm")
            rec["recall_pct"] = la.get("recall_pct")
            rec["precision_pct"] = la.get("precision_pct")
            rec["llm_error"] = la.get("llm_error")
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
    for d in run_dirs_newest_first(paths.runs):
        if (d / "run_meta.json").is_file() or (d / "report.json").is_file():
            latest_run_id = d.name
            break
    return {"scanned_files": sorted_list, "combined": combined, "latest_run_id": latest_run_id}


def scanned_file_names(paths: ArmorPaths) -> list[str]:
    deleted = load_deleted_scanned_files(paths.deleted_scanned)
    by_name: dict[str, bool] = {}
    for d in sorted(paths.runs.iterdir(), reverse=True):
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


def relabel_suspicious_when_others_said(findings: list, project_root: Path) -> None:
    try:
        import sys
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
        from app.pipeline import AADHAAR_PATTERN, PAN_PATTERN
    except ImportError:
        return
    for f in findings:
        if not isinstance(f, dict):
            continue
        if (f.get("pii_type") or "").strip().lower() != "suspicious_token":
            continue
        found_by = f.get("found_by") or []
        if not isinstance(found_by, list):
            found_by = []
        by_set = {str(s).strip().lower() for s in found_by if s}
        if not (by_set & _NON_AUDIT_SOURCES):
            continue
        value = (f.get("value") or "").strip()
        if not value:
            continue
        if AADHAAR_PATTERN.search(value):
            f["pii_type"] = "aadhaar number"
        elif PAN_PATTERN.search(value):
            f["pii_type"] = "pan number"


def migrate_dedupe_findings(paths: ArmorPaths, project_root: Path) -> dict:
    try:
        import sys
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
        from app.models import DEFAULT_MIN_NER_CONFIDENCE
        from app.pipeline import (
            _dedupe_findings,
            filter_chunk_report_scores_inplace,
            filter_finding_dicts_by_min_score,
        )
    except ImportError as e:
        return {"migrated": 0, "error": str(e)}
    min_sc = DEFAULT_MIN_NER_CONFIDENCE
    migrated = 0
    for run_dir in sorted(paths.runs.iterdir()):
        if not run_dir.is_dir():
            continue
        report_file = run_dir / "report.json"
        meta_file = run_dir / "run_meta.json"
        if not report_file.is_file():
            continue
        try:
            report = json.loads(report_file.read_text(encoding="utf-8"))
        except Exception:
            continue
        for file_rec in report.get("files", []):
            chunks = file_rec.get("chunks", [])
            all_findings_raw = []
            all_dropped = []
            for chunk in chunks:
                raw_f = chunk.get("findings", [])
                relabel_suspicious_when_others_said(raw_f, project_root)
                deduped = filter_finding_dicts_by_min_score(_dedupe_findings(raw_f), min_sc)
                chunk["findings"] = deduped
                filter_chunk_report_scores_inplace(chunk, min_sc)
                all_findings_raw.extend(chunk["findings"])
                dropped = chunk.get("dropped_findings", [])
                if dropped:
                    relabel_suspicious_when_others_said(dropped, project_root)
                    d2 = filter_finding_dicts_by_min_score(_dedupe_findings(dropped), min_sc)
                    chunk["dropped_findings"] = d2
                    all_dropped.extend(chunk["dropped_findings"])
            relabel_suspicious_when_others_said(all_findings_raw, project_root)
            file_rec["all_findings"] = filter_finding_dicts_by_min_score(
                _dedupe_findings(all_findings_raw), min_sc
            )
            if all_dropped:
                relabel_suspicious_when_others_said(all_dropped, project_root)
                file_rec["all_dropped_findings"] = filter_finding_dicts_by_min_score(
                    _dedupe_findings(all_dropped), min_sc
                )
            elif file_rec.get("all_dropped_findings"):
                relabel_suspicious_when_others_said(file_rec["all_dropped_findings"], project_root)
                file_rec["all_dropped_findings"] = filter_finding_dicts_by_min_score(
                    _dedupe_findings(file_rec["all_dropped_findings"]), min_sc
                )
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
                    meta["files"][i]["entity_types"] = sorted(
                        set(f.get("pii_type", "") for f in all_f if f.get("pii_type"))
                    )
                    for j, chunk in enumerate(file_rec.get("chunks", [])):
                        if j < len(meta["files"][i].get("chunk_logs", [])):
                            meta["files"][i]["chunk_logs"][j]["agreed_count"] = len(chunk.get("findings", []))
            meta_file.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"migrated": migrated}


def run_pipeline_background(paths: ArmorPaths, settings: dict, file_list: list[str] | None) -> None:
    from app import runtime_state as rs

    with rs.run_lock:
        if rs.running:
            return
        rs.running = True
    write_progress(paths.progress_file, {"running": True, "stage": "starting"})
    try:
        if file_list:
            existing = {p.name for p in paths.uploads.iterdir() if p.is_file() and p.suffix.lower() in SUPPORTED_EXT}
            to_run = [f for f in file_list if f in existing]
            missing = [f for f in file_list if f not in existing]
            if missing:
                write_progress(
                    paths.progress_file,
                    {"running": False, "error": "File(s) not in uploads: " + ", ".join(missing[:5])},
                )
                return
            if not to_run:
                write_progress(paths.progress_file, {"running": False, "error": "No requested files found in uploads"})
                return
            count = len(to_run)
            files_arg = ",".join(to_run)
        else:
            count = len([p for p in paths.uploads.iterdir() if p.is_file() and p.suffix.lower() in SUPPORTED_EXT])
            if count == 0:
                write_progress(paths.progress_file, {"running": False, "error": "No files in uploads"})
                return
            files_arg = None

        python_exe = pipeline_python_executable(paths.project_root, settings)
        timeout = pipeline_timeout_seconds(settings)
        pl = settings.get("pipeline") if isinstance(settings.get("pipeline"), dict) else {}
        pipeline_module = (pl.get("module") or "app.pipeline").strip()
        cmd = [
            python_exe,
            "-m",
            pipeline_module,
            "--input-dir", str(paths.uploads),
            "--report-dir", str(paths.reports),
            "--num-files", str(count),
            "--progress-file", str(paths.progress_file),
        ]
        if files_arg:
            cmd.extend(["--files", files_arg])
        cfg = load_armor_config()
        cmd.extend(["--mode", normalize_mode(cfg.get("mode", "gpu"))])
        env = os.environ.copy()
        log.info("Starting pipeline: %s", " ".join(cmd[:6]) + " ...")
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=str(paths.project_root),
            env=env,
        )
        if result.returncode != 0:
            log.error("Pipeline failed rc=%s stderr=%s", result.returncode, (result.stderr or "")[-500:])
            write_progress(
                paths.progress_file,
                {"running": False, "error": "Pipeline failed", "stderr": (result.stderr or "")[-1000:]},
            )
            return
        run_id = None
        for d in sorted(paths.runs.iterdir(), reverse=True):
            if d.is_dir() and (d / "run_meta.json").is_file():
                run_id = d.name
                break
        write_progress(paths.progress_file, {"running": False, "run_id": run_id})
    except subprocess.TimeoutExpired:
        log.error("Pipeline timed out after %s s", timeout)
        write_progress(paths.progress_file, {"running": False, "error": "Pipeline timed out"})
    except Exception as e:
        log.exception("Pipeline error")
        write_progress(paths.progress_file, {"running": False, "error": str(e)})
    finally:
        with rs.run_lock:
            rs.running = False
