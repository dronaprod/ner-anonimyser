#!/usr/bin/env python3
"""
Migration: dedupe findings, relabel suspicious_token where appropriate, and drop rows with
score below DEFAULT_MIN_NER_CONFIDENCE (0.6) from chunk findings, per-detector lists, and
all_findings — same as the current pipeline.

Run from repo root::

    python scripts/migrate_dedupe_findings.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

RUNS_DIR = REPO_ROOT / "db" / "runs"

NON_AUDIT_SOURCES = frozenset({
    "presidio", "gliner", "qwen", "qwen_cpu",
    "gliner_xlarge", "gliner_gretelai", "gliner_urchade", "gliner_arabic",
    "gretelai_gliner_large", "urchade_gliner_large_2.1_og",
})


def _relabel_suspicious_when_others_said(findings: list) -> None:
    """In-place: if pii_type is suspicious_token but found_by includes gliner/presidio/qwen, set type from value pattern."""
    try:
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
        if not (by_set & NON_AUDIT_SOURCES):
            continue
        value = (f.get("value") or "").strip()
        if not value:
            continue
        if AADHAAR_PATTERN.search(value):
            f["pii_type"] = "aadhaar number"
        elif PAN_PATTERN.search(value):
            f["pii_type"] = "pan number"


def main() -> int:
    try:
        from app.models import DEFAULT_MIN_NER_CONFIDENCE
        from app.pipeline import (
            _dedupe_findings,
            filter_chunk_report_scores_inplace,
            filter_finding_dicts_by_min_score,
        )
    except ImportError as e:
        print("Import failed:", e)
        return 1

    min_sc = DEFAULT_MIN_NER_CONFIDENCE

    if not RUNS_DIR.is_dir():
        print("No db/runs directory found.")
        return 0

    migrated = 0
    for run_dir in sorted(RUNS_DIR.iterdir()):
        if not run_dir.is_dir():
            continue
        report_file = run_dir / "report.json"
        meta_file = run_dir / "run_meta.json"
        if not report_file.is_file():
            continue

        try:
            report = json.loads(report_file.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"Skip {run_dir.name}: failed to load report: {e}")
            continue

        changed = False
        for file_rec in report.get("files", []):
            chunks = file_rec.get("chunks", [])
            all_findings_raw = []
            all_dropped = []
            for chunk in chunks:
                raw = chunk.get("findings", [])
                _relabel_suspicious_when_others_said(raw)
                deduped = _dedupe_findings(raw)
                deduped = filter_finding_dicts_by_min_score(deduped, min_sc)
                chunk["findings"] = deduped
                filter_chunk_report_scores_inplace(chunk, min_sc)
                if len(deduped) != len(raw):
                    changed = True
                all_findings_raw.extend(deduped)
                dropped = chunk.get("dropped_findings", [])
                if dropped:
                    _relabel_suspicious_when_others_said(dropped)
                    d2 = filter_finding_dicts_by_min_score(_dedupe_findings(dropped), min_sc)
                    chunk["dropped_findings"] = d2
                    all_dropped.extend(chunk["dropped_findings"])
            _relabel_suspicious_when_others_said(all_findings_raw)
            all_findings = filter_finding_dicts_by_min_score(_dedupe_findings(all_findings_raw), min_sc)
            if len(all_findings) != len(all_findings_raw):
                changed = True
            file_rec["all_findings"] = all_findings
            if all_dropped:
                file_rec["all_dropped_findings"] = filter_finding_dicts_by_min_score(
                    _dedupe_findings(all_dropped), min_sc
                )
            elif file_rec.get("all_dropped_findings"):
                file_rec["all_dropped_findings"] = filter_finding_dicts_by_min_score(
                    _dedupe_findings(file_rec["all_dropped_findings"]), min_sc
                )

        if changed or True:
            report_file.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
            migrated += 1

        if meta_file.is_file():
            try:
                meta = json.loads(meta_file.read_text(encoding="utf-8"))
            except Exception:
                continue
            files_meta = meta.get("files", [])
            files_report = report.get("files", [])
            for i, file_rec in enumerate(files_report):
                all_f = file_rec.get("all_findings", [])
                if i < len(files_meta):
                    files_meta[i]["entity_count"] = len(all_f)
                    chunk_logs = files_meta[i].get("chunk_logs", [])
                    for j, chunk in enumerate(file_rec.get("chunks", [])):
                        if j < len(chunk_logs):
                            chunk_logs[j]["agreed_count"] = len(chunk.get("findings", []))
                if i < len(files_meta):
                    files_meta[i]["entity_types"] = sorted(
                        set(t for t in (f.get("pii_type", "") for f in all_f) if t)
                    )
            meta_file.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Migrated {migrated} run(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
