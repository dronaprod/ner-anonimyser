#!/usr/bin/env python3
"""
One-time migration: apply _dedupe_findings to all existing report.json and run_meta.json
so older runs show deduped entity counts and findings (union found_by).
Run from ner-anonimyser: python migrate_dedupe_findings.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

RUNS_DIR = SCRIPT_DIR / "db" / "runs"


def main() -> int:
    try:
        from pii_anonymization_pipeline import _dedupe_findings
    except ImportError as e:
        print("Import failed:", e)
        return 1

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
                deduped = _dedupe_findings(raw)
                if len(deduped) != len(raw):
                    changed = True
                chunk["findings"] = deduped
                all_findings_raw.extend(deduped)
                dropped = chunk.get("dropped_findings", [])
                if dropped:
                    chunk["dropped_findings"] = _dedupe_findings(dropped)
                    all_dropped.extend(chunk["dropped_findings"])
            # File-level dedupe: merge same value across chunks (same as pipeline)
            all_findings = _dedupe_findings(all_findings_raw)
            if len(all_findings) != len(all_findings_raw):
                changed = True
            file_rec["all_findings"] = all_findings
            if all_dropped:
                file_rec["all_dropped_findings"] = _dedupe_findings(all_dropped)
            elif file_rec.get("all_dropped_findings"):
                file_rec["all_dropped_findings"] = _dedupe_findings(file_rec["all_dropped_findings"])

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
