"""Resolve filesystem paths from merged YAML settings (Flask app + pipeline subprocess helper)."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from app.config.settings import load_armor_config


@dataclass(frozen=True)
class ArmorPaths:
    project_root: Path
    data_root: Path
    uploads: Path
    reports: Path
    runs: Path
    progress_file: Path
    llm_analysis: Path
    deleted_scanned: Path
    ui_dir: Path
    pipeline_script: Path
    log_dir: Path


def _abs_under(root: Path, p: str | Path) -> Path:
    path = Path(p)
    return path if path.is_absolute() else (root / path)


def load_paths_and_settings(project_root: Path) -> tuple[dict, ArmorPaths]:
    raw = load_armor_config()
    paths_cfg = raw.get("paths") if isinstance(raw.get("paths"), dict) else {}
    data_root = _abs_under(project_root, paths_cfg.get("data_root", "db"))
    ui_dir = _abs_under(project_root, paths_cfg.get("ui_dir", "ui"))
    log_dir = _abs_under(project_root, paths_cfg.get("log_dir", "log"))

    pl_cfg = raw.get("pipeline") if isinstance(raw.get("pipeline"), dict) else {}
    script_name = pl_cfg.get("script") or "app/pipeline.py"
    pipeline_script = _abs_under(project_root, script_name)

    paths = ArmorPaths(
        project_root=project_root,
        data_root=data_root,
        uploads=data_root / "uploads",
        reports=data_root / "reports",
        runs=data_root / "runs",
        progress_file=data_root / "progress.json",
        llm_analysis=data_root / "llm_analysis.json",
        deleted_scanned=data_root / "deleted_scanned_files.json",
        ui_dir=ui_dir,
        pipeline_script=pipeline_script,
        log_dir=log_dir,
    )
    return raw, paths


def pipeline_python_executable(project_root: Path, settings: dict) -> str:
    pl = settings.get("pipeline") if isinstance(settings.get("pipeline"), dict) else {}
    explicit = (pl.get("python_executable") or "").strip()
    if explicit:
        return explicit
    env = os.environ.get("ARMOR_PYTHON", "").strip()
    if env:
        return env
    venv_py = project_root / ".venv" / "bin" / "python"
    return str(venv_py) if venv_py.is_file() else "python3"


def pipeline_timeout_seconds(settings: dict) -> int:
    pl = settings.get("pipeline") if isinstance(settings.get("pipeline"), dict) else {}
    try:
        return max(60, int(pl.get("timeout_seconds", 3600)))
    except (TypeError, ValueError):
        return 3600


def chunk_params(settings: dict) -> tuple[int, int]:
    ch = settings.get("chunk") if isinstance(settings.get("chunk"), dict) else {}
    try:
        size = int(ch.get("size", 1600))
    except (TypeError, ValueError):
        size = 1600
    try:
        overlap = int(ch.get("overlap", 200))
    except (TypeError, ValueError):
        overlap = 200
    return size, overlap


def flask_secret_key(settings: dict) -> str:
    for key in ("ARMOR_SECRET_KEY", "FLASK_SECRET_KEY"):
        v = os.environ.get(key, "").strip()
        if v:
            return v
    sec = settings.get("security") if isinstance(settings.get("security"), dict) else {}
    sk = (sec.get("secret_key") or "").strip()
    if sk:
        return sk
    return "dev-insecure-change-me"
