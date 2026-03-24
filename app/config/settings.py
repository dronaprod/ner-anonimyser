"""
Armor Data Anonymizer — YAML configuration (pipeline + web).

Load order (each layer overrides the previous):
  1. ``<repo>/config/default.yaml``
  2. ``<repo>/config/local.yaml`` (optional)
  3. ``<repo>/instance/state.yaml`` — ``latest_report``, ``updated_at`` (written by pipeline)
"""
from __future__ import annotations

import os
from copy import deepcopy
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None  # type: ignore

# Project root (parent of the ``app`` package)
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DOTENV_PATH = REPO_ROOT / ".env"
CONFIG_DIR = REPO_ROOT / "config"
DEFAULT_YAML = CONFIG_DIR / "default.yaml"
LOCAL_YAML = CONFIG_DIR / "local.yaml"
STATE_YAML = REPO_ROOT / "instance" / "state.yaml"

DEFAULT_CPU_OLLAMA_NER_MODEL = "qwen3.5:4b"
DEFAULT_CPU_OLLAMA_ANON_MODEL = "qwen3.5:4b"


def load_dotenv_from_repo() -> None:
    """Load ``REPO_ROOT/.env`` into ``os.environ`` (optional ``python-dotenv``).

    Call this before reading config or starting the pipeline CLI so secrets and
    URLs match the web app. Existing OS environment variables are not overwritten.
    """
    try:
        from dotenv import load_dotenv
    except ModuleNotFoundError:
        return
    if DOTENV_PATH.is_file():
        load_dotenv(DOTENV_PATH)


def _read_yaml(path: Path) -> dict[str, Any]:
    if yaml is None or not path.is_file():
        return {}
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}


def _write_yaml(path: Path, data: dict[str, Any]) -> None:
    if yaml is None:
        raise RuntimeError("PyYAML is required to write YAML config (pip install pyyaml)")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data, default_flow_style=False, allow_unicode=True, sort_keys=False), encoding="utf-8")


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    out = deepcopy(base)
    for k, v in override.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = deepcopy(v)
    return out


def load_armor_config() -> dict[str, Any]:
    merged = _read_yaml(DEFAULT_YAML)
    if LOCAL_YAML.is_file():
        merged = _deep_merge(merged, _read_yaml(LOCAL_YAML))

    state = _read_yaml(STATE_YAML)
    if state.get("latest_report") is not None:
        merged["latest_report"] = state["latest_report"]
    if state.get("updated_at") is not None:
        merged["updated_at"] = state["updated_at"]

    data_root_override = os.environ.get("ARMOR_DATA_ROOT", "").strip()
    if data_root_override:
        paths = merged.get("paths")
        if not isinstance(paths, dict):
            paths = {}
            merged["paths"] = paths
        paths["data_root"] = data_root_override

    return merged


def load_armor_config_json() -> dict[str, Any]:
    return load_armor_config()


def write_armor_state(*, latest_report: str, updated_at: str) -> None:
    existing = _read_yaml(STATE_YAML)
    existing["latest_report"] = latest_report
    existing["updated_at"] = updated_at
    _write_yaml(STATE_YAML, existing)


def normalize_mode(mode: str | None) -> str:
    m = (mode or "gpu").strip().lower()
    return m if m in ("cpu", "gpu") else "gpu"


def apply_qwen_runtime_settings(mode: str | None, cfg: dict | None = None) -> str:
    cfg = cfg if cfg is not None else load_armor_config()
    m = normalize_mode(mode if mode is not None else cfg.get("mode"))
    os.environ["ARMOR_QWEN_MODE"] = m
    qwen_block = cfg.get("qwen") if isinstance(cfg.get("qwen"), dict) else {}
    if m == "cpu":
        os.environ.setdefault("OLLAMA_NUM_GPU", "0")
        ner = str(qwen_block.get("ollama_ner_model") or DEFAULT_CPU_OLLAMA_NER_MODEL)
        anon = str(qwen_block.get("ollama_model") or DEFAULT_CPU_OLLAMA_ANON_MODEL)
        os.environ.setdefault("OLLAMA_NER_MODEL", ner)
        os.environ.setdefault("OLLAMA_MODEL", anon)
    else:
        if qwen_block.get("ollama_num_gpu") is not None:
            os.environ["OLLAMA_NUM_GPU"] = str(qwen_block["ollama_num_gpu"])
    return m


def qwen_ner_source_key(mode: str) -> str:
    return "qwen_cpu" if normalize_mode(mode) == "cpu" else "qwen"


def qwen_public_display_name(mode: str) -> str:
    return "Qwen CPU" if normalize_mode(mode) == "cpu" else "Qwen"


def merge_config_for_write(existing: dict, updates: dict) -> dict:
    out = dict(existing) if isinstance(existing, dict) else {}
    out.update(updates)
    return out
