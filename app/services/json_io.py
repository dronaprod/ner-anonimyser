"""Atomic JSON read/write with logging."""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


def read_json(path: Path, default: Any = None) -> Any:
    if not path.is_file():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        log.warning("read_json failed %s: %s", path, exc)
        return default


def write_json_atomic(path: Path, data: Any, *, indent: int | None = 2) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    dump_kw: dict = {"ensure_ascii": True}
    if indent is not None:
        dump_kw["indent"] = indent
    text = json.dumps(data, **dump_kw)
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def write_progress(path: Path, obj: dict) -> None:
    try:
        write_json_atomic(path, obj, indent=None)
    except Exception as exc:
        log.debug("write_progress failed: %s", exc)
