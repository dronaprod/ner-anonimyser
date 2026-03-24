"""
``armor_stages`` block in YAML: ordered pipeline flow, per-language NER models with weights,
deterministic NER toggle, calibration / agreement thresholds, SLM judge & anonymiser (provider + model).
Prompts live in ``app.config.prompts_loader``; GLiNER/Presidio registry in ``app.config.ner_registry``.

Providers supported in config: ``custom``, ``vllm``, ``ollama``, ``litellm`` (normalize with
:func:`normalize_provider`).
"""
from __future__ import annotations

from typing import Any

from app.config.settings import load_armor_config

PIPELINE_PROVIDERS: tuple[str, ...] = ("custom", "vllm", "ollama", "litellm")


def normalize_provider(value: str | None) -> str:
    """Return a canonical provider key; defaults to ``ollama`` if missing or unknown."""
    if value is None:
        return "ollama"
    v = str(value).strip().lower()
    if v in ("litellm", "lite_llm", "litellm_router"):
        return "litellm"
    if v in PIPELINE_PROVIDERS:
        return v
    return "ollama"


def get_armor_stages(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return the ``armor_stages`` mapping from merged config, or ``{}`` if absent."""
    cfg = load_armor_config() if cfg is None else cfg
    raw = cfg.get("armor_stages")
    return raw if isinstance(raw, dict) else {}


def pipeline_providers_list() -> list[str]:
    """Stable list for APIs and UI (same order as :data:`PIPELINE_PROVIDERS`)."""
    return list(PIPELINE_PROVIDERS)
