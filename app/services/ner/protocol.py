"""
Typing protocol for NER backends that return :class:`~app.models.PiiDetection`.

Concrete detectors today are split between:

- **Vendor wrappers** — ``vendor_gliner`` (HF GLiNER), ``vendor_presidio`` (optional Presidio).
- **Remote / LLM** — ``qwen_ollama`` (Ollama), ``litellm_ner`` (OpenAI-compatible API).
- **Pipeline** — ``app.pipeline`` hosts ``detect_pii_with_gliner`` / ``detect_pii_with_presidio``.

Implementations may adopt this protocol over time without changing HTTP or CLI entrypoints.
"""
from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from app.models import PiiDetection


@runtime_checkable
class NerSpanDetector(Protocol):
    """A named detector that maps text to scored spans."""

    name: str

    def detect(self, text: str, **kwargs: Any) -> list[PiiDetection]:
        """Return detections for ``text`` (threshold and other options via kwargs)."""
        ...
