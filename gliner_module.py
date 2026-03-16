"""
GLiNER-based PII detection module for ner-anonymysation.

- Package: gliner (see requirements.txt, e.g. gliner>=0.2.16).
- Default model in pipeline: urchade/gliner_large-v2.1 (zero-shot NER, higher recall; needs more GPU/RAM).
  Use --gliner-model urchade/gliner_medium-v2.1 for less memory, or lower --gliner-threshold to improve recall.

Requires: pip install gliner (or use sibling gliner/.venv by running from that venv).
"""
from __future__ import annotations

try:
    from gliner import GLiNER  # noqa: F401
except ImportError as e:
    raise ImportError(
        "gliner is required. Install in this environment: pip install gliner"
    ) from e

__all__ = ["GLiNER"]
