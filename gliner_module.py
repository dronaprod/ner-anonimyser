"""
GLiNER-based PII detection module for ner-anonymysation.
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
