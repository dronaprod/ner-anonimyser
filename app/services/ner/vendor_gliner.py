"""
GLiNER vendor import (package: ``app.services.ner``).

GLiNER-based PII detection for ner-anonymysation.

- Package: gliner (see requirements.txt, e.g. gliner>=0.2.16).
- Default model in pipeline: knowledgator/gliner-x-large (zero-shot NER; needs more GPU/RAM).
  Use --gliner-model urchade/gliner_medium-v2.1 for less memory, or lower --gliner-threshold to improve recall.

Requires: pip install gliner (or use sibling gliner/.venv by running from that venv).

Some HF models set ``words_splitter_type: stanza``; the pipeline patches ``gliner_config.json`` to
use **whitespace** by default so ``stanza`` is not required. For the hub config as-is:
``ARMOR_GLINER_USE_MODEL_SPLITTER=1`` (install ``stanza`` and ``langdetect`` if the config uses stanza).
"""
from __future__ import annotations

try:
    from gliner import GLiNER  # noqa: F401
except ImportError as e:
    raise ImportError(
        "gliner is required. Install in this environment: pip install gliner"
    ) from e

__all__ = ["GLiNER"]
