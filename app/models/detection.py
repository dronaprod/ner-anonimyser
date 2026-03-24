"""Domain model for detected PII spans (shared across NER backends and the pipeline)."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class PiiDetection:
    text: str
    label: str
    score: float


# Minimum score for the final combined PII list; also used when filtering migrated report rows.
DEFAULT_MIN_NER_CONFIDENCE = 0.6
