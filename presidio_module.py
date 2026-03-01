"""
Presidio-based PII detection module for ner-anonymysation.
If presidio-analyzer is not installed, AnalyzerEngine and NlpEngineProvider are None.
Install: pip install presidio-analyzer spacy && python -m spacy download en_core_web_sm
"""
from __future__ import annotations

try:
    from presidio_analyzer import AnalyzerEngine  # noqa: F401
    from presidio_analyzer.nlp_engine import NlpEngineProvider  # noqa: F401
except ImportError:
    AnalyzerEngine = None  # type: ignore[misc, assignment]
    NlpEngineProvider = None  # type: ignore[misc, assignment]

__all__ = ["AnalyzerEngine", "NlpEngineProvider"]
