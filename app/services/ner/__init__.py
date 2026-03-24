"""
NER **service implementations** (backends).

=========== ==========================================================
Module      Role
=========== ==========================================================
vendor_gliner   GLiNER model import (Hugging Face checkpoint).
vendor_presidio Presidio ``AnalyzerEngine`` import (optional dependency).
qwen_ollama     Qwen via Ollama chat API (NER + language + judge).
litellm_ner     OpenAI-compatible NER (comparison / evaluation).
huggingface_qwen Legacy HF Qwen NER helper (optional).
protocol        :class:`NerSpanDetector` typing protocol.
=========== ==========================================================
"""

from app.services.ner.protocol import NerSpanDetector
from app.services.ner.vendor_gliner import GLiNER
from app.services.ner.vendor_presidio import AnalyzerEngine, NlpEngineProvider

__all__ = [
    "AnalyzerEngine",
    "GLiNER",
    "NerSpanDetector",
    "NlpEngineProvider",
]
