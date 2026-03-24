"""Process-wide locks for pipeline and LLM NER background tasks."""
from __future__ import annotations

import threading

run_lock = threading.Lock()
running = False

llm_ner_lock = threading.Lock()
llm_ner_running = False
