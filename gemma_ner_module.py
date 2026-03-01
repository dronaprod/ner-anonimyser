"""
Gemma 2 2B for PII NER: loads google/gemma-2-2b-it and extracts PII entities from text.
Returns list of PiiDetection-compatible dicts (text, label, score).
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

PII_LABELS = [
    "person", "name", "email", "phone number", "address", "organization",
    "date", "ssn", "passport number", "credit card number", "bank account number",
    "ip address", "username", "location",
]

GEMMA_NER_SYSTEM = (
    "You are a PII extraction tool. Reply with ONLY a JSON array: no other text, no markdown. "
    "Each item: {\"text\": \"exact span from text\", \"label\": \"type\"}. "
    "Labels: person, name, email, phone number, address, organization, location, date, ssn, passport number, "
    "credit card number, bank account number, ip address, username. "
    "If no PII, reply with exactly: []"
)

_MODEL: Any = None
_TOKENIZER: Any = None
_DEVICE: str = "cpu"
_INIT_FAILED = False
MODEL_ID = "google/gemma-2-2b-it"
MAX_NEW_TOKENS = 2048
MAX_INPUT_CHARS = 2000


def _load_model() -> bool:
    global _MODEL, _TOKENIZER, _DEVICE, _INIT_FAILED
    if _INIT_FAILED:
        return False
    if _MODEL is not None:
        return True
    try:
        import torch
        from transformers import AutoModelForCausalLM, AutoTokenizer
        _DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
        _TOKENIZER = AutoTokenizer.from_pretrained(MODEL_ID, trust_remote_code=True)
        _MODEL = AutoModelForCausalLM.from_pretrained(
            MODEL_ID,
            device_map="auto" if _DEVICE == "cuda" else None,
            trust_remote_code=True,
        )
        if _DEVICE == "cpu":
            _MODEL = _MODEL.to(_DEVICE)
        return True
    except Exception as e:
        _INIT_FAILED = True
        logger.warning("Gemma NER model load failed: %s", e, exc_info=False)
        raise RuntimeError(f"Gemma NER model load failed: {e}") from e


def _strip_markdown_json(reply: str) -> str:
    """Strip ```json ... ``` or ``` ... ``` so we can parse the inner array."""
    reply = reply.strip()
    m = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", reply, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return reply


def _extract_array_slice(reply: str, start: int) -> tuple[list, int, int] | None:
    """Find balanced [...] starting at start. Returns (parsed_list, start, end) or None."""
    if start < 0 or start >= len(reply) or reply[start] != "[":
        return None
    depth = 0
    end = -1
    for i in range(start, len(reply)):
        c = reply[i]
        if c == "[":
            depth += 1
        elif c == "]":
            depth -= 1
            if depth == 0:
                end = i
                break
    if end == -1:
        return None
    try:
        arr = json.loads(reply[start : end + 1])
        return (arr, start, end) if isinstance(arr, list) else None
    except json.JSONDecodeError:
        return None


def _parse_entities(reply: str) -> list[dict]:
    """Extract JSON array from model reply. Handles markdown, multiple arrays (takes longest)."""
    reply = reply.strip()
    reply = _strip_markdown_json(reply)
    candidates: list[tuple[list, int, int]] = []
    start = 0
    while True:
        idx = reply.find("[", start)
        if idx == -1:
            break
        out = _extract_array_slice(reply, idx)
        if out is not None:
            arr, _, end = out
            if isinstance(arr, list) and all(isinstance(x, dict) for x in arr):
                candidates.append((arr, idx, end))
        start = idx + 1
    if not candidates:
        return []
    # Prefer the longest list (most entities); if tie, prefer last (often the actual answer)
    best = max(candidates, key=lambda c: (len(c[0]), c[2]))
    return best[0]


def detect_pii_with_gemma(text: str, threshold: float = 0.5) -> list[dict]:
    """
    Run Gemma 2 2B to extract PII from text. Returns list of {"text": str, "label": str, "score": float}.
    If model fails to load or run, returns [].
    """
    global _MODEL, _TOKENIZER, _DEVICE
    try:
        if not _load_model():
            return []
    except Exception as e:
        logger.warning("Gemma NER load failed: %s", e)
        return []
    if not text or not text.strip():
        return []
    chunk = text[:MAX_INPUT_CHARS] if len(text) > MAX_INPUT_CHARS else text
    user_msg = (
        GEMMA_NER_SYSTEM
        + "\n\nText to analyze:\n"
        + chunk
        + "\n\nReply with only the JSON array (start with [):"
    )
    messages = [{"role": "user", "content": user_msg}]
    try:
        prompt = _TOKENIZER.apply_chat_template(
            messages,
            tokenize=False,
            add_generation_prompt=True,
        )
        inputs = _TOKENIZER(prompt, return_tensors="pt", truncation=True, max_length=4096).to(_DEVICE)
        outputs = _MODEL.generate(
            **inputs,
            max_new_tokens=MAX_NEW_TOKENS,
            do_sample=False,
            pad_token_id=_TOKENIZER.eos_token_id,
        )
        reply = _TOKENIZER.decode(outputs[0][inputs["input_ids"].shape[1] :], skip_special_tokens=True)
    except Exception as e:
        logger.warning("Gemma NER inference failed: %s", e)
        return []
    entities = _parse_entities(reply)
    if not entities:
        logger.info(
            "Gemma NER returned no parseable entities. Raw reply (first 700 chars): %s",
            (reply[:700] if reply else "(empty)"),
        )
    result = []
    seen = set()
    for ent in entities:
        if not isinstance(ent, dict):
            continue
        t = str(ent.get("text") or ent.get("entity") or ent.get("value") or "").strip()
        label = str(ent.get("label") or ent.get("type") or "").strip().lower()
        if not t or not label:
            continue
        if label not in PII_LABELS:
            for allowed in PII_LABELS:
                if allowed in label or label in allowed:
                    label = allowed
                    break
            else:
                label = "person" if label in ("name", "per") else label
        key = (t.lower(), label)
        if key in seen:
            continue
        seen.add(key)
        result.append({"text": t, "label": label, "score": 0.8})
    return result
