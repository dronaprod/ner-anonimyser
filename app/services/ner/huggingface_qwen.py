"""
Qwen 1.5B for PII NER: loads Qwen2.5-1.5B-Instruct and extracts PII entities from text.
Returns list of PiiDetection-compatible dicts (text, label, score).
"""
from __future__ import annotations

import json
import re
from typing import Any

from app.config.prompts_loader import build_litellm_ner_system_prompt, slm_ner_canonical_labels

PII_LABELS = slm_ner_canonical_labels()

_MODEL: Any = None
_TOKENIZER: Any = None
_DEVICE: str = "cpu"
_INIT_FAILED = False
MODEL_ID = "Qwen/Qwen2.5-1.5B-Instruct"
MAX_NEW_TOKENS = 1024
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
        raise RuntimeError(f"Qwen NER model load failed: {e}") from e


def _parse_entities(reply: str) -> list[dict]:
    """Extract JSON array from model reply."""
    reply = reply.strip()
    # Find first [ and matching ]
    start = reply.find("[")
    if start == -1:
        return []
    depth = 0
    end = -1
    for i, c in enumerate(reply[start:], start=start):
        if c == "[":
            depth += 1
        elif c == "]":
            depth -= 1
            if depth == 0:
                end = i
                break
    if end == -1:
        return []
    try:
        arr = json.loads(reply[start : end + 1])
        return arr if isinstance(arr, list) else []
    except json.JSONDecodeError:
        return []


def detect_pii_with_qwen(text: str, threshold: float = 0.5) -> list[dict]:
    """
    Run Qwen 1.5B to extract PII from text. Returns list of {"text": str, "label": str, "score": float}.
    If model fails to load or run, returns [].
    """
    global _MODEL, _TOKENIZER, _DEVICE
    try:
        if not _load_model():
            return []
    except Exception:
        return []
    if not text or not text.strip():
        return []
    chunk = text[:MAX_INPUT_CHARS] if len(text) > MAX_INPUT_CHARS else text
    user_msg = f"Extract all PII from this text as JSON array:\n\n{chunk}"
    messages = [
        {"role": "system", "content": build_litellm_ner_system_prompt()},
        {"role": "user", "content": user_msg},
    ]
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
    except Exception:
        return []
    entities = _parse_entities(reply)
    result = []
    seen = set()
    for ent in entities:
        if not isinstance(ent, dict):
            continue
        t = str(ent.get("text", "")).strip()
        label = str(ent.get("label", "")).strip().lower()
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
