"""
Qwen (Ollama) for PII NER: calls Ollama chat API with Qwen 3.5 / 9B to extract PII from text.
Returns list of PiiDetection-compatible dicts (text, label, score).
Model name from env OLLAMA_NER_MODEL (default: qwen3.5). Use the tag from `ollama list` for 9B (e.g. qwen2.5:9b).
"""
from __future__ import annotations

import json
import logging
import os
import re
import urllib.request
import urllib.error

logger = logging.getLogger(__name__)

OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://127.0.0.1:11434")
OLLAMA_NER_MODEL = os.environ.get("OLLAMA_NER_MODEL", "qwen3.5:9b")
MAX_INPUT_CHARS = 2000

PII_LABELS = [
    "person", "name", "email", "phone number", "address", "organization",
    "date", "ssn", "passport number", "credit card number", "bank account number",
    "ip address", "username", "location",
]

QWEN_NER_SYSTEM = (
    "You are a PII extraction tool. Reply with ONLY a JSON array: no other text, no markdown. "
    "Each item: {\"text\": \"exact span from text\", \"label\": \"type\"}. "
    "Labels: person, name, email, phone number, address, organization, location, date, ssn, passport number, "
    "credit card number, bank account number, ip address, username. "
    "If no PII, reply with exactly: []"
)


def _strip_markdown_json(reply: str) -> str:
    reply = reply.strip()
    m = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", reply, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return reply


def _extract_array_slice(reply: str, start: int) -> tuple[list, int, int] | None:
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
    best = max(candidates, key=lambda c: (len(c[0]), c[2]))
    return best[0]


def _ollama_chat(model: str, system: str, user: str) -> str:
    url = f"{OLLAMA_HOST.rstrip('/')}/api/chat"
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "stream": False,
        "options": {"num_predict": 2048},
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=300) as resp:
            out = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace") if e.fp else ""
        raise RuntimeError(f"Ollama HTTP {e.code}: {body}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"Ollama not reachable at {OLLAMA_HOST}: {e.reason}") from e
    msg = out.get("message") or {}
    return msg.get("content", "")


def detect_pii_with_qwen_ollama(text: str, threshold: float = 0.5) -> list[dict]:
    """
    Run Qwen via Ollama to extract PII from text. Returns list of {"text": str, "label": str, "score": float}.
    If Ollama fails or reply is not parseable, returns [].
    """
    if not text or not text.strip():
        return []
    chunk = text[:MAX_INPUT_CHARS] if len(text) > MAX_INPUT_CHARS else text
    user_msg = (
        QWEN_NER_SYSTEM
        + "\n\nText to analyze:\n"
        + chunk
        + "\n\nReply with only the JSON array (start with [):"
    )
    try:
        reply = _ollama_chat(OLLAMA_NER_MODEL, QWEN_NER_SYSTEM, user_msg)
    except Exception as e:
        logger.warning("Qwen NER (Ollama) failed: %s", e)
        return []
    entities = _parse_entities(reply)
    if not entities:
        logger.info(
            "Qwen NER (Ollama) returned no parseable entities. Raw reply (first 700 chars): %s",
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
