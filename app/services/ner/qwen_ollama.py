"""
Qwen (Ollama) for PII NER: calls Ollama chat API with Qwen 3.5 to extract PII from text.
Returns list of PiiDetection-compatible dicts (text, label, score).
Model name from env OLLAMA_NER_MODEL (read on each call; default: qwen3.5:4b).
Set ARMOR_QWEN_MODE=cpu (via config ``mode: cpu``) for CPU-only + Qwen 3.5 4B defaults.
"""
from __future__ import annotations

import json
import logging
import os
import re
import urllib.request
import urllib.error

from app.config.prompts_loader import (
    build_slm_ner_system_prompt,
    get_slm_judge_system_prompt,
    ner_obligations_tuples,
    slm_ner_canonical_labels,
)

logger = logging.getLogger(__name__)

MAX_INPUT_CHARS = 5000


def get_ollama_host() -> str:
    return os.environ.get("OLLAMA_HOST", "http://127.0.0.1:11434").rstrip("/")


def get_ollama_ner_model() -> str:
    return os.environ.get("OLLAMA_NER_MODEL", "qwen3.5:4b")


def _qwen_log_prefix() -> str:
    return "Qwen CPU (Ollama)" if os.environ.get("ARMOR_QWEN_MODE", "").lower() == "cpu" else "Qwen NER (Ollama)"


# Backward compat: code that imported OLLAMA_NER_MODEL still works at import time
OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://127.0.0.1:11434")
OLLAMA_NER_MODEL = os.environ.get("OLLAMA_NER_MODEL", "qwen3.5:4b")

# Pattern-based supplement so we never miss Indian IDs and common formats when Qwen omits them.
_AADHAAR_PATTERN = re.compile(r"\b\d{4}[\s-]?\d{4}[\s-]?\d{4}\b")
_PAN_PATTERN = re.compile(r"\b[A-Z]{5}\d{4}[A-Z]\b")
_GST_PATTERN = re.compile(r"\b\d{2}[A-Z]{5}\d{4}[A-Z]\d[A-Z]\d\b")
_UDYAM_PATTERN = re.compile(r"\bUDYAM-[A-Z]{2}-\d{2}-\d{6,7}\b", re.IGNORECASE)
_DD_MON_YYYY_PATTERN = re.compile(r"\b\d{1,2}-(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*-\d{4}\b", re.IGNORECASE)
_YYYY_MM_DD_PATTERN = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")
_SSN_PATTERN = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
_EMAIL_PATTERN = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")

# Canonical labels and obligations: app.config.prompts_loader (Python, not YAML).
PII_LABELS = slm_ner_canonical_labels()
QWEN_NER_OBLIGATIONS = ner_obligations_tuples()
QWEN_NER_SYSTEM = build_slm_ner_system_prompt()


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


def _coerce_raw_entity_dicts(raw: list) -> list[dict]:
    """Normalize list elements to {text, label} dicts."""
    out: list[dict] = []
    for x in raw:
        if not isinstance(x, dict):
            continue
        t = x.get("text") if x.get("text") is not None else x.get("entity") or x.get("value") or x.get("span")
        lab = x.get("label") if x.get("label") is not None else x.get("type") or x.get("entity_type")
        if t is None or lab is None:
            continue
        ts, ls = str(t).strip(), str(lab).strip().lower()
        if ts and ls:
            out.append({"text": ts, "label": ls})
    return out


def _parse_json_object_entities(reply: str) -> list[dict]:
    """Parse {\"entities\":[...]} or top-level JSON array from model (Ollama format=json)."""
    reply = _strip_markdown_json(reply.strip())
    try:
        obj = json.loads(reply)
    except json.JSONDecodeError:
        return []
    if isinstance(obj, list):
        return _coerce_raw_entity_dicts(obj) if obj and all(isinstance(x, dict) for x in obj) else []
    if not isinstance(obj, dict):
        return []
    for key in ("entities", "spans", "pii", "items", "results", "detections"):
        v = obj.get(key)
        if isinstance(v, list) and v:
            got = _coerce_raw_entity_dicts(v)
            if got:
                return got
    return []


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


# Prose format when Qwen returns structured text (e.g. "**Full Name:** Ramesh Kumar") instead of JSON
_PROSE_LABEL_PATTERNS = [
    (re.compile(r"\*\*Full Name\*\*[:\s]*([^\n*]+)", re.I), "person"),
    (re.compile(r"\*\*Date of Birth\*\*[:\s]*([^\n*]+)", re.I), "date_of_birth"),
    (re.compile(r"\*\*Email Address\*\*[:\s]*([^\n*]+)", re.I), "email"),
    (re.compile(r"\*\*Phone Number\*\*[:\s]*([^\n*]+)", re.I), "phone number"),
    (re.compile(r"\*\*Address\*\*[:\s]*([^\n*]+)", re.I), "address"),
    (re.compile(r"\*\*Aadhaar Number\*\*[:\s]*([^\n*]+)", re.I), "aadhaar"),
    (re.compile(r"\*\*PAN (?:Card )?Number\*\*[:\s]*([^\n*]+)", re.I), "pan"),
    (re.compile(r"\*\*GST(?:IN)?\*\*[:\s]*([^\n*]+)", re.I), "gst_number"),
    (re.compile(r"\*\*State\*\*[:\s]*([^\n*]+)", re.I), "location"),
    (re.compile(r"\*\*Location\*\*[:\s]*([^\n*]+)", re.I), "location"),
    (re.compile(r"\*\*Government Identifiers\*\*[:\s]*([^\n*]+)", re.I), None),
    (re.compile(r"^\s*\*\s+\*\*([^*]+)\*\*[:\s]*([^\n*]+)", re.M), None),  # generic * **Label:** value
]
_PROSE_LABEL_MAP = {
    "full name": "person", "name": "person", "date of birth": "date_of_birth", "dob": "date_of_birth",
    "email address": "email", "email": "email", "phone number": "phone number", "phone": "phone number",
    "address": "address", "aadhaar number": "aadhaar", "aadhaar": "aadhaar",
    "pan card number": "pan", "pan number": "pan", "pan": "pan",
    "gst number": "gst_number", "gstin": "gst_number", "gst": "gst_number",
    "state": "location", "location": "location", "city": "address", "country": "location",
}


def _supplement_with_patterns(chunk: str, result: list[dict]) -> list[dict]:
    """
    Add pattern-based detections for Indian IDs, dates, SSN, email, etc. that Qwen missed.
    Ensures high recall for all key PII types even when the model omits them.
    """
    existing_text = {str(e.get("text", "")).strip().lower() for e in result if isinstance(e, dict) and e.get("text")}
    added: list[dict] = []
    seen: set[tuple[str, str]] = {(str(e.get("text", "")).lower(), str(e.get("label", "")).lower()) for e in result if isinstance(e, dict) and e.get("text") and e.get("label")}

    def _add(span: str, label: str, score: float = 0.9) -> None:
        nonlocal existing_text, seen, added
        s = span.strip()
        if not s or s.lower() in existing_text:
            return
        key = (s.lower(), label)
        if key in seen:
            return
        seen.add(key)
        existing_text.add(s.lower())
        added.append({"text": s, "label": label, "score": score})

    for pattern, label in [
        (_AADHAAR_PATTERN, "aadhaar"),
        (_PAN_PATTERN, "pan"),
        (_GST_PATTERN, "gst_number"),
        (_UDYAM_PATTERN, "udyam_number"),
    ]:
        for m in pattern.finditer(chunk):
            _add(m.group(0), label)

    for m in _DD_MON_YYYY_PATTERN.finditer(chunk):
        span = m.group(0).strip()
        if span.lower() in existing_text:
            continue
        if (span.lower(), "date_of_birth") in seen or (span.lower(), "date") in seen:
            continue
        seen.add((span.lower(), "date_of_birth"))
        existing_text.add(span.lower())
        added.append({"text": span, "label": "date_of_birth", "score": 0.85})

    for m in _YYYY_MM_DD_PATTERN.finditer(chunk):
        _add(m.group(0), "date", 0.85)

    for m in _SSN_PATTERN.finditer(chunk):
        _add(m.group(0), "ssn")

    for m in _EMAIL_PATTERN.finditer(chunk):
        _add(m.group(0), "email")

    if added:
        logger.debug("Qwen NER: supplemented %d span(s) from pattern.", len(added))
    return result + added


def _parse_prose_pii(reply: str) -> list[dict]:
    """Extract PII from Qwen prose output (e.g. **Full Name:** Ramesh Kumar). Returns list of {text, label}."""
    out: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for pat, label in _PROSE_LABEL_PATTERNS:
        if label is None:
            for m in pat.finditer(reply):
                key_label, value = m.group(1).strip().lower(), m.group(2).strip()
                if not value:
                    continue
                mapped = _PROSE_LABEL_MAP.get(key_label)
                if not mapped:
                    continue
                key = (value.lower(), mapped)
                if key in seen:
                    continue
                seen.add(key)
                out.append({"text": value, "label": mapped})
        else:
            for m in pat.finditer(reply):
                value = m.group(1).strip()
                if not value:
                    continue
                key = (value.lower(), label)
                if key in seen:
                    continue
                seen.add(key)
                out.append({"text": value, "label": label})
    return out


def detect_language(chunk: str) -> str:
    """
    Use Qwen via Ollama to detect if the chunk is primarily English or Arabic.
    Returns "en" for English, "ar" for Arabic; defaults to "en" on failure or unclear.
    """
    if not chunk or not chunk.strip():
        return "en"
    preview = (chunk[:2000] + "..." if len(chunk) > 2000 else chunk).strip()
    system = "You are a language detector. Reply with exactly one word: English or Arabic."
    user = f"What is the primary language of the following text?\n\n{preview}"
    try:
        reply = _ollama_chat(get_ollama_ner_model(), system, user, format_json=False)
    except Exception as e:
        logger.warning("Language detection (%s) failed: %s", _qwen_log_prefix(), e)
        return "en"
    reply_lower = (reply or "").strip().lower()
    if "arabic" in reply_lower or "ar " in reply_lower or reply_lower == "ar":
        return "ar"
    return "en"


def _ollama_chat_options() -> dict:
    opts: dict = {"num_predict": 4096, "temperature": 0.1}
    if os.environ.get("ARMOR_QWEN_MODE", "").lower() == "cpu":
        opts["num_gpu"] = 0
    return opts


def _ollama_chat(model: str, system: str, user: str, *, format_json: bool = False) -> str:
    url = f"{get_ollama_host()}/api/chat"
    payload: dict = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "stream": False,
        "think": False,  # Disable thinking so response is in content (required for JSON parsing)
        "options": _ollama_chat_options(),
    }
    if format_json:
        payload["format"] = "json"
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
        raise RuntimeError(f"Ollama not reachable at {get_ollama_host()}: {e.reason}") from e
    msg = out.get("message") or {}
    content = (msg.get("content") or "").strip()
    # With think=False we expect content; only fall back to thinking if content still empty (e.g. older Ollama)
    if not content and msg.get("thinking"):
        content = (msg.get("thinking") or "").strip()
        logger.info("Ollama message had empty content; using thinking field (length=%s) for NER parse.", len(content))
    if not content and msg:
        logger.warning(
            "Ollama returned message with no content. Keys: %s. Model=%s. Check: ollama list; set OLLAMA_NER_MODEL to your model tag (e.g. qwen2.5:9b).",
            list(msg.keys()), model,
        )
    return content


_JUDGE_CONTEXT_RADIUS = 72
_JUDGE_BATCH_SIZE = 8

def _span_local_context(chunk: str, span: str, radius: int = _JUDGE_CONTEXT_RADIUS) -> str:
    if not span or not chunk:
        return ""
    span_stripped = span.strip()
    if not span_stripped:
        return ""
    idx = chunk.find(span_stripped)
    if idx < 0:
        idx = chunk.lower().find(span_stripped.lower())
    if idx < 0:
        return f"(span not in chunk) «{span_stripped[:80]}»"
    start = max(0, idx - radius)
    end = min(len(chunk), idx + len(span_stripped) + radius)
    return chunk[start:end]


def _parse_judge_verdicts(reply: str) -> dict[int, bool]:
    reply = _strip_markdown_json((reply or "").strip())
    out: dict[int, bool] = {}
    try:
        obj = json.loads(reply)
    except json.JSONDecodeError:
        return out
    if not isinstance(obj, dict):
        return out
    verdicts = obj.get("verdicts")
    if not isinstance(verdicts, list):
        for key in ("results", "decisions", "items"):
            v = obj.get(key)
            if isinstance(v, list):
                verdicts = v
                break
    if not isinstance(verdicts, list):
        return out

    def _as_bool(v: object) -> bool:
        if isinstance(v, bool):
            return v
        s = str(v).strip().lower()
        return s in ("true", "1", "yes", "y")

    for item in verdicts:
        if not isinstance(item, dict):
            continue
        iid = item.get("id")
        if iid is None:
            continue
        try:
            idx = int(iid)
        except (TypeError, ValueError):
            continue
        if "is_pii" in item:
            out[idx] = _as_bool(item.get("is_pii"))
        elif "pii" in item:
            out[idx] = _as_bool(item.get("pii"))
    return out


def judge_disputed_pii_spans(
    chunk: str,
    dropped: list[dict],
    *,
    batch_size: int = _JUDGE_BATCH_SIZE,
) -> list[tuple[str, str]]:
    """
    Second-pass LLM verification for spans dropped by NER ensemble agreement.
    ``dropped`` uses pipeline keys: value, pii_type, found_by, ...
    Returns (value, pii_type) pairs approved as real PII.
    """
    if not chunk or not chunk.strip() or not dropped:
        return []
    seen: set[tuple[str, str]] = set()
    rows: list[dict] = []
    for d in dropped:
        if not isinstance(d, dict):
            continue
        value = str(d.get("value", "")).strip()
        ptype = str(d.get("pii_type", "")).strip()
        if not value or not ptype:
            continue
        key = (value.lower(), ptype.lower())
        if key in seen:
            continue
        seen.add(key)
        found_by = d.get("found_by")
        if not isinstance(found_by, list):
            found_by = [str(found_by)] if found_by else []
        rows.append({"value": value, "pii_type": ptype, "found_by": found_by})
    if not rows:
        return []
    chunk_use = chunk[:MAX_INPUT_CHARS] if len(chunk) > MAX_INPUT_CHARS else chunk
    approved: list[tuple[str, str]] = []
    model = get_ollama_ner_model()
    for start in range(0, len(rows), batch_size):
        batch = rows[start : start + batch_size]
        candidates_payload = []
        for i, row in enumerate(batch):
            candidates_payload.append({
                "id": i,
                "span": row["value"],
                "predicted_type": row["pii_type"],
                "detectors": row["found_by"],
                "context": _span_local_context(chunk_use, row["value"]),
            })
        user = (
            "Verify each candidate using its context.\n\n"
            + json.dumps({"candidates": candidates_payload}, ensure_ascii=False)
        )
        try:
            try:
                reply = _ollama_chat(model, get_slm_judge_system_prompt(), user, format_json=True)
            except RuntimeError as e:
                err = str(e).lower()
                if "400" in err or "format" in err or "json" in err:
                    logger.info("%s: retrying judge without format=json.", _qwen_log_prefix())
                    reply = _ollama_chat(model, get_slm_judge_system_prompt(), user, format_json=False)
                else:
                    raise
        except Exception as e:
            logger.warning("%s judge batch failed: %s", _qwen_log_prefix(), e)
            continue
        verdicts = _parse_judge_verdicts(reply)
        for i, row in enumerate(batch):
            if verdicts.get(i):
                approved.append((row["value"], row["pii_type"]))
    return approved


def detect_pii_with_qwen_ollama(text: str, threshold: float = 0.5) -> list[dict]:
    """
    Run Qwen via Ollama: check the chunk against OBLIGATIONS (with examples) and return
    every exact text span that satisfies any obligation. Returns list of {"text": str, "label": str, "score": float}.
    If Ollama fails or reply is not parseable, returns [].
    """
    if not text or not text.strip():
        return []
    chunk = text[:MAX_INPUT_CHARS] if len(text) > MAX_INPUT_CHARS else text
    user_msg = (
        "CHUNK:\n"
        + chunk
        + "\n\nReturn ONLY valid JSON: {\"entities\":[{\"text\":\"exact span copied from CHUNK\",\"label\":\"obligation_label\"}, ...]}. "
        "Include dates, organization names, authority acronyms (e.g. RBI, IBA), emails, phones, and person names. "
        "Use labels from the system prompt. No markdown, no explanation."
    )
    try:
        try:
            reply = _ollama_chat(get_ollama_ner_model(), build_slm_ner_system_prompt(), user_msg, format_json=True)
        except RuntimeError as e:
            err = str(e).lower()
            if "400" in err or "format" in err or "json" in err:
                logger.info("%s: retrying NER without format=json (Ollama compatibility).", _qwen_log_prefix())
                reply = _ollama_chat(get_ollama_ner_model(), build_slm_ner_system_prompt(), user_msg, format_json=False)
            else:
                raise
    except Exception as e:
        logger.warning("%s failed: %s", _qwen_log_prefix(), e)
        return []
    entities = _parse_json_object_entities(reply)
    if not entities:
        entities = _parse_entities(reply)
    if not entities:
        entities = _parse_prose_pii(reply)
    stripped = (reply or "").strip()
    if not entities:
        if not stripped:
            logger.warning(
                "%s: empty reply from model %s. Is Ollama running? Run 'ollama list' and set OLLAMA_NER_MODEL.",
                _qwen_log_prefix(),
                get_ollama_ner_model(),
            )
        elif stripped in ("[]", "{}", "null"):
            logger.warning(
                "%s: model returned empty JSON (%r); using pattern-based spans for recall (dates, emails, Indian IDs, etc.).",
                _qwen_log_prefix(),
                stripped[:120],
            )
        else:
            logger.warning(
                "%s: could not parse entities (reply length=%s). Preview: %s",
                _qwen_log_prefix(),
                len(reply),
                repr(stripped[:200]),
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
        if (t.lower() == "exact span" and "obligation" in label) or t == "<exact span>":
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
    if not result:
        result = _supplement_with_patterns(chunk, [])
        if result:
            logger.info(
                "%s: pattern fallback added %d span(s) (model gave no usable entities).",
                _qwen_log_prefix(),
                len(result),
            )
    else:
        result = _supplement_with_patterns(chunk, result)
    return [r for r in result if float(r.get("score", 0.0)) >= threshold]
