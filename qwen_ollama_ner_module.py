"""
Qwen (Ollama) for PII NER: calls Ollama chat API with Qwen 3.5 to extract PII from text.
Returns list of PiiDetection-compatible dicts (text, label, score).
Model name from env OLLAMA_NER_MODEL (default: qwen3.5:4b for faster response; use qwen3.5:9b for higher quality).
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
OLLAMA_NER_MODEL = os.environ.get("OLLAMA_NER_MODEL", "qwen3.5:4b")
MAX_INPUT_CHARS = 5000

# Pattern-based supplement so we never miss Indian IDs and common formats when Qwen omits them.
_AADHAAR_PATTERN = re.compile(r"\b\d{4}[\s-]?\d{4}[\s-]?\d{4}\b")
_PAN_PATTERN = re.compile(r"\b[A-Z]{5}\d{4}[A-Z]\b")
_GST_PATTERN = re.compile(r"\b\d{2}[A-Z]{5}\d{4}[A-Z]\d[A-Z]\d\b")
_UDYAM_PATTERN = re.compile(r"\bUDYAM-[A-Z]{2}-\d{2}-\d{6,7}\b", re.IGNORECASE)
_DD_MON_YYYY_PATTERN = re.compile(r"\b\d{1,2}-(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*-\d{4}\b", re.IGNORECASE)
_YYYY_MM_DD_PATTERN = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")
_SSN_PATTERN = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
_EMAIL_PATTERN = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")

PII_LABELS = [
    "person", "name", "email", "phone number", "address", "organization",
    "date", "ssn", "passport number", "credit card number", "bank account number",
    "ip address", "username", "location",
    "aadhaar", "pan", "gst_number", "udyam_number", "date_of_birth",
]

# Critical PII first so Qwen prioritises them (improves recall for Aadhaar, PAN, GST, dates).
QWEN_NER_OBLIGATIONS = [
    ("aadhaar", "Indian Aadhaar: exactly 12 digits, with optional spaces or dashes between groups of 4. MUST detect.", ["1234 5678 9012", "1234 5678 9123", "1234-5678-9012", "123456789012"]),
    ("pan", "Indian PAN: exactly 5 letters, 4 digits, 1 letter. MUST detect.", ["ABCDE1234F", "AABCT1234D"]),
    ("gst_number", "Indian GST number: 15 characters (2 digit state + 5 letter + 4 digit + 1 letter + 2 chars). MUST detect.", ["22AAAAA0000A1Z5", "27ABCDE1234F1Z5", "27AABCU9603R1ZM"]),
    ("date_of_birth", "Date of birth: any date format including DD-Mon-YYYY (e.g. 15-Aug-1990). MUST detect.", ["1981-04-12", "15-Aug-1990", "15-Mar-1990", "01-Apr-2020"]),
    ("date", "Date in any format: YYYY-MM-DD, DD-Mon-YYYY, DD/MM/YYYY.", ["1981-04-12", "15-Aug-1990", "01-Apr-2020", "March 15, 2025"]),
    ("person", "Full person name (first name, last name, or both; may include title).", ["Jonathan Reed", "Dr. Jane Smith", "Rahul Sharma", "Ramesh Sharma"]),
    ("name", "Person name or part of a name.", ["John", "Smith", "Jonathan Reed"]),
    ("email", "Email address (local@domain).", ["user@example.com", "name@company.co.in"]),
    ("phone number", "Phone number with digits (with or without +, spaces, dashes).", ["+1-555-123-4567", "9876543210", "+91 98765 43210"]),
    ("address", "Street address, city, or location mention.", ["123 Main Street", "Mumbai", "123, MG Road, Mumbai, Maharashtra - 400001"]),
    ("organization", "Company, institution, or organization name.", ["BlueCross PPO", "Acme Corp", "Sharma Enterprises"]),
    ("location", "Place, city, country, or geographic reference.", ["New Delhi", "California", "Maharashtra", "EU"]),
    ("ssn", "Social Security Number (XXX-XX-XXXX) or similar national ID pattern.", ["123-45-6789"]),
    ("passport number", "Passport or travel document number.", ["A12345678", "P1234567"]),
    ("credit card number", "Credit or debit card number (digits, optional spaces/dashes).", ["4111 1111 1111 1111"]),
    ("bank account number", "Bank account or IBAN.", ["1234567890", "GB82WEST12345698765432"]),
    ("ip address", "IPv4 or IPv6 address.", ["192.168.1.1", "2001:db8::1"]),
    ("username", "Username, handle, or login ID.", ["john_doe", "user123"]),
    ("udyam_number", "Udyam registration number: UDYAM-XX-XX-XXXXXX.", ["UDYAM-MH-12-1234567", "UDYAM-DL-07-0000001"]),
]


def _build_obligations_prompt() -> str:
    """Build system prompt that lists obligations and examples; instructs to return JSON array of spans satisfying them."""
    lines = [
        "You are a PII checker. Extract every PII span from the CHUNK.",
        "You MUST detect: (1) Indian Aadhaar (12 digits, e.g. 1234 5678 9123), (2) Indian PAN (5 letters + 4 digits + 1 letter), (3) Indian GST number (15 chars), (4) Dates in any format including DD-Mon-YYYY (e.g. 15-Aug-1990, 01-Apr-2020), (5) Person names, addresses, organizations.",
        "Return ONLY a JSON array: [{\"text\": \"exact span\", \"label\": \"obligation_label\"}].",
        "Use the exact label from the obligation (aadhaar, pan, gst_number, date, date_of_birth, person, address, organization, etc.). Copy spans exactly from the chunk. If nothing matches, return [].",
        "",
        "OBLIGATIONS (check the chunk for text that matches these):",
    ]
    for label, desc, examples in QWEN_NER_OBLIGATIONS:
        ex_str = ", ".join(repr(e) for e in examples)
        lines.append(f"  - {label}: {desc} Examples: {ex_str}")
    lines.append("")
    lines.append("Reply with only the JSON array, no other text.")
    return "\n".join(lines)


QWEN_NER_SYSTEM = _build_obligations_prompt()


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
        reply = _ollama_chat(OLLAMA_NER_MODEL, system, user)
    except Exception as e:
        logger.warning("Language detection (Qwen) failed: %s", e)
        return "en"
    reply_lower = (reply or "").strip().lower()
    if "arabic" in reply_lower or "ar " in reply_lower or reply_lower == "ar":
        return "ar"
    return "en"


def _ollama_chat(model: str, system: str, user: str) -> str:
    url = f"{OLLAMA_HOST.rstrip('/')}/api/chat"
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "stream": False,
        "think": False,  # Disable thinking so response is in content (required for JSON parsing)
        "options": {"num_predict": 4096},
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
        + "\n\nExtract every PII. You MUST include: (1) Aadhaar numbers (12 digits, e.g. 1234 5678 9123), (2) PAN (e.g. ABCDE1234F), (3) GST numbers (15 chars), (4) All dates including DD-Mon-YYYY (e.g. 15-Aug-1990, 01-Apr-2020), (5) Person names, addresses, organizations. Reply with ONLY a JSON array: [{\"text\": \"exact span\", \"label\": \"label\"}]. If you cannot output JSON, list each as **Label:** value on its own line."
    )
    try:
        reply = _ollama_chat(OLLAMA_NER_MODEL, QWEN_NER_SYSTEM, user_msg)
    except Exception as e:
        logger.warning("Qwen NER (Ollama) failed: %s", e)
        return []
    entities = _parse_entities(reply)
    if not entities:
        entities = _parse_prose_pii(reply)
    if not entities:
        if not reply:
            logger.warning(
                "Qwen NER (Ollama): empty reply from model %s. Is Ollama running? Run 'ollama list' and set OLLAMA_NER_MODEL to your model (e.g. qwen2.5:9b or qwen3.5:9b).",
                OLLAMA_NER_MODEL,
            )
        else:
            logger.info(
                "Qwen NER (Ollama) returned no parseable JSON array (reply length=%s chars, content not logged).",
                len(reply),
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
    # Do NOT add pattern supplement here: we want "Found by: qwen" only when the model actually
    # returned that PII. Indian IDs (Aadhaar, PAN, GST) are added by the pipeline's
    # deterministic_audit_indian_ids() and correctly attributed to "audit".
    return result
