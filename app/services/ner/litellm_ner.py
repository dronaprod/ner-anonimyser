"""
LiteLLM / OpenAI-compatible API for PII NER (same labels and obligations as Qwen).
Uses GPT OSS 20B via openai base_url. Returns list of {"text", "label", "score"}.
Credentials via env: LITELLM_OPENAI_API_KEY, LITELLM_OPENAI_BASE_URL (optional LITELLM_MODEL).
"""
from __future__ import annotations

import json
import logging
import os
import re
from typing import Any

logger = logging.getLogger(__name__)

# Same obligation labels and examples as Qwen for comparable NER
OBLIGATIONS = [
    ("aadhaar", "Indian Aadhaar: exactly 12 digits, with optional spaces or dashes between groups of 4.", ["1234 5678 9012", "1234 5678 9123"]),
    ("pan", "Indian PAN: exactly 5 letters, 4 digits, 1 letter.", ["ABCDE1234F", "AABCT1234D"]),
    ("gst_number", "Indian GST number: 15 characters.", ["22AAAAA0000A1Z5", "27ABCDE1234F1Z5"]),
    ("date_of_birth", "Date of birth: any format including DD-Mon-YYYY.", ["1981-04-12", "15-Aug-1990"]),
    ("date", "Date in any format.", ["1981-04-12", "15-Aug-1990", "01-Apr-2020"]),
    ("person", "Full person name.", ["Jonathan Reed", "Rahul Sharma", "Ramesh Sharma"]),
    ("name", "Person name or part.", ["John", "Smith"]),
    ("email", "Email address.", ["user@example.com"]),
    ("phone number", "Phone number with digits.", ["+91 98765 43210"]),
    ("address", "Street address, city, or location.", ["123 Main Street", "Mumbai", "123, MG Road, Mumbai"]),
    ("organization", "Company or institution.", ["Acme Corp", "Sharma Enterprises"]),
    ("location", "Place, city, country.", ["New Delhi", "Maharashtra"]),
    ("ssn", "Social Security Number XXX-XX-XXXX.", ["123-45-6789"]),
    ("udyam_number", "Udyam registration UDYAM-XX-XX-XXXXXX.", ["UDYAM-MH-12-1234567"]),
]

DEFAULT_BASE_URL = "https://llm-alpha.us.secloredev.io"
# OpenAI-compatible servers often expose chat at /v1; ensure we use it
OPENAI_API_PATH = "/v1"
DEFAULT_MODEL = "gpt-oss-20b"  # or model id as returned by the endpoint


def _build_system_prompt() -> str:
    lines = [
        "You are a PII checker. Extract every PII span from the CHUNK.",
        "You MUST detect: Indian Aadhaar (12 digits), PAN (5 letters+4 digits+1 letter), GST (15 chars), dates (any format including DD-Mon-YYYY), person names, addresses, organizations.",
        "Return ONLY a JSON array: [{\"text\": \"exact span\", \"label\": \"label\"}].",
        "Use these labels: aadhaar, pan, gst_number, date, date_of_birth, person, name, email, phone number, address, organization, location, ssn, udyam_number.",
        "",
        "OBLIGATIONS with examples:",
    ]
    for label, desc, examples in OBLIGATIONS:
        lines.append(f"  - {label}: {desc} Examples: {examples}")
    lines.append("")
    lines.append("Reply with only the JSON array, no other text.")
    return "\n".join(lines)


SYSTEM_PROMPT = _build_system_prompt()


def _parse_json_array(reply: str) -> list[dict]:
    reply = reply.strip()
    m = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", reply, re.IGNORECASE)
    if m:
        reply = m.group(1).strip()
    start = reply.find("[")
    if start == -1:
        return []
    depth = 0
    for i in range(start, len(reply)):
        if reply[i] == "[":
            depth += 1
        elif reply[i] == "]":
            depth -= 1
            if depth == 0:
                try:
                    arr = json.loads(reply[start : i + 1])
                    return arr if isinstance(arr, list) else []
                except json.JSONDecodeError:
                    pass
                break
    return []


def detect_pii_with_litellm(
    chunk: str,
    api_key: str | None = None,
    base_url: str | None = None,
    model: str | None = None,
) -> list[dict[str, Any]]:
    """
    Send chunk to OpenAI-compatible API (LiteLLM / GPT OSS 20B) for NER.
    Returns list of {"text": str, "label": str, "score": float}.
    """
    api_key = api_key or os.environ.get("LITELLM_OPENAI_API_KEY", "").strip()
    base_url = (base_url or os.environ.get("LITELLM_OPENAI_BASE_URL", DEFAULT_BASE_URL)).strip().rstrip("/")
    # Most OpenAI-compatible servers use /v1; set LITELLM_OPENAI_NO_V1=1 to use base_url as-is
    if base_url and not os.environ.get("LITELLM_OPENAI_NO_V1") and not base_url.endswith(OPENAI_API_PATH):
        base_url = base_url + OPENAI_API_PATH
    model = model or os.environ.get("LITELLM_MODEL", DEFAULT_MODEL).strip()
    if not api_key:
        logger.warning("LITELLM_OPENAI_API_KEY not set; LLM NER will fail.")
    try:
        from openai import OpenAI
    except ImportError:
        logger.warning("openai package not installed; pip install openai")
        return []
    client = OpenAI(api_key=api_key, base_url=base_url)
    user_content = (
        "CHUNK:\n" + chunk[:5000] + "\n\n"
        "Extract every PII. Return ONLY a JSON array: [{\"text\": \"exact span\", \"label\": \"label\"}]. Use labels: aadhaar, pan, gst_number, date, date_of_birth, person, address, organization, etc."
    )
    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_content},
            ],
            max_tokens=2048,
        )
        content = (resp.choices[0].message.content or "").strip()
        entities = _parse_json_array(content)
        if not content:
            raise ValueError("LLM returned empty content. Check model and endpoint.")
        if not entities:
            preview = content[:600].replace("\n", " ")
            raise ValueError("LLM returned no JSON entities. Raw response: " + preview)
    except Exception as e:
        logger.warning("LiteLLM NER request failed: %s", e)
        raise
    result = []
    seen = set()
    for ent in entities:
        if not isinstance(ent, dict):
            continue
        t = str(ent.get("text") or ent.get("entity") or ent.get("value") or "").strip()
        label = str(ent.get("label") or ent.get("type") or "").strip().lower()
        if not t or not label:
            continue
        key = (t.lower(), label)
        if key in seen:
            continue
        seen.add(key)
        result.append({"text": t, "label": label, "score": float(ent.get("score", 0.8))})
    return result
