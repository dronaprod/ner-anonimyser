"""
LiteLLM / OpenAI-compatible API for PII NER.
System prompt and obligations: app.config.prompts_loader.build_litellm_ner_system_prompt.
Returns list of {"text", "label", "score"}.
Credentials via env: LITELLM_OPENAI_API_KEY, LITELLM_OPENAI_BASE_URL (optional LITELLM_MODEL).
"""
from __future__ import annotations

import json
import logging
import os
import re
from typing import Any

from app.config.prompts_loader import build_litellm_ner_system_prompt

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://llm-alpha.us.secloredev.io"
# OpenAI-compatible servers often expose chat at /v1; ensure we use it
OPENAI_API_PATH = "/v1"
DEFAULT_MODEL = "gpt-oss-20b"  # or model id as returned by the endpoint


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
                {"role": "system", "content": build_litellm_ner_system_prompt()},
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
