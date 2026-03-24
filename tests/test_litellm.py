#!/usr/bin/env python3
"""
Quick connectivity test for a LiteLLM / OpenAI-compatible chat endpoint.

Run from repo root::

  export LITELLM_OPENAI_API_KEY='your-key'
  export LITELLM_OPENAI_BASE_URL='https://...'
  python tests/test_litellm.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))


def main() -> int:
    api_key = os.environ.get("LITELLM_OPENAI_API_KEY", "").strip()
    if not api_key:
        print("Set LITELLM_OPENAI_API_KEY")
        return 1
    base_url = os.environ.get("LITELLM_OPENAI_BASE_URL", "https://llm-alpha.us.secloredev.io").strip().rstrip("/")
    if not base_url.endswith("/v1"):
        base_url = base_url + "/v1"
    print("Using base_url:", base_url)
    print("Calling chat completions...")
    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key, base_url=base_url)
        resp = client.chat.completions.create(
            model=os.environ.get("LITELLM_MODEL", "openai/gpt-oss-20b"),
            messages=[{"role": "user", "content": "Say hello in one word."}],
            max_tokens=50,
        )
        content = (resp.choices[0].message.content or "").strip()
        print("Status: OK")
        print("Response:", repr(content[:500]))
    except Exception as e:
        print("Error:", type(e).__name__, str(e))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
