#!/usr/bin/env python3
"""
Quick test for LiteLLM NER endpoint. Run from ner-anonimyser with env set:
  export LITELLM_OPENAI_API_KEY='your-key'
  export LITELLM_OPENAI_BASE_URL='https://llm-alpha.us.secloredev.io'
  python test_litellm.py
"""
import os
import sys
from pathlib import Path

# run from script dir
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

def main():
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
