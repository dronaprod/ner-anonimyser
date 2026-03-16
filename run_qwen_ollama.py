#!/usr/bin/env python3
"""
Anonymisation via Ollama: reads JSON {system_prompt, user_prompt} from stdin,
calls Ollama chat API with Qwen 9B / 3.5, outputs JSON {replacements} to stdout.
Requires Ollama running locally. Use the model tag from `ollama list` (e.g. qwen3.5:9b).
Override with env OLLAMA_MODEL (default: qwen3.5:9b).
"""
from __future__ import annotations

import json
import os
import re
import sys
import urllib.request
import urllib.error

OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://127.0.0.1:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "qwen3.5:9b")
MAX_NEW_TOKENS = 1024


def parse_json_from_reply(reply: str) -> dict:
    """Extract JSON object from model output (may be wrapped in markdown)."""
    reply = reply.strip()
    start = reply.find("{")
    if start == -1:
        return {}
    depth = 0
    end = -1
    for i, c in enumerate(reply[start:], start=start):
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                end = i
                break
    if end == -1:
        return {}
    try:
        return json.loads(reply[start : end + 1])
    except json.JSONDecodeError:
        pass
    m = re.search(r"```(?:json)?\s*(\{[\s\S]*?\})\s*```", reply)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass
    return {}


def ollama_chat(model: str, system: str, user: str) -> str:
    """Call Ollama /api/chat; return assistant message content."""
    url = f"{OLLAMA_HOST.rstrip('/')}/api/chat"
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "stream": False,
        "options": {"num_predict": MAX_NEW_TOKENS},
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            out = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace") if e.fp else ""
        raise RuntimeError(f"Ollama HTTP {e.code}: {body}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"Ollama not reachable at {OLLAMA_HOST}: {e.reason}") from e
    msg = out.get("message") or {}
    return msg.get("content", "")


def main() -> None:
    line = sys.stdin.readline()
    if not line:
        print(json.dumps({"replacements": []}))
        return
    try:
        payload = json.loads(line.strip())
    except json.JSONDecodeError:
        print(json.dumps({"replacements": [], "error": "Invalid JSON input"}))
        return
    system_prompt = payload.get("system_prompt", "")
    user_prompt = payload.get("user_prompt", "")
    if not user_prompt:
        print(json.dumps({"replacements": []}))
        return

    try:
        reply = ollama_chat(OLLAMA_MODEL, system_prompt, user_prompt)
    except Exception as e:
        print(json.dumps({"replacements": [], "error": str(e)}))
        return

    out = parse_json_from_reply(reply)
    replacements = out.get("replacements", [])
    if not isinstance(replacements, list):
        replacements = []
    print(json.dumps({"replacements": replacements}))


if __name__ == "__main__":
    main()
