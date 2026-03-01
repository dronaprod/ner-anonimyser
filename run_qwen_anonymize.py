#!/usr/bin/env python3
"""
Qwen 1.5B for PII anonymization: reads JSON {system_prompt, user_prompt} from stdin,
runs Qwen2.5-1.5B-Instruct to generate anonymization replacements, outputs JSON to stdout.
Use as --qwen-script for model-generated replacements (no hardcoded synthetic values).
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

# Optional: add parent for transformers if running from ner-anonymysation
if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parent))

MODEL_ID = "Qwen/Qwen2.5-1.5B-Instruct"
MAX_NEW_TOKENS = 1024
MAX_CONTEXT = 4096


def parse_json_from_reply(reply: str) -> dict:
    """Extract JSON object from model output (may be wrapped in markdown)."""
    reply = reply.strip()
    # Prefer {"replacements": [...]}
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
    # Try ```json ... ```
    m = re.search(r"```(?:json)?\s*(\{[\s\S]*?\})\s*```", reply)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass
    return {}


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
        import torch
        from transformers import AutoModelForCausalLM, AutoTokenizer
    except ImportError as e:
        print(json.dumps({"replacements": [], "error": str(e)}))
        return

    device = "cuda" if torch.cuda.is_available() else "cpu"
    try:
        tokenizer = AutoTokenizer.from_pretrained(MODEL_ID, trust_remote_code=True)
        model = AutoModelForCausalLM.from_pretrained(
            MODEL_ID,
            device_map="auto" if device == "cuda" else None,
            trust_remote_code=True,
        )
        if device == "cpu":
            model = model.to(device)
    except Exception as e:
        print(json.dumps({"replacements": [], "error": str(e)}))
        return

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]
    try:
        prompt = tokenizer.apply_chat_template(
            messages,
            tokenize=False,
            add_generation_prompt=True,
        )
        inputs = tokenizer(prompt, return_tensors="pt", truncation=True, max_length=MAX_CONTEXT).to(device)
        outputs = model.generate(
            **inputs,
            max_new_tokens=MAX_NEW_TOKENS,
            do_sample=False,
            pad_token_id=tokenizer.eos_token_id,
        )
        reply = tokenizer.decode(outputs[0][inputs["input_ids"].shape[1] :], skip_special_tokens=True)
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
