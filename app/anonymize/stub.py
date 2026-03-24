#!/usr/bin/env python3
"""
Stub for Qwen-based anonymization: reads JSON with system_prompt/user_prompt from stdin,
parses detected PII from the user prompt, and outputs synthetic replacements as JSON.
Use as --qwen-script when no real Qwen service is available.
"""
from __future__ import annotations

import json
import random
import re
import sys


def normalize_label(label: str) -> str:
    return re.sub(r"\s+", "_", label.strip().lower())


def synthetic_value_for_type(pii_type: str, original: str) -> str:
    seed = abs(hash((pii_type.lower(), original))) % 1_000_000
    rng = random.Random(seed)
    t = pii_type.lower()
    if "email" in t:
        return f"user{seed % 100000}@example.com"
    if "phone" in t:
        return f"+1-555-{rng.randint(100, 999)}-{rng.randint(1000, 9999)}"
    if "name" in t or "person" in t:
        first = ["Alex", "Jordan", "Taylor", "Casey", "Morgan", "Avery"][seed % 6]
        last = ["Smith", "Johnson", "Clark", "Davis", "Miller", "Brown"][(seed // 7) % 6]
        return f"{first} {last}"
    if "address" in t:
        return f"{rng.randint(10, 999)} Example Street, Springfield"
    if "organization" in t or "company" in t:
        return f"Acme Holdings {seed % 1000}"
    if "ssn" in t:
        return f"{rng.randint(100, 999)}-{rng.randint(10, 99)}-{rng.randint(1000, 9999)}"
    if "passport" in t:
        return f"P{rng.randint(10000000, 99999999)}"
    if "credit" in t and "card" in t:
        return f"{rng.randint(1000, 9999)} {rng.randint(1000, 9999)} {rng.randint(1000, 9999)} {rng.randint(1000, 9999)}"
    if "bank" in t and "account" in t:
        return "".join(str(rng.randint(0, 9)) for _ in range(12))
    if "ip" in t:
        return f"10.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}"
    if "date" in t:
        return f"{rng.randint(1, 12):02d}/{rng.randint(1, 28):02d}/19{rng.randint(70, 99)}"
    if "username" in t:
        return f"user_{seed % 100000}"
    return f"<{normalize_label(pii_type)}_{seed % 100000}>"


def extract_detected_pii(user_prompt: str) -> list[dict]:
    """Extract the JSON array of detected PII from the user prompt."""
    # Pipeline sends: "PII detected using union of...\n" + json.dumps(detected_payload) + "\n\nText chunk:..."
    for line in user_prompt.split("\n"):
        line = line.strip()
        if line.startswith("["):
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                pass
    return []


def main() -> None:
    line = sys.stdin.readline()
    if not line:
        print(json.dumps({"replacements": []}))
        return
    try:
        payload = json.loads(line.strip())
    except json.JSONDecodeError:
        print(json.dumps({"replacements": [], "error": "Invalid JSON"}))
        return
    user_prompt = payload.get("user_prompt", "")
    detected = extract_detected_pii(user_prompt)
    replacements = []
    seen = set()
    for item in detected if isinstance(detected, list) else []:
        if not isinstance(item, dict):
            continue
        original = str(item.get("value", "")).strip()
        pii_type = str(item.get("pii_type", "")).strip() or "unknown"
        if not original or (original.lower(), pii_type.lower()) in seen:
            continue
        seen.add((original.lower(), pii_type.lower()))
        replacements.append({
            "original_value": original,
            "anonymized_value": synthetic_value_for_type(pii_type, original),
            "pii_type": pii_type,
        })
    print(json.dumps({"replacements": replacements}))


if __name__ == "__main__":
    main()
