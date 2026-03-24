"""
SLM / LiteLLM NER prompts, judge, anonymiser — single Python source (no YAML).

GLiNER label list and Presidio mapping: :mod:`app.config.ner_registry`.
"""
from __future__ import annotations

from typing import Any

from app.config.ner_registry import GLINER_PII_LABELS

# --- SLM NER canonical output labels (JSON entity ``label`` values) ---
SLM_NER_CANONICAL_LABELS: list[str] = [
    "person", "name", "email", "phone number", "address", "organization",
    "date", "ssn", "passport number", "credit card number", "bank account number",
    "ip address", "username", "location",
    "aadhaar", "pan", "gst_number", "udyam_number", "date_of_birth",
]

# --- Few-shot obligations (label, description, example strings) ---
NER_OBLIGATIONS: list[tuple[str, str, list[str]]] = [
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

SLM_NER_PREAMBLE_LINES: list[str] = [
    "You are a PII checker. Extract every PII span from the CHUNK.",
    "You MUST detect: (1) Indian Aadhaar (12 digits), (2) Indian PAN, (3) Indian GST (15 chars), (4) Any calendar dates, (5) Person or organization names, (6) Emails, phones, addresses.",
    "Regulatory and banking text still contains PII: dates, acronyms used as authority names (e.g. RBI, IBA), document titles with dates, and organization names.",
    'Return ONLY valid JSON (no markdown) with this exact shape: {"entities":[{"text":"exact substring from chunk","label":"obligation_label"}, ...]}.',
    "Use labels from the obligations below (aadhaar, pan, gst_number, date, date_of_birth, person, organization, email, phone number, address, location, etc.).",
    'Copy "text" exactly from the chunk. Include every distinct span. Only use {"entities":[]} if the chunk is empty or has literally no identifiable PII.',
    "",
    "OBLIGATIONS (check the chunk for text that matches these):",
]

SLM_NER_CLOSING_LINES: list[str] = ["", "Reply with only the JSON object, no other text."]

LITELLM_NER_INTRO_LINES: list[str] = [
    "You are a PII checker. Extract every PII span from the CHUNK.",
    "You MUST detect: Indian Aadhaar (12 digits), PAN (5 letters+4 digits+1 letter), GST (15 chars), dates (any format including DD-Mon-YYYY), person names, addresses, organizations.",
    'Return ONLY a JSON array: [{"text": "exact span", "label": "label"}].',
    "Use these labels: aadhaar, pan, gst_number, date, date_of_birth, person, name, email, phone number, address, organization, location, ssn, udyam_number.",
    "",
    "OBLIGATIONS with examples:",
]

LITELLM_NER_CLOSING_LINES: list[str] = ["", "Reply with only the JSON array, no other text."]

SLM_JUDGE_SYSTEM_PROMPT = """You verify candidate text spans from a document. NER tools flagged them but they did NOT reach ensemble agreement (not enough detectors agreed).

For each candidate, read CONTEXT (a snippet around the span). Decide if that exact span in context is sensitive personal or identifying information that should be redacted: real person name, government ID number, email, phone, street address, bank/account number, passport, etc.

Return ONLY valid JSON: {"verdicts":[{"id":<integer>,"is_pii":<true or false>}, ...]}.
Use id 0 for the first candidate, 1 for the second, matching the order in the user message.
Set is_pii to false for generic words, common nouns, boilerplate headers, public org names used generically, or clearly non-identifying tokens."""

ANONYMIISER_SYSTEM_PROMPT_EN = (
    'You are a PII anonymization assistant. Return JSON only: {"replacements":[{"original_value":"...","anonymized_value":"...","pii_type":"..."}]}. '
    "Rules: "
    "(1) **Language**: The text is in English (or another Latin-script language). Generate ALL anonymized replacement values in English only. "
    "(2) **Same type**: name→name, date→date, phone→phone, email→email, organisation→organisation, etc. "
    "(3) **Structurally and contextually similar**: "
    "Dates: preserve the exact format (DD/MM/YYYY vs MM/DD/YYYY vs YYYY-MM-DD, month names, separators). Same era/century if obvious. "
    "Phones: preserve country code pattern and separators (e.g. +46..., 0xx..., (0xx) ...). "
    "IDs/numbers: preserve length and separator pattern (e.g. SSN dashes, card spaces). "
    "Addresses: same country/region style (street format, postal pattern). "
    "Output only the JSON object."
)

ANONYMIISER_SYSTEM_PROMPT_AR = (
    'You are a PII anonymization assistant. Return JSON only: {"replacements":[{"original_value":"...","anonymized_value":"...","pii_type":"..."}]}. '
    "Rules: "
    "(1) **Language**: The text is in Arabic. Generate ALL anonymized replacement values in Arabic only "
    "(e.g. Arabic names, Arabic addresses, Arabic organisation names). Do not use English words for replacements. "
    "(2) **Same type**: name→name, date→date, phone→phone, email→email, organisation→organisation, etc. "
    "(3) **Structurally and contextually similar**: "
    "Dates: preserve the exact format (DD/MM/YYYY vs MM/DD/YYYY vs YYYY-MM-DD, month names, separators). Same era/century if obvious. "
    "Phones: preserve country code pattern and separators (e.g. +46..., 0xx..., (0xx) ...). "
    "IDs/numbers: preserve length and separator pattern (e.g. SSN dashes, card spaces). "
    "Addresses: same country/region style (street format, postal pattern). "
    "Output only the JSON object."
)


def ner_obligations_tuples(_cfg: dict[str, Any] | None = None) -> list[tuple[str, str, list[str]]]:
    """Few-shot rows for SLM / LiteLLM NER system prompts."""
    return list(NER_OBLIGATIONS)


def build_slm_ner_system_prompt(_cfg: dict[str, Any] | None = None) -> str:
    lines = list(SLM_NER_PREAMBLE_LINES)
    for label, desc, examples in NER_OBLIGATIONS:
        ex_str = ", ".join(repr(e) for e in examples)
        lines.append(f"  - {label}: {desc} Examples: {ex_str}")
    lines.extend(SLM_NER_CLOSING_LINES)
    return "\n".join(lines)


def build_litellm_ner_system_prompt(_cfg: dict[str, Any] | None = None) -> str:
    lines = list(LITELLM_NER_INTRO_LINES)
    for label, desc, examples in NER_OBLIGATIONS:
        ex_str = ", ".join(repr(e) for e in examples)
        lines.append(f"  - {label}: {desc} Examples: {ex_str}")
    lines.extend(LITELLM_NER_CLOSING_LINES)
    return "\n".join(lines)


def get_slm_judge_system_prompt(_cfg: dict[str, Any] | None = None) -> str:
    return SLM_JUDGE_SYSTEM_PROMPT


def get_anonymiser_system_prompt(lang: str, _cfg: dict[str, Any] | None = None) -> str:
    return ANONYMIISER_SYSTEM_PROMPT_AR if lang == "ar" else ANONYMIISER_SYSTEM_PROMPT_EN


def gliner_pii_labels(_cfg: dict[str, Any] | None = None) -> list[str]:
    return list(GLINER_PII_LABELS)


def slm_ner_canonical_labels(_cfg: dict[str, Any] | None = None) -> list[str]:
    return list(SLM_NER_CANONICAL_LABELS)


def armor_prompts_snapshot() -> dict[str, Any]:
    """Structured copy for APIs / debugging (same shape formerly served from YAML)."""
    return {
        "version": 1,
        "source": "app.config.prompts_loader",
        "pii_labels": {
            "gliner": list(GLINER_PII_LABELS),
            "slm_ner_canonical": list(SLM_NER_CANONICAL_LABELS),
        },
        "ner_obligations": [
            {"label": a, "description": b, "examples": c}
            for a, b, c in NER_OBLIGATIONS
        ],
        "slm_ner": {
            "preamble_lines": list(SLM_NER_PREAMBLE_LINES),
            "closing_lines": list(SLM_NER_CLOSING_LINES),
        },
        "litellm_ner": {
            "intro_lines": list(LITELLM_NER_INTRO_LINES),
            "closing_lines": list(LITELLM_NER_CLOSING_LINES),
        },
        "slm_judge": {"system_prompt": SLM_JUDGE_SYSTEM_PROMPT},
        "anonymiser": {
            "system_prompt_en": ANONYMIISER_SYSTEM_PROMPT_EN,
            "system_prompt_ar": ANONYMIISER_SYSTEM_PROMPT_AR,
        },
    }
