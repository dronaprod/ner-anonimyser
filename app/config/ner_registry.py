"""
GLiNER / Presidio registry: Hugging Face model IDs, internal NER stage names, GLiNER label list,
and Presidio → shared label mapping. Edit here instead of scattering across ``pipeline.py``.
"""
from __future__ import annotations

# GLiNER model IDs: English uses xlarge + gretelai + urchade; Arabic uses gretelai + urchade + arabic only.
GLINER_XLARGE_ID = "knowledgator/gliner-x-large"
GLINER_GRETELAI_ID = "gretelai/gretel-gliner-bi-large-v1.0"
GLINER_URCHADE_ID = "urchade/gliner_large-v2.1"
GLINER_ARABIC_ID = "NAMAa-Space/gliner_arabic-v2.1"

NER_NAME_XLARGE = "gliner_xlarge"
NER_NAME_GRETELAI = "gretelai_gliner_large"
NER_NAME_URCHADE = "urchade_gliner_large_2.1_og"
NER_NAME_ARABIC = "gliner_arabic"

GLINER_PII_LABELS: list[str] = [
    # Identity
    "person",
    "name",
    "first name",
    "last name",
    "date of birth",
    # Government IDs
    "aadhaar number",
    "pan number",
    "gst number",
    "national id",
    "tax id",
    "certificate license number",
    # Healthcare
    "medical record number",
    "health plan beneficiary number",
    # Contact
    "email address",
    "phone number",
    # Address
    "street address",
    "address",
    "city",
    "state",
    "postcode",
    "country",
    # Network / Device
    "ipv4 address",
    "ipv6 address",
    "device identifier",
    "unique identifier",
    # Organization IDs
    "employee id",
    "customer id",
    # Financial
    "account number",
    "bank routing number",
    # Vehicle
    "license plate number",
    "vehicle identifier",
    # Biometric
    "biometric identifier",
]

GLINER_PII_LABELS_SET = set(GLINER_PII_LABELS)

PRESIDIO_TO_SHARED_LABEL: dict[str, str] = {
    # Identity
    "PERSON": "person",
    # Contact
    "EMAIL_ADDRESS": "email address",
    "PHONE_NUMBER": "phone number",
    # Location
    "LOCATION": "location",
    # Dates
    "DATE_TIME": "date",
    # Government IDs
    "US_SSN": "ssn",
    "US_PASSPORT": "passport number",
    "US_DRIVER_LICENSE": "driver license number",
    # Indian IDs
    "IN_AADHAAR": "aadhaar number",
    "IN_PAN": "pan number",
    "IN_PASSPORT": "passport number",
    "IN_VEHICLE_REGISTRATION": "vehicle registration number",
    # Financial
    "CREDIT_CARD": "credit card number",
    "IBAN_CODE": "bank account number",
    "US_BANK_NUMBER": "bank account number",
    # Network
    "IP_ADDRESS": "ip address",
    "MAC_ADDRESS": "mac address",
    # Internet
    "URL": "url",
    # Healthcare
    "MEDICAL_LICENSE": "medical license number",
    # Crypto
    "CRYPTO": "crypto wallet address",
}
