#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import random
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

# Module-level logger; configured in main() to write to log/log_<datetime>.log and console
logger = logging.getLogger("pii_pipeline")

from docx import Document
from openpyxl import load_workbook
from pypdf import PdfReader

# Local modules: gliner, presidio (optional), and optional Gemma NER
from gliner_module import GLiNER
from presidio_module import AnalyzerEngine, NlpEngineProvider
try:
    from gemma_ner_module import detect_pii_with_gemma as _gemma_ner_detect
except Exception:  # pragma: no cover
    _gemma_ner_detect = None


SUPPORTED_SUFFIXES = {".pdf", ".docx", ".xlsx", ".txt"}
GLINER_PII_LABELS = [
    "name",
    "person",
    "email",
    "phone number",
    "address",
    "organization",
    "date of birth",
    "date",
    "ssn",
    "passport number",
    "credit card number",
    "bank account number",
    "ip address",
    "username",
]
GLINER_PII_LABELS_SET = set(GLINER_PII_LABELS)
PRESIDIO_TO_SHARED_LABEL = {
    "PERSON": "person",
    "EMAIL_ADDRESS": "email",
    "PHONE_NUMBER": "phone number",
    "LOCATION": "address",
    "DATE_TIME": "date",
    "US_SSN": "ssn",
    "US_PASSPORT": "passport number",
    "CREDIT_CARD": "credit card number",
    "IP_ADDRESS": "ip address",
    "IBAN_CODE": "bank account number",
    "US_BANK_NUMBER": "bank account number",
    "US_DRIVER_LICENSE": "username",
    "URL": "username",
}
LONG_NUMBER_PATTERN = re.compile(r"\b\d{6,}\b")
CARD_LIKE_PATTERN = re.compile(r"\b(?:\d[ -]?){12,20}\b")
UUID_PATTERN = re.compile(
    r"\b[0-9a-fA-F]{8}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{12}\b",
)
ALPHANUMERIC_PATTERN = re.compile(r"\b[A-Za-z0-9]{12,}\b")
BASE64_PATTERN = re.compile(r"\b[A-Za-z0-9+/]{20,}={0,2}\b")
HEX_32_PATTERN = re.compile(r"\b[a-fA-F0-9]{32}\b")
HEX_40_PATTERN = re.compile(r"\b[a-fA-F0-9]{40}\b")
HEX_64_PATTERN = re.compile(r"\b[a-fA-F0-9]{64}\b")
OBFUSCATED_EMAIL_PATTERN = re.compile(
    r"\b\w+\s*(?:at|\(at\))\s*\w+\s*(?:dot|\.)\s*\w+\b",
    re.IGNORECASE,
)

_PRESIDIO_ENGINE: Any = None
_PRESIDIO_INIT_FAILED = False

LOG_DIR = Path(__file__).resolve().parent / "log"
REPO_ROOT = Path(__file__).resolve().parent.parent
MSPRISIDIO_PY = REPO_ROOT / "msprisidio" / "venv" / "bin" / "python"
MSPRISIDIO_SCRIPT = REPO_ROOT / "msprisidio" / "run_presidio_analyze.py"
PRESIDIO_SUBPROCESS_TIMEOUT = 60


def setup_logging() -> None:
    """Create log directory and log file log_<datetime>.log; log to file and console."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = LOG_DIR / f"log_{timestamp}.log"
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    fh.setFormatter(fmt)
    ch.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)
    gemma_logger = logging.getLogger("gemma_ner_module")
    gemma_logger.setLevel(logging.DEBUG)
    gemma_logger.addHandler(fh)
    logger.info("Logging to %s", log_file)


@dataclass
class PiiDetection:
    text: str
    label: str
    score: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="PII anonymization pipeline with Presidio + GLiNER + Qwen.",
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "cache_105" / "files",
        help="Directory with source files (pdf/docx/xlsx/txt). Default: cache_105/files.",
    )
    parser.add_argument(
        "--num-files",
        type=int,
        default=10,
        help="How many files to process (default: 10).",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1600,
        help="Chunk size in characters.",
    )
    parser.add_argument(
        "--chunk-overlap",
        type=int,
        default=200,
        help="Chunk overlap in characters.",
    )
    parser.add_argument(
        "--gliner-model",
        type=str,
        default="urchade/gliner_medium-v2.1",
        help="GLiNER model id.",
    )
    parser.add_argument(
        "--gliner-threshold",
        type=float,
        default=0.45,
        help="GLiNER confidence threshold.",
    )
    parser.add_argument(
        "--presidio-threshold",
        type=float,
        default=0.35,
        help="Presidio confidence threshold.",
    )
    parser.add_argument(
        "--use-gemma-ner",
        action="store_true",
        default=True,
        help="Use Gemma 2 2B for PII NER (default: True).",
    )
    parser.add_argument(
        "--no-gemma-ner",
        action="store_false",
        dest="use_gemma_ner",
        help="Disable Gemma NER.",
    )
    parser.add_argument(
        "--gemma-ner-threshold",
        type=float,
        default=0.5,
        help="Gemma NER confidence threshold (default: 0.5).",
    )
    parser.add_argument(
        "--qwen-python",
        type=str,
        default=sys.executable,
        help="Python executable used to run qwen/run_qwen_sit.py.",
    )
    parser.add_argument(
        "--qwen-script",
        type=Path,
        default=Path(__file__).resolve().parent / "run_qwen_anonymize.py",
        help="Path to Qwen script for generating anonymized values (default: run_qwen_anonymize.py). Use run_qwen_stub.py for hardcoded fallback.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("PII-Anonymisation/output"),
        help="Output directory.",
    )
    return parser.parse_args()


def extract_text(file_path: Path) -> str:
    suffix = file_path.suffix.lower()
    if suffix == ".pdf":
        return _extract_pdf(file_path)
    if suffix == ".docx":
        return _extract_docx(file_path)
    if suffix == ".xlsx":
        return _extract_xlsx(file_path)
    if suffix == ".txt":
        return file_path.read_text(encoding="utf-8", errors="ignore")
    raise ValueError(f"Unsupported file type: {file_path}")


def _extract_pdf(file_path: Path) -> str:
    reader = PdfReader(str(file_path))
    pages: list[str] = []
    for page in reader.pages:
        pages.append(page.extract_text() or "")
    return "\n".join(pages)


def _extract_docx(file_path: Path) -> str:
    doc = Document(str(file_path))
    lines = [p.text for p in doc.paragraphs if p.text and p.text.strip()]
    for table in doc.tables:
        for row in table.rows:
            row_text = " | ".join(cell.text.strip() for cell in row.cells if cell.text.strip())
            if row_text:
                lines.append(row_text)
    return "\n".join(lines)


def _extract_xlsx(file_path: Path) -> str:
    wb = load_workbook(filename=str(file_path), read_only=True, data_only=True)
    lines: list[str] = []
    for ws in wb.worksheets:
        lines.append(f"[Sheet: {ws.title}]")
        for row in ws.iter_rows(values_only=True):
            values = [str(v).strip() for v in row if v is not None and str(v).strip()]
            if values:
                lines.append(" | ".join(values))
    return "\n".join(lines)


def chunk_text(text: str, chunk_size: int, chunk_overlap: int) -> list[str]:
    cleaned = re.sub(r"\s+", " ", text).strip()
    if not cleaned:
        return []
    if chunk_overlap >= chunk_size:
        chunk_overlap = 0
    chunks: list[str] = []
    start = 0
    step = chunk_size - chunk_overlap
    while start < len(cleaned):
        chunks.append(cleaned[start : start + chunk_size])
        start += step
    return chunks


def detect_pii_with_gliner(
    model: GLiNER,
    text: str,
    threshold: float,
) -> list[PiiDetection]:
    predictions = model.predict_entities(text, GLINER_PII_LABELS, threshold=threshold)
    dedup: dict[tuple[str, str], PiiDetection] = {}
    for pred in predictions:
        value = str(pred.get("text", "")).strip()
        label = str(pred.get("label", "")).strip()
        score = float(pred.get("score", 0.0))
        if not value:
            continue
        key = (value.lower(), label.lower())
        current = dedup.get(key)
        if current is None or score > current.score:
            dedup[key] = PiiDetection(text=value, label=label, score=score)
    return sorted(dedup.values(), key=lambda x: (-len(x.text), x.text.lower()))


def _map_to_shared_label(raw_label: str, mapping: dict[str, str]) -> str | None:
    normalized = raw_label.strip().upper()
    mapped = mapping.get(normalized)
    if mapped:
        return mapped
    fallback = normalized.replace("_", " ").lower()
    return fallback if fallback in GLINER_PII_LABELS_SET else None


def _dedup_detections(items: list[PiiDetection]) -> list[PiiDetection]:
    dedup: dict[tuple[str, str], PiiDetection] = {}
    for item in items:
        if not item.text.strip() or not item.label.strip():
            continue
        key = (item.text.strip().lower(), item.label.strip().lower())
        current = dedup.get(key)
        if current is None or item.score > current.score:
            dedup[key] = PiiDetection(text=item.text.strip(), label=item.label.strip().lower(), score=item.score)
    return sorted(dedup.values(), key=lambda x: (-len(x.text), x.text.lower()))


def shannon_entropy(text: str) -> float:
    if not text:
        return 0.0
    prob = [float(text.count(c)) / len(text) for c in set(text)]
    return -sum(p * math.log2(p) for p in prob)


def regex_entropy_audit(text: str) -> list[str]:
    findings: set[str] = set()
    patterns = [
        LONG_NUMBER_PATTERN,
        CARD_LIKE_PATTERN,
        UUID_PATTERN,
        ALPHANUMERIC_PATTERN,
        BASE64_PATTERN,
        HEX_32_PATTERN,
        HEX_40_PATTERN,
        HEX_64_PATTERN,
        OBFUSCATED_EMAIL_PATTERN,
    ]

    for pattern in patterns:
        for match in pattern.finditer(text):
            findings.add(match.group(0))

    for token in text.split():
        clean = token.strip(".,;:()[]{}<>\"'")
        if len(clean) >= 16:
            entropy = shannon_entropy(clean)
            if entropy >= 3.5:
                findings.add(clean)
    return list(findings)


def _ensure_spacy_model(model_name: str = "en_core_web_sm") -> None:
    """Ensure spaCy model is installed so Presidio can use it; download if missing."""
    try:
        import spacy
        spacy.load(model_name)
    except OSError:
        logger.info("Downloading spaCy model %s for Presidio...", model_name)
        try:
            import spacy.cli
            spacy.cli.download(model_name)
            import spacy
            spacy.load(model_name)
        except Exception as e:
            logger.warning("Could not auto-download spaCy model %s: %s", model_name, e)
            raise


def _get_msprisidio_site_packages() -> Path | None:
    """Return msprisidio venv site-packages path for current Python version if it exists."""
    py_ver = f"python{sys.version_info.major}.{sys.version_info.minor}"
    sp = REPO_ROOT / "msprisidio" / "venv" / "lib" / py_ver / "site-packages"
    return sp if sp.is_dir() else None


def _detect_pii_with_presidio_subprocess(text: str, threshold: float) -> list[PiiDetection]:
    """Run Presidio via msprisidio (venv python or current python + PYTHONPATH) when in-process Presidio is not available."""
    run_cmd: list[str] | None = None
    env = os.environ.copy()
    if MSPRISIDIO_PY.exists() and os.access(str(MSPRISIDIO_PY), os.X_OK) and MSPRISIDIO_SCRIPT.is_file():
        run_cmd = [str(MSPRISIDIO_PY), str(MSPRISIDIO_SCRIPT)]
    else:
        sp = _get_msprisidio_site_packages()
        if sp is not None and MSPRISIDIO_SCRIPT.is_file():
            env["PYTHONPATH"] = str(sp) + os.pathsep + env.get("PYTHONPATH", "")
            run_cmd = [sys.executable, str(MSPRISIDIO_SCRIPT)]
    if run_cmd is None:
        logger.debug(
            "Presidio: msprisidio venv not runnable and no msprisidio site-packages for this Python; install presidio-analyzer in this env or use same Python as msprisidio.",
        )
        return []
    try:
        proc = subprocess.run(
            run_cmd,
            input=text.encode("utf-8"),
            capture_output=True,
            timeout=PRESIDIO_SUBPROCESS_TIMEOUT,
            cwd=str(REPO_ROOT),
            env=env,
        )
        out = proc.stdout.decode("utf-8", errors="replace").strip()
        if proc.returncode != 0 and proc.stderr:
            logger.debug("Presidio subprocess stderr: %s", proc.stderr.decode("utf-8", errors="replace")[:500])
        if not out:
            return []
        raw = json.loads(out)
        if not isinstance(raw, list):
            return []
    except (subprocess.TimeoutExpired, json.JSONDecodeError, OSError) as e:
        logger.warning("Presidio subprocess failed: %s", e)
        return []
    dedup: dict[tuple[str, str], PiiDetection] = {}
    for r in raw:
        score = float(r.get("score", 0.0) or 0.0)
        if score < threshold:
            continue
        start = int(r.get("start", 0))
        end = int(r.get("end", 0))
        if end <= start:
            continue
        value = text[start:end].strip()
        if not value:
            continue
        raw_label = str(r.get("entity_type", "unknown")).strip()
        label = _map_to_shared_label(raw_label, PRESIDIO_TO_SHARED_LABEL)
        if not label:
            label = raw_label.lower().replace("_", " ").strip() or "entity"
        key = (value.lower(), label.lower())
        current = dedup.get(key)
        if current is None or score > current.score:
            dedup[key] = PiiDetection(text=value, label=label, score=score)
    return sorted(dedup.values(), key=lambda x: (-len(x.text), x.text.lower()))


def detect_pii_with_presidio(text: str, threshold: float) -> list[PiiDetection]:
    global _PRESIDIO_ENGINE, _PRESIDIO_INIT_FAILED
    if AnalyzerEngine is None or _PRESIDIO_INIT_FAILED:
        return _detect_pii_with_presidio_subprocess(text, threshold)
    if _PRESIDIO_ENGINE is None:
        try:
            _ensure_spacy_model("en_core_web_sm")
            nlp_provider = NlpEngineProvider(
                nlp_configuration={
                    "nlp_engine_name": "spacy",
                    "models": [{"lang_code": "en", "model_name": "en_core_web_sm"}],
                },
            )
            nlp_engine = nlp_provider.create_engine()
            _PRESIDIO_ENGINE = AnalyzerEngine(
                nlp_engine=nlp_engine,
                supported_languages=["en"],
            )
        except Exception as exc:  # pragma: no cover
            try:
                _PRESIDIO_ENGINE = AnalyzerEngine(supported_languages=["en"])
                logger.debug("Presidio engine created (default config)")
            except Exception as exc2:
                _PRESIDIO_INIT_FAILED = True
                logger.warning("Presidio unavailable, continuing without presidio detection: %s", exc2)
                return []
        else:
            logger.debug("Presidio engine created successfully")
    try:
        results = _PRESIDIO_ENGINE.analyze(text=text, language="en")
    except Exception as exc:  # pragma: no cover
        logger.warning("Presidio analyze failed for chunk, continuing: %s", exc)
        return []
    logger.debug("Presidio analyze returned %d raw results", len(results))
    dedup: dict[tuple[str, str], PiiDetection] = {}
    for result in results:
        score = float(getattr(result, "score", 0.0) or 0.0)
        if score < threshold:
            continue
        start = int(getattr(result, "start", 0))
        end = int(getattr(result, "end", 0))
        if end <= start:
            continue
        value = text[start:end].strip()
        if not value:
            continue
        raw_label = str(getattr(result, "entity_type", "unknown")).strip()
        label = _map_to_shared_label(raw_label, PRESIDIO_TO_SHARED_LABEL)
        if not label:
            label = raw_label.lower().replace("_", " ").strip() or "entity"
        key = (value.lower(), label.lower())
        current = dedup.get(key)
        if current is None or score > current.score:
            dedup[key] = PiiDetection(text=value, label=label, score=score)
    return sorted(dedup.values(), key=lambda x: (-len(x.text), x.text.lower()))


def merge_pii_detections(
    detection_groups: list[list[PiiDetection]],
) -> list[PiiDetection]:
    merged: dict[tuple[str, str], PiiDetection] = {}
    for group in detection_groups:
        for item in group:
            key = (item.text.lower(), item.label.lower())
            existing = merged.get(key)
            if existing is None or item.score > existing.score:
                merged[key] = item
    return sorted(merged.values(), key=lambda x: (-len(x.text), x.text.lower()))


def call_qwen_json(
    qwen_python: str,
    qwen_script: Path,
    system_prompt: str,
    user_prompt: str,
) -> dict[str, Any]:
    if not qwen_script.is_file():
        raise FileNotFoundError(f"Qwen script not found: {qwen_script}")

    payload = json.dumps({"system_prompt": system_prompt, "user_prompt": user_prompt}) + "\n"
    completed = subprocess.run(
        [qwen_python, str(qwen_script)],
        input=payload,
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"Qwen invocation failed with code {completed.returncode}: {completed.stderr.strip()}",
        )

    output = completed.stdout.strip()
    if not output:
        return {"error": "Empty response from Qwen"}
    return parse_json_like(output)


def parse_json_like(text: str) -> dict[str, Any]:
    text = text.strip()
    if not text:
        return {"error": "Empty text"}

    candidates = [text]
    fenced = re.search(r"```(?:json)?\s*(\{.*?\}|\[.*?\])\s*```", text, re.DOTALL)
    if fenced:
        candidates.append(fenced.group(1).strip())
    for start_match in re.finditer(r"[\{\[]", text):
        snippet = text[start_match.start() :]
        end_obj = snippet.rfind("}")
        end_arr = snippet.rfind("]")
        end = max(end_obj, end_arr)
        if end >= 0:
            candidates.append(snippet[: end + 1].strip())

    for candidate in candidates:
        try:
            loaded = json.loads(candidate)
            if isinstance(loaded, dict):
                return loaded
            return {"result": loaded}
        except json.JSONDecodeError:
            continue
    return {"raw_response": text}


def build_qwen_replacement_prompt(chunk: str, detected_pii: list[PiiDetection]) -> tuple[str, str]:
    system = (
        "You are a strict PII anonymization assistant. "
        "Return JSON only with schema: "
        "{\"replacements\":[{\"original_value\":\"...\",\"anonymized_value\":\"...\",\"pii_type\":\"...\"}]}. "
        "Replacement must preserve datatype (name->synthetic name, email->synthetic email, phone->synthetic phone, etc)."
    )
    detected_payload = [
        {"value": item.text, "pii_type": item.label, "confidence": round(item.score, 4)}
        for item in detected_pii
    ]
    user = (
        "PII detected using union of Presidio, GLiNER, and Gemma NER:\n"
        f"{json.dumps(detected_payload, ensure_ascii=True)}\n\n"
        "Text chunk:\n"
        f"{chunk}\n\n"
        "Generate anonymization replacements in JSON only."
    )
    return system, user


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


def datatype_match(value: str, pii_type: str) -> bool:
    t = pii_type.lower()
    if "email" in t:
        return bool(re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", value))
    if "phone" in t:
        return bool(re.search(r"\d{7,}", re.sub(r"\D", "", value)))
    if "ssn" in t:
        return bool(re.fullmatch(r"\d{3}-\d{2}-\d{4}", value))
    if "ip" in t:
        return bool(re.fullmatch(r"(?:\d{1,3}\.){3}\d{1,3}", value))
    if "name" in t or "person" in t:
        return bool(re.fullmatch(r"[A-Za-z]+(?:[ \-'][A-Za-z]+)+", value.strip()))
    if "credit" in t and "card" in t:
        digits = re.sub(r"\D", "", value)
        return 13 <= len(digits) <= 19
    if "bank" in t and "account" in t:
        digits = re.sub(r"\D", "", value)
        return len(digits) >= 8
    if "date" in t:
        return bool(re.search(r"\d", value))
    return bool(value.strip())


def apply_replacements(chunk: str, replacements: list[dict[str, str]]) -> str:
    updated = chunk
    # Replace longest strings first to reduce partial overlaps.
    sorted_replacements = sorted(
        replacements,
        key=lambda r: len(r.get("original_value", "")),
        reverse=True,
    )
    for item in sorted_replacements:
        src = item.get("original_value", "")
        dst = item.get("anonymized_value", "")
        if not src or src == dst:
            continue
        updated = re.sub(re.escape(src), dst, updated)
    return updated


def process_chunk(
    chunk: str,
    chunk_index: int,
    gliner_model: GLiNER,
    gliner_threshold: float,
    presidio_threshold: float,
    use_gemma_ner: bool,
    gemma_ner_threshold: float,
    qwen_python: str,
    qwen_script: Path,
) -> tuple[str, bool]:
    logger.info("chunk number : %s", chunk_index)
    presidio_pii = detect_pii_with_presidio(chunk, threshold=presidio_threshold)
    gliner_pii = detect_pii_with_gliner(gliner_model, chunk, threshold=gliner_threshold)
    gemma_pii: list[PiiDetection] = []
    if use_gemma_ner and _gemma_ner_detect is not None:
        try:
            gemma_raw = _gemma_ner_detect(chunk, threshold=gemma_ner_threshold)
            gemma_pii = [
                PiiDetection(text=x["text"], label=x["label"], score=float(x.get("score", 0.8)))
                for x in gemma_raw
                if isinstance(x, dict) and x.get("text") and x.get("label")
            ]
        except Exception as exc:  # pragma: no cover
            logger.warning("Gemma NER failed for chunk: %s", exc)
    combined_pii = merge_pii_detections(
        [presidio_pii, gliner_pii, gemma_pii],
    )
    audit_hits = regex_entropy_audit(chunk)
    detected_values = {d.text for d in combined_pii}
    for hit in audit_hits:
        if hit not in detected_values:
            combined_pii.append(PiiDetection(text=hit, label="suspicious_token", score=1.0))
    combined_pii = _dedup_detections(combined_pii)

    if not combined_pii:
        logger.info("no pii in the chunk")
        return chunk, True

    logger.info(
        "Piis found in the chunk by presidio : %s",
        json.dumps([{"value": p.text, "pii_type": p.label} for p in presidio_pii], ensure_ascii=True),
    )
    logger.info(
        "Piis found in the chunk by gliner : %s",
        json.dumps([{"value": p.text, "pii_type": p.label} for p in gliner_pii], ensure_ascii=True),
    )
    logger.info(
        "Piis found in the chunk by gemma : %s",
        json.dumps([{"value": p.text, "pii_type": p.label} for p in gemma_pii], ensure_ascii=True),
    )
    logger.info(
        "Piis found in the chunk by union : %s",
        json.dumps([{"value": p.text, "pii_type": p.label} for p in combined_pii], ensure_ascii=True),
    )
    r_system, r_user = build_qwen_replacement_prompt(chunk, combined_pii)
    qwen_response = call_qwen_json(qwen_python, qwen_script, r_system, r_user)
    replacements_raw = qwen_response.get("replacements", [])

    normalized: list[dict[str, str]] = []
    seen_values = set()
    for item in replacements_raw if isinstance(replacements_raw, list) else []:
        original = str(item.get("original_value", "")).strip()
        anonymized = str(item.get("anonymized_value", "")).strip()
        pii_type = str(item.get("pii_type", "")).strip() or "unknown"
        if not original:
            continue
        if original not in chunk:
            continue
        if not datatype_match(anonymized, pii_type):
            anonymized = synthetic_value_for_type(pii_type, original)
        key = (original.lower(), pii_type.lower())
        if key in seen_values:
            continue
        seen_values.add(key)
        normalized.append(
            {
                "original_value": original,
                "anonymized_value": anonymized,
                "pii_type": pii_type,
            },
        )

    # Ensure every detected hit gets a replacement even if Qwen misses some.
    existing_originals = {r["original_value"].lower() for r in normalized}
    for p in combined_pii:
        if p.text.lower() in existing_originals:
            continue
        normalized.append(
            {
                "original_value": p.text,
                "anonymized_value": synthetic_value_for_type(p.label, p.text),
                "pii_type": p.label,
            },
        )

    logger.info("original and anonymized value in the chunk : %s", json.dumps(normalized, ensure_ascii=True))
    replaced_chunk = apply_replacements(chunk, normalized)
    logger.info("replacement done")
    return replaced_chunk, bool(normalized)


def process_file(
    file_path: Path,
    gliner_model: GLiNER,
    args: argparse.Namespace,
) -> tuple[str, int, int]:
    logger.info("Processing file name start : %s", file_path.name)
    text = extract_text(file_path)
    chunks = chunk_text(text, chunk_size=args.chunk_size, chunk_overlap=args.chunk_overlap)

    anonymized_chunks: list[str] = []
    anonymized_count = 0
    not_anonymized_count = 0
    for index, chunk in enumerate(chunks, start=1):
        anonymized_chunk, success = process_chunk(
            chunk=chunk,
            chunk_index=index,
            gliner_model=gliner_model,
            gliner_threshold=args.gliner_threshold,
            presidio_threshold=args.presidio_threshold,
            use_gemma_ner=args.use_gemma_ner,
            gemma_ner_threshold=args.gemma_ner_threshold,
            qwen_python=args.qwen_python,
            qwen_script=args.qwen_script,
        )
        anonymized_chunks.append(anonymized_chunk)
        if success:
            anonymized_count += 1
        else:
            not_anonymized_count += 1

    logger.info("Processing file name end : %s", file_path.name)
    return "\n".join(anonymized_chunks), anonymized_count, not_anonymized_count


def main() -> None:
    args = parse_args()
    setup_logging()
    if AnalyzerEngine is not None:
        logger.info("Presidio: available (in-process)")
    else:
        logger.info(
            "Presidio: not available. To enable: pip install presidio-analyzer spacy && python -m spacy download en_core_web_sm",
        )
    logger.info(
        "Gemma NER: %s",
        "enabled" if (args.use_gemma_ner and _gemma_ner_detect is not None) else "disabled or unavailable",
    )
    args.input_dir = args.input_dir.resolve()
    args.output_dir = args.output_dir.resolve()
    args.qwen_script = args.qwen_script.resolve()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    if not args.input_dir.is_dir():
        raise NotADirectoryError(f"Input dir not found: {args.input_dir}")
    if not args.qwen_script.is_file():
        raise FileNotFoundError(f"Qwen script not found: {args.qwen_script}")

    candidates = sorted(
        [
            path
            for path in args.input_dir.iterdir()
            if path.is_file() and path.suffix.lower() in SUPPORTED_SUFFIXES
        ],
        key=lambda p: p.name.lower(),
    )
    selected_files = candidates[: args.num_files]
    if not selected_files:
        raise RuntimeError(f"No supported files found in {args.input_dir}")

    logger.info("Loading GLiNER model: %s", args.gliner_model)
    gliner_model = GLiNER.from_pretrained(args.gliner_model)

    total_anonymized = 0
    total_not_anonymized = 0
    for file_path in selected_files:
        anonymized_text, anonymized_count, not_anonymized_count = process_file(
            file_path=file_path,
            gliner_model=gliner_model,
            args=args,
        )
        total_anonymized += anonymized_count
        total_not_anonymized += not_anonymized_count
        out_file = args.output_dir / f"{file_path.name}.anonymized.txt"
        out_file.write_text(anonymized_text, encoding="utf-8")

    logger.info("number of times all pii anonymized : %s", total_anonymized)
    logger.info("number of times it was not : %s", total_not_anonymized)


if __name__ == "__main__":
    main()
