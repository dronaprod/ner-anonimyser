#!/usr/bin/env bash
# Install Presidio + spaCy without building from source (use spacy 3.7.x wheels).
# Run from repo root: ./ner-anonymysation/install_presidio.sh
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
VENV_PY="${SCRIPT_DIR}/.venv/bin/python"
if [[ ! -x "$VENV_PY" ]]; then
  echo "Error: .venv not found or broken. Recreate with: python3 -m venv .venv"
  exit 1
fi
echo "Installing spacy 3.7.x (wheel) + presidio-analyzer..."
"$VENV_PY" -m pip install "spacy>=3.5,<3.8" "presidio-analyzer>=2.2.355"
echo "Downloading spaCy model en_core_web_sm..."
"$VENV_PY" -m spacy download en_core_web_sm
echo "Checking Presidio..."
"$VENV_PY" -c "
from presidio_analyzer import AnalyzerEngine
from presidio_analyzer.nlp_engine import NlpEngineProvider
import spacy
spacy.load('en_core_web_sm')
print('Presidio + spaCy OK')
"
