#!/usr/bin/env python3
"""Test Presidio in isolation to find why it returns []."""
import sys
from pathlib import Path

# Run from ner-anonymysation so imports work
sys.path.insert(0, str(Path(__file__).resolve().parent))

TEST_TEXT = "My name is John Doe and my email is john@example.com. Call me at 555-123-4567."

def main():
    print("Step 1: Import presidio_analyzer...")
    try:
        from presidio_analyzer import AnalyzerEngine
        from presidio_analyzer.nlp_engine import NlpEngineProvider
        print("   OK: presidio_analyzer imported")
    except Exception as e:
        print(f"   FAIL: {e}")
        return

    print("\nStep 2: Load spaCy model en_core_web_sm...")
    try:
        import spacy
        nlp = spacy.load("en_core_web_sm")
        print("   OK: spacy model loaded")
    except OSError as e:
        print(f"   FAIL (model not found): {e}")
        print("   Run: python -m spacy download en_core_web_sm")
        return
    except Exception as e:
        print(f"   FAIL: {e}")
        return

    print("\nStep 3: Create Presidio engine via NlpEngineProvider (pipeline way)...")
    engine = None
    try:
        nlp_provider = NlpEngineProvider(
            nlp_configuration={
                "nlp_engine_name": "spacy",
                "models": [{"lang_code": "en", "model_name": "en_core_web_sm"}],
            },
        )
        nlp_engine = nlp_provider.create_engine()
        engine = AnalyzerEngine(nlp_engine=nlp_engine, supported_languages=["en"])
        print("   OK: Engine created with NlpEngineProvider")
    except Exception as e:
        print(f"   FAIL: {e}")
        print("\nStep 3b: Try default AnalyzerEngine()...")
        try:
            engine = AnalyzerEngine(supported_languages=["en"])
            print("   OK: Engine created with default config")
        except Exception as e2:
            print(f"   FAIL: {e2}")
            return

    if engine is None:
        return

    print("\nStep 4: Run engine.analyze() on test text...")
    print(f"   Text: {TEST_TEXT!r}")
    try:
        results = engine.analyze(text=TEST_TEXT, language="en")
        print(f"   Raw results count: {len(results)}")
        for i, r in enumerate(results):
            print(f"   [{i}] entity_type={getattr(r,'entity_type',None)} start={getattr(r,'start',None)} end={getattr(r,'end',None)} score={getattr(r,'score',None)} value={TEST_TEXT[getattr(r,'start',0):getattr(r,'end',0)]!r}")
    except Exception as e:
        print(f"   FAIL: {e}")
        return

    if not results:
        print("\n   No entities returned. Checking supported entities...")
        try:
            recognizers = engine.get_recognizers(language="en")
            print(f"   Recognizers for 'en': {len(recognizers)}")
            for r in recognizers[:10]:
                print(f"      - {r.name}: supported_entities={getattr(r,'supported_entities',None)}")
        except Exception as e:
            print(f"   Could not list recognizers: {e}")

if __name__ == "__main__":
    main()
