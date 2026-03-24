"""Support ``python -m app`` (same as ``python main.py``)."""
from __future__ import annotations

from app.cli import main

if __name__ == "__main__":
    main()
