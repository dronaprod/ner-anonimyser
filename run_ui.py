#!/usr/bin/env python3
"""
Serve the NER anonymisation report UI. Reads config.json for latest_report path.
Run after the pipeline (without --no-report); then open the printed URL.
"""
from __future__ import annotations

import json
import os
import sys
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
CONFIG_FILE = SCRIPT_DIR / "config.json"
UI_DIR = SCRIPT_DIR / "ui"
DEFAULT_PORT = 8765


class ReportHandler(SimpleHTTPRequestHandler):
    report_path: Path | None = None

    def __init__(self, *args, **kwargs):
        kwargs["directory"] = str(UI_DIR)
        super().__init__(*args, **kwargs)

    def do_GET(self):
        if self.path == "/" or self.path == "/index.html":
            self.path = "/index.html"
            return super().do_GET()
        if self.path == "/report.json":
            self.serve_report()
            return
        if self.path.startswith("/"):
            self.path = self.path.lstrip("/")
            return super().do_GET()
        self.send_error(404)

    def serve_report(self):
        if not getattr(ReportHandler, "report_path", None) or not ReportHandler.report_path.is_file():
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(
                json.dumps({"error": "No report found. Run the NER pipeline first (without --no-report)."}).encode("utf-8")
            )
            return
        try:
            body = ReportHandler.report_path.read_text(encoding="utf-8")
        except Exception as e:
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"error": str(e)}).encode("utf-8"))
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))

    def log_message(self, format, *args):
        pass  # quiet by default; remove to enable request logging


def main() -> None:
    port = int(os.environ.get("NER_UI_PORT", DEFAULT_PORT))
    report_path: Path | None = None
    if CONFIG_FILE.is_file():
        try:
            config = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
            rel = config.get("latest_report")
            if rel:
                report_path = (SCRIPT_DIR / rel).resolve()
        except Exception:
            pass
    ReportHandler.report_path = report_path
    server = HTTPServer(("0.0.0.0", port), ReportHandler)
    url = f"http://127.0.0.1:{port}/"
    print("NER Anonymisation Report UI")
    print("Open in browser:", url)
    if not ReportHandler.report_path or not ReportHandler.report_path.is_file():
        print("(No report loaded. Run the pipeline without --no-report first.)")
    else:
        print("Report:", ReportHandler.report_path)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
        sys.exit(0)


if __name__ == "__main__":
    main()
