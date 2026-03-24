"""Web UI routes: static HTML shell and legacy ``/report.json``."""
from __future__ import annotations

import json
import logging
from flask import Blueprint, current_app, jsonify, request, send_from_directory

from app.config import load_armor_config

log = logging.getLogger(__name__)

bp = Blueprint("armor_web", __name__)

_UI_PAGES = "pages"


def _paths():
    return current_app.config["ARMOR_PATHS"]


@bp.route("/")
def index():
    p = _paths()
    return send_from_directory(p.ui_dir / _UI_PAGES, "armor.html")


@bp.route("/report-viewer.html")
def report_viewer():
    p = _paths()
    return send_from_directory(p.ui_dir / _UI_PAGES, "report-viewer.html")


@bp.route("/index.html")
def simple_report_page():
    """Standalone latest-report viewer (same as static ``pages/index.html``)."""
    p = _paths()
    return send_from_directory(p.ui_dir / _UI_PAGES, "index.html")


@bp.route("/report.json")
def report_json_legacy():
    """Latest report by ``run_id`` query or ``latest_report`` from config state."""
    run_id = request.args.get("run_id")
    paths = _paths()
    if run_id:
        report_file = paths.runs / run_id / "report.json"
        if report_file.is_file():
            try:
                data = json.loads(report_file.read_text(encoding="utf-8"))
                return jsonify(data)
            except Exception as exc:
                log.warning("report.json run_id read failed: %s", exc)
    cfg = load_armor_config()
    rel = cfg.get("latest_report")
    if rel:
        path = (paths.project_root / str(rel)).resolve()
        try:
            path.relative_to(paths.project_root.resolve())
        except ValueError:
            log.warning("latest_report path outside project: %s", path)
            return jsonify({"error": "Invalid report path"}), 400
        if path.is_file():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                return jsonify(data)
            except Exception as exc:
                log.warning("latest_report read failed: %s", exc)
    return jsonify({"error": "No report found"}), 404
