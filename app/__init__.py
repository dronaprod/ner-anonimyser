"""
Armor Data Anonymizer — Flask application factory.

Layers under ``app``: ``config/``, ``exceptions/``, ``logging/``, ``routers/``,
``services/`` (NER + web helpers), ``models/``, ``utils/``, ``views/``, ``pipeline.py``.

Configuration: YAML under ``config/`` plus ``instance/state.yaml``. Secrets: ``.env``.

Run (development): ``python main.py`` or ``flask --app wsgi run``.

Production: ``gunicorn -c deployment/gunicorn.conf.py 'wsgi:app'``.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from flask import Flask

from app.config import flask_secret_key, load_dotenv_from_repo, load_paths_and_settings
from app.exceptions import register_error_handlers
from app.logging import configure_app_logging
from app.routers import register_routers


def create_app(test_config: dict | None = None) -> Flask:
    project_root = Path(__file__).resolve().parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    load_dotenv_from_repo()

    raw, paths = load_paths_and_settings(project_root)
    if test_config:
        raw = {**raw, **test_config}

    paths.uploads.mkdir(parents=True, exist_ok=True)
    paths.runs.mkdir(parents=True, exist_ok=True)
    paths.reports.mkdir(parents=True, exist_ok=True)
    paths.log_dir.mkdir(parents=True, exist_ok=True)

    flask_app = Flask(
        __name__,
        static_folder=str(paths.ui_dir),
        static_url_path="",
    )
    flask_app.config["ARMOR_PATHS"] = paths
    flask_app.config["ARMOR_SETTINGS"] = raw
    flask_app.config["SECRET_KEY"] = flask_secret_key(raw)

    flask_cfg = raw.get("flask") if isinstance(raw.get("flask"), dict) else {}
    try:
        max_mb = int(flask_cfg.get("max_content_length_mb", 200))
    except (TypeError, ValueError):
        max_mb = 200
    flask_app.config["MAX_CONTENT_LENGTH"] = max(1, max_mb) * 1024 * 1024

    log_cfg = raw.get("logging") if isinstance(raw.get("logging"), dict) else {}
    level_name = str(log_cfg.get("level", "INFO"))
    rel_log = log_cfg.get("file")
    log_file_path = None
    if rel_log:
        p = Path(rel_log)
        log_file_path = p.resolve() if p.is_absolute() else (project_root / p).resolve()

    configure_app_logging(flask_app, level_name=level_name, log_file=log_file_path)

    if test_config is not None:
        flask_app.config.update(test_config)

    flask_app.debug = os.environ.get("FLASK_DEBUG", "").strip().lower() in ("1", "true", "yes")

    register_error_handlers(flask_app)
    register_routers(flask_app)

    flask_app.logger.info("Armor data root: %s", paths.data_root)
    return flask_app
