"""URL routers (Flask blueprints). Register with :func:`register_routers`."""
from __future__ import annotations

from flask import Flask

from app.routers.api_router import bp as api_bp
from app.routers.web_router import bp as web_bp


def register_routers(app: Flask) -> None:
    """Attach HTTP routes (web UI first, then API under ``/api``)."""
    app.register_blueprint(web_bp)
    app.register_blueprint(api_bp)
