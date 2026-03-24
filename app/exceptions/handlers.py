"""Flask error handler registration (JSON for ``/api/*``, HTML for browser)."""
from __future__ import annotations

import logging

from flask import Flask, jsonify, request
from werkzeug.exceptions import HTTPException

from app.exceptions.app_exceptions import ArmorApiError

log = logging.getLogger("app.exceptions")


def register_error_handlers(app: Flask) -> None:
    @app.errorhandler(ArmorApiError)
    def handle_api_error(exc: ArmorApiError):
        if not str(request.path or "").startswith("/api/"):
            raise exc
        return jsonify({"error": exc.message, "code": exc.code}), exc.status_code

    @app.errorhandler(404)
    def handle_404(_e):
        if str(request.path or "").startswith("/api/"):
            return jsonify({"error": "Not found", "code": "not_found"}), 404
        return "<h1>Not found</h1>", 404, {"Content-Type": "text/html; charset=utf-8"}

    @app.errorhandler(413)
    def handle_413(_e):
        return jsonify({"error": "Payload too large", "code": "payload_too_large"}), 413

    @app.errorhandler(Exception)
    def handle_unexpected(exc: Exception):
        if isinstance(exc, HTTPException):
            raise exc
        if str(request.path or "").startswith("/api/"):
            log.exception("Unhandled API error")
            return jsonify({"error": "Internal server error", "code": "internal_error"}), 500
        log.exception("Unhandled error")
        if app.debug:
            raise exc
        return "<h1>Internal error</h1>", 500, {"Content-Type": "text/html; charset=utf-8"}
