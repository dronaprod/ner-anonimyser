"""Application-specific exceptions (HTTP layer maps these to JSON)."""


class ArmorApiError(Exception):
    """Raise from API route handlers; :func:`~app.exceptions.register_error_handlers` converts to JSON."""

    def __init__(self, message: str, status_code: int = 400, code: str = "error") -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.code = code
