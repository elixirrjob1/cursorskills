"""API package: FastAPI app exposing database data as JSON with Bearer auth."""

__all__ = ["app"]


def __getattr__(name: str):
    if name == "app":
        from api.main import app

        return app
    raise AttributeError(name)
