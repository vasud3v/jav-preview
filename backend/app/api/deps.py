"""API dependencies."""
from backend.app.core.database import get_db
from backend.app.core.config import settings

__all__ = ["get_db", "settings"]
