"""Database connection placeholder.

In REST API mode, SQLAlchemy is not needed as all database
operations go through the Supabase REST API client.
"""

# Placeholder - database not available in REST API mode
engine = None
SessionLocal = None
Base = None


def get_db():
    """Dependency for database sessions - not available in REST API mode."""
    raise RuntimeError("Database not available in REST API mode. Use Supabase REST API.")
