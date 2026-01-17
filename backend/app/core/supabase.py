"""Supabase client for authentication."""
from supabase import create_client, Client
from backend.app.core.config import settings

_supabase: Client | None = None


def get_supabase() -> Client:
    """Get Supabase client instance."""
    global _supabase
    if _supabase is None:
        if not settings.supabase_url or not settings.supabase_anon_key:
            raise RuntimeError("Supabase URL and anon key must be configured")
        _supabase = create_client(settings.supabase_url, settings.supabase_anon_key)
    return _supabase


def get_supabase_admin() -> Client:
    """Get Supabase client with service role key for admin operations."""
    if not settings.supabase_url or not settings.supabase_service_key:
        raise RuntimeError("Supabase URL and service key must be configured")
    return create_client(settings.supabase_url, settings.supabase_service_key)
