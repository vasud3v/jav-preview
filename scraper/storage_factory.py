"""
Storage factory to create Supabase REST API storage backend.
All data is stored in Supabase via REST API - no direct PostgreSQL connection.
"""

import os


def create_storage():
    """
    Create Supabase REST storage instance.
    
    Returns:
        SupabaseRestStorage instance
    
    Raises:
        ValueError: If SUPABASE_URL or SUPABASE_KEY is not set
    """
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_KEY')
    
    if not supabase_url or not supabase_key:
        raise ValueError(
            "SUPABASE_URL and SUPABASE_KEY environment variables must be set. "
            "Check your GitHub secrets or .env file."
        )
    
    from supabase_rest_storage import SupabaseRestStorage
    print("Using Supabase REST API storage")
    return SupabaseRestStorage(supabase_url, supabase_key)


def create_progress_tracker():
    """
    Create Supabase REST progress tracker instance.
    
    Returns:
        SupabaseRestProgressTracker instance
    
    Raises:
        ValueError: If SUPABASE_URL or SUPABASE_KEY is not set
    """
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_KEY')
    
    if not supabase_url or not supabase_key:
        raise ValueError(
            "SUPABASE_URL and SUPABASE_KEY environment variables must be set. "
            "Check your GitHub secrets or .env file."
        )
    
    from supabase_rest_progress import SupabaseRestProgressTracker
    print("Using Supabase REST API progress tracking")
    return SupabaseRestProgressTracker(supabase_url, supabase_key)
