"""Application configuration."""
from pathlib import Path
from typing import Union
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings."""
    
    model_config = SettingsConfigDict(
        env_file=Path(__file__).parent.parent.parent / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )
    
    # API
    app_name: str = "Prevue API"
    api_version: str = "1.0.0"
    debug: bool = False
    
    # Supabase (for everything - auth, database, storage)
    supabase_url: str = ""
    supabase_anon_key: str = ""
    supabase_service_key: str = ""
    supabase_db_url: str = ""  # Optional - not needed for REST API mode
    
    # Server
    host: str = "0.0.0.0"
    port: int = 8000
    
    # CORS - accepts comma-separated string from env
    cors_origins: str = "https://jav-preview.netlify.app,http://localhost:5173,http://localhost:3000"
    
    @property
    def cors_origins_list(self) -> list[str]:
        """Return CORS origins as a list."""
        if not self.cors_origins:
            return ["http://localhost:5173", "http://localhost:3000"]
        return [origin.strip() for origin in self.cors_origins.split(",") if origin.strip()]
    
    # Pagination
    default_page_size: int = 20
    max_page_size: int = 100


settings = Settings()
