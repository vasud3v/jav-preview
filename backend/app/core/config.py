"""Application configuration."""
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings."""
    
    model_config = SettingsConfigDict(
        env_file=Path(__file__).parent.parent.parent.parent / "backend" / ".env",
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
    supabase_db_url: str = ""  # PostgreSQL connection string
    
    # Server
    host: str = "0.0.0.0"
    port: int = 8000
    
    # CORS
    cors_origins: list[str] = ["http://localhost:5173", "http://localhost:3000", "http://127.0.0.1:5173", "http://127.0.0.1:3000"]
    
    @property
    def cors_origins_list(self) -> list[str]:
        """Return CORS origins as a list, handling string input from env."""
        if isinstance(self.cors_origins, str):
            return [origin.strip() for origin in self.cors_origins.split(",")]
        return self.cors_origins
    
    # Pagination
    default_page_size: int = 20
    max_page_size: int = 100


settings = Settings()
