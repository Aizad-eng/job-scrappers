from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    # Database
    DATABASE_URL: str
    
    # Application
    APP_ENV: str = "production"
    DEBUG: bool = False
    SECRET_KEY: str
    
    # Apify
    DEFAULT_APIFY_TOKEN: Optional[str] = None
    APIFY_POLL_INTERVAL_SECONDS: int = 30
    APIFY_MAX_WAIT_MINUTES: int = 15
    
    # Server
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    
    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()
