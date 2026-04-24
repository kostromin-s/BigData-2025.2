from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    """Application settings."""
    
    QDRANT_URL: str = "http://localhost:6333"
    
    EMBED_MODEL: str = "intfloat/multilingual-e5-base"

    class Config:
        env_file = ".env"

settings = Settings()