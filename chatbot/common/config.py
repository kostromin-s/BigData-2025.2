from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    """Application settings."""
    
    QDRANT_URL: str = "https://2a110e29-778a-42a5-8ae8-4b4c2dc66bfd.sa-east-1-0.aws.cloud.qdrant.io"
    QDRANT_API_KEY: str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3MiOiJtIiwic3ViamVjdCI6ImFwaS1rZXk6MzQ0MDM2YWMtNmNlMC00NDgzLWI2NzMtNWRjOWU2ZmI0ZjEzIn0.2-sm0NrkefTMkKABX4XvAzw0vt4I-nLRXPZNRumI0ew"
    QDRANT_COLLECTION: str = "real_estate"
    EMBED_MODEL: str = "intfloat/multilingual-e5-base"
    OLLAMA_URL: str = "http://localhost:11434/api/generate"
    MODEL: str = "llama-3.1-8b-instant"
    GRSK: str = "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"
    class Config:
        env_file = ".env"

settings = Settings()
