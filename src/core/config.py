import os
from pydantic import BaseModel

class Settings(BaseModel):
    # Metadata TTL (default: 24 hours)
    metadata_ttl_seconds: int = int(os.getenv("METADATA_TTL", 86400))
    
    # Discovery TTL (default: 1 hour)
    discovery_ttl_seconds: int = int(os.getenv("DISCOVERY_TTL", 3600))
    
    # Metadata persistence path
    metadata_db_path: str = os.getenv("METADATA_DB_PATH", "~/.data-spectrum/metadata.db")

# Global settings instance
settings = Settings()
