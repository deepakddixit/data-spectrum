import sqlite3
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Any
from src.core.models import TableMetadata
from src.core.config import settings

class MetadataStore:
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = Path(db_path or settings.metadata_db_path).expanduser()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS table_metadata (
                    id TEXT PRIMARY KEY,
                    database TEXT,
                    name TEXT,
                    content JSON,
                    last_updated TIMESTAMP
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS discovery_cache (
                    key TEXT PRIMARY KEY,
                    content JSON,
                    last_updated TIMESTAMP
                )
            """)

    def save_metadata(self, metadata: TableMetadata):
        """Save or update table metadata."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO table_metadata (id, database, name, content, last_updated)
                VALUES (?, ?, ?, ?, ?)
            """, (
                metadata.id,
                metadata.database,
                metadata.name,
                metadata.model_dump_json(),
                datetime.utcnow().isoformat()
            ))

    def get_metadata(self, table_id: str, ttl_seconds: Optional[int] = None) -> Optional[TableMetadata]:
        """Get table metadata if it exists and is within TTL."""
        if ttl_seconds is None:
            ttl_seconds = settings.metadata_ttl_seconds
            
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT content, last_updated FROM table_metadata WHERE id = ?", 
                (table_id,)
            )
            row = cursor.fetchone()
            
            if not row:
                return None
            
            content_json, last_updated_str = row
            last_updated = datetime.fromisoformat(last_updated_str)
            
            # Check TTL
            if datetime.utcnow() - last_updated > timedelta(seconds=ttl_seconds):
                return None
                
            return TableMetadata.model_validate_json(content_json)

    def save_discovery(self, key: str, data: Any):
        """Save discovery results (list of strings or objects)."""
        # Ensure data is JSON serializable. 
        # If it's a list of Pydantic objects, we need to dump them.
        # But discovery returns List[str] or List[DatabaseObject]
        try:
             # Basic serialization try
             json_content = json.dumps(data, default=str) 
             # If DatabaseObject, we might need explicit dump. 
             # Let's rely on caller to pass pure dicts/lists or handle pydantic serialization here?
             # Better: assume caller passes a list of Pydantic models (DatabaseObject) or strings.
             if isinstance(data, list) and len(data) > 0 and hasattr(data[0], 'model_dump'):
                 json_content = json.dumps([d.model_dump() for d in data])
             else:
                 json_content = json.dumps(data, default=str)
        except:
             return # Fail silently on serialization

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO discovery_cache (key, content, last_updated)
                VALUES (?, ?, ?)
            """, (
                key,
                json_content,
                datetime.utcnow().isoformat()
            ))

    def get_discovery(self, key: str, ttl_seconds: Optional[int] = None) -> Optional[Any]:
        """Get discovery results if valid."""
        if ttl_seconds is None:
            ttl_seconds = settings.discovery_ttl_seconds
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT content, last_updated FROM discovery_cache WHERE key = ?", 
                (key,)
            )
            row = cursor.fetchone()
            
            if not row:
                return None
            
            content_json, last_updated_str = row
            last_updated = datetime.fromisoformat(last_updated_str)
            
            if datetime.utcnow() - last_updated > timedelta(seconds=ttl_seconds):
                return None
            
            return json.loads(content_json)

    def list_metadata_ids(self) -> List[str]:
        """List all cached table IDs."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT id FROM table_metadata")
            return [row[0] for row in cursor.fetchall()]

    def clear_cache(self, table_id: Optional[str] = None):
        """Clear cache for a specific table or all."""
        with sqlite3.connect(self.db_path) as conn:
            if table_id:
                conn.execute("DELETE FROM table_metadata WHERE id = ?", (table_id,))
            else:
                conn.execute("DELETE FROM table_metadata")
                conn.execute("DELETE FROM discovery_cache")
