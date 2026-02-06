import os
import yaml
from pathlib import Path
from typing import List, Optional, Dict, Any
from cryptography.fernet import Fernet
from src.core.models import DataSourceConfig, SourceType

class SourceRegistry:
    def __init__(self, config_dir: str = "~/.data-spectrum"):
        self.config_dir = Path(config_dir).expanduser()
        self.sources_dir = self.config_dir / "sources"
        self.key_file = self.config_dir / "secret.key"
        
        self.config_dir.mkdir(parents=True, exist_ok=True)
        self.sources_dir.mkdir(parents=True, exist_ok=True)
        
        self._load_key()

    def _load_key(self):
        if not self.key_file.exists():
            key = Fernet.generate_key()
            with open(self.key_file, "wb") as f:
                f.write(key)
        
        with open(self.key_file, "rb") as f:
            self.cipher_suite = Fernet(f.read())

    def _encrypt(self, text: str) -> str:
        if not text:
            return text
        return self.cipher_suite.encrypt(text.encode()).decode()

    def _decrypt(self, text: str) -> str:
        if not text:
            return text
        return self.cipher_suite.decrypt(text.encode()).decode()

    def _encrypt_credentials(self, credentials: Dict[str, Any]) -> Dict[str, Any]:
        """Encrypts sensitive values in credentials."""
        encrypted = {}
        for k, v in credentials.items():
            if isinstance(v, str):
                encrypted[k] = self._encrypt(v)
            else:
                encrypted[k] = v
        return encrypted

    def _decrypt_credentials(self, credentials: Dict[str, Any]) -> Dict[str, Any]:
        """Decrypts sensitive values in credentials."""
        decrypted = {}
        for k, v in credentials.items():
            if isinstance(v, str):
                try:
                    decrypted[k] = self._decrypt(v)
                except Exception:
                    # Fallback if value wasn't encrypted (e.g. legacy or manual edit)
                    decrypted[k] = v
            else:
                decrypted[k] = v
        return decrypted

    def save_source(self, config: DataSourceConfig):
        """Save a data source configuration."""
        # Create a deep copy to modify credentials for storage
        storage_config = config.model_dump(mode='json')
        storage_config['credentials'] = self._encrypt_credentials(storage_config['credentials'])
        
        file_path = self.sources_dir / f"{config.name}.yaml"
        with open(file_path, "w") as f:
            yaml.dump(storage_config, f)

    def get_source(self, name: str) -> Optional[DataSourceConfig]:
        """Load a data source configuration by name."""
        file_path = self.sources_dir / f"{name}.yaml"
        if not file_path.exists():
            return None
            
        with open(file_path, "r") as f:
            data = yaml.safe_load(f)
            
        # Decrypt credentials
        data['credentials'] = self._decrypt_credentials(data.get('credentials', {}))
        
        return DataSourceConfig(**data)

    def list_sources(self) -> List[DataSourceConfig]:
        """List all registered data sources."""
        sources = []
        for file_path in self.sources_dir.glob("*.yaml"):
            try:
                name = file_path.stem
                source = self.get_source(name)
                if source:
                    sources.append(source)
            except Exception as e:
                print(f"Error loading source {file_path}: {e}")
        return sources

    def delete_source(self, name: str):
        """Delete a data source configuration."""
        file_path = self.sources_dir / f"{name}.yaml"
        if file_path.exists():
            file_path.unlink()
