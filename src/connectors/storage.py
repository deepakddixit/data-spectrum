import fsspec
from typing import List, Any, Dict
from src.core.models import DataSourceConfig

class Storage:
    def __init__(self, config: DataSourceConfig):
        self.config = config
        self.fs = None
        self.protocol = "file"
        self._init_fs()

    def _init_fs(self):
        # Determine protocol from url or config
        url = self.config.connection_details.get("url", "")
        if "://" in url:
            self.protocol = url.split("://")[0]
        elif url in ["s3", "gs", "gcs", "abfss", "adl", "az"]:
            self.protocol = url
        else:
            self.protocol = "file"

        # Prepare storage options (credentials)
        storage_options = {}
        creds = self.config.credentials
        
        # Mapping common credential keys to fsspec args
        if "aws_access_key_id" in creds:
             storage_options["key"] = creds["aws_access_key_id"]
             storage_options["secret"] = creds["aws_secret_access_key"]
             if "aws_session_token" in creds:
                 storage_options["token"] = creds["aws_session_token"]
             
             client_kwargs = {}
             if "endpoint_url" in creds:
                 storage_options["endpoint_url"] = creds["endpoint_url"]
                 client_kwargs["endpoint_url"] = creds["endpoint_url"]
             
             if "region_name" in creds:
                 client_kwargs["region_name"] = creds["region_name"]
            
             if client_kwargs:
                 storage_options["client_kwargs"] = client_kwargs

             if "use_ssl" in creds:
                 storage_options["use_ssl"] = creds["use_ssl"]
                 
             if "s3_additional_kwargs" in creds:
                 storage_options["s3_additional_kwargs"] = creds["s3_additional_kwargs"]
            
             if "config_kwargs" in creds:
                 storage_options["config_kwargs"] = creds["config_kwargs"]
                 
        # TODO: Add Azure / GCS mappings

        try:
            self.fs = fsspec.filesystem(self.protocol, **storage_options)
        except Exception:
            # Fallback or generic init
            self.fs = fsspec.filesystem(self.protocol)

    def list_files(self, path: str, recursive: bool = False) -> List[str]:
        """List files in directory."""
        if self.protocol == "file":
             # Local paths might need expansion
             pass 
        return self.fs.find(path) if recursive else self.fs.ls(path)

    def list_directories(self, path: str) -> List[str]:
        """List directories (for discovery)."""
        # fsspec ls returns list of dicts or paths depending on implementation
        # Unified behavior needed
        entries = self.fs.ls(path, detail=True)
        dirs = [e['name'] for e in entries if e['type'] == 'directory']
        return dirs

    def open(self, path: str, mode: str = "rb"):
        return self.fs.open(path, mode)
