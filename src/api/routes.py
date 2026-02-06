from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from typing import List, Optional
from src.core.models import DataSourceConfig, TableMetadata, TableSchema, TableStats, SamplingMethod
from src.core.registry import SourceRegistry
from src.core.persistence import MetadataStore
from src.core.factory import get_datasource

router = APIRouter()

# Dependency helpers
def get_registry():
    return SourceRegistry()

def get_metadata_store():
    return MetadataStore()

# --- Source Management ---
@router.post("/sources", response_model=DataSourceConfig)
def create_source(config: DataSourceConfig, registry: SourceRegistry = Depends(get_registry)):
    registry.save_source(config)
    return config

@router.get("/sources", response_model=List[DataSourceConfig])
def list_sources(registry: SourceRegistry = Depends(get_registry)):
    return registry.list_sources()

@router.get("/sources/{name}", response_model=DataSourceConfig)
def get_source(name: str, registry: SourceRegistry = Depends(get_registry)):
    source = registry.get_source(name)
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")
    return source

@router.delete("/sources/{name}")
def delete_source(name: str, registry: SourceRegistry = Depends(get_registry)):
    registry.delete_source(name)
    return {"status": "deleted"}

# --- Discovery & Metadata ---
@router.post("/discovery/databases/{source_name}")
def discover_databases(
    source_name: str, 
    refresh: bool = False,
    registry: SourceRegistry = Depends(get_registry),
    store: MetadataStore = Depends(get_metadata_store)
):
    # Check cache
    cache_key = f"discovery:databases:{source_name}"
    if not refresh:
        cached = store.get_discovery(cache_key)
        if cached:
            return cached

    config = registry.get_source(source_name)
    if not config:
        raise HTTPException(status_code=404, detail="Source not found")
    
    try:
        with get_datasource(config) as datasource:
            result = datasource.discover_databases()
            store.save_discovery(cache_key, result)
            return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/discovery/tables/{source_name}/{database}")
def discover_tables(
    source_name: str, 
    database: str, 
    refresh: bool = False,
    registry: SourceRegistry = Depends(get_registry),
    store: MetadataStore = Depends(get_metadata_store)
):
    # Check cache
    cache_key = f"discovery:tables:{source_name}:{database}"
    if not refresh:
        cached = store.get_discovery(cache_key)
        if cached:
            return cached

    config = registry.get_source(source_name)
    if not config:
        raise HTTPException(status_code=404, detail="Source not found")
    
    try:
        with get_datasource(config) as datasource:
            result = datasource.discover_tables(database)
            store.save_discovery(cache_key, result)
            return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/metadata/{table_id}", response_model=TableMetadata)
def get_table_metadata(
    table_id: str, 
    source_name: str,
    refresh: bool = False,
    include_column_stats: bool = False,
    store: MetadataStore = Depends(get_metadata_store),
    registry: SourceRegistry = Depends(get_registry)
):
    # 1. Check cache (skip if stats requested as they might not be fully cached or we want fresh?)
    # For now, simplistic cache check
    if not refresh and not include_column_stats:
        cached = store.get_metadata(table_id)
        if cached:
            return cached

    # 2. Fetch live
    config = registry.get_source(source_name)
    if not config:
        raise HTTPException(status_code=404, detail="Source not found")

    try:
        with get_datasource(config) as datasource:
            metadata = datasource.extract_full_metadata(table_id)
            
            if include_column_stats:
                 if metadata.stats:
                    # Iterate columns and fetch stats
                    for col in metadata.schema_info.columns:
                        try:
                            cstat = datasource.get_column_stats(metadata.id, col.name)
                            metadata.stats.column_stats[col.name] = cstat
                        except:
                            pass
            
            store.save_metadata(metadata)
            return metadata
    except Exception as e:
        # Log error in real app
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/sample/{source_name}/{table_id}")
def sample_data(
    source_name: str, 
    table_id: str, 
    limit: Optional[int] = None, 
    method: SamplingMethod = SamplingMethod.LIMIT,
    percent: Optional[float] = None,
    registry: SourceRegistry = Depends(get_registry)
):
    config = registry.get_source(source_name)
    if not config:
        raise HTTPException(status_code=404, detail="Source not found")

    try:
        with get_datasource(config) as datasource:
            data = datasource.sample_data(table_id, limit=limit, method=method, percent=percent)
            return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
