from fastapi import FastAPI, Depends
from src.core.registry import SourceRegistry
from src.core.persistence import MetadataStore
from src.api.routes import router as api_router

app = FastAPI(title="Data Spectrum API", version="0.1.0")

# Dependency Injection
def get_registry():
    return SourceRegistry()

def get_metadata_store():
    return MetadataStore()

app.include_router(api_router)

@app.get("/health")
def health_check():
    return {"status": "ok"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
