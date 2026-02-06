import sys
import os

# Add project root to path
sys.path.insert(0, os.path.abspath("."))

def test_imports():
    print("Testing imports...")
    try:
        from src.core.models import DataSourceConfig
        from src.core.interface import DataSource
        from src.core.registry import SourceRegistry
        from src.core.persistence import MetadataStore
        from src.api.server import app
        from src.connectors.rdbms import RDBMSConnector
        from src.connectors.databricks import DatabricksConnector
        from src.connectors.databricks_uc import DatabricksUCConnector
        from src.connectors.snowflake import SnowflakeConnector
        from src.connectors.file_system import FileConnector
        from src.connectors.duckdb import DuckDBConnector
        from src.cli.repl import DataSpectrumREPL
        
        # Check external deps explicitly
        import duckdb
        import typer
        import rich
        import prompt_toolkit
        import databricks.sdk
        print("✅ All imports successful.")
        return True
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        return False
    except Exception as e:
        print(f"❌ An error occurred: {e}")
        return False

if __name__ == "__main__":
    if test_imports():
        sys.exit(0)
    else:
        sys.exit(1)
