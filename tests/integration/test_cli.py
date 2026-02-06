import os
import duckdb
import pytest
from src.cli.repl import DataSpectrumREPL

TEST_DB = "cli_test.duckdb"

@pytest.fixture
def setup_cli_db():
    """Setup temporary DuckDB for CLI testing."""
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
    conn = duckdb.connect(TEST_DB)
    conn.execute("CREATE TABLE users (id INTEGER, name VARCHAR)")
    conn.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
    conn.close()
    
    yield TEST_DB
    
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)

def test_cli_flow(setup_cli_db):
    """Verify CLI commands change state correctly."""
    db_file = setup_cli_db
    
    # Initialize REPL
    # We might need to mock Console if we want to suppress output, 
    # but for integration test seeing output in logs checks is fine.
    repl = DataSpectrumREPL()
    
    # 1. Connect
    # Command: connect <name> <type> <key=val>
    repl._handle_command(f"connect testdb duckdb database={db_file}")
    assert "testdb" in repl.active_sources
    
    # 2. Show (Root) - should not crash
    repl._handle_command("show")
    
    # 3. Set Path
    repl._handle_command("set testdb.main")
    assert repl.current_path == "testdb.main"
    
    # 4. Show Tables - currently prints to console, we assume it works if no error
    repl._handle_command("show")
    
    # 5. Desc Table
    repl._handle_command("desc users")
    
    # 6. Sample Data
    repl._handle_command("sample users --limit 1")
    
    # Verify internal state didn't corrupt
    assert repl.current_path == "testdb.main"
