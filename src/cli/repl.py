
import shlex
import sys
from typing import Optional, List, Dict, Any
from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory
from prompt_toolkit.completion import WordCompleter
from rich.console import Console
from rich.table import Table

from src.core.models import DataSourceConfig, SourceType, SamplingMethod, DatabaseObject
from src.core.registry import SourceRegistry
from src.core.factory import get_datasource
from src.core.interface import DataSource

class DataSpectrumREPL:
    def __init__(self):
        self.registry = SourceRegistry()
        self.console = Console()
        self.session = PromptSession(history=FileHistory(".dataspectrum_history"))
        self.active_sources: Dict[str, DataSource] = {}
        self.current_path: str = "" # fully qualified path e.g. "source.db"
        
        # Determine autocomplete words
        self.completer = WordCompleter([
            "connect", "set", "show", "desc", "sample", "help", "exit", "quit"
        ], ignore_case=True)

    def start(self):
        self.console.print("[bold green]Welcome to Data Spectrum CLI[/bold green]")
        self.console.print("Type 'help' for commands. Type 'exit' to quit.")
        
        while True:
            try:
                # Dynamic prompt based on path
                prompt_text = f"ds({self.current_path or '/'})> "
                text = self.session.prompt(prompt_text, completer=self.completer)
                
                if not text.strip():
                    continue
                    
                # Strip trailing semicolon
                text = text.strip()
                if text.endswith(";"):
                    text = text[:-1].strip()
                    
                self._handle_command(text)
                
            except KeyboardInterrupt:
                continue
            except EOFError:
                break
            except Exception as e:
                self.console.print(f"[red]Error: {e}[/red]")
        
        self.console.print("Goodbye!")

    def _handle_command(self, text: str):
        parts = shlex.split(text)
        cmd = parts[0].lower()
        args = parts[1:]

        if cmd in ["exit", "quit"]:
            sys.exit(0)
        elif cmd == "help":
            self._print_help()
        elif cmd == "connect":
            self._cmd_connect(args)
        elif cmd == "set":
            self._cmd_set(args)
        elif cmd == "show":
            self._cmd_show(args)
        elif cmd == "desc":
            self._cmd_desc(args)
        elif cmd == "sample":
            self._cmd_sample(args)
        else:
            self.console.print(f"[yellow]Unknown command: {cmd}[/yellow]")

    def _print_help(self):
        table = Table(title="Available Commands")
        table.add_column("Command", style="cyan")
        table.add_column("Description")
        table.add_column("Usage")
        
        table.add_row("connect", "Connect/Register a source", "connect <name> <type> [key=val...]")
        table.add_row("set", "Set current path", "set <path>")
        table.add_row("show", "List contents", "show [path]")
        table.add_row("desc", "Describe table", "desc <path>")
        table.add_row("sample", "Sample data", "sample <path> [--limit N] [--method M] [--full]")
        
        self.console.print(table)

    def _resolve_path(self, path_arg: Optional[str] = None) -> str:
        """Resolves path argument relative to current path."""
        if not path_arg:
            return self.current_path
            
        if path_arg == "/":
            return ""
        if path_arg == "..":
            if not self.current_path:
                return ""
            parts = self.current_path.split(".")
            return ".".join(parts[:-1])
            
        # Absolute path starts with / logic? No, let's say if we want to reset to root, use set /
        # Here we just treat as relative unless ... 
        # Actually, let's treat dotted notation as absolute if it matches a source?
        # Simpler: All paths provided to SET/SHOW are either relative to current or absolute?
        # Let's assume absolute for now if it contains dots, or relative if single token?
        # But 'source' is single token. 'table' is single token.
        
        # Simple Logic:
        # If current_path is empty, path_arg is the new path.
        # If current_path is not empty:
        #   new_path = current_path + "." + path_arg
        
        # Handle "ROOT" concept
        if self.current_path:
            return f"{self.current_path}.{path_arg}"
        return path_arg

    # --- Commands ---

    def _cmd_connect(self, args: List[str]):
        if len(args) < 2:
            self.console.print("[red]Usage: connect <name> <type> [key=value...][/red]")
            self.console.print("\n[bold]Type-Specific Parameters:[/bold]")
            self.console.print("  [cyan]duckdb[/cyan]:      database=<path> OR database=:memory:")
            self.console.print("  [cyan]rdbms[/cyan]:       url=<sqlalchemy_url> OR driver=<driver> host=<host> port=<port> database=<db> username=<user> password=<pass>")
            self.console.print("  [cyan]file_system[/cyan]: path=<path> (e.g. s3://bucket/data)")
            self.console.print("  [cyan]sqlite[/cyan]:      driver=sqlite database=<path>")
            return
            
        name = args[0]
        type_str = args[1].lower()
        
        # Parse kwargs
        conn_details = {}
        for arg in args[2:]:
            if "=" in arg:
                k, v = arg.split("=", 1)
                conn_details[k] = v
        
        # Create config
        try:
            source_type = SourceType(type_str)
        except ValueError:
            self.console.print(f"[red]Invalid source type: {type_str}. Valid: {list(SourceType)}[/red]")
            return

        # Special handling for duckdb if no database specified, default to memory?
        # or just pass details as is.
        
        config = DataSourceConfig(
            name=name,
            type=source_type,
            connection_details=conn_details
        )
        
        # Save and Connect
        self.registry.save_source(config)
        try:
            ds = get_datasource(config)
            ds.connect() # Verify connection
            self.active_sources[name] = ds
            self.console.print(f"[green]Successfully connected to {name}[/green]")
        except Exception as e:
            self.console.print(f"[red]Connection failed: {e}[/red]")

    def _ensure_source_loaded(self, source_name: str) -> Optional[DataSource]:
        if source_name in self.active_sources:
            return self.active_sources[source_name]
        
        # Try load from registry
        config = self.registry.get_source(source_name)
        if config:
            try:
                ds = get_datasource(config)
                ds.connect()
                self.active_sources[source_name] = ds
                return ds
            except Exception as e:
                self.console.print(f"[red]Error connecting to {source_name}: {e}[/red]")
                return None
        return None

    def _cmd_set(self, args: List[str]):
        if not args:
            self.current_path = ""
            return
        
        path = args[0]
        if path == "/":
            self.current_path = ""
        elif path == "..":
             if self.current_path:
                parts = self.current_path.split(".")
                self.current_path = ".".join(parts[:-1])
        else:
            # Smart navigation:
            # 1. If path matches a known source, switch to it (absolute)
            # Check active sources or registry
            sources = [s.name for s in self.registry.list_sources()]
            
            if path in sources:
                 self.current_path = path
            else:
                # 2. Else relative append
                if self.current_path:
                     target = f"{self.current_path}.{path}"
                else:
                     target = path
                self.current_path = target
            
        self.console.print(f"Context: {self.current_path or '/'}")

    def _cmd_show(self, args: List[str]):
        # Determine target path
        if args:
            path = self._resolve_path(args[0])
        else:
            path = self.current_path

        if not path:
            # List sources
            sources = self.registry.list_sources()
            table = Table(title="Data Sources")
            table.add_column("Name", style="cyan")
            table.add_column("Type", style="magenta")
            for s in sources:
                table.add_row(s.name, s.type.value)
            self.console.print(table)
            return

        # Path exists: source[.database[.table]] ??
        parts = path.split(".")
        source_name = parts[0]
        
        ds = self._ensure_source_loaded(source_name)
        if not ds:
            self.console.print(f"[red]Source {source_name} not found or failed to connect[/red]")
            return
            
        if len(parts) == 1:
            # List databases (or tables if no db concept)
            try:
                # Some sources like duckdb/generic rdbms might return dbs
                try:
                    dbs = ds.discover_databases()
                    if dbs:
                        table = Table(title=f"Databases in {source_name}")
                        table.add_column("Name", style="green")
                        table.add_column("Type")
                        for db in dbs:
                            table.add_row(db, "Database")
                        self.console.print(table)
                        return
                    # Fallback to tables if no databases found or empty
                except:
                    pass
                
                dbs = ds.discover_databases() # Retrying logic is bit redundant here but keeping flow
                if not dbs:
                     # try listing tables in default
                     tables = ds.discover_tables("default") # or None?
                     if tables:
                        table = Table(title=f"Tables in {source_name} (default)")
                        table.add_column("Name", style="green")
                        table.add_column("Type")
                        for t in tables:
                            name = t.name if isinstance(t, DatabaseObject) else str(t)
                            obj_type = t.type.value if hasattr(t, 'type') else "Table"
                            table.add_row(name, obj_type.title())
                            
                        self.console.print(table)
                     else:
                        self.console.print(f"[yellow]No databases or tables found in {source_name}[/yellow]")
                else:
                     # Logic duplicate from above but handling if dbs was returned second time?
                     # Simplified flow:
                     table = Table(title=f"Databases in {source_name}")
                     table.add_column("Name", style="cyan")
                     table.add_column("Type")
                     for db in dbs:
                         table.add_row(db, "Database")
                     self.console.print(table)

            except Exception as e:
                self.console.print(f"[red]Error listing: {e}[/red]")

        elif len(parts) >= 2:
            # Database selected, list tables
            db_name = parts[1] 
            
            try:
                tables = ds.discover_tables(db_name)
                # Tables is now List[DatabaseObject]
                
                table = Table(title=f"Objects in {db_name}")
                table.add_column("Name", style="green")
                table.add_column("Type") 
                
                for t in tables:
                    name = t.name if isinstance(t, DatabaseObject) else str(t)
                    obj_type = t.type.value if hasattr(t, 'type') else "Unknown"
                    table.add_row(name, obj_type.title())
                    
                self.console.print(table)
            except NameError: # If DatabaseObject not imported yet? nah, runtime should be fine
                 pass
            except Exception as e:
                self.console.print(f"[red]Error listing tables: {e}[/red]")

    def _cmd_desc(self, args: List[str]):
        if not args:
             self.console.print("[red]Usage: desc <table> (relative to current path or full)[/red]")
             return
             
        # Resolve full table path
        # If current path is source.db, arg is table -> source.db.table
        target = self._resolve_path(args[0])
        parts = target.split(".")
        
        if len(parts) < 2:
             self.console.print("[red]Invalid path for table. Need at least source.table or source.db.table[/red]")
             return
             
        source_name = parts[0]
        ds = self._ensure_source_loaded(source_name)
        if not ds:
             return
             
        # Table ID for the connector is usually everything after source name
        # e.g. "db.table" or "table"
        table_id = ".".join(parts[1:])
        
        try:
            schema = ds.get_table_schema(table_id)
            stats = ds.get_table_stats(table_id)
            
            self.console.print(f"[bold]Table:[/bold] {table_id}")
            self.console.print(f"[bold]Rows:[/bold] {stats.row_count}")
            
            # Schema Table
            st = Table(title="Schema & Statistics")
            st.add_column("Column", style="cyan")
            st.add_column("Type", style="magenta")
            st.add_column("Orig Type")
            st.add_column("Min")
            st.add_column("Max")
            st.add_column("Nulls")
            st.add_column("Distinct")
            
            from rich.progress import track
            
            # Fetch stats for each column
            columns = schema.columns
            # Use track for progress bar if interactive
            iterator = track(columns, description="Fetching column stats...") if len(columns) > 0 else columns
            
            for col in iterator:
                try:
                    cstat = ds.get_column_stats(table_id, col.name)
                    min_v = str(cstat.min_value) if cstat.min_value is not None else ""
                    max_v = str(cstat.max_value) if cstat.max_value is not None else ""
                    nulls = str(cstat.null_count) if cstat.null_count is not None else ""
                    distinct = str(cstat.distinct_count) if cstat.distinct_count is not None else ""
                except Exception as e:
                    # Fallback if specific column stat fails
                    min_v = max_v = nulls = distinct = "Error"
                    
                st.add_row(
                    col.name, 
                    col.data_type.value, 
                    str(col.metadata.get("original_type", "")),
                    min_v,
                    max_v, 
                    nulls,
                    distinct
                )
            
            self.console.print(st)
             
        except Exception as e:
             self.console.print(f"[red]Error describing table: {e}[/red]")

    def _cmd_sample(self, args: List[str]):
        if not args:
            self.console.print("[red]Usage: sample <table_path> [--limit N] [--method M] [--percent P] [--full][/red]")
            return
            
        # Manually parse args to handle flags
        path_arg = args[0]
        limit = None
        method = "limit"
        percent = None
        full_output = False
        
        i = 1
        while i < len(args):
            arg = args[i]
            if arg == "--limit":
                if i+1 < len(args):
                    limit = int(args[i+1])
                    i += 1
            elif arg == "--method":
                if i+1 < len(args):
                    method = args[i+1]
                    i += 1
            elif arg == "--percent":
                if i+1 < len(args):
                    percent = float(args[i+1])
                    i += 1
            elif arg == "--full":
                full_output = True
            i += 1
            
        # Execute
        target = self._resolve_path(path_arg)
        parts = target.split(".")
        source_name = parts[0]
        ds = self._ensure_source_loaded(source_name)
        if not ds: return
        
        table_id = ".".join(parts[1:])
        
        try:
             sm_enum = SamplingMethod(method)
             
             # Default limit to 10 if method is LIMIT and no limit specified
             if sm_enum == SamplingMethod.LIMIT and limit is None:
                 limit = 10
                 
             data = ds.sample_data(table_id, limit=limit, method=sm_enum, percent=percent)
             
             if not data:
                 self.console.print("No data returned.")
                 return
                 
             # Dynamic Table
             dt = Table(title=f"Sample ({len(data)} rows)")
             cols = data[0].keys()
             for c in cols:
                 if full_output:
                     dt.add_column(c, overflow="fold", no_wrap=False)
                 else:
                     dt.add_column(c)
                 
             for row in data:
                 dt.add_row(*[str(row.get(c, "")) for c in cols])
                 
             self.console.print(dt)
             
        except Exception as e:
             self.console.print(f"[red]Error sampling: {e}[/red]")
