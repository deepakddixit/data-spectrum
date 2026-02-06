
import typer
from src.cli.repl import DataSpectrumREPL

app = typer.Typer()

@app.command()
def repl():
    """Start the interactive Data Spectrum REPL."""
    repl_instance = DataSpectrumREPL()
    repl_instance.start()

if __name__ == "__main__":
    app()
