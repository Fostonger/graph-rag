"""GraphRAG CLI for querying external indexer databases."""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from ..config import Settings, load_settings
from ..db.connection import get_connection
from ..db.schema import get_index_state, get_metadata

console = Console()
app = typer.Typer(add_completion=False, no_args_is_help=True)


def _resolve_settings(
    config_path: Optional[Path],
    repo: Optional[Path],
    db: Optional[Path],
) -> Settings:
    settings = load_settings(config_path)
    if repo:
        settings.repo_path = Path(repo).expanduser().resolve()
    if db:
        settings.db_path = Path(db).expanduser().resolve()
    return settings


@app.command()
def status(
    db: Optional[Path] = typer.Option(None, "--db", help="Path to indexer database"),
    config: Optional[Path] = typer.Option(None, "--config", "-c", help="Path to config.yaml"),
):
    """Show information about the indexed database."""
    settings = _resolve_settings(config, None, db)
    
    if not settings.db_path.exists():
        console.print(f"[red]Database not found:[/red] {settings.db_path}")
        raise typer.Exit(1)
    
    with get_connection(settings.db_path) as conn:
        # Get metadata
        version = get_metadata(conn, "version") or "unknown"
        tool = get_metadata(conn, "tool") or "unknown"
        project_root = get_metadata(conn, "project_root") or "unknown"
        
        # Get index state
        state = get_index_state(conn)
        last_commit = state["last_commit_hash"] if state else "N/A"
        last_indexed = state["last_indexed_at"] if state else "N/A"
        
        # Count records
        doc_count = conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
        symbol_count = conn.execute("SELECT COUNT(*) FROM symbols").fetchone()[0]
        occ_count = conn.execute("SELECT COUNT(*) FROM occurrences").fetchone()[0]
        rel_count = conn.execute("SELECT COUNT(*) FROM relationships").fetchone()[0]
        
        table = Table(title="External Indexer Database")
        table.add_column("Field", style="cyan")
        table.add_column("Value")
        
        table.add_row("Database Path", str(settings.db_path))
        table.add_row("Schema Version", version)
        table.add_row("Indexer Tool", tool)
        table.add_row("Project Root", project_root)
        table.add_row("Last Commit", last_commit)
        table.add_row("Last Indexed", str(last_indexed))
        table.add_row("Documents", str(doc_count))
        table.add_row("Symbols", str(symbol_count))
        table.add_row("Occurrences", str(occ_count))
        table.add_row("Relationships", str(rel_count))
        
        console.print(table)


@app.command()
def search(
    query: str = typer.Argument(..., help="Symbol name to search for"),
    kind: Optional[str] = typer.Option(None, "--kind", "-k", help="Filter by kind"),
    db: Optional[Path] = typer.Option(None, "--db", help="Path to indexer database"),
    config: Optional[Path] = typer.Option(None, "--config", "-c", help="Path to config.yaml"),
    limit: int = typer.Option(25, "--limit", "-n", help="Max results"),
):
    """Search for symbols in the database."""
    settings = _resolve_settings(config, None, db)
    
    from ..db.query_service import QueryService
    service = QueryService(settings)
    
    results = service.search_symbols(query, kind=kind, limit=limit)
    
    if not results:
        console.print(f"[yellow]No symbols found matching '{query}'[/yellow]")
        return
    
    table = Table(title=f"Symbols matching '{query}'")
    table.add_column("Name", style="cyan")
    table.add_column("Kind")
    table.add_column("Module")
    table.add_column("File")
    table.add_column("Line")
    
    for r in results:
        table.add_row(
            r.get("name", ""),
            r.get("kind", ""),
            r.get("module", "") or "",
            r.get("file", "") or "",
            str(r.get("line", "")) if r.get("line") else "",
        )
    
    console.print(table)


@app.command()
def definition(
    symbol: str = typer.Argument(..., help="Symbol name to find"),
    db: Optional[Path] = typer.Option(None, "--db", help="Path to indexer database"),
    config: Optional[Path] = typer.Option(None, "--config", "-c", help="Path to config.yaml"),
):
    """Find the definition of a symbol."""
    settings = _resolve_settings(config, None, db)
    
    from ..db.query_service import QueryService
    service = QueryService(settings)
    
    result = service.go_to_definition(symbol)
    
    if not result:
        console.print(f"[yellow]Symbol '{symbol}' not found[/yellow]")
        return
    
    console.print(f"[cyan bold]{result.get('symbol', symbol)}[/cyan bold] ({result.get('kind', 'unknown')})")
    
    if result.get("module"):
        console.print(f"  Module: {result['module']}")
    
    if result.get("definition"):
        defn = result["definition"]
        console.print(f"  File: {defn.get('file')}:{defn.get('line')}")
        if defn.get("snippet"):
            console.print(f"  [dim]{defn['snippet']}[/dim]")
    
    if result.get("inherits"):
        console.print(f"  Inherits: {', '.join(result['inherits'])}")
    
    if result.get("conformances"):
        console.print(f"  Conforms to: {', '.join(result['conformances'])}")
    
    if result.get("members"):
        console.print(f"  Members: {', '.join(result['members'][:10])}")
        if len(result['members']) > 10:
            console.print(f"    ... and {len(result['members']) - 10} more")


@app.command()
def references(
    symbol: str = typer.Argument(..., help="Symbol name to find references for"),
    db: Optional[Path] = typer.Option(None, "--db", help="Path to indexer database"),
    config: Optional[Path] = typer.Option(None, "--config", "-c", help="Path to config.yaml"),
    limit: int = typer.Option(50, "--limit", "-n", help="Max references"),
):
    """Find all references to a symbol."""
    settings = _resolve_settings(config, None, db)
    
    from ..db.query_service import QueryService
    service = QueryService(settings)
    
    result = service.find_references(symbol, limit=limit)
    
    if result.get("reference_count", 0) == 0:
        console.print(f"[yellow]No references found for '{symbol}'[/yellow]")
        return
    
    console.print(f"[cyan bold]{result.get('symbol', symbol)}[/cyan bold] - {result['reference_count']} references")
    
    for ref in result.get("references", []):
        loc = f"{ref.get('file')}:{ref.get('line')}"
        context = ref.get("context", "")
        snippet = ref.get("snippet", "")
        
        if context:
            console.print(f"  {loc} (in {context})")
        else:
            console.print(f"  {loc}")
        
        if snippet:
            console.print(f"    [dim]{snippet}[/dim]")
