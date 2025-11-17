from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from ..config import Settings, load_settings
from ..db import schema
from ..db.connection import get_connection
from ..db.repository import MetadataRepository
from ..indexer.service import IndexerService, build_registry

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
def init(
    config: Optional[Path] = typer.Option(None, "--config", "-c"),
    repo: Optional[Path] = typer.Option(None, "--repo"),
    db: Optional[Path] = typer.Option(None, "--db"),
):
    """Perform a first-time full index of the repository."""

    settings = _resolve_settings(config, repo, db)
    service = IndexerService(settings, registry=build_registry(settings.languages))
    head_hash = service.initialize()
    console.print(f"[green]Indexed HEAD commit[/green] {head_hash}")


@app.command()
def update(
    config: Optional[Path] = typer.Option(None, "--config", "-c"),
    repo: Optional[Path] = typer.Option(None, "--repo"),
    db: Optional[Path] = typer.Option(None, "--db"),
):
    """Apply incremental updates since the last indexed master commit."""

    settings = _resolve_settings(config, repo, db)
    service = IndexerService(settings, registry=build_registry(settings.languages))
    commits = service.update()
    if not commits:
        console.print("[yellow]Already up to date[/yellow]")
    else:
        console.print(f"[green]Indexed commits:[/green] {', '.join(commits)}")


@app.command()
def status(
    db: Optional[Path] = typer.Option(None, "--db"),
    config: Optional[Path] = typer.Option(None, "--config", "-c"),
):
    """Show metadata about the indexed store."""

    settings = _resolve_settings(config, None, db)
    with get_connection(settings.db_path) as conn:
        schema.apply_schema(conn)
        store = MetadataRepository(conn)
        last_hash = store.latest_master_commit() or "<none>"
        entity_count = conn.execute("SELECT COUNT(*) AS c FROM entities").fetchone()["c"]
        member_count = conn.execute("SELECT COUNT(*) AS c FROM members").fetchone()["c"]
        table = Table(title="GraphRAG DB")
        table.add_column("Field")
        table.add_column("Value")
        table.add_row("DB Path", str(settings.db_path))
        table.add_row("Repo", str(settings.repo_path))
        table.add_row("Last master hash", last_hash)
        table.add_row("Entities", str(entity_count))
        table.add_row("Members", str(member_count))
        console.print(table)

