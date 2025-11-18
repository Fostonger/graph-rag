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
from ..indexer.feature_service import FeatureBranchIndexer
from ..indexer.service import IndexerService, build_registry

console = Console()
app = typer.Typer(add_completion=False, no_args_is_help=True)


def _resolve_settings(
    config_path: Optional[Path],
    repo: Optional[Path],
    db: Optional[Path],
    feature_db: Optional[Path],
) -> Settings:
    settings = load_settings(config_path)
    if repo:
        settings.repo_path = Path(repo).expanduser().resolve()
    if db:
        settings.db_path = Path(db).expanduser().resolve()
    if feature_db:
        settings.feature_db_path = Path(feature_db).expanduser().resolve()
    return settings


@app.command()
def init(
    config: Optional[Path] = typer.Option(None, "--config", "-c"),
    repo: Optional[Path] = typer.Option(None, "--repo"),
    db: Optional[Path] = typer.Option(None, "--db"),
    feature_db: Optional[Path] = typer.Option(None, "--feature-db"),
):
    """Perform a first-time full index of the repository."""

    settings = _resolve_settings(config, repo, db, feature_db)
    registry = build_registry(settings.languages)
    service = IndexerService(settings, registry=registry)
    head_hash = service.initialize()
    console.print(f"[green]Indexed HEAD commit[/green] {head_hash}")


@app.command()
def update(
    config: Optional[Path] = typer.Option(None, "--config", "-c"),
    repo: Optional[Path] = typer.Option(None, "--repo"),
    db: Optional[Path] = typer.Option(None, "--db"),
    feature_db: Optional[Path] = typer.Option(None, "--feature-db"),
):
    """Apply incremental updates since the last indexed master commit."""

    settings = _resolve_settings(config, repo, db, feature_db)
    registry = build_registry(settings.languages)
    service = IndexerService(settings, registry=registry)
    commits = service.update()
    if not commits:
        console.print("[yellow]Already up to date[/yellow]")
    else:
        console.print(f"[green]Indexed commits:[/green] {', '.join(commits)}")

    feature_service = FeatureBranchIndexer(settings, registry=registry)
    feature_result = feature_service.update()
    if feature_result.skipped:
        console.print(
            f"[yellow]Feature indexing skipped:[/yellow] {feature_result.skipped_reason}"
        )
    else:
        if feature_result.commits:
            console.print(
                f"[green]Feature branch {feature_result.branch} commits:[/green] "
                + ", ".join(feature_result.commits)
            )
        else:
            console.print(
                f"[yellow]Feature branch {feature_result.branch} has no new commits[/yellow]"
            )
        if feature_result.worktree_files:
            console.print(
                "[green]Indexed worktree files:[/green] "
                + ", ".join(feature_result.worktree_files)
            )
        else:
            console.print("[yellow]No unstaged Swift changes detected[/yellow]")


@app.command()
def status(
    db: Optional[Path] = typer.Option(None, "--db"),
    config: Optional[Path] = typer.Option(None, "--config", "-c"),
    feature_db: Optional[Path] = typer.Option(None, "--feature-db"),
):
    """Show metadata about the indexed store."""

    settings = _resolve_settings(config, None, db, feature_db)
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

