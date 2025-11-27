from __future__ import annotations

import sqlite3
from typing import Any, Dict, Iterable, List, Optional

from git import GitCommandError, Repo

from ..config import Settings
from . import schema
from .connection import connect, get_connection
from .queries import find_entities, get_entity_graph, get_members


class QueryService:
    """Service for executing queries with branch-aware database connections.
    
    This service encapsulates the logic for determining which databases to use
    based on the current git branch state. It ensures that feature database
    overlays are only applied when the current branch matches the indexed
    feature branch.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def find_entities(
        self,
        needle: str,
        limit: int = 25,
        include_code: bool = False,
    ) -> List[dict]:
        """Search for entities by name/module/path."""
        with get_connection(self.settings.db_path) as conn:
            schema.apply_schema(conn)
            return find_entities(conn, needle, limit=limit, include_code=include_code)

    def get_members(
        self,
        entity_names: Iterable[str],
        member_filters: Iterable[str],
        include_code: bool = True,
    ) -> List[dict]:
        """Fetch member details for specific entities."""
        with get_connection(self.settings.db_path) as conn:
            schema.apply_schema(conn)
            return get_members(
                conn,
                entity_names=entity_names,
                member_filters=member_filters,
                include_code=include_code,
            )

    def get_graph(
        self,
        entity_name: str,
        stop_name: Optional[str] = None,
        direction: str = "both",
        include_sibling_subgraphs: bool = False,
        max_hops: Optional[int] = None,
        target_type: str = "app",
        stop_at_module_boundary: bool = False,
    ) -> dict:
        """Build a graph centered on the specified entity.
        
        Automatically determines whether to use the feature database overlay
        based on the current git branch state.
        
        Args:
            entity_name: Name of the entity to center the graph on
            stop_name: Optional entity name to stop traversal at
            direction: Graph traversal direction (upstream, downstream, both)
            include_sibling_subgraphs: Deprecated, kept for compatibility
            max_hops: Maximum number of hops from start entity
            target_type: Filter by target type (app, test, all)
            stop_at_module_boundary: If True, entities from different modules become leaf nodes
        """
        feature_conn: Optional[sqlite3.Connection] = None
        with get_connection(self.settings.db_path) as master_conn:
            schema.apply_schema(master_conn)
            
            if self._should_use_feature_db():
                feature_conn = connect(self.settings.feature_db_path)
                schema.apply_schema(feature_conn)
            
            try:
                return get_entity_graph(
                    master_conn,
                    feature_conn,
                    entity_name=entity_name,
                    stop_name=stop_name,
                    direction=direction,
                    include_sibling_subgraphs=include_sibling_subgraphs,
                    max_hops=max_hops,
                    target_type=target_type,
                    stop_at_module_boundary=stop_at_module_boundary,
                )
            finally:
                if feature_conn is not None:
                    feature_conn.commit()
                    feature_conn.close()

    def _should_use_feature_db(self) -> bool:
        """Determine if the feature database should be used for queries.
        
        The feature database should only be used when:
        1. A feature_db_path is configured
        2. The feature database file exists
        3. The current git branch is NOT the default branch
        4. The feature database was indexed for the current branch
        
        Returns:
            True if feature DB should be used, False otherwise.
        """
        # Check if feature DB is configured and exists
        if not self.settings.feature_db_path:
            return False
        if not self.settings.feature_db_path.exists():
            return False
        
        # Get current git branch
        current_branch = self._get_current_branch()
        if not current_branch:
            # Detached HEAD or unable to determine branch
            return False
        
        # Don't use feature DB when on default branch
        if current_branch == self.settings.default_branch:
            return False
        
        # Verify feature DB was indexed for this specific branch
        indexed_branch = self._get_feature_db_branch()
        if not indexed_branch:
            return False
        
        return indexed_branch == current_branch

    def _get_current_branch(self) -> Optional[str]:
        """Get the current git branch name.
        
        Returns:
            Branch name, or None if on detached HEAD or error.
        """
        try:
            repo = Repo(str(self.settings.repo_path))
            return repo.active_branch.name
        except (TypeError, GitCommandError, Exception):
            return None

    def _get_feature_db_branch(self) -> Optional[str]:
        """Get the branch name stored in the feature database.
        
        Returns:
            Branch name the feature DB was indexed for, or None if not found.
        """
        try:
            conn = connect(self.settings.feature_db_path)
            try:
                row = conn.execute(
                    "SELECT value FROM schema_meta WHERE key = ?",
                    ("feature_branch",),
                ).fetchone()
                return row["value"] if row else None
            except sqlite3.OperationalError:
                # Table doesn't exist or query failed
                return None
            finally:
                conn.close()
        except Exception:
            return None

