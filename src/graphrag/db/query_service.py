"""Query service for code navigation on external indexer databases.

Provides high-level API for SCIP-based code navigation:
- go_to_definition: Find where a symbol is defined
- find_references: Find all usages of a symbol
- find_implementations: Find all implementations of a protocol
- search_symbols: Search symbols by name with filters
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from ..config import Settings
from .connection import get_connection
from .scip_queries import (
    find_implementations,
    find_references,
    go_to_definition,
    search_symbols,
)


class QueryService:
    """Service for executing code navigation queries.
    
    Reads from external indexer SQLite databases to provide
    IDE-like navigation features.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def go_to_definition(
        self,
        symbol: str,
        file_path: Optional[str] = None,
        line: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        """Find the definition of a symbol.
        
        Args:
            symbol: Symbol name to find (e.g., "FeaturePresenter")
            file_path: Optional file context for disambiguation
            line: Optional line number context
            
        Returns:
            Definition information or None if not found.
        """
        with get_connection(self.settings.db_path) as conn:
            return go_to_definition(conn, symbol, file_path, line)

    def find_references(
        self,
        symbol: str,
        include_definitions: bool = False,
        limit: int = 50,
    ) -> Dict[str, Any]:
        """Find all references to a symbol.
        
        Args:
            symbol: Symbol name to find references for
            include_definitions: If True, also include definition occurrences
            limit: Maximum number of references to return
            
        Returns:
            Reference information with locations and context.
        """
        with get_connection(self.settings.db_path) as conn:
            return find_references(conn, symbol, include_definitions, limit)

    def find_implementations(
        self,
        protocol: str,
        limit: int = 50,
    ) -> Dict[str, Any]:
        """Find all implementations of a protocol/interface.
        
        Args:
            protocol: Protocol/interface name
            limit: Maximum implementations to return
            
        Returns:
            Implementation information with locations.
        """
        with get_connection(self.settings.db_path) as conn:
            return find_implementations(conn, protocol, limit)

    def search_symbols(
        self,
        query: str,
        kind: Optional[str] = None,
        module: Optional[str] = None,
        limit: int = 25,
    ) -> List[Dict[str, Any]]:
        """Search for symbols by name.
        
        Args:
            query: Search query (supports * wildcards)
            kind: Optional kind filter (class, struct, protocol, etc.)
            module: Optional module filter
            limit: Maximum results
            
        Returns:
            List of matching symbols.
        """
        with get_connection(self.settings.db_path) as conn:
            return search_symbols(conn, query, kind, module, limit)
