"""In-memory caching layer for graph data.

This module provides a cache for entities and relationships loaded from
the database. The cache is designed to:
1. Speed up repeated graph queries against the same master database
2. Automatically invalidate when the underlying data changes (commit hash changes)
3. Support optional TTL-based expiration

Usage:
    cache = GraphCache(ttl_seconds=300)
    
    # Get cached or load fresh
    entities = cache.get_entities(conn, current_commit_hash)
    relationships = cache.get_relationships(conn, current_commit_hash)
    
    # Force refresh
    cache.invalidate()
"""

from __future__ import annotations

import time
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

from .queries import _load_entities_fast, _load_relationships_fast


class GraphCache:
    """In-memory cache for graph entities and relationships.
    
    The cache stores data loaded from materialized views and automatically
    invalidates when the commit hash changes, ensuring consistency.
    
    Attributes:
        ttl_seconds: Optional time-to-live in seconds. If set, cache expires
                     after this duration regardless of commit hash.
    """
    
    def __init__(self, ttl_seconds: Optional[int] = None):
        """Initialize the cache.
        
        Args:
            ttl_seconds: Optional TTL for cache entries. If None, cache only
                        invalidates on commit hash change or explicit invalidation.
        """
        self._ttl_seconds = ttl_seconds
        self._entities: Optional[Dict[str, dict]] = None
        self._relationships: Optional[List[Dict[str, Any]]] = None
        self._last_commit_hash: Optional[str] = None
        self._last_load_time: float = 0.0
        self._hits: int = 0
        self._misses: int = 0
    
    def get_entities(
        self, conn: sqlite3.Connection, commit_hash: str
    ) -> Dict[str, dict]:
        """Get entities from cache or load from database.
        
        Args:
            conn: Database connection (must have entity_latest table populated)
            commit_hash: Current commit hash for cache validation
        
        Returns:
            Dictionary mapping stable_id to entity data
        """
        if self._is_cache_valid(commit_hash):
            self._hits += 1
            return self._entities  # type: ignore
        
        self._misses += 1
        self._refresh_cache(conn, commit_hash)
        return self._entities  # type: ignore
    
    def get_relationships(
        self, conn: sqlite3.Connection, commit_hash: str
    ) -> List[Dict[str, Any]]:
        """Get relationships from cache or load from database.
        
        Args:
            conn: Database connection (must have relationship_latest table populated)
            commit_hash: Current commit hash for cache validation
        
        Returns:
            List of relationship dictionaries
        """
        if self._is_cache_valid(commit_hash):
            self._hits += 1
            return self._relationships  # type: ignore
        
        self._misses += 1
        self._refresh_cache(conn, commit_hash)
        return self._relationships  # type: ignore
    
    def invalidate(self) -> None:
        """Force cache invalidation.
        
        Call this after indexing to ensure the next query reloads data.
        """
        self._entities = None
        self._relationships = None
        self._last_commit_hash = None
        self._last_load_time = 0.0
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics.
        
        Returns:
            Dictionary with hit_count, miss_count, hit_rate, and cached info
        """
        total = self._hits + self._misses
        hit_rate = self._hits / total if total > 0 else 0.0
        return {
            "hit_count": self._hits,
            "miss_count": self._misses,
            "hit_rate": hit_rate,
            "is_populated": self._entities is not None,
            "last_commit_hash": self._last_commit_hash,
            "entity_count": len(self._entities) if self._entities else 0,
            "relationship_count": len(self._relationships) if self._relationships else 0,
        }
    
    def _is_cache_valid(self, commit_hash: str) -> bool:
        """Check if cache is valid for the given commit hash."""
        if self._entities is None or self._relationships is None:
            return False
        
        if self._last_commit_hash != commit_hash:
            return False
        
        if self._ttl_seconds is not None:
            age = time.time() - self._last_load_time
            if age > self._ttl_seconds:
                return False
        
        return True
    
    def _refresh_cache(self, conn: sqlite3.Connection, commit_hash: str) -> None:
        """Reload cache from database."""
        self._entities = _load_entities_fast(conn, "master")
        self._relationships = _load_relationships_fast(conn, "master")
        self._last_commit_hash = commit_hash
        self._last_load_time = time.time()


# Global cache instance for shared use across MCP server requests
_global_cache: Optional[GraphCache] = None


def get_global_cache(ttl_seconds: Optional[int] = 300) -> GraphCache:
    """Get or create the global cache instance.
    
    Args:
        ttl_seconds: TTL for cache entries (only used on first call)
    
    Returns:
        The global GraphCache instance
    """
    global _global_cache
    if _global_cache is None:
        _global_cache = GraphCache(ttl_seconds=ttl_seconds)
    return _global_cache


def reset_global_cache() -> None:
    """Reset the global cache instance.
    
    Useful for testing or when you need a fresh cache.
    """
    global _global_cache
    if _global_cache is not None:
        _global_cache.invalidate()
    _global_cache = None

