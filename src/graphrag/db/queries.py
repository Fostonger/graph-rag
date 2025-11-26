from __future__ import annotations

import sqlite3
import json
from collections import defaultdict, deque
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple


LIKE_ESCAPE_CHAR = "\\"
STRUCTURAL_EDGE_TYPES = {"superclass", "conforms"}


def _split_query_terms(raw: str) -> List[str]:
    if not raw:
        return []
    if "," in raw:
        pieces = raw.split(",")
    else:
        pieces = raw.split()
    return [piece.strip() for piece in pieces if piece.strip()]


def _escape_like_pattern(pattern: str) -> str:
    escaped = pattern.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return escaped.replace("*", "%")


def _normalize_patterns(needle: str) -> List[Tuple[str, str]]:
    """
    Return a list of (kind, value) pairs where kind is 'exact' or 'like'.
    """
    terms = _split_query_terms(needle)
    if not terms:
        return [("like", "%")]
    normalized: List[Tuple[str, str]] = []
    for term in terms:
        if "*" in term:
            normalized.append(("like", _escape_like_pattern(term.lower())))
        else:
            normalized.append(("exact", term.lower()))
    return normalized


def find_entities(
    conn: sqlite3.Connection,
    needle: str,
    limit: int = 25,
    include_code: bool = False,
) -> List[dict]:
    patterns = _normalize_patterns(needle)
    params: Dict[str, Any] = {"limit": limit}
    where_clauses: List[str] = []
    for idx, (kind, value) in enumerate(patterns):
        key = f"p{idx}"
        if kind == "exact":
            params[key] = value
            where_clauses.append(f"LOWER(e.name) = :{key}")
        else:
            params[key] = value
            clause = " OR ".join(
                [
                    f"LOWER(e.name) LIKE :{key} ESCAPE '{LIKE_ESCAPE_CHAR}'",
                    f"LOWER(e.module) LIKE :{key} ESCAPE '{LIKE_ESCAPE_CHAR}'",
                    f"LOWER(f.path) LIKE :{key} ESCAPE '{LIKE_ESCAPE_CHAR}'",
                ]
            )
            where_clauses.append(f"({clause})")
    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " OR ".join(where_clauses)

    rows = conn.execute(
        f"""
        WITH latest AS (
            SELECT entity_id, MAX(commit_id) AS commit_id
            FROM entity_versions
            WHERE is_deleted = 0
            GROUP BY entity_id
        )
        SELECT
            e.name,
            e.kind,
            e.module,
            f.path AS file_path,
            ev.start_line,
            ev.end_line,
            ev.signature,
            ev.docstring,
            ev.properties,
            {"ev.code," if include_code else "NULL AS code,"}
            commits.hash AS commit_hash
        FROM latest
        JOIN entity_versions ev
            ON ev.entity_id = latest.entity_id
           AND ev.commit_id = latest.commit_id
        JOIN entities e ON e.id = latest.entity_id
        LEFT JOIN files f ON f.id = ev.file_id
        JOIN commits ON commits.id = ev.commit_id
        {where_sql}
        ORDER BY e.name
        LIMIT :limit
        """,
        params,
    ).fetchall()
    entities: List[dict] = []
    for row in rows:
        payload = dict(row)
        props_raw = payload.pop("properties", None)
        if props_raw:
            try:
                props = json.loads(props_raw)
            except json.JSONDecodeError:
                props = {}
            payload["target_type"] = props.get("target_type")
        else:
            payload["target_type"] = None
        entities.append(payload)
    return entities


def get_members(
    conn: sqlite3.Connection,
    entity_names: Iterable[str],
    member_filters: Iterable[str],
    include_code: bool = True,
) -> List[dict]:
    names = list(entity_names)
    if not names:
        return []

    member_terms = [f"%{m}%" for m in member_filters if m]
    where_member = ""
    params: dict[str, object] = {}
    if member_terms:
        clauses = []
        for idx, term in enumerate(member_terms):
            key = f"m{idx}"
            clauses.append(f"m.name LIKE :{key}")
            params[key] = term
        where_member = "AND (" + " OR ".join(clauses) + ")"

    entity_clause = ", ".join([f":e{idx}" for idx in range(len(names))])
    for idx, name in enumerate(names):
        params[f"e{idx}"] = name

    rows = conn.execute(
        f"""
        WITH latest AS (
            SELECT member_id, MAX(commit_id) AS commit_id
            FROM member_versions
            WHERE is_deleted = 0
            GROUP BY member_id
        )
        SELECT
            e.name AS entity_name,
            m.name AS member_name,
            m.kind AS member_kind,
            mv.start_line,
            mv.end_line,
            mv.signature,
            {"mv.code," if include_code else "NULL AS code,"}
            f.path AS file_path,
            commits.hash AS commit_hash
        FROM latest
        JOIN member_versions mv
            ON mv.member_id = latest.member_id
           AND mv.commit_id = latest.commit_id
        JOIN members m ON m.id = latest.member_id
        JOIN entities e ON e.id = m.entity_id
        LEFT JOIN files f ON f.id = mv.file_id
        JOIN commits ON commits.id = mv.commit_id
        WHERE e.name IN ({entity_clause})
        {where_member}
        ORDER BY e.name, m.name
        """,
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def get_entity_graph(
    master_conn: sqlite3.Connection,
    feature_conn: Optional[sqlite3.Connection],
    entity_name: str,
    stop_name: Optional[str] = None,
    direction: str = "both",
    include_sibling_subgraphs: bool = False,
    max_hops: Optional[int] = None,
    target_type: str = "app",
    use_fast_path: bool = False,
) -> dict:
    """Build a graph centered on the specified entity.
    
    Args:
        master_conn: Connection to master database
        feature_conn: Optional connection to feature database for overlay
        entity_name: Name of the entity to center the graph on
        stop_name: Optional entity name to stop traversal at
        direction: Graph traversal direction (upstream, downstream, both)
        include_sibling_subgraphs: Include sibling entity subgraphs
        max_hops: Maximum number of hops from start entity
        target_type: Filter by target type (app, test, all)
        use_fast_path: Use materialized views for faster loading (master only)
    
    Returns:
        Graph payload with nodes and edges
    """
    direction = (direction or "both").lower()
    if direction not in {"upstream", "downstream", "both"}:
        raise ValueError("direction must be one of upstream, downstream, both")
    
    # Load entities and relationships
    if use_fast_path and feature_conn is None:
        # Fast path: use materialized views (master only, no feature overlay)
        master_entities = _load_entities_fast(master_conn, "master")
        master_deleted: Set[str] = set()  # No tombstones in fast path
        master_rels = _load_relationships_fast(master_conn, "master")
        master_rel_deleted: Set[Tuple[Any, ...]] = set()
    else:
        # Standard path: use versioned tables
        master_entities, master_deleted = _load_entities(master_conn, "master")
        master_rels, master_rel_deleted = _load_relationships(master_conn, "master")
    
    feature_entities, feature_deleted = (
        _load_entities(feature_conn, "feature") if feature_conn else ({}, set())
    )
    entities = _merge_entities(
        master_entities, feature_entities, master_deleted, feature_deleted
    )
    
    feature_rels, feature_rel_deleted = (
        _load_relationships(feature_conn, "feature") if feature_conn else ([], set())
    )
    relationships = _merge_relationships(
        master_rels,
        feature_rels,
        master_rel_deleted,
        feature_rel_deleted,
    )
    deleted_stable_ids = master_deleted.union(feature_deleted)
    relationships = _prune_relationships_for_deleted_entities(
        relationships, deleted_stable_ids
    )
    start_node = _pick_entity_by_name(entities, entity_name)
    if not start_node:
        raise ValueError(f"Entity '{entity_name}' was not found in indexed metadata")
    stop_node = _pick_entity_by_name(entities, stop_name) if stop_name else None
    target_type = (target_type or "app").lower()
    if target_type not in {"app", "test", "all"}:
        raise ValueError("targetType must be one of app, test, all")
    if not _matches_target_filter(start_node, target_type):
        raise ValueError(
            f"Entity '{entity_name}' does not belong to targetType '{target_type}'"
        )
    entities, relationships = _filter_by_target_type(
        entities, relationships, target_type
    )
    start_node = entities[start_node["stable_id"]]
    stop_node = _pick_entity_by_name(entities, stop_name) if stop_name else None
    graph_payload = _build_graph_payload(
        entities=entities,
        relationships=relationships,
        start_node=start_node,
        stop_node=stop_node,
        direction=direction,
        include_siblings=include_sibling_subgraphs,
        max_hops=max_hops,
    )
    graph_payload["target_type_filter"] = target_type
    graph_payload["max_hops"] = max_hops
    return graph_payload


def _load_entities_fast(conn: sqlite3.Connection, origin: str) -> Dict[str, dict]:
    """Load entities from materialized entity_latest table.
    
    This is significantly faster than _load_entities() as it reads from
    a pre-computed denormalized table with no joins or aggregations.
    
    Note: Returns only entities dict, no tombstones (materialized view
    only contains non-deleted entities).
    """
    if conn is None:
        return {}
    
    rows = conn.execute(
        """
        SELECT
            stable_id,
            entity_id,
            name,
            kind,
            module,
            file_path,
            signature,
            properties,
            member_names,
            target_type,
            visibility,
            commit_hash
        FROM entity_latest
        """
    ).fetchall()
    
    entities: Dict[str, dict] = {}
    for row in rows:
        member_names = row["member_names"].split("|") if row["member_names"] else []
        entities[row["stable_id"]] = {
            "entity_id": int(row["entity_id"]),
            "stable_id": row["stable_id"],
            "name": row["name"],
            "kind": row["kind"],
            "module": row["module"],
            "file_path": row["file_path"],
            "signature": row["signature"],
            "commit_hash": row["commit_hash"],
            "member_names": member_names,
            "origin": origin,
            "target_type": row["target_type"],
            "visibility": row["visibility"],
            "extensions": [],  # Will be populated by _load_extensions_fast
        }
    
    # Load extensions and attach to entities
    extensions = _load_extensions_fast(conn, origin)
    for ext in extensions:
        entity_stable_id = ext.get("entity_stable_id")
        if entity_stable_id and entity_stable_id in entities:
            entities[entity_stable_id]["extensions"].append(ext)
    
    return entities


def _load_extensions_fast(conn: sqlite3.Connection, origin: str) -> List[Dict[str, Any]]:
    """Load extensions from materialized extension_latest table."""
    if conn is None:
        return []
    
    try:
        rows = conn.execute(
            """
            SELECT
                stable_id,
                extension_id,
                entity_id,
                entity_stable_id,
                extended_type,
                module,
                file_path,
                signature,
                visibility,
                constraints,
                conformances,
                member_names,
                target_type,
                commit_hash
            FROM extension_latest
            """
        ).fetchall()
    except sqlite3.OperationalError:
        # Table doesn't exist or has old schema - return empty list
        return []
    
    extensions: List[Dict[str, Any]] = []
    for row in rows:
        member_names = row["member_names"].split("|") if row["member_names"] else []
        conformances = json.loads(row["conformances"]) if row["conformances"] else []
        extensions.append({
            "stable_id": row["stable_id"],
            "entity_stable_id": row["entity_stable_id"],
            "extended_type": row["extended_type"],
            "module": row["module"],
            "file_path": row["file_path"],
            "signature": row["signature"],
            "visibility": row["visibility"],
            "constraints": row["constraints"],
            "conformances": conformances,
            "member_names": member_names,
            "target_type": row["target_type"],
            "commit_hash": row["commit_hash"],
            "origin": origin,
        })
    return extensions


def _load_relationships_fast(
    conn: sqlite3.Connection, origin: str
) -> List[Dict[str, Any]]:
    """Load relationships from materialized relationship_latest table.
    
    This is significantly faster than _load_relationships() as it reads from
    a pre-computed denormalized table with no complex window functions.
    
    Note: Returns only relationships list, no tombstones (materialized view
    only contains non-deleted relationships).
    """
    if conn is None:
        return []
    
    rows = conn.execute(
        """
        SELECT
            source_stable_id,
            source_name,
            target_stable_id,
            target_name,
            target_module,
            edge_type,
            metadata
        FROM relationship_latest
        """
    ).fetchall()
    
    relationships: List[Dict[str, Any]] = []
    for row in rows:
        metadata = json.loads(row["metadata"]) if row["metadata"] else {}
        relationships.append({
            "source_entity_id": None,  # Not needed for graph building
            "target_entity_id": None,
            "target_name": row["target_name"],
            "target_module": row["target_module"],
            "edge_type": row["edge_type"],
            "metadata": metadata,
            "source_stable_id": row["source_stable_id"],
            "source_name": row["source_name"],
            "target_stable_id": row["target_stable_id"],
            "target_entity_name": row["target_name"],
            "origin": origin,
        })
    return relationships


# ============================================================================
# Lazy Loading Functions - On-demand entity/relationship loading
# ============================================================================


def get_entity_graph_lazy(
    master_conn: sqlite3.Connection,
    feature_conn: Optional[sqlite3.Connection],
    entity_name: str,
    stop_name: Optional[str] = None,
    direction: str = "both",
    max_hops: Optional[int] = None,
    target_type: str = "app",
) -> dict:
    """Build a graph using lazy loading - only fetches reachable entities.
    
    This function traverses the graph on-demand, loading only the entities
    and relationships that are reachable from the start entity. This is
    significantly faster for large databases when the resulting graph is small.
    
    Args:
        master_conn: Connection to master database
        feature_conn: Optional connection to feature database
        entity_name: Name of the entity to center the graph on
        stop_name: Optional entity name to stop traversal at
        direction: Graph traversal direction (upstream, downstream, both)
        max_hops: Maximum number of hops from start entity
        target_type: Filter by target type (app, test, all)
    
    Returns:
        Graph payload with nodes and edges
    
    Note:
        This function does not support include_sibling_subgraphs as the lazy
        approach loads only directly reachable entities.
    """
    direction = (direction or "both").lower()
    if direction not in {"upstream", "downstream", "both"}:
        raise ValueError("direction must be one of upstream, downstream, both")
    
    target_type = (target_type or "app").lower()
    if target_type not in {"app", "test", "all"}:
        raise ValueError("targetType must be one of app, test, all")
    
    # Find the starting entity
    start_node = _load_single_entity_by_name(master_conn, entity_name, "master")
    if not start_node and feature_conn:
        start_node = _load_single_entity_by_name(feature_conn, entity_name, "feature")
    
    if not start_node:
        raise ValueError(f"Entity '{entity_name}' was not found in indexed metadata")
    
    if not _matches_target_filter(start_node, target_type):
        raise ValueError(
            f"Entity '{entity_name}' does not belong to targetType '{target_type}'"
        )
    
    # Find stop node if specified
    stop_node = None
    stop_id = None
    if stop_name:
        stop_node = _load_single_entity_by_name(master_conn, stop_name, "master")
        if not stop_node and feature_conn:
            stop_node = _load_single_entity_by_name(feature_conn, stop_name, "feature")
        if stop_node:
            stop_id = stop_node["stable_id"]
    
    # Lazy traversal
    visited_entities: Dict[str, dict] = {start_node["stable_id"]: start_node}
    visited_edge_keys: Set[Tuple[Any, ...]] = set()
    edges_payload: List[dict] = []
    
    # BFS queue: (stable_id, depth, direction_tag)
    queue: deque[Tuple[str, int, str]] = deque([(start_node["stable_id"], 0, "start")])
    
    while queue:
        current_id, depth, dir_tag = queue.popleft()
        
        if max_hops is not None and depth >= max_hops:
            continue
        
        if current_id == stop_id:
            continue
        
        # Load relationships for this entity
        rels = _load_relationships_for_entity(
            master_conn, feature_conn, current_id, direction
        )
        
        for rel in rels:
            # Skip if doesn't match target type
            target_id = rel.get("target_stable_id")
            source_id = rel["source_stable_id"]
            
            # Build edge key for deduplication
            edge_key = (
                source_id,
                target_id,
                rel["target_name"],
                rel["edge_type"],
            )
            if edge_key in visited_edge_keys:
                continue
            visited_edge_keys.add(edge_key)
            
            # Add edge to payload
            metadata = dict(rel.get("metadata") or {})
            metadata["origin"] = rel.get("origin", "master")
            edge = {
                "type": rel["edge_type"],
                "source": rel.get("source_name") or source_id,
                "target": rel.get("target_entity_name") or rel["target_name"],
                "metadata": metadata,
            }
            edges_payload.append(edge)
            
            # Load and queue target entity if not visited
            if target_id and target_id not in visited_entities and target_id != stop_id:
                target_entity = _load_single_entity_by_stable_id(
                    master_conn, feature_conn, target_id
                )
                if target_entity and _matches_target_filter(target_entity, target_type):
                    visited_entities[target_id] = target_entity
                    queue.append((target_id, depth + 1, "downstream"))
            
            # For upstream/both, also follow incoming edges
            if direction in {"upstream", "both"} and source_id not in visited_entities:
                if source_id != stop_id:
                    source_entity = _load_single_entity_by_stable_id(
                        master_conn, feature_conn, source_id
                    )
                    if source_entity and _matches_target_filter(source_entity, target_type):
                        visited_entities[source_id] = source_entity
                        queue.append((source_id, depth + 1, "upstream"))
    
    # Build node payloads
    node_payloads = [
        _serialize_node(entity)
        for entity in sorted(
            visited_entities.values(),
            key=lambda e: e["name"],
        )
        if entity["stable_id"] != stop_id
    ]
    
    return {
        "entity": _summarize_node(start_node),
        "stop_at": stop_node["name"] if stop_node else None,
        "direction": direction,
        "include_sibling_subgraphs": False,
        "edges": edges_payload,
        "nodes": node_payloads,
        "target_type_filter": target_type,
        "max_hops": max_hops,
    }


def _load_single_entity_by_name(
    conn: sqlite3.Connection, name: str, origin: str
) -> Optional[dict]:
    """Load a single entity by name from entity_latest table."""
    if conn is None:
        return None
    
    row = conn.execute(
        """
        SELECT
            stable_id,
            entity_id,
            name,
            kind,
            module,
            file_path,
            signature,
            properties,
            member_names,
            target_type,
            visibility,
            commit_hash
        FROM entity_latest
        WHERE name = ?
        LIMIT 1
        """,
        (name,),
    ).fetchone()
    
    if not row:
        return None
    
    member_names = row["member_names"].split("|") if row["member_names"] else []
    stable_id = row["stable_id"]
    
    # Load extensions for this entity
    extensions = _load_extensions_for_entity(conn, stable_id, origin)
    
    return {
        "entity_id": int(row["entity_id"]),
        "stable_id": stable_id,
        "name": row["name"],
        "kind": row["kind"],
        "module": row["module"],
        "file_path": row["file_path"],
        "signature": row["signature"],
        "commit_hash": row["commit_hash"],
        "member_names": member_names,
        "origin": origin,
        "target_type": row["target_type"],
        "visibility": row["visibility"],
        "extensions": extensions,
    }


def _load_extensions_for_entity(
    conn: sqlite3.Connection, entity_stable_id: str, origin: str
) -> List[Dict[str, Any]]:
    """Load extensions for a single entity from extension_latest table."""
    if conn is None:
        return []
    
    try:
        rows = conn.execute(
            """
            SELECT
                stable_id,
                file_path,
                signature,
                visibility,
                constraints,
                conformances,
                member_names,
                target_type,
                commit_hash
            FROM extension_latest
            WHERE entity_stable_id = ?
            """,
            (entity_stable_id,),
        ).fetchall()
    except sqlite3.OperationalError:
        # Table doesn't exist or has old schema - return empty list
        return []
    
    extensions: List[Dict[str, Any]] = []
    for row in rows:
        member_names = row["member_names"].split("|") if row["member_names"] else []
        conformances = json.loads(row["conformances"]) if row["conformances"] else []
        extensions.append({
            "stable_id": row["stable_id"],
            "file_path": row["file_path"],
            "signature": row["signature"],
            "visibility": row["visibility"],
            "constraints": row["constraints"],
            "conformances": conformances,
            "member_names": member_names,
            "target_type": row["target_type"],
            "commit_hash": row["commit_hash"],
            "origin": origin,
        })
    return extensions


def _load_single_entity_by_stable_id(
    master_conn: sqlite3.Connection,
    feature_conn: Optional[sqlite3.Connection],
    stable_id: str,
) -> Optional[dict]:
    """Load a single entity by stable_id, checking feature first."""
    # Check feature first (overlay)
    if feature_conn:
        row = feature_conn.execute(
            """
            SELECT
                stable_id,
                entity_id,
                name,
                kind,
                module,
                file_path,
                signature,
                properties,
                member_names,
                target_type,
                visibility,
                commit_hash
            FROM entity_latest
            WHERE stable_id = ?
            """,
            (stable_id,),
        ).fetchone()
        if row:
            member_names = row["member_names"].split("|") if row["member_names"] else []
            extensions = _load_extensions_for_entity(feature_conn, stable_id, "feature")
            return {
                "entity_id": int(row["entity_id"]),
                "stable_id": row["stable_id"],
                "name": row["name"],
                "kind": row["kind"],
                "module": row["module"],
                "file_path": row["file_path"],
                "signature": row["signature"],
                "commit_hash": row["commit_hash"],
                "member_names": member_names,
                "origin": "feature",
                "target_type": row["target_type"],
                "visibility": row["visibility"],
                "extensions": extensions,
            }
    
    # Fall back to master
    if master_conn:
        row = master_conn.execute(
            """
            SELECT
                stable_id,
                entity_id,
                name,
                kind,
                module,
                file_path,
                signature,
                properties,
                member_names,
                target_type,
                visibility,
                commit_hash
            FROM entity_latest
            WHERE stable_id = ?
            """,
            (stable_id,),
        ).fetchone()
        if row:
            member_names = row["member_names"].split("|") if row["member_names"] else []
            extensions = _load_extensions_for_entity(master_conn, stable_id, "master")
            return {
                "entity_id": int(row["entity_id"]),
                "stable_id": row["stable_id"],
                "name": row["name"],
                "kind": row["kind"],
                "module": row["module"],
                "file_path": row["file_path"],
                "signature": row["signature"],
                "commit_hash": row["commit_hash"],
                "member_names": member_names,
                "origin": "master",
                "target_type": row["target_type"],
                "visibility": row["visibility"],
                "extensions": extensions,
            }
    
    return None


def _load_relationships_for_entity(
    master_conn: sqlite3.Connection,
    feature_conn: Optional[sqlite3.Connection],
    stable_id: str,
    direction: str,
) -> List[Dict[str, Any]]:
    """Load relationships for a single entity based on direction.
    
    Args:
        master_conn: Master database connection
        feature_conn: Optional feature database connection
        stable_id: Entity stable_id to load relationships for
        direction: 'upstream' (incoming), 'downstream' (outgoing), or 'both'
    
    Returns:
        List of relationship dictionaries
    """
    relationships: List[Dict[str, Any]] = []
    seen_keys: Set[Tuple[Any, ...]] = set()
    
    def add_rels_from_conn(conn: sqlite3.Connection, origin: str) -> None:
        if conn is None:
            return
        
        # Build query based on direction
        if direction == "downstream":
            query = """
                SELECT
                    source_stable_id,
                    source_name,
                    target_stable_id,
                    target_name,
                    target_module,
                    edge_type,
                    metadata
                FROM relationship_latest
                WHERE source_stable_id = ?
            """
            params = (stable_id,)
        elif direction == "upstream":
            query = """
                SELECT
                    source_stable_id,
                    source_name,
                    target_stable_id,
                    target_name,
                    target_module,
                    edge_type,
                    metadata
                FROM relationship_latest
                WHERE target_stable_id = ?
            """
            params = (stable_id,)
        else:  # both
            query = """
                SELECT
                    source_stable_id,
                    source_name,
                    target_stable_id,
                    target_name,
                    target_module,
                    edge_type,
                    metadata
                FROM relationship_latest
                WHERE source_stable_id = ? OR target_stable_id = ?
            """
            params = (stable_id, stable_id)
        
        rows = conn.execute(query, params).fetchall()
        
        for row in rows:
            key = (
                row["source_stable_id"],
                row["target_stable_id"],
                row["target_name"],
                row["edge_type"],
            )
            if key in seen_keys:
                continue
            seen_keys.add(key)
            
            metadata = json.loads(row["metadata"]) if row["metadata"] else {}
            relationships.append({
                "source_stable_id": row["source_stable_id"],
                "source_name": row["source_name"],
                "target_stable_id": row["target_stable_id"],
                "target_name": row["target_name"],
                "target_module": row["target_module"],
                "target_entity_name": row["target_name"],
                "edge_type": row["edge_type"],
                "metadata": metadata,
                "origin": origin,
            })
    
    # Feature relationships overlay master
    if feature_conn:
        add_rels_from_conn(feature_conn, "feature")
    add_rels_from_conn(master_conn, "master")
    
    return relationships


def _load_entities(conn: sqlite3.Connection, origin: str) -> Tuple[Dict[str, dict], Set[str]]:
    """Load all entities with their latest version state.
    
    Optimized to use 2 queries instead of N+1 pattern:
    1. Load entities without member names
    2. Batch load all member names and join in Python
    """
    if conn is None:
        return {}, set()
    
    # Query 1: Load all entities without the correlated subquery
    rows = conn.execute(
        """
        WITH latest AS (
            SELECT entity_id, MAX(commit_id) AS commit_id
            FROM entity_versions
            GROUP BY entity_id
        )
        SELECT
            e.id,
            e.stable_id,
            e.name,
            e.kind,
            e.module,
            f.path AS file_path,
            ev.signature,
            commits.hash AS commit_hash,
            ev.is_deleted,
            ev.properties
        FROM latest
        JOIN entity_versions ev
            ON ev.entity_id = latest.entity_id
           AND ev.commit_id = latest.commit_id
        JOIN entities e ON e.id = latest.entity_id
        LEFT JOIN files f ON f.id = ev.file_id
        JOIN commits ON commits.id = ev.commit_id
        """
    ).fetchall()
    
    # Query 2: Batch load all member names in a single query
    member_rows = conn.execute(
        """
        SELECT entity_id, GROUP_CONCAT(name, '|') AS names
        FROM members
        GROUP BY entity_id
        """
    ).fetchall()
    
    # Build member name lookup map
    member_map: Dict[int, List[str]] = {}
    for row in member_rows:
        entity_id = int(row["entity_id"])
        names_str = row["names"]
        member_map[entity_id] = names_str.split("|") if names_str else []
    
    # Build entities dict
    entities: Dict[str, dict] = {}
    tombstones: Set[str] = set()
    for row in rows:
        if row["is_deleted"]:
            tombstones.add(row["stable_id"])
            continue
        props = json.loads(row["properties"]) if row["properties"] else {}
        entity_id = int(row["id"])
        entities[row["stable_id"]] = {
            "entity_id": entity_id,
            "stable_id": row["stable_id"],
            "name": row["name"],
            "kind": row["kind"],
            "module": row["module"],
            "file_path": row["file_path"],
            "signature": row["signature"],
            "commit_hash": row["commit_hash"],
            "member_names": member_map.get(entity_id, []),
            "origin": origin,
            "target_type": props.get("target_type"),
            "visibility": props.get("visibility"),
            "extensions": [],  # Will be populated below
        }
    
    # Load extensions and attach to entities
    extensions = _load_extensions(conn, origin)
    for ext in extensions:
        entity_stable_id = ext.get("entity_stable_id")
        if entity_stable_id and entity_stable_id in entities:
            entities[entity_stable_id]["extensions"].append(ext)
    
    return entities, tombstones


def _load_extensions(conn: sqlite3.Connection, origin: str) -> List[Dict[str, Any]]:
    """Load all extensions with their latest version state."""
    if conn is None:
        return []
    
    try:
        rows = conn.execute(
            """
            WITH latest AS (
                SELECT extension_id, MAX(commit_id) AS commit_id
                FROM extension_versions
                GROUP BY extension_id
            )
            SELECT
                ext.stable_id,
                ext.entity_id,
                e.stable_id AS entity_stable_id,
                ext.extended_type,
                ext.module,
                f.path AS file_path,
                ev.signature,
                ev.visibility,
                ev.constraints,
                ev.conformances,
                ev.properties,
                commits.hash AS commit_hash,
                ev.is_deleted
            FROM latest
            JOIN extension_versions ev
                ON ev.extension_id = latest.extension_id
               AND ev.commit_id = latest.commit_id
            JOIN extensions ext ON ext.id = latest.extension_id
            JOIN entities e ON e.id = ext.entity_id
            LEFT JOIN files f ON f.id = ev.file_id
            JOIN commits ON commits.id = ev.commit_id
            WHERE ev.is_deleted = 0
            """
        ).fetchall()
    except sqlite3.OperationalError:
        # Table doesn't exist or has old schema - return empty list
        return []
    
    extensions: List[Dict[str, Any]] = []
    for row in rows:
        props = json.loads(row["properties"]) if row["properties"] else {}
        conformances = json.loads(row["conformances"]) if row["conformances"] else []
        extensions.append({
            "stable_id": row["stable_id"],
            "entity_stable_id": row["entity_stable_id"],
            "extended_type": row["extended_type"],
            "module": row["module"],
            "file_path": row["file_path"],
            "signature": row["signature"],
            "visibility": row["visibility"],
            "constraints": row["constraints"],
            "conformances": conformances,
            "member_names": [],  # Could be loaded separately if needed
            "target_type": props.get("target_type"),
            "commit_hash": row["commit_hash"],
            "origin": origin,
        })
    return extensions


def _load_relationships(
    conn: sqlite3.Connection, origin: str
) -> Tuple[List[Dict[str, Any]], Set[Tuple[Any, ...]]]:
    """Load all relationships with their latest version state.
    
    Optimized to use MAX aggregation instead of ROW_NUMBER() window function.
    The MAX approach leverages indexes better and avoids full table sorting.
    """
    if conn is None:
        return [], set()
    
    # Use MAX to find the latest version of each unique relationship
    # This is more efficient than ROW_NUMBER() as it can use indexes
    rows = conn.execute(
        """
        WITH latest AS (
            SELECT
                source_entity_id,
                COALESCE(target_entity_id, -1) AS target_entity_id_key,
                target_name,
                COALESCE(target_module, '') AS target_module_key,
                edge_type,
                MAX(commit_id) AS max_commit_id
            FROM entity_relationships
            GROUP BY
                source_entity_id,
                COALESCE(target_entity_id, -1),
                target_name,
                COALESCE(target_module, ''),
                edge_type
        ),
        latest_with_id AS (
            SELECT
                er.id,
                er.source_entity_id,
                er.target_entity_id,
                er.target_name,
                er.target_module,
                er.edge_type,
                er.metadata,
                er.is_deleted
            FROM latest
            JOIN entity_relationships er ON
                er.source_entity_id = latest.source_entity_id
                AND COALESCE(er.target_entity_id, -1) = latest.target_entity_id_key
                AND er.target_name = latest.target_name
                AND COALESCE(er.target_module, '') = latest.target_module_key
                AND er.edge_type = latest.edge_type
                AND er.commit_id = latest.max_commit_id
        ),
        deduplicated AS (
            SELECT
                source_entity_id,
                target_entity_id,
                target_name,
                target_module,
                edge_type,
                metadata,
                is_deleted,
                MAX(id) AS id
            FROM latest_with_id
            GROUP BY
                source_entity_id,
                target_entity_id,
                target_name,
                target_module,
                edge_type
        )
        SELECT
            d.source_entity_id,
            d.target_entity_id,
            d.target_name,
            d.target_module,
            d.edge_type,
            lwi.metadata,
            lwi.is_deleted,
            src.stable_id AS source_stable_id,
            src.name AS source_name,
            tgt.stable_id AS target_stable_id,
            tgt.name AS target_entity_name
        FROM deduplicated d
        JOIN latest_with_id lwi ON lwi.id = d.id
        JOIN entities src ON src.id = d.source_entity_id
        LEFT JOIN entities tgt ON tgt.id = d.target_entity_id
        """
    ).fetchall()
    
    relationships: List[Dict[str, Any]] = []
    tombstones: Set[Tuple[Any, ...]] = set()
    for row in rows:
        metadata = json.loads(row["metadata"]) if row["metadata"] else {}
        rel = {
            "source_entity_id": row["source_entity_id"],
            "target_entity_id": row["target_entity_id"],
            "target_name": row["target_name"],
            "target_module": row["target_module"],
            "edge_type": row["edge_type"],
            "metadata": metadata,
            "source_stable_id": row["source_stable_id"],
            "source_name": row["source_name"],
            "target_stable_id": row["target_stable_id"],
            "target_entity_name": row["target_entity_name"],
            "origin": origin,
        }
        key = _relationship_key(rel)
        if row["is_deleted"]:
            tombstones.add(key)
            continue
        relationships.append(rel)
    return relationships, tombstones


def _merge_entities(
    master: Dict[str, dict],
    feature: Dict[str, dict],
    master_deleted: Set[str],
    feature_deleted: Set[str],
) -> Dict[str, dict]:
    merged: Dict[str, dict] = {
        stable_id: entity
        for stable_id, entity in master.items()
        if stable_id not in master_deleted
    }
    for stable_id, entity in feature.items():
        if stable_id in feature_deleted:
            continue
        merged[stable_id] = entity
    for stable_id in feature_deleted:
        merged.pop(stable_id, None)
    return merged


def _relationship_key(rel: Dict[str, Any]) -> Tuple[Any, ...]:
    return (
        rel["source_stable_id"],
        rel.get("target_stable_id"),
        rel["target_name"],
        rel.get("target_module"),
        rel["edge_type"],
    )


def _merge_relationships(
    master: List[Dict[str, Any]],
    feature: List[Dict[str, Any]],
    master_deleted: Set[Tuple[Any, ...]],
    feature_deleted: Set[Tuple[Any, ...]],
) -> List[Dict[str, Any]]:
    merged: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    for rel in master:
        key = _relationship_key(rel)
        if key in master_deleted:
            continue
        merged[key] = rel
    for rel in feature:
        key = _relationship_key(rel)
        if key in feature_deleted:
            continue
        merged[key] = rel
    for key in feature_deleted:
        merged.pop(key, None)
    return list(merged.values())


def _prune_relationships_for_deleted_entities(
    relationships: List[Dict[str, Any]], deleted_nodes: Set[str]
) -> List[Dict[str, Any]]:
    if not deleted_nodes:
        return relationships
    pruned: List[Dict[str, Any]] = []
    for rel in relationships:
        if rel["source_stable_id"] in deleted_nodes:
            continue
        target_id = rel.get("target_stable_id")
        if target_id and target_id in deleted_nodes:
            continue
        pruned.append(rel)
    return pruned


def _filter_by_target_type(
    entities: Dict[str, dict],
    relationships: List[Dict[str, Any]],
    filter_type: str,
) -> Tuple[Dict[str, dict], List[Dict[str, Any]]]:
    if filter_type == "all":
        return entities, relationships
    filtered_entities: Dict[str, dict] = {}
    removed: Set[str] = set()
    for stable_id, entity in entities.items():
        if _matches_target_filter(entity, filter_type):
            filtered_entities[stable_id] = entity
        else:
            removed.add(stable_id)
    if not removed:
        return filtered_entities, relationships
    filtered_relationships: List[Dict[str, Any]] = []
    for rel in relationships:
        if rel["source_stable_id"] in removed:
            continue
        target_id = rel.get("target_stable_id")
        if target_id and target_id in removed:
            continue
        filtered_relationships.append(rel)
    return filtered_entities, filtered_relationships


def _matches_target_filter(entity: Optional[dict], filter_type: str) -> bool:
    if filter_type == "all":
        return True
    if entity is None:
        return True
    entity_type = entity.get("target_type") or "app"
    if filter_type == "test":
        return entity_type == "test"
    return entity_type != "test"


def _pick_entity_by_name(entities: Dict[str, dict], name: Optional[str]) -> Optional[dict]:
    if not name:
        return None
    matches = [node for node in entities.values() if node["name"] == name]
    if not matches:
        return None
    matches.sort(
        key=lambda node: (
            0 if node["origin"] == "feature" else 1,
            node.get("module") or "",
            node["stable_id"],
        )
    )
    return matches[0]


def _build_graph_payload(
    entities: Dict[str, dict],
    relationships: List[Dict[str, Any]],
    start_node: dict,
    stop_node: Optional[dict],
    direction: str,
    include_siblings: bool,
    max_hops: Optional[int],
) -> dict:
    start_id = start_node["stable_id"]
    stop_id = stop_node["stable_id"] if stop_node else None
    nodes_included: set[str] = set()
    edges_payload: List[dict] = []
    edge_keys: set[Tuple[Any, ...]] = set()
    (
        creates_by_child,
        creates_by_parent,
        refs_outgoing,
        reference_edges,
    ) = _categorize_relationships(relationships)

    incoming_refs: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for rel in reference_edges:
        target_id = rel.get("target_stable_id")
        if target_id:
            incoming_refs[target_id].append(rel)

    focus_nodes = _collect_focus_nodes(start_id, stop_id, creates_by_child)
    display_nodes = set(focus_nodes)
    for rel in reference_edges:
        source_id = rel["source_stable_id"]
        target_id = rel.get("target_stable_id")
        if (source_id and source_id in focus_nodes) or (
            target_id and target_id in focus_nodes
        ):
            if source_id:
                display_nodes.add(source_id)
            if target_id:
                display_nodes.add(target_id)

    nodes_included: set[str] = set()
    edge_keys: set[Tuple[Any, ...]] = set()
    edges_payload: List[dict] = []

    if include_siblings:
        if direction in {"downstream", "both"}:
            _append_reference_edges_full(
                start_id,
                refs_outgoing,
                incoming_refs,
                display_nodes,
                entities,
                edges_payload,
                edge_keys,
                nodes_included,
                stop_id,
                max_hops,
            )
        _attach_created_by_edges(
            display_nodes,
            creates_by_child,
            entities,
            edges_payload,
            edge_keys,
            nodes_included,
            stop_id,
        )
    else:
        _attach_created_by_edges(
            display_nodes,
            creates_by_child,
            entities,
            edges_payload,
            edge_keys,
            nodes_included,
            stop_id,
        )
        if direction in {"downstream", "both"}:
            _append_reference_edges_limited(
                refs_outgoing,
                incoming_refs,
                focus_nodes,
                entities,
                edges_payload,
                edge_keys,
                nodes_included,
                stop_id,
                max_hops,
            )

    if direction in {"upstream", "both"} and not include_siblings:
        # ensure ancestor nodes stay visible even when no edges were added
        for node_id in focus_nodes:
            if node_id != stop_id:
                nodes_included.add(node_id)

    if start_id not in nodes_included and (not stop_id or start_id != stop_id):
        nodes_included.add(start_id)

    _attach_structural_edges(
        reference_edges,
        nodes_included,
        entities,
        edges_payload,
        edge_keys,
        stop_id,
    )

    visible_nodes = [sid for sid in nodes_included if sid in entities]
    node_payloads = [
        _serialize_node(entities[sid])
        for sid in sorted(
            visible_nodes,
            key=lambda stable_id: entities[stable_id]["name"],
        )
    ]
    return {
        "entity": _summarize_node(start_node),
        "stop_at": stop_node["name"] if stop_node else None,
        "direction": direction,
        "include_sibling_subgraphs": include_siblings,
        "edges": edges_payload,
        "nodes": node_payloads,
    }


def _categorize_relationships(
    relationships: List[Dict[str, Any]]
) -> Tuple[
    Dict[str, List[Dict[str, Any]]],
    Dict[str, List[Dict[str, Any]]],
    Dict[str, List[Dict[str, Any]]],
    List[Dict[str, Any]],
]:
    creates_by_child: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    creates_by_parent: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    refs_outgoing: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    references: List[Dict[str, Any]] = []
    for rel in relationships:
        if rel["edge_type"] == "creates":
            parent_id = rel["source_stable_id"]
            child_id = rel.get("target_stable_id")
            creates_by_parent[parent_id].append(rel)
            if child_id:
                creates_by_child[child_id].append(rel)
        else:
            refs_outgoing[rel["source_stable_id"]].append(rel)
            references.append(rel)
    return creates_by_child, creates_by_parent, refs_outgoing, references


def _collect_focus_nodes(
    start_id: str,
    stop_id: Optional[str],
    creates_by_child: Dict[str, List[Dict[str, Any]]],
) -> set[str]:
    focus: set[str] = set()
    queue: deque[str] = deque([start_id])
    while queue:
        node_id = queue.popleft()
        if node_id in focus:
            continue
        focus.add(node_id)
        for rel in creates_by_child.get(node_id, []):
            parent_id = rel["source_stable_id"]
            if parent_id and parent_id not in focus:
                focus.add(parent_id)
                if not stop_id or parent_id != stop_id:
                    queue.append(parent_id)
    return focus


def _attach_created_by_edges(
    display_nodes: set[str],
    creates_by_child: Dict[str, List[Dict[str, Any]]],
    entities: Dict[str, dict],
    edges: List[dict],
    edge_keys: set[Tuple[Any, ...]],
    nodes_included: set[str],
    stop_id: Optional[str],
) -> None:
    for node_id in display_nodes:
        for rel in creates_by_child.get(node_id, []):
            _append_created_by_edge(
                rel,
                entities,
                edges,
                edge_keys,
                nodes_included,
                stop_id,
            )


def _append_reference_edges_limited(
    refs_outgoing: Dict[str, List[Dict[str, Any]]],
    incoming_refs: Dict[str, List[Dict[str, Any]]],
    focus_nodes: set[str],
    entities: Dict[str, dict],
    edges: List[dict],
    edge_keys: set[Tuple[Any, ...]],
    nodes_included: set[str],
    stop_id: Optional[str],
    max_hops: Optional[int],
) -> None:
    queue: deque[Tuple[str, int]] = deque([(node_id, 0) for node_id in focus_nodes])
    visited: set[str] = set()
    while queue:
        node_id, depth = queue.popleft()
        if not node_id or node_id in visited:
            continue
        visited.add(node_id)
        if max_hops is not None and depth >= max_hops:
            continue
        for rel in refs_outgoing.get(node_id, []):
            _append_reference_edge(
                rel,
                entities,
                edges,
                edge_keys,
                nodes_included,
                stop_id,
            )
        for rel in incoming_refs.get(node_id, []):
            _append_reference_edge(
                rel,
                entities,
                edges,
                edge_keys,
                nodes_included,
                stop_id,
            )


def _append_reference_edges_full(
    start_id: str,
    refs_outgoing: Dict[str, List[Dict[str, Any]]],
    incoming_refs: Dict[str, List[Dict[str, Any]]],
    display_nodes: set[str],
    entities: Dict[str, dict],
    edges: List[dict],
    edge_keys: set[Tuple[Any, ...]],
    nodes_included: set[str],
    stop_id: Optional[str],
    max_hops: Optional[int],
) -> None:
    queue: deque[Tuple[str, int]] = deque([(start_id, 0)])
    visited: set[str] = set()
    while queue:
        node_id, depth = queue.popleft()
        if node_id in visited:
            continue
        visited.add(node_id)
        if max_hops is not None and depth >= max_hops:
            continue
        for rel in refs_outgoing.get(node_id, []):
            _append_reference_edge(
                rel,
                entities,
                edges,
                edge_keys,
                nodes_included,
                stop_id,
            )
            target_id = rel.get("target_stable_id")
            if target_id and target_id not in visited:
                display_nodes.add(target_id)
                queue.append((target_id, depth + 1))
        for rel in incoming_refs.get(node_id, []):
            _append_reference_edge(
                rel,
                entities,
                edges,
                edge_keys,
                nodes_included,
                stop_id,
            )
            source_id = rel["source_stable_id"]
            if source_id and source_id not in visited:
                display_nodes.add(source_id)
                queue.append((source_id, depth + 1))


def _append_reference_edge(
    rel: Dict[str, Any],
    entities: Dict[str, dict],
    edges: List[dict],
    edge_keys: set,
    nodes: set[str],
    stop_id: Optional[str],
) -> None:
    key = (
        rel["source_stable_id"],
        rel.get("target_stable_id"),
        rel["target_name"],
        rel["edge_type"],
    )
    if key in edge_keys:
        return
    edge_keys.add(key)
    metadata = dict(rel.get("metadata") or {})
    metadata["origin"] = rel["origin"]
    edge = {
        "type": rel["edge_type"],
        "source": _entity_label(
            entities, rel["source_stable_id"], rel["source_name"]
        ),
        "target": _entity_label(
            entities,
            rel.get("target_stable_id"),
            rel.get("target_entity_name") or rel["target_name"],
        ),
        "metadata": metadata,
    }
    edges.append(edge)
    _add_node(nodes, rel["source_stable_id"], stop_id)
    target_id = rel.get("target_stable_id")
    if target_id:
        _add_node(nodes, target_id, stop_id)


def _append_created_by_edge(
    rel: Dict[str, Any],
    entities: Dict[str, dict],
    edges: List[dict],
    edge_keys: set,
    nodes: set[str],
    stop_id: Optional[str],
) -> None:
    child_id = rel.get("target_stable_id")
    key = (
        "createdBy",
        child_id or rel["target_name"],
        rel["source_stable_id"],
    )
    if key in edge_keys:
        return
    edge_keys.add(key)
    metadata = dict(rel.get("metadata") or {})
    metadata["origin"] = rel["origin"]
    metadata["creator"] = rel["source_name"]
    edge = {
        "type": "createdBy",
        "source": _entity_label(
            entities, child_id, rel.get("target_entity_name") or rel["target_name"]
        ),
        "target": _entity_label(entities, rel["source_stable_id"], rel["source_name"]),
        "metadata": metadata,
    }
    edges.append(edge)
    if child_id:
        _add_node(nodes, child_id, stop_id)
    _add_node(nodes, rel["source_stable_id"], stop_id)


def _add_node(nodes: set[str], stable_id: Optional[str], stop_id: Optional[str]) -> None:
    if not stable_id:
        return
    if stop_id and stable_id == stop_id:
        return
    nodes.add(stable_id)


def _attach_structural_edges(
    reference_edges: List[Dict[str, Any]],
    included_nodes: set[str],
    entities: Dict[str, dict],
    edges: List[dict],
    edge_keys: set,
    stop_id: Optional[str],
) -> None:
    for rel in reference_edges:
        if rel["edge_type"] not in STRUCTURAL_EDGE_TYPES:
            continue
        source_id = rel["source_stable_id"]
        if not source_id or source_id not in included_nodes:
            continue
        _append_reference_edge(
            rel,
            entities,
            edges,
            edge_keys,
            included_nodes,
            stop_id,
        )


def _entity_label(
    entities: Dict[str, dict], stable_id: Optional[str], fallback: Optional[str]
) -> str:
    if stable_id and stable_id in entities:
        return entities[stable_id]["name"]
    return fallback or "<unknown>"


def _serialize_node(node: dict) -> dict:
    result = {
        "name": node["name"],
        "stable_id": node["stable_id"],
        "module": node.get("module"),
        "kind": node.get("kind"),
        "target_type": node.get("target_type"),
        "visibility": node.get("visibility"),
        "file_path": node.get("file_path"),
        "signature": node.get("signature"),
        "members": node.get("member_names", []),
        "origin": node.get("origin"),
    }
    
    # Include extensions if present
    extensions = node.get("extensions", [])
    if extensions:
        result["extensions"] = [
            {
                "stable_id": ext.get("stable_id"),
                "visibility": ext.get("visibility"),
                "file_path": ext.get("file_path"),
                "signature": ext.get("signature"),
                "members": ext.get("member_names", []),
                "conformances": ext.get("conformances", []),
                "constraints": ext.get("constraints"),
                "origin": ext.get("origin"),
            }
            for ext in extensions
        ]
    
    return result


def _summarize_node(node: dict) -> dict:
    return {
        "name": node["name"],
        "module": node.get("module"),
        "kind": node.get("kind"),
        "stable_id": node.get("stable_id"),
    }

