from __future__ import annotations

import sqlite3
import json
from collections import defaultdict, deque
from typing import Any, Dict, Iterable, List, Optional, Tuple


LIKE_ESCAPE_CHAR = "\\"


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
    return [dict(row) for row in rows]


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
) -> dict:
    direction = (direction or "both").lower()
    if direction not in {"upstream", "downstream", "both"}:
        raise ValueError("direction must be one of upstream, downstream, both")
    entities = _merge_entities(
        _load_entities(master_conn, "master"),
        _load_entities(feature_conn, "feature") if feature_conn else {},
    )
    relationships = _merge_relationships(
        _load_relationships(master_conn, "master"),
        _load_relationships(feature_conn, "feature") if feature_conn else [],
    )
    start_node = _pick_entity_by_name(entities, entity_name)
    if not start_node:
        raise ValueError(f"Entity '{entity_name}' was not found in indexed metadata")
    stop_node = _pick_entity_by_name(entities, stop_name) if stop_name else None
    graph_payload = _build_graph_payload(
        entities=entities,
        relationships=relationships,
        start_node=start_node,
        stop_node=stop_node,
        direction=direction,
        include_siblings=include_sibling_subgraphs,
    )
    return graph_payload


def _load_entities(conn: sqlite3.Connection, origin: str) -> Dict[str, dict]:
    if conn is None:
        return {}
    rows = conn.execute(
        """
        WITH latest AS (
            SELECT entity_id, MAX(commit_id) AS commit_id
            FROM entity_versions
            WHERE is_deleted = 0
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
            (
                SELECT GROUP_CONCAT(m.name, '|')
                FROM members m
                WHERE m.entity_id = e.id
            ) AS member_names
        FROM latest
        JOIN entity_versions ev
            ON ev.entity_id = latest.entity_id
           AND ev.commit_id = latest.commit_id
        JOIN entities e ON e.id = latest.entity_id
        LEFT JOIN files f ON f.id = ev.file_id
        JOIN commits ON commits.id = ev.commit_id
        """
    ).fetchall()
    entities: Dict[str, dict] = {}
    for row in rows:
        member_names = (
            row["member_names"].split("|") if row["member_names"] else []
        )
        entities[row["stable_id"]] = {
            "entity_id": int(row["id"]),
            "stable_id": row["stable_id"],
            "name": row["name"],
            "kind": row["kind"],
            "module": row["module"],
            "file_path": row["file_path"],
            "signature": row["signature"],
            "commit_hash": row["commit_hash"],
            "member_names": member_names,
            "origin": origin,
        }
    return entities


def _load_relationships(
    conn: sqlite3.Connection, origin: str
) -> List[Dict[str, Any]]:
    if conn is None:
        return []
    rows = conn.execute(
        """
        WITH ranked AS (
            SELECT
                er.*,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        er.source_entity_id,
                        COALESCE(er.target_entity_id, -1),
                        er.target_name,
                        COALESCE(er.target_module, ''),
                        er.edge_type
                    ORDER BY er.commit_id DESC, er.id DESC
                ) AS rn
            FROM entity_relationships er
        )
        SELECT
            ranked.source_entity_id,
            ranked.target_entity_id,
            ranked.target_name,
            ranked.target_module,
            ranked.edge_type,
            ranked.metadata,
            src.stable_id AS source_stable_id,
            src.name AS source_name,
            tgt.stable_id AS target_stable_id,
            tgt.name AS target_entity_name
        FROM ranked
        JOIN entities src ON src.id = ranked.source_entity_id
        LEFT JOIN entities tgt ON tgt.id = ranked.target_entity_id
        WHERE ranked.rn = 1
          AND ranked.is_deleted = 0
        """
    ).fetchall()
    relationships: List[Dict[str, Any]] = []
    for row in rows:
        metadata = json.loads(row["metadata"]) if row["metadata"] else {}
        relationships.append(
            {
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
        )
    return relationships


def _merge_entities(master: Dict[str, dict], feature: Dict[str, dict]) -> Dict[str, dict]:
    merged: Dict[str, dict] = {}
    merged.update(master)
    merged.update(feature)
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
) -> List[Dict[str, Any]]:
    merged: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    for rel in master:
        merged[_relationship_key(rel)] = rel
    for rel in feature:
        merged[_relationship_key(rel)] = rel
    return list(merged.values())


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
                reference_edges,
                display_nodes,
                entities,
                edges_payload,
                edge_keys,
                nodes_included,
                stop_id,
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
                reference_edges,
                focus_nodes,
                entities,
                edges_payload,
                edge_keys,
                nodes_included,
                stop_id,
            )

    if direction in {"upstream", "both"} and not include_siblings:
        # ensure ancestor nodes stay visible even when no edges were added
        for node_id in focus_nodes:
            if node_id != stop_id:
                nodes_included.add(node_id)

    if start_id not in nodes_included and (not stop_id or start_id != stop_id):
        nodes_included.add(start_id)

    node_payloads = [
        _serialize_node(entities[sid])
        for sid in sorted(
            nodes_included,
            key=lambda stable_id: entities[stable_id]["name"],
        )
        if sid in entities
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
    reference_edges: List[Dict[str, Any]],
    focus_nodes: set[str],
    entities: Dict[str, dict],
    edges: List[dict],
    edge_keys: set[Tuple[Any, ...]],
    nodes_included: set[str],
    stop_id: Optional[str],
) -> None:
    for rel in reference_edges:
        source_id = rel["source_stable_id"]
        target_id = rel.get("target_stable_id")
        if not (
            (source_id and source_id in focus_nodes)
            or (target_id and target_id in focus_nodes)
        ):
            continue
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
    reference_edges: List[Dict[str, Any]],
    display_nodes: set[str],
    entities: Dict[str, dict],
    edges: List[dict],
    edge_keys: set[Tuple[Any, ...]],
    nodes_included: set[str],
    stop_id: Optional[str],
) -> None:
    incoming: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for rel in reference_edges:
        target_id = rel.get("target_stable_id")
        if target_id:
            incoming[target_id].append(rel)

    queue: deque[str] = deque([start_id])
    visited: set[str] = set()
    while queue:
        node_id = queue.popleft()
        if node_id in visited:
            continue
        visited.add(node_id)
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
                queue.append(target_id)
        for rel in incoming.get(node_id, []):
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
                queue.append(source_id)


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


def _entity_label(
    entities: Dict[str, dict], stable_id: Optional[str], fallback: Optional[str]
) -> str:
    if stable_id and stable_id in entities:
        return entities[stable_id]["name"]
    return fallback or "<unknown>"


def _serialize_node(node: dict) -> dict:
    return {
        "name": node["name"],
        "stable_id": node["stable_id"],
        "module": node.get("module"),
        "kind": node.get("kind"),
        "file_path": node.get("file_path"),
        "signature": node.get("signature"),
        "members": node.get("member_names", []),
        "origin": node.get("origin"),
    }


def _summarize_node(node: dict) -> dict:
    return {
        "name": node["name"],
        "module": node.get("module"),
        "kind": node.get("kind"),
        "stable_id": node.get("stable_id"),
    }

