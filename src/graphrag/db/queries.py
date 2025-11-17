from __future__ import annotations

import sqlite3
from typing import Iterable, List


def find_entities(
    conn: sqlite3.Connection,
    needle: str,
    limit: int = 25,
    include_code: bool = False,
) -> List[dict]:
    like = f"%{needle}%" if needle else "%"
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
        WHERE (e.name LIKE :q OR e.module LIKE :q OR f.path LIKE :q)
        ORDER BY e.name
        LIMIT :limit
        """,
        {"q": like, "limit": limit},
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

