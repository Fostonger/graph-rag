"""SCIP-based code navigation queries for external indexer database.

These queries implement IDE-like navigation features:
- go_to_definition: Find where a symbol is defined
- find_references: Find all usages of a symbol
- find_implementations: Find all implementations of a protocol/interface

The database schema is produced by external indexers and uses:
- documents: file information with relative_path
- symbols: symbol definitions with symbol_id (name extracted from ID)
- occurrences: symbol usages with file_id FK to documents
- relationships: symbol relationships (conforms, inherits, overrides)
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional

from .schema import SymbolRole


# =============================================================================
# Symbol ID Parsing Utilities
# =============================================================================

def _extract_name_from_symbol_id(symbol_id: str) -> str:
    """Extract the human-readable name from a SCIP symbol ID.
    
    SCIP symbol IDs follow a specific format:
    - Local symbols: "local N" where N is a number
    - Package symbols: "scheme manager package descriptor"
    
    Examples:
    - "swift MyModule MyClass#" -> "MyClass"
    - "swift MyModule MyClass#myMethod()." -> "myMethod"
    - "local 42" -> "local_42"
    """
    if symbol_id.startswith("local "):
        return f"local_{symbol_id[6:]}"
    
    # Split by common SCIP delimiters
    parts = symbol_id.replace("#", " ").replace(".", " ").replace("()", " ").split()
    if parts:
        # Return the last meaningful part (usually the name)
        return parts[-1].strip("`.") or symbol_id
    
    return symbol_id


def _extract_module_from_symbol_id(symbol_id: str) -> Optional[str]:
    """Extract the module name from a SCIP symbol ID.
    
    For Swift, the format is typically: "swift ModuleName TypeName#member."
    """
    if symbol_id.startswith("local "):
        return None
    
    parts = symbol_id.split()
    if len(parts) >= 2:
        # Second part is typically the module
        return parts[1] if parts[1] not in {"#", "."} else None
    
    return None


# =============================================================================
# Main Query Functions
# =============================================================================

def go_to_definition(
    conn: sqlite3.Connection,
    symbol: str,
    file_path: Optional[str] = None,
    line: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    """Find the definition of a symbol.
    
    Args:
        conn: SQLite connection
        symbol: Symbol name to find (e.g., "FeaturePresenter")
        file_path: Optional file context to disambiguate (for overloaded names)
        line: Optional line context to find the specific symbol at that location
        
    Returns:
        Definition information with file location, snippet, and symbol metadata.
        Returns None if symbol not found.
    """
    # If we have file and line context, try to find the specific symbol at that location
    if file_path and line:
        row = conn.execute(
            """
            SELECT o.symbol_id
            FROM occurrences o
            JOIN documents d ON d.id = o.file_id
            WHERE d.relative_path = ?
              AND o.start_line <= ? AND o.end_line >= ?
            ORDER BY 
                CASE WHEN o.roles & ? != 0 THEN 0 ELSE 1 END,
                o.start_line DESC
            LIMIT 1
            """,
            (file_path, line, line, SymbolRole.REFERENCE),
        ).fetchone()
        
        if row:
            symbol_id = row["symbol_id"]
            return _get_definition_by_symbol_id(conn, symbol_id)
    
    # Search by name (extracted from symbol_id)
    return _get_definition_by_name(conn, symbol)


def _get_definition_by_name(
    conn: sqlite3.Connection,
    name: str,
) -> Optional[Dict[str, Any]]:
    """Find definition by symbol name."""
    # Find matching symbols by searching symbol_id patterns
    # The name appears as the last component in the symbol_id
    rows = conn.execute(
        """
        SELECT s.symbol_id, s.kind, s.documentation
        FROM symbols s
        WHERE s.symbol_id LIKE ? OR s.symbol_id LIKE ?
        ORDER BY 
            CASE WHEN s.symbol_id LIKE ? THEN 0 ELSE 1 END
        LIMIT 20
        """,
        (f"%{name}#", f"%{name}%", f"%{name}#"),
    ).fetchall()
    
    if not rows:
        return None
    
    # Find the definition occurrence for the best matching symbol
    for row in rows:
        symbol_id = row["symbol_id"]
        result = _get_definition_by_symbol_id(conn, symbol_id)
        if result:
            return result
    
    # No definition found, return symbol info without location
    row = rows[0]
    symbol_name = _extract_name_from_symbol_id(row["symbol_id"])
    module = _extract_module_from_symbol_id(row["symbol_id"])
    return {
        "symbol": symbol_name,
        "kind": row["kind"],
        "module": module,
        "definition": None,
        "documentation": row["documentation"],
    }


def _get_definition_by_symbol_id(
    conn: sqlite3.Connection,
    symbol_id: str,
) -> Optional[Dict[str, Any]]:
    """Get definition info for a specific symbol ID."""
    # Get symbol info
    symbol_row = conn.execute(
        """
        SELECT symbol_id, kind, documentation
        FROM symbols
        WHERE symbol_id = ?
        """,
        (symbol_id,),
    ).fetchone()
    
    if not symbol_row:
        return None
    
    symbol_name = _extract_name_from_symbol_id(symbol_row["symbol_id"])
    module = _extract_module_from_symbol_id(symbol_row["symbol_id"])
    
    # Find definition occurrence (JOIN with documents for file path)
    def_row = conn.execute(
        """
        SELECT d.relative_path, o.start_line, o.start_column, o.end_line, o.end_column, o.snippet
        FROM occurrences o
        JOIN documents d ON d.id = o.file_id
        WHERE o.symbol_id = ? AND o.roles & ? != 0
        ORDER BY o.start_line
        LIMIT 1
        """,
        (symbol_id, SymbolRole.DEFINITION),
    ).fetchone()
    
    # Get inheritance/conformance relationships
    inherits = []
    conformances = []
    rel_rows = conn.execute(
        """
        SELECT r.kind, r.target_symbol_id
        FROM relationships r
        WHERE r.symbol_id = ?
        """,
        (symbol_id,),
    ).fetchall()
    
    for rel in rel_rows:
        target_name = _extract_name_from_symbol_id(rel["target_symbol_id"])
        if rel["kind"] == "inherits":
            inherits.append(target_name)
        elif rel["kind"] in ("conforms", "implements"):
            conformances.append(target_name)
    
    # Get members (symbols that reference this as enclosing)
    member_rows = conn.execute(
        """
        SELECT DISTINCT o.symbol_id, s.kind
        FROM occurrences o
        JOIN symbols s ON s.symbol_id = o.symbol_id
        WHERE o.enclosing_symbol = ? AND o.roles & ? != 0
        ORDER BY o.symbol_id
        """,
        (symbol_id, SymbolRole.DEFINITION),
    ).fetchall()
    
    members = [
        _format_member(_extract_name_from_symbol_id(r["symbol_id"]), r["kind"]) 
        for r in member_rows
    ]
    
    result: Dict[str, Any] = {
        "symbol": symbol_name,
        "kind": symbol_row["kind"],
        "module": module,
    }
    
    if def_row:
        result["definition"] = {
            "file": def_row["relative_path"],
            "line": def_row["start_line"],
            "column": def_row["start_column"],
            "snippet": def_row["snippet"],
        }
    
    if inherits:
        result["inherits"] = inherits
    if conformances:
        result["conformances"] = conformances
    if members:
        result["members"] = members
    if symbol_row["documentation"]:
        result["documentation"] = symbol_row["documentation"]
    
    return result


def find_references(
    conn: sqlite3.Connection,
    symbol: str,
    include_definitions: bool = False,
    limit: int = 50,
) -> Dict[str, Any]:
    """Find all references to a symbol.
    
    Args:
        conn: SQLite connection
        symbol: Symbol name to find references for
        include_definitions: If True, also include definition occurrences
        limit: Maximum number of references to return
        
    Returns:
        Reference information with file locations and snippets.
    """
    # Find matching symbols by searching symbol_id patterns
    symbol_rows = conn.execute(
        """
        SELECT symbol_id, kind
        FROM symbols
        WHERE symbol_id LIKE ? OR symbol_id LIKE ?
        ORDER BY CASE WHEN symbol_id LIKE ? THEN 0 ELSE 1 END
        """,
        (f"%{symbol}#", f"%{symbol}%", f"%{symbol}#"),
    ).fetchall()
    
    if not symbol_rows:
        return {
            "symbol": symbol,
            "reference_count": 0,
            "references": [],
        }
    
    # Use the best matching symbol
    symbol_id = symbol_rows[0]["symbol_id"]
    symbol_name = _extract_name_from_symbol_id(symbol_id)
    
    # Build role filter
    if include_definitions:
        role_filter = "1=1"  # All roles
    else:
        role_filter = f"o.roles & {SymbolRole.DEFINITION} = 0"  # Exclude definitions
    
    # Get references (JOIN with documents for file path)
    ref_rows = conn.execute(
        f"""
        SELECT 
            d.relative_path AS file_path, 
            o.start_line, 
            o.start_column, 
            o.snippet,
            o.enclosing_symbol,
            o.roles
        FROM occurrences o
        JOIN documents d ON d.id = o.file_id
        WHERE o.symbol_id = ? AND {role_filter}
        ORDER BY d.relative_path, o.start_line
        LIMIT ?
        """,
        (symbol_id, limit),
    ).fetchall()
    
    # Count total references
    count_row = conn.execute(
        f"""
        SELECT COUNT(*) as cnt
        FROM occurrences o
        WHERE o.symbol_id = ? AND {role_filter}
        """,
        (symbol_id,),
    ).fetchone()
    
    total_count = count_row["cnt"] if count_row else 0
    
    # Group by module for summary
    module = _extract_module_from_symbol_id(symbol_id)
    module_counts: Dict[str, int] = {}
    references = []
    for row in ref_rows:
        mod = module or "Unknown"
        module_counts[mod] = module_counts.get(mod, 0) + 1
        
        ref: Dict[str, Any] = {
            "file": row["file_path"],
            "line": row["start_line"],
            "column": row["start_column"],
        }
        if row["snippet"]:
            ref["snippet"] = row["snippet"]
        if row["enclosing_symbol"]:
            ref["context"] = _extract_name_from_symbol_id(row["enclosing_symbol"])
        
        references.append(ref)
    
    result: Dict[str, Any] = {
        "symbol": symbol_name,
        "reference_count": total_count,
        "references": references,
    }
    
    if module_counts:
        result["grouped_by_module"] = module_counts
    
    return result


def find_implementations(
    conn: sqlite3.Connection,
    protocol: str,
    limit: int = 50,
) -> Dict[str, Any]:
    """Find all implementations of a protocol/interface.
    
    Args:
        conn: SQLite connection
        protocol: Protocol/interface name to find implementations for
        limit: Maximum number of implementations to return
        
    Returns:
        Implementation information with locations and member lists.
    """
    # Find the protocol symbol
    protocol_rows = conn.execute(
        """
        SELECT symbol_id, kind
        FROM symbols
        WHERE symbol_id LIKE ? OR symbol_id LIKE ?
        ORDER BY 
            CASE WHEN symbol_id LIKE ? THEN 0 ELSE 1 END,
            CASE WHEN kind = 'protocol' THEN 0 ELSE 1 END
        """,
        (f"%{protocol}#", f"%{protocol}%", f"%{protocol}#"),
    ).fetchall()
    
    if not protocol_rows:
        return {
            "protocol": protocol,
            "implementation_count": 0,
            "implementations": [],
        }
    
    protocol_id = protocol_rows[0]["symbol_id"]
    protocol_name = _extract_name_from_symbol_id(protocol_id)
    
    # Find implementations (symbols that conform to / implement this protocol)
    impl_rows = conn.execute(
        """
        SELECT DISTINCT 
            s.symbol_id,
            s.kind,
            r.kind AS relationship_kind
        FROM relationships r
        JOIN symbols s ON s.symbol_id = r.symbol_id
        WHERE r.target_symbol_id = ?
          AND r.kind IN ('conforms', 'implements', 'inherits')
        ORDER BY s.symbol_id
        LIMIT ?
        """,
        (protocol_id, limit),
    ).fetchall()
    
    implementations = []
    for row in impl_rows:
        impl_symbol_id = row["symbol_id"]
        impl_name = _extract_name_from_symbol_id(impl_symbol_id)
        impl_module = _extract_module_from_symbol_id(impl_symbol_id)
        
        # Get definition location (JOIN with documents)
        def_row = conn.execute(
            """
            SELECT d.relative_path, o.start_line, o.snippet
            FROM occurrences o
            JOIN documents d ON d.id = o.file_id
            WHERE o.symbol_id = ? AND o.roles & ? != 0
            LIMIT 1
            """,
            (impl_symbol_id, SymbolRole.DEFINITION),
        ).fetchone()
        
        # Get implemented members
        member_rows = conn.execute(
            """
            SELECT DISTINCT o.symbol_id, s.kind
            FROM occurrences o
            JOIN symbols s ON s.symbol_id = o.symbol_id
            WHERE o.enclosing_symbol = ? AND o.roles & ? != 0
            ORDER BY o.symbol_id
            """,
            (impl_symbol_id, SymbolRole.DEFINITION),
        ).fetchall()
        
        impl: Dict[str, Any] = {
            "name": impl_name,
            "kind": row["kind"],
            "module": impl_module,
        }
        
        if def_row:
            impl["file"] = def_row["relative_path"]
            impl["line"] = def_row["start_line"]
            if def_row["snippet"]:
                impl["snippet"] = def_row["snippet"]
        
        if member_rows:
            impl["members_implemented"] = [
                _format_member(_extract_name_from_symbol_id(m["symbol_id"]), m["kind"]) 
                for m in member_rows
            ]
        
        implementations.append(impl)
    
    return {
        "protocol": protocol_name,
        "implementation_count": len(implementations),
        "implementations": implementations,
    }


def search_symbols(
    conn: sqlite3.Connection,
    query: str,
    kind: Optional[str] = None,
    module: Optional[str] = None,
    limit: int = 25,
) -> List[Dict[str, Any]]:
    """Search for symbols by name, optionally filtered by kind and module.
    
    Args:
        conn: SQLite connection
        query: Search query (supports wildcards with *)
        kind: Optional kind filter (class, struct, protocol, function, etc.)
        module: Optional module filter
        limit: Maximum results to return
        
    Returns:
        List of matching symbols with their definition locations.
    """
    # Build query conditions
    conditions = []
    params: List[Any] = []
    
    # Name search (supports wildcards) - search in symbol_id
    if "*" in query:
        pattern = query.replace("*", "%")
        conditions.append("s.symbol_id LIKE ?")
        params.append(f"%{pattern}%")
    else:
        conditions.append("(s.symbol_id LIKE ? OR s.symbol_id LIKE ?)")
        params.extend([f"%{query}#", f"%{query}%"])
    
    if kind:
        conditions.append("s.kind = ?")
        params.append(kind)
    
    if module:
        # Module is part of symbol_id pattern
        conditions.append("s.symbol_id LIKE ?")
        params.append(f"% {module} %")
    
    where_clause = " AND ".join(conditions)
    
    rows = conn.execute(
        f"""
        SELECT 
            s.symbol_id,
            s.kind,
            s.documentation,
            d.relative_path AS file_path,
            o.start_line,
            o.snippet
        FROM symbols s
        LEFT JOIN occurrences o ON o.symbol_id = s.symbol_id AND o.roles & ? != 0
        LEFT JOIN documents d ON d.id = o.file_id
        WHERE {where_clause}
        ORDER BY 
            CASE WHEN s.symbol_id LIKE ? THEN 0 ELSE 1 END,
            s.symbol_id
        LIMIT ?
        """,
        [SymbolRole.DEFINITION] + params + [f"%{query}#", limit],
    ).fetchall()
    
    results = []
    seen_symbols = set()
    
    for row in rows:
        if row["symbol_id"] in seen_symbols:
            continue
        seen_symbols.add(row["symbol_id"])
        
        symbol_name = _extract_name_from_symbol_id(row["symbol_id"])
        symbol_module = _extract_module_from_symbol_id(row["symbol_id"])
        
        result: Dict[str, Any] = {
            "name": symbol_name,
            "kind": row["kind"],
            "module": symbol_module,
        }
        
        if row["file_path"]:
            result["file"] = row["file_path"]
            result["line"] = row["start_line"]
        
        if row["snippet"]:
            result["snippet"] = row["snippet"]
        
        results.append(result)
    
    return results


def _format_member(name: str, kind: Optional[str]) -> str:
    """Format a member name for display."""
    if kind == "function":
        return f"{name}()"
    return name
