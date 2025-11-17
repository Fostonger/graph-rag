from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any, Optional

from mcp.server import NotificationOptions, Server
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

from ..config import Settings, load_settings
from ..db import schema
from ..db.connection import get_connection
from ..db.queries import find_entities, get_members

server = Server("graphrag-mcp")
runtime_settings: Optional[Settings] = None


def _resolve_settings(
    config: Optional[Path],
    repo: Optional[Path],
    db: Optional[Path],
) -> Settings:
    settings = load_settings(config)
    if repo:
        settings.repo_path = Path(repo).expanduser().resolve()
    if db:
        settings.db_path = Path(db).expanduser().resolve()
    return settings


def _json_text(payload: Any) -> TextContent:
    return TextContent(type="text", text=json.dumps(payload, indent=2, ensure_ascii=False))


def _with_connection(func):
    if runtime_settings is None:
        raise RuntimeError("MCP server has not been initialized with settings.")
    with get_connection(runtime_settings.db_path) as conn:
        schema.apply_schema(conn)
        return func(conn)


@server.list_tools()
async def handle_list_tools() -> list[Tool]:
    return [
        Tool(
            name="find_entities",
            description="Look up entities by name/module/path and return metadata (optionally code).",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Entity name or fragment to search for.",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max number of entities to return.",
                        "default": 25,
                        "minimum": 1,
                        "maximum": 200,
                    },
                    "include_code": {
                        "type": "boolean",
                        "description": "If true, include entity code snippets.",
                        "default": False,
                    },
                },
                "required": ["query"],
            },
        ),
        Tool(
            name="get_members",
            description="Fetch member details for specific entities (functions/properties/etc).",
            inputSchema={
                "type": "object",
                "properties": {
                    "entities": {
                        "type": "array",
                        "items": {"type": "string"},
                        "minItems": 1,
                        "description": "List of entity names.",
                    },
                    "members": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Subset of member names to filter (optional).",
                    },
                    "include_code": {
                        "type": "boolean",
                        "description": "Include member implementation code.",
                        "default": True,
                    },
                },
                "required": ["entities"],
            },
        ),
    ]


@server.call_tool()
async def handle_call_tool(
    name: str, arguments: dict[str, Any]
) -> list[TextContent]:
    if name == "find_entities":
        query = arguments.get("query", "")
        limit = int(arguments.get("limit", 25))
        include_code = bool(arguments.get("include_code", False))

        def _run(conn):
            rows = find_entities(conn, query, limit=limit, include_code=include_code)
            return rows

        rows = _with_connection(_run)
        return [_json_text({"tool": name, "count": len(rows), "entities": rows})]

    if name == "get_members":
        entities = arguments.get("entities") or []
        members = arguments.get("members") or []
        include_code = bool(arguments.get("include_code", True))

        if not entities:
            raise ValueError("entities array cannot be empty")

        def _run(conn):
            rows = get_members(
                conn,
                entity_names=entities,
                member_filters=members,
                include_code=include_code,
            )
            return rows

        rows = _with_connection(_run)
        return [_json_text({"tool": name, "count": len(rows), "members": rows})]

    raise ValueError(f"Unknown tool: {name}")


async def _main(settings: Settings) -> None:
    global runtime_settings
    runtime_settings = settings
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="graphrag-mcp",
                server_version="0.1.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )


def run_server() -> None:
    parser = argparse.ArgumentParser(description="GraphRAG MCP server")
    parser.add_argument("--config", type=Path, help="Path to config.yaml")
    parser.add_argument("--repo", type=Path, help="Repository path override")
    parser.add_argument("--db", type=Path, help="Database path override")
    args = parser.parse_args()
    settings = _resolve_settings(args.config, args.repo, args.db)
    asyncio.run(_main(settings))

