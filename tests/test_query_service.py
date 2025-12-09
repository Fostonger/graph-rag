"""Tests for QueryService."""

from pathlib import Path

import pytest

from graphrag.config import Settings
from graphrag.db.query_service import QueryService
from graphrag.db.schema import SchemaError

from conftest import create_external_indexer_db, seed_test_data


def _settings(db_path: Path) -> Settings:
    return Settings(db_path=db_path)


class TestQueryServiceNavigation:
    """Tests for SCIP-based navigation methods."""

    def test_go_to_definition(self, tmp_path: Path):
        """Test go_to_definition finds symbol definitions."""
        db_path = tmp_path / "test.db"
        conn = create_external_indexer_db(db_path)
        seed_test_data(conn)
        conn.close()

        service = QueryService(_settings(db_path))
        result = service.go_to_definition("MyClass")

        assert result is not None
        assert result["symbol"] == "MyClass"
        assert result["kind"] == "class"
        assert result["definition"]["file"] == "Sources/MyClass.swift"

    def test_go_to_definition_not_found(self, tmp_path: Path):
        """Test go_to_definition returns None for unknown symbols."""
        db_path = tmp_path / "test.db"
        conn = create_external_indexer_db(db_path)
        seed_test_data(conn)
        conn.close()

        service = QueryService(_settings(db_path))
        result = service.go_to_definition("NonExistent")

        assert result is None

    def test_find_references(self, tmp_path: Path):
        """Test find_references finds symbol usages."""
        db_path = tmp_path / "test.db"
        conn = create_external_indexer_db(db_path)
        seed_test_data(conn)
        conn.close()

        service = QueryService(_settings(db_path))
        result = service.find_references("MyClass")

        assert result["symbol"] == "MyClass"
        assert result["reference_count"] >= 1
        assert len(result["references"]) >= 1

    def test_find_implementations(self, tmp_path: Path):
        """Test find_implementations finds protocol implementers."""
        db_path = tmp_path / "test.db"
        conn = create_external_indexer_db(db_path)
        seed_test_data(conn)
        conn.close()

        service = QueryService(_settings(db_path))
        result = service.find_implementations("IMyProtocol")

        assert result["protocol"] == "IMyProtocol"
        assert result["implementation_count"] == 2
        impl_names = {i["name"] for i in result["implementations"]}
        assert "MyClass" in impl_names
        assert "MockMyClass" in impl_names

    def test_search_symbols(self, tmp_path: Path):
        """Test search_symbols finds matching symbols."""
        db_path = tmp_path / "test.db"
        conn = create_external_indexer_db(db_path)
        seed_test_data(conn)
        conn.close()

        service = QueryService(_settings(db_path))
        results = service.search_symbols("My*")

        names = {r["name"] for r in results}
        assert "MyClass" in names

    def test_search_symbols_with_filters(self, tmp_path: Path):
        """Test search_symbols applies kind filter."""
        db_path = tmp_path / "test.db"
        conn = create_external_indexer_db(db_path)
        seed_test_data(conn)
        conn.close()

        service = QueryService(_settings(db_path))
        results = service.search_symbols("*", kind="protocol")

        assert all(r["kind"] == "protocol" for r in results)


class TestQueryServiceErrors:
    """Tests for error handling."""

    def test_raises_for_missing_database(self, tmp_path: Path):
        """Test that missing database raises FileNotFoundError."""
        db_path = tmp_path / "nonexistent.db"
        service = QueryService(_settings(db_path))

        with pytest.raises(FileNotFoundError):
            service.go_to_definition("anything")

    def test_raises_for_invalid_schema(self, tmp_path: Path):
        """Test that invalid schema raises SchemaError."""
        import sqlite3
        
        db_path = tmp_path / "invalid.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE metadata (key TEXT, value TEXT)")
        conn.close()

        service = QueryService(_settings(db_path))

        with pytest.raises(SchemaError):
            service.go_to_definition("anything")
