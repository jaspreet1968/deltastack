"""DuckDB persistence layer – auto-creates tables on first use."""

from deltastack.db.connection import get_db, ensure_tables

__all__ = ["get_db", "ensure_tables"]
