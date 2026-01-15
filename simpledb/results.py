"""
simpledb/result.py

Result objects returned by Database.execute()/execute_script().

The engine returns one of:
- CommandOk: for statements that do not return rows (DDL, INSERT/UPDATE/DELETE)
- QueryResult: for SELECT statements

These are intentionally simple, serializable Python objects so they can be used
by both the REPL and a demo web app without extra dependencies.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class CommandOk:
    """
    Represents successful execution of a non-SELECT statement.

    Attributes:
        rows_affected: Number of logical rows affected (INSERT/UPDATE/DELETE).
        message: Human-readable status message.
    """
    rows_affected: int = 0
    message: str = "OK"


@dataclass(frozen=True)
class QueryResult:
    """
    Represents the output of a SELECT query.

    Attributes:
        columns: Output column names in order. For JOIN queries, columns are typically
                 qualified like 'table.column' to avoid ambiguity.
        rows: A list of rows; each row is a list of Python values aligned with `columns`.
        stats: Optional execution stats for debugging/teaching purposes (e.g., plan type,
               index used, join method).
    """
    columns: list[str]
    rows: list[list[Any]]
    stats: dict[str, Any] | None = None