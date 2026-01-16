"""
simpledb/db.py

Public Database API for the SimpleDB mini-RDBMS.

Responsibilities:
- Provide a simple library interface:
    - Database.open(path)
    - db.execute(sql) -> CommandOk | QueryResult
    - db.execute_script(sql_script) -> list[CommandOk|QueryResult]
- Load/persist the schema catalog
- Maintain an index cache shared across executions (performance + fewer disk reads)

This module is intentionally minimal so it can be used from:
- the REPL (repl.py)
- a trivial demo web app (e.g., finance tracker)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from .catalog import Catalog
from .exec.executor import Executor
from .index.hash_index import HashIndex
from .parser import parse_script, parse_sql


@dataclass
class Database:
    """
    Database instance bound to a root directory on disk.

    Attributes:
        root_dir: DB root folder on disk.
        catalog: Loaded schema catalog.
        index_cache: Cache of opened HashIndex objects.
    """
    root_dir: Path
    catalog: Catalog
    index_cache: dict[str, HashIndex] = field(default_factory=dict)

    @classmethod
    def open(cls, path: str | Path) -> "Database":
        """
        Open (or create) a database at a directory path.

        Args:
            path: Directory path (string or Path). If it doesn't exist, it is created.

        Returns:
            Database instance.
        """
        root = Path(path)
        root.mkdir(parents=True, exist_ok=True)
        catalog = Catalog.load(root)
        return cls(root_dir=root, catalog=catalog)

    def execute(self, sql: str):
        """
        Execute a single SQL statement.

        Args:
            sql: SQL string containing exactly one statement (semicolon optional).

        Returns:
            CommandOk for non-SELECT statements, or QueryResult for SELECT.

        Raises:
            SqlSyntaxError: on parse errors.
            ExecutionError / ConstraintError: on execution failure.
        """
        stmt = parse_sql(sql)
        ex = Executor(db_dir=self.root_dir, catalog=self.catalog, index_cache=self.index_cache)
        return ex.execute(stmt)

    def execute_script(self, sql: str):
        """
        Execute a script containing one or more semicolon-separated SQL statements.

        Args:
            sql: SQL script string.

        Returns:
            List of results in statement order.
        """
        stmts = parse_script(sql)
        ex = Executor(db_dir=self.root_dir, catalog=self.catalog, index_cache=self.index_cache)
        return [ex.execute(s) for s in stmts]