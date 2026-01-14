Step 9 — Interactive REPL (IMPLEMENTED)
Step Goal
Provide an interactive REPL mode to run your SQL-like commands in real time against your RDBMS library.

What was implemented
repl.py CLI program:
Opens a DB directory
Reads input (supports multiline until ; outside quotes)
Executes statements and prints results
Basic meta-commands:
.help
.exit / .quit
.tables (list tables from catalog)
.schema <table> (show columns + constraints)
Pretty printing for QueryResult as a table
Clean error display using your exception hierarchy
Files added/changed
Added: repl.py
(Optional but recommended) Added: Database.execute_script() to run ;-separated statements in one call
1) Optional improvement: Database.execute_script() (recommended for REPL)
simpledb/db.py (add this method; keep existing execute() as-is)
Python

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from .catalog import Catalog
from .exec.executor import Executor
from .index.hash_index import HashIndex
from .parser import parse_script, parse_sql


@dataclass
class Database:
    root_dir: Path
    catalog: Catalog
    index_cache: dict[str, HashIndex] = field(default_factory=dict)

    @classmethod
    def open(cls, path: str | Path) -> "Database":
        root = Path(path)
        root.mkdir(parents=True, exist_ok=True)
        catalog = Catalog.load(root)
        return cls(root_dir=root, catalog=catalog)

    def execute(self, sql: str):
        stmt = parse_sql(sql)
        ex = Executor(db_dir=self.root_dir, catalog=self.catalog, index_cache=self.index_cache)
        return ex.execute(stmt)

    def execute_script(self, sql: str):
        stmts = parse_script(sql)
        ex = Executor(db_dir=self.root_dir, catalog=self.catalog, index_cache=self.index_cache)
        return [ex.execute(s) for s in stmts]
