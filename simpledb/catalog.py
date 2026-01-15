"""
simpledb/catalog.py

Schema catalog and metadata persistence for the SimpleDB mini-RDBMS.

Responsibilities:
- Maintain a persistent catalog of tables, columns, and indexes.
- Enforce basic DDL validation rules (supported types, duplicate names, etc.).
- Serialize/deserialize the catalog to/from JSON in the database directory.

Persistence:
- Stored at: <db_dir>/catalog.json

Design notes:
- This is a small educational RDBMS, so the catalog is intentionally simple.
- We support a single-column PRIMARY KEY per table (Phase 1 simplification).
- Index names are globally unique (simplifies management).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .ast import ColumnDef, TypeSpec
from .errors import ExecutionError

CATALOG_FILE = "catalog.json"

SUPPORTED_TYPES = {"INTEGER", "VARCHAR", "TEXT", "DATE", "BOOLEAN"}


@dataclass
class IndexMeta:
    """
    Index metadata.

    Attributes:
        name: Index name (globally unique across DB).
        table_name: Table the index belongs to.
        column_name: Column the index indexes.
    """
    name: str
    table_name: str
    column_name: str


@dataclass
class TableMeta:
    """
    Table metadata stored in the catalog.

    Attributes:
        name: Table name.
        columns: Column definitions (name/type/constraints).
        indexes: Index metadata keyed by index name.
    """
    name: str
    columns: list[ColumnDef]
    indexes: dict[str, IndexMeta]

    def column_names(self) -> set[str]:
        """Return the set of column names in this table."""
        return {c.name for c in self.columns}

    def get_column(self, name: str) -> ColumnDef | None:
        """Return ColumnDef by name, or None if not found."""
        for c in self.columns:
            if c.name == name:
                return c
        return None

    def primary_key_column(self) -> str | None:
        """
        Return the primary key column name if present, else None.

        Note:
            This DB supports only ONE primary key column per table.
        """
        pks = [c.name for c in self.columns if c.primary_key]
        if not pks:
            return None
        return pks[0]


@dataclass
class Catalog:
    """
    Database catalog containing all tables and indexes.

    Attributes:
        version: Catalog format version (for future migrations).
        tables: Mapping of table name -> TableMeta.
        indexes: Mapping of index name -> IndexMeta (global namespace).
    """
    version: int
    tables: dict[str, TableMeta]
    indexes: dict[str, IndexMeta]

    @classmethod
    def empty(cls) -> "Catalog":
        """Create an empty catalog."""
        return cls(version=1, tables={}, indexes={})

    @classmethod
    def load(cls, db_dir: Path) -> "Catalog":
        """
        Load catalog.json if present, otherwise return an empty catalog.

        Args:
            db_dir: Root directory of the database.

        Returns:
            Catalog instance.
        """
        path = db_dir / CATALOG_FILE
        if not path.exists():
            return cls.empty()

        raw = json.loads(path.read_text(encoding="utf-8"))
        version = int(raw.get("version", 1))

        tables: dict[str, TableMeta] = {}
        indexes: dict[str, IndexMeta] = {}

        # Deserialize tables
        for tname, t in raw.get("tables", {}).items():
            cols: list[ColumnDef] = []
            for c in t.get("columns", []):
                typ_raw = c.get("typ", {})
                typ = TypeSpec(
                    name=str(typ_raw.get("name", "")).upper(),
                    params=list(typ_raw.get("params", [])),
                )
                cols.append(
                    ColumnDef(
                        name=c["name"],
                        typ=typ,
                        not_null=bool(c.get("not_null", False)),
                        unique=bool(c.get("unique", False)),
                        primary_key=bool(c.get("primary_key", False)),
                    )
                )

            t_indexes: dict[str, IndexMeta] = {}
            for iname, im in t.get("indexes", {}).items():
                idx = IndexMeta(
                    name=iname,
                    table_name=im["table_name"],
                    column_name=im["column_name"],
                )
                t_indexes[iname] = idx
                indexes[iname] = idx

            tables[tname] = TableMeta(name=tname, columns=cols, indexes=t_indexes)

        # Optional: merge any global index list (kept for forward compatibility)
        for iname, im in raw.get("indexes", {}).items():
            if iname not in indexes:
                indexes[iname] = IndexMeta(
                    name=iname,
                    table_name=im["table_name"],
                    column_name=im["column_name"],
                )

        return cls(version=version, tables=tables, indexes=indexes)

    def save(self, db_dir: Path) -> None:
        """
        Persist catalog state to <db_dir>/catalog.json.

        Args:
            db_dir: Database root directory.
        """
        path = db_dir / CATALOG_FILE

        def col_to_dict(c: ColumnDef) -> dict[str, Any]:
            return {
                "name": c.name,
                "typ": {"name": c.typ.name, "params": list(c.typ.params)},
                "not_null": c.not_null,
                "unique": c.unique,
                "primary_key": c.primary_key,
            }

        tables_dict: dict[str, Any] = {}
        for tname, t in self.tables.items():
            tables_dict[tname] = {
                "columns": [col_to_dict(c) for c in t.columns],
                "indexes": {
                    iname: {"table_name": idx.table_name, "column_name": idx.column_name}
                    for iname, idx in t.indexes.items()
                },
            }

        out = {
            "version": self.version,
            "tables": tables_dict,
            "indexes": {
                iname: {"table_name": idx.table_name, "column_name": idx.column_name}
                for iname, idx in self.indexes.items()
            },
        }

        path.write_text(json.dumps(out, indent=2, sort_keys=True), encoding="utf-8")

    # ---------- lookup helpers ----------

    def require_table(self, table_name: str) -> TableMeta:
        """
        Fetch a table by name or raise ExecutionError.

        Args:
            table_name: Name of the table.

        Returns:
            TableMeta.

        Raises:
            ExecutionError: if table does not exist.
        """
        t = self.tables.get(table_name)
        if not t:
            raise ExecutionError(f"Table not found: {table_name}")
        return t

    # ---------- validation helpers ----------

    def validate_type(self, typ: TypeSpec) -> None:
        """
        Validate a column type is supported and parameters are correct.

        Args:
            typ: TypeSpec.

        Raises:
            ExecutionError: if type is unsupported or invalid parameters.
        """
        tname = typ.name.upper()
        if tname not in SUPPORTED_TYPES:
            raise ExecutionError(f"Unsupported type: {typ.name}")

        if tname == "VARCHAR":
            if len(typ.params) != 1 or not isinstance(typ.params[0], int) or typ.params[0] <= 0:
                raise ExecutionError("VARCHAR requires exactly one positive integer length parameter, e.g. VARCHAR(255)")
        else:
            if typ.params:
                raise ExecutionError(f"Type {tname} does not accept parameters")

    def validate_create_table(self, table_name: str, columns: list[ColumnDef]) -> None:
        """
        Validate CREATE TABLE request.

        Checks:
        - Table does not already exist
        - No duplicate column names
        - Supported types + parameter validation
        - At most one PRIMARY KEY column

        Args:
            table_name: Table name.
            columns: Parsed columns.

        Raises:
            ExecutionError: on invalid schema.
        """
        if table_name in self.tables:
            raise ExecutionError(f"Table already exists: {table_name}")

        col_names = [c.name for c in columns]
        if len(set(col_names)) != len(col_names):
            raise ExecutionError("Duplicate column name in CREATE TABLE")

        pk_cols = [c.name for c in columns if c.primary_key]
        if len(pk_cols) > 1:
            raise ExecutionError("Only one PRIMARY KEY column is supported")

        for c in columns:
            self.validate_type(c.typ)

    def validate_create_index(self, index_name: str, table_name: str, column_name: str) -> None:
        """
        Validate CREATE INDEX request.

        Checks:
        - Index name not already used
        - Table exists
        - Column exists in table

        Args:
            index_name: Index name.
            table_name: Table name.
            column_name: Column name.

        Raises:
            ExecutionError: if invalid.
        """
        if index_name in self.indexes:
            raise ExecutionError(f"Index already exists: {index_name}")

        table = self.require_table(table_name)
        if column_name not in table.column_names():
            raise ExecutionError(f"Column not found: {table_name}.{column_name}")