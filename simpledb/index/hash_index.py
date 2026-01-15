"""
simpledb/index/hash_index.py

A simple persisted hash index for equality predicates.

Responsibilities:
- Maintain mapping: typed_key(value) -> set of row IDs (rids)
- Persist index to JSON: <db_dir>/indexes/<index_name>.json
- Support basic operations:
    - add(value, rid)
    - remove(value, rid)
    - lookup(value) -> list[rid]

Design notes:
- This index is intentionally simple and only supports equality lookups.
- Key encoding includes type information to avoid collisions (e.g., int 1 vs str "1").
- We do not index NULL values (consistent with many SQL systems' index behavior).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..errors import ExecutionError


def encode_key(value: Any) -> str:
    """
    Encode a Python value into a stable, typed string key for index mapping.

    Args:
        value: A literal value used in indexed columns (int, str, bool).

    Returns:
        A typed string key such as:
          - i:123
          - s:hello
          - b:true

    Raises:
        ExecutionError if the type is unsupported.
    """
    if value is None:
        # We don't index NULLs, but returning a key is harmless if needed.
        return "n:null"
    if isinstance(value, bool):
        return f"b:{str(value).lower()}"
    if isinstance(value, int) and not isinstance(value, bool):
        return f"i:{value}"
    if isinstance(value, str):
        return f"s:{value}"
    raise ExecutionError(f"Unsupported index key type: {type(value).__name__}")


@dataclass
class HashIndex:
    """
    Simple hash index persisted as JSON.

    Attributes:
        name: Index name.
        table_name: Table name.
        column_name: Indexed column name.
        path: Path to JSON index file.
        mapping: Dict of encoded keys -> set of rids.
    """
    name: str
    table_name: str
    column_name: str
    path: Path
    mapping: dict[str, set[int]]

    @classmethod
    def open(cls, path: Path, *, name: str, table_name: str, column_name: str) -> "HashIndex":
        """
        Open an index from disk if it exists; otherwise create a new empty one.

        Args:
            path: Index JSON file path.
            name: Index name.
            table_name: Target table.
            column_name: Indexed column.

        Returns:
            HashIndex instance.
        """
        if path.exists():
            raw = json.loads(path.read_text(encoding="utf-8"))
            mp = {k: set(v) for k, v in raw.get("mapping", {}).items()}
            return cls(
                name=str(raw.get("name", name)),
                table_name=str(raw.get("table_name", table_name)),
                column_name=str(raw.get("column_name", column_name)),
                path=path,
                mapping=mp,
            )
        return cls(name=name, table_name=table_name, column_name=column_name, path=path, mapping={})

    def save(self) -> None:
        """
        Persist this index to disk as JSON.

        The mapping is stored as key -> sorted list of rids for stable diffs.
        """
        out = {
            "name": self.name,
            "table_name": self.table_name,
            "column_name": self.column_name,
            "mapping": {k: sorted(list(v)) for k, v in self.mapping.items()},
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(out, indent=2, sort_keys=True), encoding="utf-8")

    def clear(self) -> None:
        """Remove all entries from the index in memory (call save() to persist)."""
        self.mapping.clear()

    def add(self, value: Any, rid: int) -> None:
        """
        Add a row reference to the index.

        Args:
            value: Column value to index (NULL is ignored).
            rid: Row id to reference.
        """
        if value is None:
            return
        k = encode_key(value)
        self.mapping.setdefault(k, set()).add(int(rid))

    def remove(self, value: Any, rid: int) -> None:
        """
        Remove a row reference from the index.

        Args:
            value: Column value (NULL is ignored).
            rid: Row id.
        """
        if value is None:
            return
        k = encode_key(value)
        s = self.mapping.get(k)
        if not s:
            return
        s.discard(int(rid))
        if not s:
            # Keep mapping compact
            self.mapping.pop(k, None)

    def lookup(self, value: Any) -> list[int]:
        """
        Lookup rids for a given column value.

        Args:
            value: Column value to lookup.

        Returns:
            Sorted list of rids matching the value.
            Returns empty list if value is NULL or no match.
        """
        if value is None:
            return []
        k = encode_key(value)
        return sorted(self.mapping.get(k, set()))