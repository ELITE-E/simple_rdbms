"""
simpledb/storage/rid_directory.py

RID directory (row-id -> byte offset) for HeapTable.

Responsibilities:
- Maintain a mapping from integer row id (rid) to byte offset in the table's JSONL file
- Persist mapping as JSON for simplicity and debuggability:
    <db_dir>/data/<table>.dir.json

This enables fast random access when combined with indexes:
- index lookup -> rid list
- rid directory -> offset
- seek() + readline() -> retrieve row record without scanning entire file
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass
class RidDirectory:
    """
    Persistent mapping of rid -> byte offset in a JSONL file.

    Attributes:
        path: Path to directory JSON file.
        mapping: Dict[int, int] mapping rid -> offset.
    """
    path: Path
    mapping: dict[int, int]

    @classmethod
    def open(cls, path: Path) -> "RidDirectory":
        """
        Open a rid directory from disk or create an empty one.

        Args:
            path: Path to the rid directory JSON file.

        Returns:
            RidDirectory instance.
        """
        if not path.exists():
            path.write_text("{}", encoding="utf-8")
        raw = json.loads(path.read_text(encoding="utf-8"))
        mapping = {int(k): int(v) for k, v in raw.items()}
        return cls(path=path, mapping=mapping)

    def save(self) -> None:
        """Persist mapping to disk as JSON."""
        out = {str(k): v for k, v in sorted(self.mapping.items())}
        self.path.write_text(json.dumps(out, indent=2, sort_keys=True), encoding="utf-8")

    def set(self, rid: int, offset: int) -> None:
        """
        Set or update the offset for a rid.

        Args:
            rid: Row id.
            offset: Byte offset into JSONL file.
        """
        self.mapping[int(rid)] = int(offset)

    def get(self, rid: int) -> int | None:
        """
        Get the offset for a rid.

        Args:
            rid: Row id.

        Returns:
            Byte offset if present, else None.
        """
        return self.mapping.get(int(rid))