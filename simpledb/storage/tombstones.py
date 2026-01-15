"""
simpledb/storage/tombstones.py

Deletion tracking for HeapTable.

Responsibilities:
- Track logically deleted row ids (rids) in a compact persisted form:
    <db_dir>/data/<table>.tombstones.json
- Provide a fast membership check to hide deleted rows during scans and rid fetch.

Design notes:
- We store tombstones separately from the main JSONL data file to avoid
  inflating the data file with many tombstone records.
- This is a pragmatic choice for this educational RDBMS: it keeps the main
  heap file append-only for rows while deletions are maintained in a small file.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Tombstones:
    """
    A persisted set of deleted rids.

    Attributes:
        path: Path to tombstone JSON file.
        deleted: Set of deleted rids.
    """
    path: Path
    deleted: set[int]

    @classmethod
    def open(cls, path: Path) -> "Tombstones":
        """
        Open tombstones from disk or create an empty file.

        Args:
            path: Path to tombstone JSON file.

        Returns:
            Tombstones instance.
        """
        if not path.exists():
            path.write_text("[]", encoding="utf-8")
        raw = json.loads(path.read_text(encoding="utf-8"))
        return cls(path=path, deleted={int(x) for x in raw})

    def save(self) -> None:
        """Persist tombstones to disk."""
        self.path.write_text(json.dumps(sorted(self.deleted), indent=2), encoding="utf-8")

    def add(self, rid: int) -> None:
        """
        Mark rid as deleted and persist immediately.

        Args:
            rid: Row id to delete.
        """
        self.deleted.add(int(rid))
        self.save()

    def contains(self, rid: int) -> bool:
        """
        Check if rid is deleted.

        Args:
            rid: Row id.

        Returns:
            True if deleted, else False.
        """
        return int(rid) in self.deleted