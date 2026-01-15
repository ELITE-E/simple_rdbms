"""
simpledb/storage/heap.py

Heap table storage implementation using JSONL as an append-only row log.

Responsibilities:
- Store rows for each table in <db_dir>/data/<table>.jsonl (JSON Lines format).
- Maintain a RID directory mapping rid -> byte offset for O(1) random access:
    <db_dir>/data/<table>.dir.json
- Maintain tombstones for logical deletes:
    <db_dir>/data/<table>.tombstones.json
- Provide:
    - insert(row) -> rid
    - scan_active() -> iterator of active (not deleted) rows
    - get_by_rid(rid) -> row dict or None if not found/deleted
    - tombstone(rid) to logically delete

Design notes:
- JSONL is chosen for readability and ease of debugging.
- A separate tombstone file avoids inflating the JSONL file with delete records.
- The RID directory enables index-backed point reads (seek + readline).
- This is not crash-safe (no WAL/FSYNC/transactions) by design for this assignment.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from ..errors import ExecutionError
from .rid_directory import RidDirectory
from .tombstones import Tombstones


@dataclass
class HeapTable:
    """
    HeapTable represents on-disk storage for a single table.

    Attributes:
        table_name: Logical table name.
        data_path: Path to table JSONL file.
        meta_path: Path to table meta JSON file (currently only next_rid).
        rid_dir: RidDirectory for rid -> byte offset.
        tombstones: Tombstones set for logical deletions.
    """
    table_name: str
    data_path: Path
    meta_path: Path
    rid_dir: RidDirectory
    tombstones: Tombstones

    @classmethod
    def open(cls, db_dir: Path, table_name: str) -> "HeapTable":
        """
        Open (or initialize) heap storage for a table.

        Args:
            db_dir: Database root directory.
            table_name: Table name.

        Returns:
            HeapTable instance.
        """
        data_dir = db_dir / "data"
        data_dir.mkdir(parents=True, exist_ok=True)

        data_path = data_dir / f"{table_name}.jsonl"
        meta_path = data_dir / f"{table_name}.meta.json"
        dir_path = data_dir / f"{table_name}.dir.json"
        tomb_path = data_dir / f"{table_name}.tombstones.json"

        if not data_path.exists():
            data_path.write_bytes(b"")

        if not meta_path.exists():
            meta_path.write_text(json.dumps({"next_rid": 1}, indent=2), encoding="utf-8")

        rid_dir = RidDirectory.open(dir_path)
        tombstones = Tombstones.open(tomb_path)

        ht = cls(
            table_name=table_name,
            data_path=data_path,
            meta_path=meta_path,
            rid_dir=rid_dir,
            tombstones=tombstones,
        )

        # If directory is empty but file has content (e.g., upgraded DB), rebuild.
        if not ht.rid_dir.mapping and data_path.stat().st_size > 0:
            ht.rebuild_directory_from_data()

        return ht

    def _load_meta(self) -> dict[str, Any]:
        """
        Load meta file.

        Returns:
            Dict containing at least 'next_rid'.
        """
        return json.loads(self.meta_path.read_text(encoding="utf-8"))

    def _save_meta(self, meta: dict[str, Any]) -> None:
        """Persist meta file."""
        self.meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    def rebuild_directory_from_data(self) -> None:
        """
        Rebuild the rid -> byte offset directory by scanning the JSONL data file.

        This is useful if:
          - the directory file is missing/corrupt
          - migrating from earlier versions that didn't track offsets

        Raises:
            ExecutionError: on corrupt JSON records.
        """
        self.rid_dir.mapping.clear()
        with self.data_path.open("rb") as f:
            while True:
                offset = f.tell()
                line = f.readline()
                if not line:
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line.decode("utf-8"))
                except json.JSONDecodeError as e:
                    raise ExecutionError(f"Corrupt record in {self.data_path}: {e}") from e

                # Ignore any legacy tombstone records if present
                if obj.get("_op") == "DELETE":
                    continue

                rid = obj.get("_rid")
                if isinstance(rid, int):
                    self.rid_dir.set(rid, offset)

        self.rid_dir.save()

    def insert(self, row: dict[str, Any]) -> int:
        """
        Append a row to the heap file and return its assigned rid.

        Args:
            row: Dict of logical column -> value. Do NOT include '_rid'.

        Returns:
            Assigned integer rid.
        """
        meta = self._load_meta()
        rid = int(meta["next_rid"])
        meta["next_rid"] = rid + 1
        self._save_meta(meta)

        stored = {"_rid": rid, **row}
        line = (json.dumps(stored, separators=(",", ":")) + "\n").encode("utf-8")

        # Write and capture byte offset for directory
        with self.data_path.open("ab") as f:
            offset = f.tell()
            f.write(line)

        self.rid_dir.set(rid, offset)
        self.rid_dir.save()

        return rid

    def tombstone(self, rid: int) -> None:
        """
        Logically delete a row by rid.

        Args:
            rid: Row id.
        """
        self.tombstones.add(int(rid))

    def scan_active(self) -> Iterable[dict[str, Any]]:
        """
        Iterate all active rows (not tombstoned).

        Yields:
            Row dicts including '_rid' and column keys.

        Notes:
            This is a full scan of the heap file. Indexes can avoid scans for
            selective WHERE queries.
        """
        with self.data_path.open("rb") as f:
            for bline in f:
                line = bline.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line.decode("utf-8"))
                except json.JSONDecodeError as e:
                    raise ExecutionError(f"Corrupt record in {self.data_path}: {e}") from e

                # Ignore any legacy delete markers if encountered
                if obj.get("_op") == "DELETE" or obj.get("_deleted") is True:
                    continue

                rid = obj.get("_rid")
                if isinstance(rid, int) and self.tombstones.contains(rid):
                    continue

                yield obj

    def get_by_rid(self, rid: int) -> dict[str, Any] | None:
        """
        Retrieve a row by rid using the rid directory (fast path).

        Args:
            rid: Row id.

        Returns:
            Row dict if present and not deleted; otherwise None.

        Raises:
            ExecutionError: on directory mismatch or corrupt data.
        """
        rid = int(rid)
        if self.tombstones.contains(rid):
            return None

        off = self.rid_dir.get(rid)
        if off is None:
            return None

        with self.data_path.open("rb") as f:
            f.seek(off)
            line = f.readline()
            if not line:
                raise ExecutionError(f"RID offset past EOF: {self.table_name} rid={rid}")
            try:
                obj = json.loads(line.decode("utf-8"))
            except json.JSONDecodeError as e:
                raise ExecutionError(f"Corrupt record at rid={rid} in {self.data_path}: {e}") from e

            actual = obj.get("_rid")
            if int(actual) != rid:
                # If this happens, the directory is out-of-sync with the file.
                raise ExecutionError(
                    f"RID directory mismatch for {self.table_name}: expected {rid}, got {actual}. "
                    "Consider rebuilding the directory."
                )

            if self.tombstones.contains(rid):
                return None

            return obj