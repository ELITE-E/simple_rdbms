from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..errors import ExecutionError


def encode_key(value: Any) -> str:
    """
    Typed key encoding to avoid collisions:
      int 1  -> "i:1"
      str "1"-> "s:1"
      bool True -> "b:true"
    """
    if value is None:
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
    name: str
    table_name: str
    column_name: str
    path: Path
    mapping: dict[str, set[int]]

    @classmethod
    def open(cls, path: Path, *, name: str, table_name: str, column_name: str) -> "HashIndex":
        if path.exists():
            raw = json.loads(path.read_text(encoding="utf-8"))
            mp: dict[str, set[int]] = {k: set(v) for k, v in raw.get("mapping", {}).items()}
            return cls(
                name=raw.get("name", name),
                table_name=raw.get("table_name", table_name),
                column_name=raw.get("column_name", column_name),
                path=path,
                mapping=mp,
            )
        return cls(name=name, table_name=table_name, column_name=column_name, path=path, mapping={})

    def save(self) -> None:
        out = {
            "name": self.name,
            "table_name": self.table_name,
            "column_name": self.column_name,
            "mapping": {k: sorted(list(v)) for k, v in self.mapping.items()},
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(out, indent=2, sort_keys=True), encoding="utf-8")

    def clear(self) -> None:
        self.mapping.clear()

    def add(self, value: Any, rid: int) -> None:
        if value is None:
            return
        k = encode_key(value)
        self.mapping.setdefault(k, set()).add(int(rid))

    def remove(self, value: Any, rid: int) -> None:
        if value is None:
            return
        k = encode_key(value)
        s = self.mapping.get(k)
        if not s:
            return
        s.discard(int(rid))
        if not s:
            self.mapping.pop(k, None)

    def lookup(self, value: Any) -> list[int]:
        if value is None:
            return []
        k = encode_key(value)
        return sorted(list(self.mapping.get(k, set())))