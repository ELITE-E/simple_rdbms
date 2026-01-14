from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Tombstones:
    path: Path
    deleted: set[int]

    @classmethod
    def open(cls, path: Path) -> "Tombstones":
        if not path.exists():
            path.write_text("[]", encoding="utf-8")
        raw = json.loads(path.read_text(encoding="utf-8"))
        return cls(path=path, deleted={int(x) for x in raw})

    def save(self) -> None:
        self.path.write_text(json.dumps(sorted(self.deleted), indent=2), encoding="utf-8")

    def add(self, rid: int) -> None:
        self.deleted.add(int(rid))
        self.save()

    def contains(self, rid: int) -> bool:
        return int(rid) in self.deleted