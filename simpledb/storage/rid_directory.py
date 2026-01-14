from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass
class RidDirectory:
    path: Path
    mapping: dict[int, int]  # rid -> byte offset

    @classmethod
    def open(cls, path: Path) -> "RidDirectory":
        if not path.exists():
            path.write_text("{}", encoding="utf-8")
        raw = json.loads(path.read_text(encoding="utf-8"))
        mapping = {int(k): int(v) for k, v in raw.items()}
        return cls(path=path, mapping=mapping)

    def save(self) -> None:
        out = {str(k): v for k, v in sorted(self.mapping.items())}
        self.path.write_text(json.dumps(out, indent=2, sort_keys=True), encoding="utf-8")

    def set(self, rid: int, offset: int) -> None:
        self.mapping[int(rid)] = int(offset)

    def get(self, rid: int) -> int | None:
        return self.mapping.get(int(rid))