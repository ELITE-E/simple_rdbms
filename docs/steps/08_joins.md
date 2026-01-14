Step 8 — JOIN (INNER JOIN on equality) + keep storage/index modular (IMPLEMENTED)
You asked for JOIN and to keep Step 6 tombstones + Step 7 directory/index consistent, but without letting the main data file explode. So Step 8 includes a small storage refactor that stays modular:

Rows remain append-only in data/<table>.jsonl
Deletions are tracked in a separate small file data/<table>.tombstones.json (rewritten, but tiny)
RID directory remains data/<table>.dir.json for fast rid -> offset
Index files remain indexes/<index>.json
This avoids bloating the main JSONL with tombstone records while keeping deletes/updates correct.

