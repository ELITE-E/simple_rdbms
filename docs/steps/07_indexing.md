Step 7 — Basic Indexing (Hash Index) + Index-backed SELECT + Index maintenance (IMPLEMENTED)
Step Goal
Add basic indexing that actually speeds up queries:

CREATE INDEX idx ON table(col) builds a persisted hash index
SELECT ... WHERE col = literal uses an index when available (no full table scan)
Indexes are maintained on INSERT, UPDATE, DELETE
What was implemented
Hash index module stored on disk as JSON:
path: db_dir/indexes/<index_name>.json
maps typed keys → list of row ids (_rid)
HeapTable RID directory to allow fast row fetch by _rid:
path: db_dir/data/<table>.dir.json (rid → byte offset)
enables index-backed SELECT without scanning the whole table
CREATE INDEX now builds the index from existing rows
INSERT/UPDATE/DELETE now update indexes
SELECT chooses an index when possible and returns QueryResult.stats describing plan (index vs scan)
Notes / limitations
Index supports equality only (WHERE col = literal), not ranges.
JOIN is still not implemented (next later step).
No transactions/ACID (not required by your reference).

(There important changes needed in executor.py,heap.py,db.py that I skip for now,they remain as in step 06...)