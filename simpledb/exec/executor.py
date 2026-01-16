"""
simpledb/exec/executor.py

Statement execution engine for the SimpleDB mini-RDBMS.

Responsibilities:
- Execute AST statements produced by the parser:
    - DDL: CREATE TABLE, CREATE INDEX
    - DML: INSERT, SELECT, UPDATE, DELETE
- Enforce constraints (PRIMARY KEY, UNIQUE, NOT NULL)
- Maintain and use basic hash indexes for equality lookups
- Execute simple INNER JOINs (delegates join mechanics to simpledb/exec/join.py)

Core design:
- Storage is provided by HeapTable (JSONL heap + rid directory + tombstones).
- Indexes are HashIndex persisted as JSON.
- Query processing is intentionally simple (scan or index-based point filtering).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from ..ast import (
    ColumnRef,
    CreateIndex,
    CreateTable,
    Delete,
    Insert,
    Select,
    Statement,
    Update,
)
from ..catalog import Catalog, IndexMeta, TableMeta
from ..errors import ConstraintError, ExecutionError
from ..index.hash_index import HashIndex
from ..result import CommandOk, QueryResult
from ..storage.heap import HeapTable
from .join import CombinedRow, inner_join, where_matches


@dataclass
class Executor:
    """
    Executes parsed AST statements against a database directory.

    Args:
        db_dir: Database root directory.
        catalog: In-memory Catalog (also persisted as catalog.json).
        index_cache: Cache of loaded HashIndex objects (kept at Database level).
    """
    db_dir: Path
    catalog: Catalog
    index_cache: dict[str, HashIndex]

    # --------------------------
    # public entry point
    # --------------------------

    def execute(self, stmt: Statement):
        """
        Execute a single statement.

        Args:
            stmt: AST statement.

        Returns:
            CommandOk for non-SELECT or QueryResult for SELECT.

        Raises:
            ExecutionError / ConstraintError on failure.
        """
        if isinstance(stmt, CreateTable):
            return self._create_table(stmt)
        if isinstance(stmt, CreateIndex):
            return self._create_index(stmt)
        if isinstance(stmt, Insert):
            return self._insert(stmt)
        if isinstance(stmt, Select):
            return self._select(stmt)
        if isinstance(stmt, Update):
            return self._update(stmt)
        if isinstance(stmt, Delete):
            return self._delete(stmt)

        raise ExecutionError(f"Unsupported statement: {type(stmt).__name__}")

    # --------------------------
    # index helpers
    # --------------------------

    def _index_path(self, index_name: str) -> Path:
        """Return the filesystem path for an index JSON file."""
        return self.db_dir / "indexes" / f"{index_name}.json"

    def _open_index(self, meta: IndexMeta) -> HashIndex:
        """
        Open an index from cache/disk.

        Args:
            meta: Index metadata.

        Returns:
            HashIndex instance (cached).
        """
        idx = self.index_cache.get(meta.name)
        if idx is not None:
            return idx

        idx = HashIndex.open(
            self._index_path(meta.name),
            name=meta.name,
            table_name=meta.table_name,
            column_name=meta.column_name,
        )
        self.index_cache[meta.name] = idx
        return idx

    def _table_indexes(self, table: TableMeta) -> list[HashIndex]:
        """
        Open all indexes for a table.

        Args:
            table: Table metadata.

        Returns:
            List of HashIndex instances.
        """
        return [self._open_index(m) for m in table.indexes.values()]

    # --------------------------
    # DDL
    # --------------------------

    def _create_table(self, stmt: CreateTable) -> CommandOk:
        """
        CREATE TABLE execution.

        - Validates schema
        - Stores TableMeta in catalog + persists catalog.json
        - Initializes table storage files

        Returns:
            CommandOk
        """
        self.catalog.validate_create_table(stmt.table_name, stmt.columns)

        table = TableMeta(name=stmt.table_name, columns=stmt.columns, indexes={})
        self.catalog.tables[stmt.table_name] = table
        self.catalog.save(self.db_dir)

        # Ensure storage exists
        HeapTable.open(self.db_dir, stmt.table_name)

        return CommandOk(rows_affected=0, message=f"Table created: {stmt.table_name}")

    def _create_index(self, stmt: CreateIndex) -> CommandOk:
        """
        CREATE INDEX execution.

        - Validates that index/table/column exist and name is unique
        - Adds index metadata to catalog (table-local and global)
        - Builds the hash index from existing active rows and persists it

        Returns:
            CommandOk
        """
        self.catalog.validate_create_index(stmt.index_name, stmt.table_name, stmt.column_name)

        idx_meta = IndexMeta(name=stmt.index_name, table_name=stmt.table_name, column_name=stmt.column_name)
        self.catalog.indexes[stmt.index_name] = idx_meta

        table = self.catalog.require_table(stmt.table_name)
        table.indexes[stmt.index_name] = idx_meta
        self.catalog.save(self.db_dir)

        # Build index from storage
        heap = HeapTable.open(self.db_dir, stmt.table_name)
        idx = self._open_index(idx_meta)
        idx.clear()
        for row in heap.scan_active():
            idx.add(row.get(stmt.column_name), int(row["_rid"]))
        idx.save()

        return CommandOk(
            rows_affected=0,
            message=f"Index created and built: {stmt.index_name} ON {stmt.table_name}({stmt.column_name})",
        )

    # --------------------------
    # validation helpers
    # --------------------------

    def _validate_types(self, table: TableMeta, row: dict[str, Any]) -> None:
        """
        Validate row values match the table schema types.

        Args:
            table: Table metadata.
            row: Dict of column -> value (None allowed).

        Raises:
            ExecutionError on type mismatch.
        """
        for col_def in table.columns:
            if col_def.name not in row:
                continue
            val = row[col_def.name]
            if val is None:
                continue

            t = col_def.typ.name.upper()

            if t == "INTEGER":
                # bool is a subclass of int in Python, so explicitly reject.
                if not isinstance(val, int) or isinstance(val, bool):
                    raise ExecutionError(f"Type error: {table.name}.{col_def.name} expects INTEGER")
            elif t in ("TEXT", "DATE", "VARCHAR"):
                if not isinstance(val, str):
                    raise ExecutionError(f"Type error: {table.name}.{col_def.name} expects TEXT/DATE")
                if t == "VARCHAR":
                    max_len = col_def.typ.params[0]
                    if len(val) > max_len:
                        raise ExecutionError(
                            f"Type error: {table.name}.{col_def.name} exceeds VARCHAR({max_len})"
                        )
            elif t == "BOOLEAN":
                if not isinstance(val, bool):
                    raise ExecutionError(f"Type error: {table.name}.{col_def.name} expects BOOLEAN")
            else:
                raise ExecutionError(f"Unsupported type: {t}")

    def _resolve_col_single_table(self, table_name: str, colref: ColumnRef, ctx: str) -> str:
        """
        Resolve a ColumnRef in a single-table context.

        Args:
            table_name: The table in scope.
            colref: Column reference (may be qualified).
            ctx: Context string used in error messages.

        Returns:
            Column name.

        Raises:
            ExecutionError if qualified table does not match the table in scope.
        """
        if colref.table is not None and colref.table != table_name:
            raise ExecutionError(f"{ctx}: column qualifier {colref.table}.{colref.column} does not match {table_name}")
        return colref.column

    def _row_matches_where_single_table(self, table_name: str, row: dict[str, Any], where) -> bool:
        """
        Evaluate WHERE conditions against a single-table row dict.

        Args:
            table_name: Table in scope.
            row: Row dict (includes _rid and columns).
            where: WhereClause or None.

        Returns:
            True if all conditions match.
        """
        if where is None:
            return True
        for cond in where.conditions:
            if cond.op != "=":
                raise ExecutionError("Only '=' is supported in WHERE")
            col = self._resolve_col_single_table(table_name, cond.left, "WHERE")
            if row.get(col) != cond.right:
                return False
        return True

    # --------------------------
    # constraint enforcement
    # --------------------------

    def _enforce_constraints_batch(
        self,
        table: TableMeta,
        existing_rows: list[dict[str, Any]],
        new_rows: list[dict[str, Any]],
        exclude_rids: set[int],
    ) -> None:
        """
        Enforce NOT NULL / PRIMARY KEY / UNIQUE constraints for a batch of new rows.

        This is used for:
        - INSERT (new_rows length 1, exclude_rids empty)
        - UPDATE (new_rows length N, exclude_rids are the old rids being replaced)

        Args:
            table: Table metadata.
            existing_rows: Active rows currently in table (including _rid).
            new_rows: Candidate logical rows (no _rid required).
            exclude_rids: Existing rids to ignore during conflict checks (rows being updated).

        Raises:
            ConstraintError if a constraint is violated.
        """
        # Existing rows excluding those we are replacing (UPDATE case)
        existing_kept = [r for r in existing_rows if int(r.get("_rid")) not in exclude_rids]

        # NOT NULL + PK implies NOT NULL
        for nr in new_rows:
            for c in table.columns:
                if c.not_null or c.primary_key:
                    if nr.get(c.name) is None:
                        if c.primary_key:
                            raise ConstraintError(f"PRIMARY KEY column cannot be NULL: {table.name}.{c.name}")
                        raise ConstraintError(f"NOT NULL constraint failed: {table.name}.{c.name}")

        # PRIMARY KEY uniqueness (single column)
        pk_col = table.primary_key_column()
        if pk_col is not None:
            existing_pks = {r.get(pk_col) for r in existing_kept}
            seen_new: set[Any] = set()
            for nr in new_rows:
                pk_val = nr.get(pk_col)
                if pk_val in existing_pks:
                    raise ConstraintError(
                        f"PRIMARY KEY constraint failed: duplicate value {pk_val!r} for {table.name}.{pk_col}"
                    )
                if pk_val in seen_new:
                    raise ConstraintError(
                        f"PRIMARY KEY constraint failed: duplicate value {pk_val!r} within statement batch"
                    )
                seen_new.add(pk_val)

        # UNIQUE uniqueness (NULLs ignored)
        unique_cols = [c.name for c in table.columns if c.unique]
        for ucol in unique_cols:
            existing_vals = {r.get(ucol) for r in existing_kept if r.get(ucol) is not None}
            seen_new_vals: set[Any] = set()
            for nr in new_rows:
                v = nr.get(ucol)
                if v is None:
                    continue
                if v in existing_vals:
                    raise ConstraintError(
                        f"UNIQUE constraint failed: duplicate value {v!r} for {table.name}.{ucol}"
                    )
                if v in seen_new_vals:
                    raise ConstraintError(
                        f"UNIQUE constraint failed: duplicate value {v!r} within statement batch for {table.name}.{ucol}"
                    )
                seen_new_vals.add(v)

    # --------------------------
    # WHERE planning (scan vs index)
    # --------------------------

    def _choose_index_candidates(
        self,
        table: TableMeta,
        where,
    ) -> tuple[str, list[int]] | None:
        """
        Attempt to choose an index to reduce candidates for WHERE.

        Strategy:
        - For each condition of form col = literal, if there is an index on `col`,
          compute candidate rid list from that index.
        - Choose the smallest candidate list.

        Args:
            table: Table metadata.
            where: WhereClause or None.

        Returns:
            (index_name, candidate_rids) or None if no usable index.
        """
        if where is None:
            return None

        best: tuple[str, list[int]] | None = None

        for cond in where.conditions:
            if cond.op != "=":
                continue
            # Ignore conditions qualified to some other table
            if cond.left.table is not None and cond.left.table != table.name:
                continue

            col = cond.left.column
            idx_meta = next((m for m in table.indexes.values() if m.column_name == col), None)
            if idx_meta is None:
                continue

            idx = self._open_index(idx_meta)
            rids = idx.lookup(cond.right)

            if best is None or len(rids) < len(best[1]):
                best = (idx_meta.name, rids)

        return best

    def _fetch_rows_by_candidates(
        self,
        heap: HeapTable,
        rids: list[int],
    ) -> list[dict[str, Any]]:
        """
        Fetch rows for a candidate rid list using heap.get_by_rid.

        Args:
            heap: HeapTable.
            rids: Candidate row ids.

        Returns:
            List of row dicts for those rids that still exist and are not deleted.
        """
        out: list[dict[str, Any]] = []
        for rid in rids:
            row = heap.get_by_rid(rid)
            if row is None:
                continue
            out.append(row)
        return out

    # --------------------------
    # DML: INSERT
    # --------------------------

    def _insert(self, stmt: Insert) -> CommandOk:
        """
        INSERT execution.

        - Validates table and columns
        - Builds a full row dict with missing columns as None
        - Type checks
        - Constraint enforcement (NOT NULL / PK / UNIQUE)
        - Appends row to heap storage and updates indexes

        Returns:
            CommandOk(rows_affected=1)
        """
        table = self.catalog.require_table(stmt.table_name)
        heap = HeapTable.open(self.db_dir, stmt.table_name)

        # Validate referenced columns
        cols_set = table.column_names()
        for c in stmt.columns:
            if c not in cols_set:
                raise ExecutionError(f"Unknown column in INSERT: {stmt.table_name}.{c}")

        # Build full row with all columns
        row: dict[str, Any] = {c.name: None for c in table.columns}
        for c, v in zip(stmt.columns, stmt.values):
            row[c] = v

        # Type + constraint checks
        self._validate_types(table, row)
        existing = list(heap.scan_active())
        self._enforce_constraints_batch(table, existing_rows=existing, new_rows=[row], exclude_rids=set())

        # Write row and update indexes
        rid = heap.insert(row)
        indexes = self._table_indexes(table)
        for idx in indexes:
            idx.add(row.get(idx.column_name), rid)
        for idx in indexes:
            idx.save()

        return CommandOk(rows_affected=1, message="1 row inserted")

    # --------------------------
    # DML: SELECT (single table or JOIN)
    # --------------------------

    def _select(self, stmt: Select) -> QueryResult:
        """
        SELECT execution.

        Supports:
        - Single-table SELECT with optional WHERE:
            - scan plan
            - index plan if an indexed equality predicate exists
        - JOIN SELECT (INNER JOIN with equality):
            - nested-loop or index nested-loop per join step

        Returns:
            QueryResult(columns, rows, stats)
        """
        if stmt.joins:
            return self._select_join(stmt)
        return self._select_single_table(stmt)

    def _select_single_table(self, stmt: Select) -> QueryResult:
        """
        Execute a SELECT without JOINs (single-table query).

        Uses index if possible for WHERE equality predicates.
        """
        table = self.catalog.require_table(stmt.from_table)
        heap = HeapTable.open(self.db_dir, stmt.from_table)

        # Determine output columns
        if stmt.columns is None:
            out_cols = [c.name for c in table.columns]
        else:
            out_cols = []
            table_cols = table.column_names()
            for c in stmt.columns:
                col = self._resolve_col_single_table(stmt.from_table, c, "SELECT")
                if col not in table_cols:
                    raise ExecutionError(f"Unknown column in SELECT: {stmt.from_table}.{col}")
                out_cols.append(col)

        # Choose index plan if possible
        chosen = self._choose_index_candidates(table, stmt.where)
        rows_out: list[list[Any]] = []

        if chosen is not None:
            idx_name, rids = chosen
            candidate_rows = self._fetch_rows_by_candidates(heap, rids)

            for row in candidate_rows:
                if not self._row_matches_where_single_table(stmt.from_table, row, stmt.where):
                    continue
                rows_out.append([row.get(c) for c in out_cols])

            return QueryResult(
                columns=out_cols,
                rows=rows_out,
                stats={"plan": "index", "index": idx_name, "candidates": len(rids)},
            )

        # Fallback scan plan
        for row in heap.scan_active():
            if not self._row_matches_where_single_table(stmt.from_table, row, stmt.where):
                continue
            rows_out.append([row.get(c) for c in out_cols])

        return QueryResult(columns=out_cols, rows=rows_out, stats={"plan": "scan"})

    def _select_join(self, stmt: Select) -> QueryResult:
        """
        Execute a SELECT with one or more JOIN clauses.

        Implementation:
        - Seed intermediate combined rows from base table (qualified keys)
        - Apply join steps sequentially using join.inner_join()
        - Apply WHERE on combined rows
        - Project selected columns

        Notes:
        - For explicit SELECT columns in JOIN queries, columns MUST be qualified (table.column)
          to avoid ambiguity.
        """
        base_table = self.catalog.require_table(stmt.from_table)
        base_heap = HeapTable.open(self.db_dir, stmt.from_table)

        # Seed combined rows from base table
        combined_rows: list[CombinedRow] = []
        for r in base_heap.scan_active():
            cr: CombinedRow = {}
            for k, v in r.items():
                if k == "_rid":
                    continue
                cr[(stmt.from_table, k)] = v
            combined_rows.append(cr)

        plan_steps: list[dict[str, Any]] = []
        for j in stmt.joins:
            combined_rows, step = inner_join(
                catalog=self.catalog,
                db_dir=self.db_dir,
                index_cache=self.index_cache,
                left_rows=combined_rows,
                join=j,
            )
            plan_steps.append({"right_table": step.right_table, "method": step.method, "index": step.index_name})

        # Apply WHERE after joins
        combined_rows = [r for r in combined_rows if where_matches(r, stmt.where)]

        # Output projection
        if stmt.columns is None:
            # SELECT * => all columns from base + join tables, qualified
            out_cols: list[str] = []
            out_cols += [f"{stmt.from_table}.{c.name}" for c in base_table.columns]

            for j in stmt.joins:
                t = self.catalog.require_table(j.table_name)
                out_cols += [f"{j.table_name}.{c.name}" for c in t.columns]

            rows_out: list[list[Any]] = []
            for r in combined_rows:
                vals: list[Any] = []
                for qc in out_cols:
                    tname, cname = qc.split(".", 1)
                    vals.append(r.get((tname, cname)))
                rows_out.append(vals)

            return QueryResult(
                columns=out_cols,
                rows=rows_out,
                stats={"plan": "join", "steps": plan_steps, "rows": len(rows_out)},
            )

        # Explicit column list => require qualification
        out_cols = []
        for c in stmt.columns:
            if c.table is None:
                raise ExecutionError("In JOIN queries, qualify selected columns with table (e.g., users.id).")
            out_cols.append(f"{c.table}.{c.column}")

        rows_out = []
        for r in combined_rows:
            rows_out.append([r.get((c.table, c.column)) for c in stmt.columns])  # type: ignore[union-attr]

        return QueryResult(
            columns=out_cols,
            rows=rows_out,
            stats={"plan": "join", "steps": plan_steps, "rows": len(rows_out)},
        )

    # --------------------------
    # DML: DELETE
    # --------------------------

    def _delete(self, stmt: Delete) -> CommandOk:
        """
        DELETE execution.

        - Finds matching active rows (index plan if possible)
        - Removes rows from indexes
        - Tombstones rows in heap storage

        Returns:
            CommandOk(rows_affected=N)
        """
        table = self.catalog.require_table(stmt.table_name)
        heap = HeapTable.open(self.db_dir, stmt.table_name)
        indexes = self._table_indexes(table)

        # Determine candidate rows using index if possible
        chosen = self._choose_index_candidates(table, stmt.where)
        if chosen is not None:
            _, rids = chosen
            candidates = self._fetch_rows_by_candidates(heap, rids)
        else:
            candidates = list(heap.scan_active())

        matched: list[dict[str, Any]] = []
        for row in candidates:
            if self._row_matches_where_single_table(stmt.table_name, row, stmt.where):
                matched.append(row)

        # Apply deletions
        for row in matched:
            rid = int(row["_rid"])
            # Remove from indexes first (keeps index-backed queries correct)
            for idx in indexes:
                idx.remove(row.get(idx.column_name), rid)
            # Tombstone storage (keeps scan-backed queries correct)
            heap.tombstone(rid)

        for idx in indexes:
            idx.save()

        return CommandOk(rows_affected=len(matched), message=f"{len(matched)} rows deleted")

    # --------------------------
    # DML: UPDATE
    # --------------------------

    def _update(self, stmt: Update) -> CommandOk:
        """
        UPDATE execution.

        Approach:
        - Identify matching rows (index plan if possible)
        - Build candidate new rows (full row dicts)
        - Type check all candidate rows
        - Enforce constraints in a batch (prevents partial updates)
        - Apply update as:
            - insert(new_row) -> new_rid
            - tombstone(old_rid)
            - update indexes (remove old rid, add new rid)

        Returns:
            CommandOk(rows_affected=N)
        """
        table = self.catalog.require_table(stmt.table_name)
        heap = HeapTable.open(self.db_dir, stmt.table_name)
        indexes = self._table_indexes(table)

        table_cols = table.column_names()

        # Validate assignment columns exist
        for a in stmt.assignments:
            if a.column not in table_cols:
                raise ExecutionError(f"Unknown column in UPDATE: {stmt.table_name}.{a.column}")

        # Determine candidate rows using index if possible
        chosen = self._choose_index_candidates(table, stmt.where)
        if chosen is not None:
            _, rids = chosen
            candidates = self._fetch_rows_by_candidates(heap, rids)
        else:
            candidates = list(heap.scan_active())

        # Filter to rows that match full WHERE
        to_update: list[dict[str, Any]] = []
        for row in candidates:
            if self._row_matches_where_single_table(stmt.table_name, row, stmt.where):
                to_update.append(row)

        if not to_update:
            return CommandOk(rows_affected=0, message="0 rows updated")

        # Build new candidate rows and collect exclude_rids
        exclude_rids = {int(r["_rid"]) for r in to_update}
        new_rows: list[dict[str, Any]] = []

        for old in to_update:
            # Start with old values for all schema columns
            candidate: dict[str, Any] = {c.name: old.get(c.name) for c in table.columns}

            # Apply assignments
            for a in stmt.assignments:
                candidate[a.column] = a.value

            # Validate types early (cheaper to fail before constraint checks)
            self._validate_types(table, candidate)
            new_rows.append(candidate)

        # Constraint enforcement must consider existing active rows excluding old rids
        existing_all = list(heap.scan_active())
        self._enforce_constraints_batch(
            table,
            existing_rows=existing_all,
            new_rows=new_rows,
            exclude_rids=exclude_rids,
        )

        # Apply updates
        for old, candidate in zip(to_update, new_rows):
            old_rid = int(old["_rid"])
            new_rid = heap.insert(candidate)
            heap.tombstone(old_rid)

            # Maintain indexes (remove old rid from old value, add new rid for new value)
            for idx in indexes:
                old_val = old.get(idx.column_name)
                new_val = candidate.get(idx.column_name)
                idx.remove(old_val, old_rid)
                idx.add(new_val, new_rid)

        for idx in indexes:
            idx.save()

        return CommandOk(rows_affected=len(to_update), message=f"{len(to_update)} rows updated")