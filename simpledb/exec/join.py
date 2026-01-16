"""
simpledb/exec/join.py

JOIN execution logic for the SimpleDB mini-RDBMS.

Responsibilities:
- Implement INNER JOIN on equality: t1.col = t2.col
- Produce combined rows as a mapping keyed by (table, column) tuples
- Support WHERE filtering on joined rows
- Provide a simple plan step indicating whether an index-assisted join was used

Join methods:
- Nested-loop join (scan right table)
- Index nested-loop join (if the right join column has a hash index)

Design notes:
- This module is intentionally separated to keep Executor readable and modular.
- We require qualified columns in JOIN ON (e.g., transactions.category_id = categories.id).
- For SELECT on joined results, we recommend fully qualifying column names to avoid ambiguity.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from ..ast import ColumnRef, JoinClause, WhereClause
from ..catalog import Catalog
from ..errors import ExecutionError
from ..index.hash_index import HashIndex
from ..storage.heap import HeapTable

# Combined row representation: (table, column) -> value
CombinedRow = dict[tuple[str, str], Any]


def _resolve_in_combined(row: CombinedRow, colref: ColumnRef) -> Any:
    """
    Resolve a ColumnRef within a combined (joined) row.

    Args:
        row: CombinedRow mapping (table, column) -> value.
        colref: Column reference; may or may not be qualified.

    Returns:
        The value from the combined row.

    Raises:
        ExecutionError if the reference is unknown or ambiguous when unqualified.
    """
    if colref.table is not None:
        key = (colref.table, colref.column)
        if key not in row:
            raise ExecutionError(f"Unknown column in joined row: {colref.table}.{colref.column}")
        return row[key]

    # Unqualified: require uniqueness across all joined tables.
    matches = [(t, c) for (t, c) in row.keys() if c == colref.column]
    if not matches:
        raise ExecutionError(f"Unknown column: {colref.column}")
    if len(matches) > 1:
        raise ExecutionError(f"Ambiguous column: {colref.column} (qualify with table.)")
    return row[matches[0]]


def where_matches(row: CombinedRow, where: WhereClause | None) -> bool:
    """
    Evaluate a WHERE clause against a combined row.

    Args:
        row: CombinedRow produced by JOIN pipeline.
        where: WhereClause or None.

    Returns:
        True if the row satisfies all conditions; False otherwise.
    """
    if where is None:
        return True
    for cond in where.conditions:
        if cond.op != "=":
            raise ExecutionError("Only '=' supported in WHERE")
        left_val = _resolve_in_combined(row, cond.left)
        if left_val != cond.right:
            return False
    return True


@dataclass(frozen=True)
class JoinPlanStep:
    """
    Debug/teaching stats for one join step.

    Attributes:
        right_table: Name of the table joined in this step.
        method: 'index' or 'scan'
        index_name: Index used if method='index'
    """
    right_table: str
    method: str
    index_name: str | None = None


def inner_join(
    *,
    catalog: Catalog,
    db_dir,
    index_cache: dict[str, HashIndex],
    left_rows: Iterable[CombinedRow],
    join: JoinClause,
) -> tuple[list[CombinedRow], JoinPlanStep]:
    """
    Perform one INNER JOIN step, joining left_rows with join.table_name.

    Args:
        catalog: Catalog for schema/index metadata.
        db_dir: Database directory Path.
        index_cache: Cache of opened HashIndex objects.
        left_rows: Intermediate combined rows from previous steps (or base table).
        join: JoinClause defining right table and equality condition.

    Returns:
        (joined_rows, join_plan_step)

    Raises:
        ExecutionError for invalid join definitions.
    """
    right_table = catalog.require_table(join.table_name)
    right_heap = HeapTable.open(db_dir, join.table_name)

    # Determine which side of ON references the right table.
    # We require qualified ON columns for safety.
    if join.left.table == join.table_name:
        right_col = join.left.column
        left_colref = join.right
    elif join.right.table == join.table_name:
        right_col = join.right.column
        left_colref = join.left
    else:
        raise ExecutionError(
            "JOIN ON must reference the joining table with qualification, e.g. t1.x = t2.y"
        )

    # If the right join column has an index, do an index nested-loop join.
    idx_meta = next((m for m in right_table.indexes.values() if m.column_name == right_col), None)

    out: list[CombinedRow] = []

    if idx_meta is not None:
        idx = index_cache.get(idx_meta.name)
        if idx is None:
            idx_path = db_dir / "indexes" / f"{idx_meta.name}.json"
            idx = HashIndex.open(
                idx_path,
                name=idx_meta.name,
                table_name=idx_meta.table_name,
                column_name=idx_meta.column_name,
            )
            index_cache[idx_meta.name] = idx

        for lrow in left_rows:
            key_val = _resolve_in_combined(lrow, left_colref)
            rids = idx.lookup(key_val)
            for rid in rids:
                r = right_heap.get_by_rid(rid)
                if r is None:
                    continue  # deleted or missing
                combined = dict(lrow)
                for k, v in r.items():
                    if k == "_rid":
                        continue
                    combined[(join.table_name, k)] = v
                out.append(combined)

        return out, JoinPlanStep(right_table=join.table_name, method="index", index_name=idx_meta.name)

    # Fallback: nested-loop scan join
    right_rows = list(right_heap.scan_active())
    for lrow in left_rows:
        key_val = _resolve_in_combined(lrow, left_colref)
        for r in right_rows:
            if r.get(right_col) == key_val:
                combined = dict(lrow)
                for k, v in r.items():
                    if k == "_rid":
                        continue
                    combined[(join.table_name, k)] = v
                out.append(combined)

    return out, JoinPlanStep(right_table=join.table_name, method="scan", index_name=None)