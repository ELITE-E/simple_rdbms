"""
simpledb/ast.py

AST (Abstract Syntax Tree) node definitions for the SimpleDB SQL subset.

The parser converts token streams into instances of these dataclasses.
The executor then uses the AST to perform DDL/DML operations.

Design notes:
- We keep the AST small and explicit, supporting only the required SQL subset.
- Column references can be qualified (table.column) to support JOIN queries.
- WHERE supports only conjunctions of equality predicates (col = literal AND ...).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


# ---------- Core nodes ----------

class Statement:
    """Base class marker for all statements."""


@dataclass(frozen=True)
class TypeSpec:
    """
    Type specification for a column.

    Attributes:
        name: Uppercased type name, e.g. "INTEGER", "VARCHAR", "DATE".
        params: Optional integer parameters, e.g. VARCHAR(255) => [255].
    """
    name: str
    params: list[int]


@dataclass(frozen=True)
class ColumnDef:
    """
    Column definition in CREATE TABLE.

    Attributes:
        name: Column name.
        typ: TypeSpec object.
        not_null: Whether NOT NULL is required.
        unique: Whether UNIQUE is required.
        primary_key: Whether this column is the (single) PRIMARY KEY.
    """
    name: str
    typ: TypeSpec
    not_null: bool = False
    unique: bool = False
    primary_key: bool = False


@dataclass(frozen=True)
class ColumnRef:
    """
    Reference to a column in SELECT / WHERE / JOIN ON.

    Attributes:
        column: Column name.
        table: Optional table name for qualified references.
    """
    column: str
    table: str | None = None


@dataclass(frozen=True)
class Condition:
    """
    WHERE condition.

    Attributes:
        left: ColumnRef on the left side.
        op: Operator string (Phase: only "=").
        right: Literal value (int | str | bool | None).
    """
    left: ColumnRef
    op: str
    right: Any


@dataclass(frozen=True)
class WhereClause:
    """
    WHERE clause represented as AND-separated conditions.

    Attributes:
        conditions: List of Condition, interpreted as conjunction (AND).
    """
    conditions: list[Condition]


# ---------- Statements ----------

@dataclass(frozen=True)
class CreateTable(Statement):
    """CREATE TABLE statement."""
    table_name: str
    columns: list[ColumnDef]


@dataclass(frozen=True)
class CreateIndex(Statement):
    """CREATE INDEX statement."""
    index_name: str
    table_name: str
    column_name: str


@dataclass(frozen=True)
class Insert(Statement):
    """INSERT statement."""
    table_name: str
    columns: list[str]
    values: list[Any]


@dataclass(frozen=True)
class JoinClause:
    """
    JOIN clause for INNER JOIN.

    Attributes:
        table_name: The right-side table being joined in.
        left: Left side ColumnRef in ON condition.
        right: Right side ColumnRef in ON condition.
    """
    table_name: str
    left: ColumnRef
    right: ColumnRef


@dataclass(frozen=True)
class Select(Statement):
    """
    SELECT statement.

    Attributes:
        columns: None means '*' (all columns). Otherwise explicit ColumnRef list.
        from_table: Base table.
        joins: List of JoinClause; only INNER JOIN equality supported.
        where: Optional WHERE clause (AND of equality).
    """
    columns: list[ColumnRef] | None
    from_table: str
    joins: list[JoinClause]
    where: WhereClause | None


@dataclass(frozen=True)
class Assignment:
    """A single SET assignment in UPDATE."""
    column: str
    value: Any


@dataclass(frozen=True)
class Update(Statement):
    """UPDATE statement."""
    table_name: str
    assignments: list[Assignment]
    where: WhereClause | None


@dataclass(frozen=True)
class Delete(Statement):
    """DELETE statement."""
    table_name: str
    where: WhereClause | None