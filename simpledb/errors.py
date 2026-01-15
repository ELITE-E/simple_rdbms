"""
simpledb/errors.py

Centralized exception types for the SimpleDB mini-RDBMS.

This module defines:
- A common base exception for all DB-related errors
- A lightweight Position structure for reporting syntax errors with line/column context
- Specialized error types used across lexer/parser/executor/storage layers
"""

from __future__ import annotations

from dataclasses import dataclass


class SimpleDBError(Exception):
    """
    Base class for all SimpleDB errors.

    Catching this exception allows callers (REPL/web app) to handle all DB errors
    without accidentally swallowing unrelated system exceptions.
    """


@dataclass(frozen=True)
class Position:
    """
    Represents a location in an input SQL string.

    Attributes:
        line: 1-based line number
        col:  1-based column number
    """
    line: int
    col: int


class SqlSyntaxError(SimpleDBError):
    """
    Raised when tokenization/parsing fails due to invalid SQL syntax.

    Args:
        message: Human readable explanation.
        position: Optional Position indicating where the error occurred.
    """

    def __init__(self, message: str, position: Position | None = None):
        self.message = message
        self.position = position
        super().__init__(self.__str__())

    def __str__(self) -> str:
        if self.position is None:
            return f"SqlSyntaxError: {self.message}"
        return f"SqlSyntaxError at line {self.position.line}, col {self.position.col}: {self.message}"


class ExecutionError(SimpleDBError):
    """
    Raised when a statement is syntactically valid but cannot be executed.

    Examples:
      - Missing table/column
      - Unsupported operation in current feature set
      - Type mismatch detected during execution
    """


class ConstraintError(SimpleDBError):
    """
    Raised when a data integrity constraint is violated.

    Examples:
      - PRIMARY KEY duplicate
      - UNIQUE duplicate
      - NOT NULL violation
    """