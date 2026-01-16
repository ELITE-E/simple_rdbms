"""
repl.py

Interactive REPL (Read-Eval-Print Loop) for the SimpleDB mini-RDBMS.

Responsibilities:
- Provide a CLI shell for executing SQL-like statements against a DB directory.
- Support multiline SQL input until a semicolon ';' is entered outside of quotes.
- Display SELECT results in a readable table format.
- Provide small meta-commands for introspection:
    - .help
    - .exit / .quit
    - .tables
    - .schema <table>

Usage:
    python repl.py ./my_db_dir
If no directory is provided, defaults to ./simpledb_data
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Iterable

try:
    import readline  # noqa: F401
except Exception:
    # readline is optional; if missing, REPL still works.
    readline = None  # type: ignore[assignment]

from simpledb import CommandOk, Database, QueryResult
from simpledb.errors import SimpleDBError


PROMPT = "simpledb> "
PROMPT_CONT = "....> "


def is_complete_statement(buf: str) -> bool:
    """
    Decide whether the current buffer contains at least one complete statement.

    A statement is considered complete when a semicolon ';' appears outside of
    single-quoted string literals.

    Args:
        buf: Current accumulated input buffer.

    Returns:
        True if complete, else False.
    """
    in_str = False
    for ch in buf:
        if ch == "'":
            in_str = not in_str
        elif ch == ";" and not in_str:
            return True
    return False


def format_table(columns: list[str], rows: list[list[object]]) -> str:
    """
    Pretty-print a QueryResult as an aligned ASCII table.

    Args:
        columns: Column header list.
        rows: Row values list.

    Returns:
        A formatted string suitable for printing to console.
    """
    cols = [str(c) for c in columns]
    str_rows = [[("" if v is None else str(v)) for v in r] for r in rows]

    widths = [len(c) for c in cols]
    for r in str_rows:
        for i, cell in enumerate(r):
            widths[i] = max(widths[i], len(cell))

    def fmt_row(r: Iterable[str]) -> str:
        return " | ".join(cell.ljust(widths[i]) for i, cell in enumerate(r))

    sep = "-+-".join("-" * w for w in widths)

    out: list[str] = []
    out.append(fmt_row(cols))
    out.append(sep)
    for r in str_rows:
        out.append(fmt_row(r))
    return "\n".join(out)


def print_result(res) -> None:
    """
    Print a Database execution result.

    Args:
        res: CommandOk or QueryResult (or unexpected object).
    """
    if isinstance(res, CommandOk):
        print(res.message)
        if res.rows_affected:
            print(f"rows_affected={res.rows_affected}")
        return

    if isinstance(res, QueryResult):
        print(format_table(res.columns, res.rows))
        print(f"({len(res.rows)} row(s))")
        if res.stats:
            print(f"stats: {res.stats}")
        return

    print(res)


def cmd_tables(db: Database) -> None:
    """
    Meta-command: list all tables from catalog.

    Args:
        db: Database instance.
    """
    names = sorted(db.catalog.tables.keys())
    if not names:
        print("(no tables)")
        return
    for n in names:
        print(n)


def cmd_schema(db: Database, table: str) -> None:
    """
    Meta-command: print table schema and indexes.

    Args:
        db: Database instance.
        table: Table name.
    """
    t = db.catalog.tables.get(table)
    if not t:
        print(f"Table not found: {table}")
        return

    print(f"TABLE {t.name}")
    for c in t.columns:
        type_str = c.typ.name + (f"({','.join(map(str, c.typ.params))})" if c.typ.params else "")
        flags: list[str] = []
        if c.primary_key:
            flags.append("PRIMARY KEY")
        if c.unique:
            flags.append("UNIQUE")
        if c.not_null:
            flags.append("NOT NULL")
        suffix = (" " + " ".join(flags)) if flags else ""
        print(f"  - {c.name} {type_str}{suffix}")

    if t.indexes:
        print("INDEXES")
        for iname, idx in t.indexes.items():
            print(f"  - {iname} ON {idx.table_name}({idx.column_name})")


def repl(db_dir: Path) -> int:
    """
    Run the interactive REPL.

    Args:
        db_dir: Database directory.

    Returns:
        Process exit code (0 on normal exit).
    """
    db = Database.open(db_dir)
    print(f"SimpleDB REPL (db_dir={db_dir})")
    print("Type .help for commands. End SQL with ';'.")

    buf = ""
    while True:
        try:
            prompt = PROMPT if not buf else PROMPT_CONT
            line = input(prompt)
        except EOFError:
            print()
            return 0
        except KeyboardInterrupt:
            # Clear current buffer on Ctrl+C
            print()
            buf = ""
            continue

        line_stripped = line.strip()

        # Meta commands only apply if we're not in the middle of a multi-line SQL buffer.
        if not buf and line_stripped.startswith("."):
            parts = line_stripped.split()
            cmd = parts[0].lower()

            if cmd in (".exit", ".quit"):
                return 0

            if cmd == ".help":
                print("Meta commands:")
                print("  .help              show this help")
                print("  .tables            list tables")
                print("  .schema <table>    show table schema + indexes")
                print("  .exit / .quit      exit")
                print()
                print("SQL statements end with ';'. Example:")
                print("  CREATE TABLE users (id INTEGER PRIMARY KEY, email VARCHAR(255) UNIQUE NOT NULL);")
                print("  INSERT INTO users (id, email) VALUES (1, 'a@b.com');")
                print("  SELECT * FROM users;")
                continue

            if cmd == ".tables":
                cmd_tables(db)
                continue

            if cmd == ".schema":
                if len(parts) != 2:
                    print("Usage: .schema <table>")
                else:
                    cmd_schema(db, parts[1])
                continue

            print(f"Unknown command: {cmd}. Type .help")
            continue

        buf += line + "\n"
        if not is_complete_statement(buf):
            continue

        # Execute buffer as a script (supports multiple statements separated by ;)
        try:
            results = db.execute_script(buf)
            for r in results:
                print_result(r)
        except SimpleDBError as e:
            print(e)
        except Exception as e:
            # Unexpected internal error; keep REPL alive but show message
            print(f"Internal error: {e}")

        buf = ""


def main(argv: list[str]) -> int:
    """
    CLI entrypoint.

    Args:
        argv: sys.argv list.

    Returns:
        Exit code.
    """
    db_dir = Path(argv[1]) if len(argv) > 1 else Path("./simpledb_data")
    return repl(db_dir)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))