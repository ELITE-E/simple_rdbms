from __future__ import annotations

import sys
from pathlib import Path
from typing import Iterable

try:
    import readline  # noqa: F401  # enables history/editing on many systems
except Exception:
    readline = None  # type: ignore[assignment]

from simpledb import Database, CommandOk, QueryResult
from simpledb.errors import SimpleDBError


PROMPT = "simpledb> "
PROMPT_CONT = "....> "


def is_complete_statement(buf: str) -> bool:
    """
    Consider input complete if we have a semicolon outside single quotes.
    """
    in_str = False
    for ch in buf:
        if ch == "'":
            in_str = not in_str
        elif ch == ";" and not in_str:
            return True
    return False


def format_table(columns: list[str], rows: list[list[object]]) -> str:
    cols = [str(c) for c in columns]
    str_rows = [[("" if v is None else str(v)) for v in r] for r in rows]

    widths = [len(c) for c in cols]
    for r in str_rows:
        for i, cell in enumerate(r):
            widths[i] = max(widths[i], len(cell))

    def fmt_row(r: Iterable[str]) -> str:
        return " | ".join(cell.ljust(widths[i]) for i, cell in enumerate(r))

    sep = "-+-".join("-" * w for w in widths)

    out = []
    out.append(fmt_row(cols))
    out.append(sep)
    for r in str_rows:
        out.append(fmt_row(r))
    return "\n".join(out)


def print_result(res):
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
    names = sorted(db.catalog.tables.keys())
    if not names:
        print("(no tables)")
        return
    for n in names:
        print(n)


def cmd_schema(db: Database, table: str) -> None:
    t = db.catalog.tables.get(table)
    if not t:
        print(f"Table not found: {table}")
        return

    print(f"TABLE {t.name}")
    for c in t.columns:
        parts = [c.name, c.typ.name + (f"({','.join(map(str, c.typ.params))})" if c.typ.params else "")]
        if c.primary_key:
            parts.append("PRIMARY KEY")
        if c.unique:
            parts.append("UNIQUE")
        if c.not_null:
            parts.append("NOT NULL")
        print("  - " + " ".join(parts))

    if t.indexes:
        print("INDEXES")
        for iname, idx in t.indexes.items():
            print(f"  - {iname} ON {idx.table_name}({idx.column_name})")


def repl(db_dir: Path) -> int:
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
            print()
            buf = ""
            continue

        line_stripped = line.strip()

        # meta commands (only when buffer is empty)
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

        # Execute SQL buffer (can include multiple statements)
        try:
            results = db.execute_script(buf)
            for r in results:
                print_result(r)
        except SimpleDBError as e:
            print(e)
        except Exception as e:
            # unexpected error
            print(f"Internal error: {e}")

        buf = ""


def main(argv: list[str]) -> int:
    db_dir = Path(argv[1]) if len(argv) > 1 else Path("./simpledb_data")
    return repl(db_dir)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))