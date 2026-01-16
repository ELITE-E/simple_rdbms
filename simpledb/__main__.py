"""
simpledb/__main__.py

Package entry point for running SimpleDB as a module:

    python -m simpledb [db_dir]

This also serves as the target for the console script entry point defined in
pyproject.toml:

    simpledb [db_dir]

Implementation notes:
- We reuse the existing top-level `repl.py` module (installed via py-modules)
  to avoid duplicating CLI logic.
"""

from __future__ import annotations

import sys


def main() -> int:
    """
    Entry point for `python -m simpledb` and the installed `simpledb` command.

    Returns:
        Exit code (0 for normal exit).
    """
    # Import here so packaging/runtime errors show cleanly at entry time.
    from repl import main as repl_main  # type: ignore

    # repl.main expects argv-like list and returns an int exit code.
    return int(repl_main(sys.argv))


if __name__ == "__main__":
    raise SystemExit(main())