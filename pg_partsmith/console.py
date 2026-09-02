"""The console script: what ``pip install pg-partsmith`` puts on PATH, with or without the extra.

The package registers ``pg-partsmith`` unconditionally, since a script cannot
be conditional on an extra. A bare install therefore has the command too, and
there one sentence and exit 64 beat a ModuleNotFoundError traceback. This
module imports nothing the extra brings until it is asked to run.
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = ["main"]

USAGE = 64
"""``EX_USAGE``: the command line itself is not usable here."""


def main(argv: Sequence[str] | None = None) -> int:
    """Run the command line, or say what is missing."""
    try:
        from pg_partsmith.cli import main as run  # noqa: PLC0415 - the import that needs the extra
    except ImportError as exc:
        sys.stderr.write(f'pg-partsmith needs the cli extra: pip install "pg-partsmith[cli]" ({exc})\n')
        return USAGE
    return run(argv)
