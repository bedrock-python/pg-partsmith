"""The ``pg-partsmith`` command line.

Requires the ``cli`` extra::

    pip install "pg-partsmith[cli]"

Five commands over a configuration document and a DSN: ``inspect``, ``plan``
and ``validate`` issue no DDL, ``apply`` withholds every destructive
operation unless it is asked for them, and ``backfill`` moves a DEFAULT
partition's rows into the partitions they belong in. ``plan`` and ``apply`` stay separable,
with a saved plan as the artifact between them: what this reads, prints and
writes is the library's own :class:`~pg_partsmith.MaintenancePlan`, not a
summary of one.
"""

from .exit_codes import ExitCode
from .loader import ConfigError
from .main import app, main

__all__ = ["ConfigError", "ExitCode", "app", "main"]
