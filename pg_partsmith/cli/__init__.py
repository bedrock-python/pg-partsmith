"""The ``pg-partsmith`` command line.

Requires the ``cli`` extra::

    pip install "pg-partsmith[cli]"

Three read-only commands so far -- ``inspect``, ``plan`` and ``validate`` --
over a configuration document and a DSN. ``plan`` and ``apply`` stay separable:
what this reads and prints is the library's own
:class:`~pg_partsmith.MaintenancePlan`, not a summary of one.
"""

from .exit_codes import ExitCode
from .loader import ConfigError
from .main import build_parser, main

__all__ = ["ConfigError", "ExitCode", "build_parser", "main"]
