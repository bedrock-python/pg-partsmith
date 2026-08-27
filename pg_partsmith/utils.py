"""Utility functions for PostgreSQL partitioning."""

from __future__ import annotations

import functools
import hashlib
import logging
import re
import time
from datetime import UTC, tzinfo

from sqlalchemy import TextClause, text

from .constants import DEFAULT_LOCK_PREFIX, MAX_IDENTIFIER_LENGTH, PG_CHECK_VIOLATION
from .protocols import DdlTimezoneAware, TimezoneAwareCalculator

logger = logging.getLogger(__name__)

# Placeholder syntax for build_ddl_statement templates.
_ID_PLACEHOLDER_PATTERN = re.compile(r"\{([a-zA-Z0-9_]+)\}")
_LIT_PLACEHOLDER_PATTERN = re.compile(r"\[([a-zA-Z0-9_]+)\]")

# Safe characters for marker_prefix (no SQL injection risk in COMMENT).
_MARKER_PREFIX_ALLOWED = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-=:")

_TIMEZONE_NAME_PATTERN = re.compile(r"^[A-Za-z0-9/_+.\-]+$")


def to_regclass_argument(identifier: str) -> str:
    """Return a safe ``to_regclass(text)`` argument preserving identifier case.

    Partition names come from ``pg_class.relname`` and may be mixed-case for
    tables adopted from other tools; quoting each part keeps such names
    resolvable and blocks case-folding. Deliberately an intent-alias of
    :func:`quote_identifier` marking values bound into ``to_regclass(:name)``
    rather than spliced into DDL.
    """
    return quote_identifier(identifier)


def split_qualified_name(name: str) -> tuple[str | None, str]:
    """Split a potentially schema-qualified name into (schema, relname)."""
    name = name.strip()
    if not name:
        msg = "Name cannot be empty"
        raise ValueError(msg)

    parts = name.split(".")
    if len(parts) == 1:
        return None, parts[0]
    if len(parts) == 2 and parts[0] and parts[1]:
        return parts[0], parts[1]

    msg = f"Invalid qualified name (expected schema.name): {name!r}"
    raise ValueError(msg)


def qualify(schema: str | None, relname: str) -> str:
    """Return schema-qualified name when schema is provided."""
    return f"{schema}.{relname}" if schema else relname


def build_ddl_statement(template: str, **params: str) -> TextClause:
    """Safely build DDL statement with quoted identifiers and literals.

    Placeholders like {table} are treated as identifiers (quoted with ").
    Placeholders like [value] are treated as literals (quoted with ').

    Args:
        template: SQL template with placeholders like {table} and [value].
        **params: Mapping of placeholder keys to unquoted values.

    Returns:
        SQLAlchemy text clause with safe values.
    """
    format_params = {}

    for match in _ID_PLACEHOLDER_PATTERN.finditer(template):
        key = match.group(1)
        if key in params:
            format_params[key] = quote_identifier(params[key])

    for match in _LIT_PLACEHOLDER_PATTERN.finditer(template):
        key = match.group(1)
        if key in params:
            format_params[key] = quote_literal(params[key])

    # Convert literal placeholders [key] to standard {key} for .format()
    formatted_template = _LIT_PLACEHOLDER_PATTERN.sub(r"{\1}", template)
    statement = formatted_template.format(**format_params)
    # Escape colons so identifiers/literals containing ":" (e.g. a pre-existing
    # table comment) are not parsed by text() as bind parameters.
    return text(statement.replace(":", r"\:"))


def quote_identifier(identifier: str) -> str:
    """Quote SQL identifier to prevent injection and validate length."""
    parts = identifier.split(".")
    if any(not p for p in parts):
        msg = f"Invalid qualified identifier: {identifier!r}"
        raise ValueError(msg)
    if len(parts) > 2:
        msg = f"Only schema-qualified identifiers are supported (schema.name): {identifier!r}"
        raise ValueError(msg)

    for p in parts:
        if len(p.encode("utf-8")) > MAX_IDENTIFIER_LENGTH:
            raise ValueError(f"Identifier part too long (max {MAX_IDENTIFIER_LENGTH} bytes): {p!r}")

    # Always wrap each part in double quotes to prevent case-folding and SQL injection.
    return ".".join('"' + p.replace('"', '""') + '"' for p in parts)


def quote_literal(value: str) -> str:
    """Quote SQL string literal to prevent injection.

    Args:
        value: Value to quote.

    Returns:
        Quoted string literal.
    """
    return "'" + value.replace("'", "''") + "'"


def coerce_str(value: object, encoding: str = "utf-8") -> str | None:
    """Coerce a driver-returned value to ``str``.

    ``None`` stays ``None``; ``bytes`` are decoded with ``errors="replace"``;
    anything else goes through ``str()``.
    """
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode(encoding, errors="replace")
    return str(value)


def format_duration_ms(duration_ms: int) -> str:
    """Format duration in milliseconds to human-readable string.

    Args:
        duration_ms: Duration in milliseconds.

    Returns:
        Formatted duration string.
    """
    if duration_ms < 1000:
        return f"{duration_ms}ms"

    seconds = duration_ms / 1000
    if seconds < 60:
        return f"{seconds:.2f}s"

    minutes = seconds / 60
    if minutes < 60:
        return f"{minutes:.1f}m"

    hours = minutes / 60
    return f"{hours:.1f}h"


def elapsed_ms(start: float) -> int:
    """Milliseconds elapsed since a ``time.perf_counter()`` reading."""
    return int((time.perf_counter() - start) * 1000)


def describe_exception(exc: BaseException) -> str:
    """Render an exception as ``TypeName: message`` (bare ``TypeName`` when empty)."""
    msg = str(exc)
    return f"{type(exc).__name__}: {msg}" if msg else type(exc).__name__


@functools.lru_cache(maxsize=1024)
def calculate_lock_id(table_name: str, prefix: str = DEFAULT_LOCK_PREFIX) -> int:
    """Calculate PostgreSQL advisory lock ID from table name.

    Uses SHA256 hash to generate a consistent 64-bit lock ID.
    Lru_cache is used to avoid redundant hashing for the same table and prefix.

    Args:
        table_name: Table name.
        prefix: Prefix for lock namespace.

    Returns:
        Advisory lock ID (64-bit integer).
    """
    key = f"{prefix}:{table_name}".encode()
    digest = hashlib.sha256(key).digest()
    # Use 64 bits for the lock ID to minimize collision probability.
    # PostgreSQL bigints are signed 8-byte integers.
    return int.from_bytes(digest[:8], byteorder="big", signed=True)


def orphan_table_comment(parent_table: str, *, marker_prefix: str | None = None) -> str:
    """Return the COMMENT marker used to track detached-but-not-dropped tables."""
    return f"{_resolve_marker_prefix(marker_prefix)}{parent_table}"


def orphan_comment_prefix(*, marker_prefix: str | None = None) -> str:
    """Return the COMMENT marker prefix used for orphaned partitions."""
    return _resolve_marker_prefix(marker_prefix)


def pg_sqlstate(exc: BaseException) -> str | None:
    """Extract PostgreSQL SQLSTATE code from SQLAlchemy/DBAPI exception.

    Works with asyncpg (exc.orig.sqlstate) and psycopg2 (exc.orig.pgcode).

    Args:
        exc: Exception from database operation.

    Returns:
        SQLSTATE code if available, None otherwise.
    """
    # SQLAlchemy typically wraps the original DBAPI exception in .orig
    orig = getattr(exc, "orig", exc)

    # Check for common SQLSTATE attribute names (asyncpg, psycopg2, etc.)
    for attr in ("sqlstate", "pgcode"):
        state = getattr(orig, attr, None)
        if isinstance(state, str):
            return state

    return None


def is_default_partition_conflict(exc: BaseException) -> bool:
    """Check if an error is a DEFAULT-partition constraint conflict on ATTACH."""
    if pg_sqlstate(exc) != PG_CHECK_VIOLATION:
        return False

    error_text = str(exc).lower()
    return (
        "updated partition constraint" in error_text
        and "default partition" in error_text
        and "would be violated" in error_text
    )


def validate_ddl_timeout(val: float) -> float:
    """Validate DDL timeout value."""
    val = float(val)
    if val <= 0:
        raise ValueError(f"ddl_timeout_seconds must be positive, got {val!r}")
    return val


def validate_timezone(tz: str | None) -> str | None:
    """Validate PostgreSQL timezone string."""
    if tz is None:
        return None
    tz = tz.strip()
    if not tz:
        raise ValueError("ddl_timezone must be a non-empty string or None")
    if not _TIMEZONE_NAME_PATTERN.match(tz):
        raise ValueError(f"Invalid characters in ddl_timezone: {tz!r}")
    return tz


def timezone_name(tz: tzinfo) -> str:
    """Return the IANA name of a timezone object, usable in ``SET LOCAL TIME ZONE``.

    Only ``datetime.UTC`` and :class:`zoneinfo.ZoneInfo` instances carry a name
    PostgreSQL understands; fixed-offset or third-party tzinfo objects do not
    and are rejected.

    Args:
        tz: Timezone object.

    Returns:
        ``"UTC"`` or the ZoneInfo IANA key.

    Raises:
        ValueError: If ``tz`` is not ``datetime.UTC`` or a keyed ``ZoneInfo``.
    """
    if tz is UTC:
        return "UTC"
    key = getattr(tz, "key", None)
    if isinstance(key, str) and key:
        validated = validate_timezone(key)
        if validated is not None:
            return validated
    msg = f"Unsupported timezone object {tz!r}: pass datetime.UTC or a zoneinfo.ZoneInfo instance with an IANA key"
    raise ValueError(msg)


def validate_timezone_alignment(repo: object, calculator: object) -> None:
    """Refuse a wiring whose calculator and DDL timezones disagree.

    Periods and partition names are computed in the calculator's timezone,
    while naive boundary literals are materialized under the repository's
    ``ddl_timezone`` — a silent mismatch would shift real partition bounds
    relative to their names. Implementations without timezone metadata
    (custom repositories/calculators) are not checked.
    """
    if not isinstance(calculator, TimezoneAwareCalculator) or not isinstance(repo, DdlTimezoneAware):
        return
    # Runtime-checkable protocols only verify attribute presence, so mocks and
    # loose implementations may still yield non-string values — re-check them.
    calc_tz: object = calculator.timezone_name
    ddl_tz: object = repo.ddl_timezone
    if not isinstance(calc_tz, str):
        return
    if ddl_tz is None:
        if calc_tz.lower() != "utc":
            logger.warning(
                "ddl_timezone=None trusts the session timezone; alignment with the "
                "calculator timezone cannot be guaranteed",
                extra={"calculator_timezone": calc_tz},
            )
        return
    if isinstance(ddl_tz, str) and ddl_tz.lower() != calc_tz.lower():
        msg = (
            f"Timezone mismatch: the period calculator works in {calc_tz!r} but repository "
            f"DDL runs in {ddl_tz!r}. Pass ddl_timezone={calc_tz!r} to the repository, or "
            "align the calculator's tz."
        )
        raise ValueError(msg)


def validate_int(val: int, name: str, min_val: int | None = None) -> int:
    """Validate integer value."""
    if not isinstance(val, int) or isinstance(val, bool):
        raise TypeError(f"{name} must be an int, got {type(val).__name__}")
    if min_val is not None and val < min_val:
        raise ValueError(f"{name} must be >= {min_val}, got {val!r}")
    return val


def validate_float(val: float, name: str, min_val: float | None = None) -> float:
    """Validate float value."""
    if not isinstance(val, (int, float)) or isinstance(val, bool):
        raise TypeError(f"{name} must be a number, got {type(val).__name__}")
    val = float(val)
    if min_val is not None and val < min_val:
        raise ValueError(f"{name} must be >= {min_val}, got {val!r}")
    return val


def _resolve_marker_prefix(marker_prefix: object) -> str:
    """Resolve the orphan marker prefix.

    Custom marker_prefix is restricted to alphanumeric, dot, underscore, hyphen, and
    colon to prevent SQL injection when the prefix is used in COMMENT ON TABLE.

    Args:
        marker_prefix: Custom marker prefix. When None, the library default is used.

    Returns:
        A non-empty prefix string.

    Raises:
        TypeError: If marker_prefix is not a str or None.
        ValueError: If marker_prefix is empty, blank, or contains disallowed characters.
    """
    if marker_prefix is None:
        return _orphan_comment_prefix()

    if not isinstance(marker_prefix, str):
        msg = f"marker_prefix must be a str or None, got {type(marker_prefix).__name__}"
        raise TypeError(msg)

    prefix = marker_prefix.strip()
    if not prefix:
        msg = "marker_prefix must be a non-empty string or None"
        raise ValueError(msg)

    if not all(c in _MARKER_PREFIX_ALLOWED for c in prefix):
        msg = "marker_prefix may only contain alphanumeric characters, dot, underscore, hyphen, and colon"
        raise ValueError(msg)

    return prefix


@functools.lru_cache(maxsize=1)
def _orphan_comment_prefix() -> str:
    """Return the COMMENT marker prefix for orphaned partitions.

    The prefix is deterministic and derived from the import package name
    (``pg_partsmith`` -> ``pg-partsmith``).
    A stable default is important for orphan discovery across environments.
    """
    pkg = __package__.split(".", 1)[0]
    name = pkg.replace("_", "-").strip().lower()

    return f"{name}:orphan-parent="
