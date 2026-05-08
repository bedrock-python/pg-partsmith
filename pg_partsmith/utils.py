"""Utility functions for PostgreSQL partitioning."""

from __future__ import annotations

import functools
import hashlib
import re
from datetime import UTC, datetime

from sqlalchemy import TextClause, text

from .constants import MAX_IDENTIFIER_LENGTH


def utc_now() -> datetime:
    """Return current time as timezone-aware datetime in UTC."""
    return datetime.now(UTC)


# Pre-compiled regex patterns for DDL statement building.
_ID_PLACEHOLDER_PATTERN = re.compile(r"\{([a-zA-Z0-9_]+)\}")
_LIT_PLACEHOLDER_PATTERN = re.compile(r"\[([a-zA-Z0-9_]+)\]")


def to_regclass_argument(identifier: str) -> str:
    """Return a safe ``to_regclass(text)`` argument preserving identifier case.

    PostgreSQL folds unquoted identifiers to lowercase when parsing relation
    names in ``to_regclass``. Quoting each identifier part keeps case-sensitive
    names (for example, weekly partitions with ``..._W12``) resolvable.
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

    # Handle identifier placeholders {key}
    for match in _ID_PLACEHOLDER_PATTERN.finditer(template):
        key = match.group(1)
        if key in params:
            format_params[key] = quote_identifier(params[key])

    # Handle literal placeholders [key]
    for match in _LIT_PLACEHOLDER_PATTERN.finditer(template):
        key = match.group(1)
        if key in params:
            format_params[key] = quote_literal(params[key])

    # Convert literal placeholders [key] to standard {key} for .format()
    formatted_template = _LIT_PLACEHOLDER_PATTERN.sub(r"{\1}", template)
    return text(formatted_template.format(**format_params))


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


@functools.lru_cache(maxsize=1024)
def calculate_lock_id(table_name: str, prefix: str = "partitioner") -> int:
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


# Safe characters for marker_prefix (no SQL injection risk in COMMENT).
_MARKER_PREFIX_ALLOWED = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-=:")


def orphan_table_comment(parent_table: str, *, marker_prefix: str | None = None) -> str:
    """Return the COMMENT marker used to track detached-but-not-dropped tables."""
    return f"{_resolve_marker_prefix(marker_prefix)}{parent_table}"


def orphan_comment_prefix(*, marker_prefix: str | None = None) -> str:
    """Return the COMMENT marker prefix used for orphaned partitions."""
    return _resolve_marker_prefix(marker_prefix)


def _resolve_marker_prefix(marker_prefix: str | None) -> str:
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

    # Defensive: protect against non-typed callers passing non-str values.
    if not isinstance(marker_prefix, str):  # type: ignore[unreachable]
        msg = f"marker_prefix must be a str or None, got {type(marker_prefix).__name__}"  # type: ignore[unreachable]
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
    if not re.match(r"^[A-Za-z0-9/_+.\-]+$", tz):
        raise ValueError(f"Invalid characters in ddl_timezone: {tz!r}")
    return tz


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
