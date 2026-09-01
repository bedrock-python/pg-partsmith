"""Pure parsing of partition bound expressions and relation-name safety checks.

Shared by the aio and sync metadata providers: everything here is IO-free, so
one implementation serves both mirrors.
"""

from __future__ import annotations

import logging
import re
from datetime import UTC, datetime, tzinfo

from dateutil.parser import isoparse

from .topology import DefaultBounds, HashBounds, ListBounds, PartitionBounds, RangeBounds

logger = logging.getLogger(__name__)

# Pre-compiled regex patterns for partition boundary parsing.
_RANGE_BOUND_PATTERN = re.compile(r"^\s*FOR\s+VALUES\s+FROM\s*\(", re.IGNORECASE)
_BOUNDARY_SEP_PATTERN = re.compile(r"\)\s+TO\s+\(", re.IGNORECASE)
_FROM_PREFIX_PATTERN = re.compile(r"^.*?FROM\s*\(", re.IGNORECASE | re.DOTALL)
_TRAILING_PAREN_PATTERN = re.compile(r"\)\s*$", re.IGNORECASE | re.DOTALL)
_CAST_PATTERN = re.compile(r"^CAST\((?P<inner>.*)\s+AS\s+.*\)$", re.IGNORECASE | re.DOTALL)
_STR_LITERAL_PATTERN = re.compile(r"'(?P<s>(?:[^']|'')*)'")
# Anchored, not searched: an unanchored search matches the same words *inside*
# a LIST value or a RANGE literal, and a partition whose bounds were misread as
# HASH is invisible to the planner, which then plans a duplicate.
_HASH_BOUND_PATTERN = re.compile(
    r"^\s*FOR\s+VALUES\s+WITH\s*\(\s*MODULUS\s+(?P<modulus>\d+)\s*,"
    r"\s*REMAINDER\s+(?P<remainder>\d+)\s*\)\s*$",
    re.IGNORECASE,
)
_LIST_BOUND_PATTERN = re.compile(r"^\s*FOR\s+VALUES\s+IN\s*\((?P<values>.*)\)\s*$", re.IGNORECASE | re.DOTALL)
_DATE_ONLY_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}")
_DATE_PREFIX_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}[T ]")

# Bound spellings that carry no instant at all.
_UNBOUNDED_LITERALS = frozenset({"MINVALUE", "MAXVALUE"})


def parse_range_boundaries(boundaries_expr: str | None) -> tuple[str | None, str | None]:
    """Parse boundary values from ``pg_get_expr(relpartbound, oid)`` output.

    The rendered expression can include casts and varying whitespace depending
    on the PostgreSQL version and the partition key type. This parser extracts
    stable boundary values for the common cases without fully parsing SQL.

    Under a composite key the leading element is returned: trailing columns are
    bounded with MINVALUE at both ends, so the partition holds exactly the rows
    whose leading column falls in that range, and that is the value retention
    and pruning reason about.

    Examples:
      FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')
      FOR VALUES FROM ('2024-01-01'::date) TO ('2024-02-01'::date)
      FOR VALUES FROM (1::bigint) TO (5::bigint)
      FOR VALUES FROM (MINVALUE) TO (MAXVALUE)
      FOR VALUES FROM ('2024-01-01', MINVALUE) TO ('2024-02-01', MINVALUE)
    """
    if not boundaries_expr:
        return None, None

    if boundaries_expr.strip().upper() == "DEFAULT":
        return None, None

    # Only RANGE bounds carry FROM/TO values; LIST/HASH expressions could
    # contain a ") TO (" inside a string value and must not be mis-parsed.
    if not _RANGE_BOUND_PATTERN.match(boundaries_expr):
        return None, None

    # Split into FROM and TO parts by finding ") TO (" which is the most
    # reliable separator for range boundaries.
    parts = _BOUNDARY_SEP_PATTERN.split(boundaries_expr)
    if len(parts) != 2:
        return None, None

    from_part = _FROM_PREFIX_PATTERN.sub("", parts[0])
    to_part = _TRAILING_PAREN_PATTERN.sub("", parts[1])

    return _leading_value(from_part), _leading_value(to_part)


def is_addressable(schema: str, relname: str) -> bool:
    """Return False for relations whose schema or name contains a dot.

    The library addresses relations as ``schema.relname`` strings, so a dot
    inside either part would be re-split into a different relation by
    ``quote_identifier`` — DDL could then target the wrong table.  Such
    partitions are never created by this library; skip them with a warning.
    """
    if "." in schema or "." in relname:
        logger.warning(
            "Skipping partition with '.' in its schema or name; not addressable by qualified-name DDL",
            extra={"partition_schema": schema, "partition_name": relname},
        )
        return False
    return True


def _strip_outer_parens(s: str) -> str:
    s = s.strip()
    # pg_get_expr sometimes wraps constants in extra parentheses
    while s.startswith("(") and s.endswith(")") and len(s) > 1:
        s = s[1:-1].strip()
    return s


def _normalize(expr: str) -> str:
    expr = _strip_outer_parens(expr)

    # CAST(x AS type) → x
    cast_match = _CAST_PATTERN.match(expr)
    if cast_match:
        expr = _strip_outer_parens(cast_match.group("inner"))

    # Prefer extracting a string literal if present.
    str_match = _STR_LITERAL_PATTERN.search(expr)
    if str_match:
        return str_match.group("s").replace("''", "'")

    # Strip ::type casts.
    if "::" in expr:
        expr = expr.split("::", 1)[0].strip()

    return expr.strip()


def parse_partition_bounds(boundaries_expr: str | None) -> PartitionBounds | None:
    """Parse ``pg_get_expr(relpartbound, oid)`` into a structured bound.

    Recognises every spelling PostgreSQL emits for a partition bound; returns
    ``None`` when the expression is absent or in a shape this parser does not
    understand, so callers can fall back to the raw text rather than acting on
    a guess.

    Examples:
      FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')
      FOR VALUES WITH (modulus 4, remainder 1)
      FOR VALUES IN ('eu', 'us')
      DEFAULT
    """
    if not boundaries_expr:
        return None

    expr = boundaries_expr.strip()
    if expr.upper() == "DEFAULT":
        return DefaultBounds()

    hash_match = _HASH_BOUND_PATTERN.match(expr)
    if hash_match:
        try:
            return HashBounds(
                modulus=int(hash_match.group("modulus")),
                remainder=int(hash_match.group("remainder")),
            )
        except ValueError:
            logger.warning("Unparseable hash bounds", extra={"boundaries_expr": expr})
            return None

    list_match = _LIST_BOUND_PATTERN.match(expr)
    if list_match:
        inner = _TRAILING_PAREN_PATTERN.sub("", list_match.group("values"))
        parts = _split_top_level(inner)
        # A bare NULL keyword and the three-character string 'NULL' normalise to
        # the same text, and they are not the same partition. Keeping them apart
        # is what stops the planner proposing one PostgreSQL already has.
        values = tuple(_normalize(part) for part in parts if not _is_null_keyword(part))
        includes_null = any(_is_null_keyword(part) for part in parts)
        return ListBounds(values=values, includes_null=includes_null)

    from_value, to_value = parse_range_boundaries(expr)
    if from_value is not None and to_value is not None:
        return RangeBounds(from_value=from_value, to_value=to_value)

    return None


def _split_top_level(values: str) -> list[str]:
    """Split a comma-separated bound list, ignoring commas inside quotes.

    Doubled quotes are how SQL escapes a quote inside a literal. Both
    characters have to be consumed together: stepping over only the first would
    leave the second to flip the in-quotes state, and every comma after it
    would then be read as part of the value.
    """
    parts: list[str] = []
    current: list[str] = []
    in_quotes = False
    index = 0

    while index < len(values):
        char = values[index]

        if char == "'":
            if in_quotes and values[index + 1 : index + 2] == "'":
                # Keep both characters: _normalize unescapes them later.
                current.append("''")
                index += 2
                continue
            in_quotes = not in_quotes

        if char == "," and not in_quotes:
            parts.append("".join(current))
            current = []
        else:
            current.append(char)
        index += 1

    parts.append("".join(current))
    return [p for p in (part.strip() for part in parts) if p]


def parse_boundary_literal(value: str | None, boundary_tz: tzinfo) -> datetime | None:
    """Parse a timestamp partition boundary into a UTC instant.

    Naive values (bare dates, timestamps without an offset) are interpreted in
    ``boundary_tz``; values carrying an offset are converted as-is. Comparisons
    downstream always happen between UTC instants.

    This is the decoder for the default, timestamp-keyed case. Tables keyed by
    an encoded identifier decode through their
    :class:`~pg_partsmith.boundaries.RangeBoundaryCodec` instead.
    """
    if value is None:
        return None
    v = value.strip()
    if not v:
        return None

    if v.upper() in _UNBOUNDED_LITERALS:
        return None

    if "-" not in v and ":" not in v:
        return None

    if _DATE_ONLY_PATTERN.fullmatch(v):
        try:
            return datetime.fromisoformat(v).replace(tzinfo=boundary_tz).astimezone(UTC)
        except ValueError:
            return None

    try:
        parsed = isoparse(v)
    except (KeyboardInterrupt, SystemExit):
        raise
    except (ValueError, TypeError):
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=boundary_tz).astimezone(UTC)
    return parsed.astimezone(UTC)


def is_naive_timestamp_literal(value: str | None) -> bool:
    """True when a boundary literal is a timestamp carrying no UTC offset.

    A ``timestamp`` or ``date`` key renders its bounds without one, so reading
    such a literal back means resolving it in *some* timezone -- and only the
    one it was written under gives the same instant. Anything that is not a
    timestamp at all (an encoded identifier, an integer, ``MAXVALUE``) is not
    naive in this sense: it answers False.
    """
    if value is None:
        return False
    v = value.strip()
    if not v or v.upper() in _UNBOUNDED_LITERALS:
        return False
    if _DATE_ONLY_PATTERN.fullmatch(v):
        return True
    if not _DATE_PREFIX_PATTERN.match(v):
        # ``isoparse`` reads reduced forms -- "2024-01", "2024-W01", "2024-001" --
        # as timestamps, and a text or encoded key may well look like one. Only a
        # full calendar date is taken as evidence that this is a timestamp at all.
        return False
    try:
        parsed = isoparse(v)
    except (KeyboardInterrupt, SystemExit):
        raise
    except (ValueError, TypeError):
        return False
    return parsed.tzinfo is None


def _leading_value(expr: str) -> str:
    """Normalise a bound element, taking the leading one of a composite tuple."""
    parts = _split_top_level(expr)
    return _normalize(parts[0]) if parts else _normalize(expr)


def _is_null_keyword(part: str) -> bool:
    """True when a list element is the NULL keyword rather than a string."""
    stripped = _strip_outer_parens(part)
    if "::" in stripped:
        stripped = stripped.split("::", 1)[0].strip()
    return _strip_outer_parens(stripped).upper() == "NULL"
