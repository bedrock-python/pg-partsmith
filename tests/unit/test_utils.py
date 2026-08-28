from datetime import UTC, timedelta, timezone, tzinfo
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy.dialects import postgresql

from pg_partsmith.utils import (
    build_ddl_statement,
    calculate_lock_id,
    format_duration_ms,
    orphan_comment_prefix,
    quote_identifier,
    quote_literal,
    split_qualified_name,
    timezone_name,
    to_regclass_argument,
)

# ── quote_identifier ────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "identifier,expected",
    [
        ("events", '"events"'),
        ("my_table", '"my_table"'),
        ("public.events", '"public"."events"'),
        ('a"b', '"a""b"'),
    ],
)
def test__quote_identifier__various_inputs__wraps_in_double_quotes(identifier: str, expected: str) -> None:
    # Arrange / Act / Assert
    assert quote_identifier(identifier) == expected


# ── to_regclass_argument ────────────────────────────────────────────────────────


def test__to_regclass_argument__mixed_case_relname__quotes_to_preserve_case() -> None:
    # Arrange / Act / Assert — unquoted identifiers fold to lowercase; quoting keeps
    # adopted mixed-case relnames resolvable via to_regclass()
    assert to_regclass_argument("events__2024_W12") == '"events__2024_W12"'


def test__to_regclass_argument__schema_qualified_mixed_case_relname__quotes_both_parts() -> None:
    # Arrange / Act / Assert
    assert to_regclass_argument("public.events__2024_W12") == '"public"."events__2024_W12"'


# ── build_ddl_statement ─────────────────────────────────────────────────────────


def test__build_ddl_statement__literal_brackets_and_template__replaces_only_template_placeholders() -> None:
    # Arrange
    sql = "SELECT ARRAY[1,2,3] AS arr, [value] AS v FROM {table}"

    # Act
    stmt = build_ddl_statement(sql, value="x", table="events")
    result = str(stmt)

    # Assert
    assert "ARRAY[1,2,3]" in result
    assert "'x'" in result
    assert '"events"' in result


def test__build_ddl_statement__literal_containing_colon__no_phantom_bind_parameters() -> None:
    # Arrange — e.g. a pre-existing table comment; ":tag" must not become a bind parameter
    stmt = build_ddl_statement(
        "COMMENT ON TABLE {partition} IS [comment]",
        partition="events__2024_01",
        comment="pg-partsmith:orphan-parent=public.events see :tag",
    )

    # Act
    compiled = stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})

    # Assert — the literal colons survive verbatim and no bind params were created
    assert "pg-partsmith:orphan-parent=public.events see :tag" in str(compiled)
    assert compiled.params == {}


# ── calculate_lock_id ───────────────────────────────────────────────────────────


def test__calculate_lock_id__returns_64bit_signed_integer() -> None:
    # Arrange / Act
    lock_id = calculate_lock_id("events")

    # Assert
    assert isinstance(lock_id, int)
    assert -0x8000000000000000 <= lock_id <= 0x7FFFFFFFFFFFFFFF


def test__calculate_lock_id__same_table__returns_same_id() -> None:
    # Arrange / Act / Assert
    assert calculate_lock_id("events") == calculate_lock_id("events")


def test__calculate_lock_id__different_tables__return_different_ids() -> None:
    # Arrange / Act / Assert
    assert calculate_lock_id("events") != calculate_lock_id("users")


def test__calculate_lock_id__different_prefixes__return_different_ids() -> None:
    # Arrange / Act
    a = calculate_lock_id("events", prefix="app1")
    b = calculate_lock_id("events", prefix="app2")

    # Assert
    assert a != b


# ── format_duration_ms ──────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "ms,expected",
    [
        (0, "0ms"),
        (500, "500ms"),
        (2500, "2.50s"),
        (125000, "2.1m"),
        (7500000, "2.1h"),
    ],
)
def test__format_duration_ms__various_durations__formats_with_correct_unit(ms: int, expected: str) -> None:
    # Arrange / Act / Assert
    assert format_duration_ms(ms) == expected


# ── orphan_comment_prefix ───────────────────────────────────────────────────────


def test__orphan_comment_prefix__default__returns_stable_string() -> None:
    # Arrange / Act / Assert
    assert orphan_comment_prefix() == "pg-partsmith:orphan-parent="


def test__orphan_comment_prefix__non_string_marker_prefix__raises_type_error() -> None:
    # Arrange / Act / Assert
    with pytest.raises(TypeError, match="marker_prefix must be a str or None"):
        orphan_comment_prefix(marker_prefix=123)


def test__orphan_comment_prefix__blank_marker_prefix__raises_value_error() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="marker_prefix must be a non-empty string or None"):
        orphan_comment_prefix(marker_prefix="")


# ── split_qualified_name ────────────────────────────────────────────────────────


def test__split_qualified_name__empty_string__raises_value_error() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="Name cannot be empty"):
        split_qualified_name("")


def test__split_qualified_name__three_part_name__raises_value_error() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="Invalid qualified name"):
        split_qualified_name("a.b.c")


# ── timezone_name ───────────────────────────────────────────────────────────────


class _KeylessTz(tzinfo):
    """A tzinfo-like object carrying no IANA key."""


def test__timezone_name__datetime_utc__returns_utc() -> None:
    # Arrange / Act / Assert
    assert timezone_name(UTC) == "UTC"


def test__timezone_name__zoneinfo__returns_iana_key() -> None:
    # Arrange / Act / Assert
    assert timezone_name(ZoneInfo("Europe/Moscow")) == "Europe/Moscow"


def test__timezone_name__fixed_offset__raises_value_error() -> None:
    # Arrange / Act / Assert — timezone(timedelta(...)) has no name PostgreSQL understands
    with pytest.raises(ValueError, match="Unsupported timezone"):
        timezone_name(timezone(timedelta(hours=3)))


def test__timezone_name__keyless_tzinfo_like_object__raises_value_error() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="Unsupported timezone"):
        timezone_name(_KeylessTz())


# ── quote_literal and hostile escaping rules ────────────────────────────────────


def test__quote_literal__plain_value__stays_a_plain_literal() -> None:
    # Arrange / Act / Assert -- the common case must not grow an E prefix.
    assert quote_literal("eu-west") == "'eu-west'"
    assert quote_literal("O'Brien") == "'O''Brien'"


def test__quote_literal__value_with_a_backslash__escapes_it_explicitly() -> None:
    # Arrange / Act -- a backslash is an escape character while
    # standard_conforming_strings is off, which any client can turn off.
    quoted = quote_literal("c:\\tmp\\")

    # Assert -- an E-string reads the same either way, so a trailing backslash
    # cannot swallow the closing quote and let the rest of the value run as SQL.
    assert quoted == "E'c:\\\\tmp\\\\'"


def test__quote_literal__backslash_and_quote_together__escapes_both() -> None:
    # Arrange / Act
    quoted = quote_literal("a\\'; DROP TABLE t; --")

    # Assert
    assert quoted == "E'a\\\\''; DROP TABLE t; --'"


def test__quote_literal__value_with_a_nul__is_refused() -> None:
    # Arrange / Act / Assert -- the driver truncates at the NUL rather than
    # escaping it, so what reaches the server is a prefix of what was built.
    with pytest.raises(ValueError, match="NUL byte"):
        quote_literal("a\x00b")


def test__quote_identifier__name_with_a_nul__is_refused() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="NUL byte"):
        quote_identifier("a\x00b")


def test__build_ddl_statement__bracket_in_the_template_itself__is_left_alone() -> None:
    # Arrange / Act -- rewriting every [word] made a template mentioning an
    # array subscript fail on a format field that was never a placeholder.
    stmt = build_ddl_statement("SELECT 1 -- array[0]")

    # Assert
    assert "array[0]" in str(stmt)


def test__build_ddl_statement__named_literal_placeholder__is_still_substituted() -> None:
    # Arrange / Act
    stmt = build_ddl_statement("COMMENT ON TABLE {table} IS [note]", table="events", note="owner:pg-partsmith")

    # Assert -- and the colon in the value is not read as a bind parameter.
    assert str(stmt) == "COMMENT ON TABLE \"events\" IS 'owner:pg-partsmith'"
