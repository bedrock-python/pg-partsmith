"""Quoting, naming, orphan markers and the small validators."""

import logging
import time
from datetime import UTC, datetime, timedelta, timezone, tzinfo
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy.dialects import postgresql

from pg_partsmith.utils import (
    DETACHED_AT_MARKER,
    build_ddl_statement,
    calculate_lock_id,
    coerce_str,
    describe_exception,
    elapsed_ms,
    format_duration_ms,
    is_default_partition_conflict,
    orphan_comment,
    orphan_comment_prefix,
    orphan_table_comment,
    parse_orphan_comment,
    pg_sqlstate,
    qualify,
    quote_identifier,
    quote_literal,
    split_qualified_name,
    timezone_name,
    to_regclass_argument,
    validate_ddl_timeout,
    validate_float,
    validate_int,
    validate_marker_alignment,
    validate_timezone,
    validate_timezone_alignment,
)

_DETACHED_AT = datetime(2026, 8, 24, 12, tzinfo=UTC)
_MARKER = "pg-partsmith:orphan-parent=public.events"
_DETACHED_LINE = "pg-partsmith:detached-at=2026-08-24T12:00:00+00:00"


class _KeylessTz(tzinfo):
    """A tzinfo-like object carrying no IANA key."""


class _Calculator:
    """A calculator declaring the zone it works in."""

    def __init__(self, name: object) -> None:
        self.tz = UTC
        self.timezone_name = name


class _Repository:
    """A repository declaring the zone its DDL runs in."""

    def __init__(self, ddl_timezone: object) -> None:
        self.ddl_timezone = ddl_timezone


class _Marked:
    """A repository or metadata provider declaring the prefix it marks orphans with."""

    def __init__(self, marker_prefix: object) -> None:
        self.marker_prefix = marker_prefix


class _AsyncpgError:
    def __init__(self, sqlstate: str) -> None:
        self.sqlstate = sqlstate


class _Psycopg2Error:
    def __init__(self, pgcode: str) -> None:
        self.pgcode = pgcode


class _DriverError(Exception):
    """What SQLAlchemy raises: the DBAPI exception hangs off ``orig``."""

    def __init__(self, message: str, orig: object) -> None:
        super().__init__(message)
        self.orig = orig


# -- quote_identifier ---------------------------------------------------------------------


@pytest.mark.parametrize(
    ("identifier", "expected"),
    [
        ("events", '"events"'),
        ("my_table", '"my_table"'),
        ("public.events", '"public"."events"'),
        ('a"b', '"a""b"'),
        ("Events", '"Events"'),
    ],
)
def test__quote_identifier__various_inputs__wraps_in_double_quotes(identifier: str, expected: str) -> None:
    # Arrange / Act / Assert
    assert quote_identifier(identifier) == expected


@pytest.mark.parametrize("identifier", ["a..b", ".events", "events."])
def test__quote_identifier__empty_part__is_refused(identifier: str) -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="Invalid qualified identifier"):
        quote_identifier(identifier)


def test__quote_identifier__three_parts__is_refused() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="Only schema-qualified identifiers are supported"):
        quote_identifier("db.public.events")


def test__quote_identifier__part_over_63_bytes__is_refused() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="Identifier part too long"):
        quote_identifier("public." + "x" * 64)


def test__quote_identifier__name_with_a_nul__is_refused() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="NUL byte"):
        quote_identifier("a\x00b")


# -- to_regclass_argument -------------------------------------------------------------


def test__to_regclass_argument__mixed_case_relname__quotes_to_preserve_case() -> None:
    # Arrange / Act / Assert -- unquoted identifiers fold to lowercase; quoting keeps adopted
    # mixed-case relnames resolvable via to_regclass()
    assert to_regclass_argument("events__2024_W12") == '"events__2024_W12"'


def test__to_regclass_argument__schema_qualified_mixed_case_relname__quotes_both_parts() -> None:
    # Arrange / Act / Assert
    assert to_regclass_argument("public.events__2024_W12") == '"public"."events__2024_W12"'


# -- quote_literal ---------------------------------------------------------------------


def test__quote_literal__plain_value__stays_a_plain_literal() -> None:
    # Arrange / Act / Assert -- the common case must not grow an E prefix
    assert quote_literal("eu-west") == "'eu-west'"
    assert quote_literal("O'Brien") == "'O''Brien'"


def test__quote_literal__value_with_a_backslash__escapes_it_explicitly() -> None:
    # Arrange / Act -- a backslash is an escape character while standard_conforming_strings is off,
    # which any client can turn off
    quoted = quote_literal("c:\\tmp\\")

    # Assert -- an E-string reads the same either way, so a trailing backslash cannot swallow the
    # closing quote and let the rest of the value run as SQL
    assert quoted == "E'c:\\\\tmp\\\\'"


def test__quote_literal__backslash_and_quote_together__escapes_both() -> None:
    # Arrange / Act
    quoted = quote_literal("a\\'; DROP TABLE t; --")

    # Assert
    assert quoted == "E'a\\\\''; DROP TABLE t; --'"


def test__quote_literal__value_with_a_nul__is_refused() -> None:
    # Arrange / Act / Assert -- the driver truncates at the NUL rather than escaping it
    with pytest.raises(ValueError, match="NUL byte"):
        quote_literal("a\x00b")


# -- build_ddl_statement ------------------------------------------------------------------


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
    # Arrange -- e.g. a pre-existing table comment; ":tag" must not become a bind parameter
    stmt = build_ddl_statement(
        "COMMENT ON TABLE {partition} IS [comment]",
        partition="events__2024_01",
        comment="pg-partsmith:orphan-parent=public.events see :tag",
    )

    # Act
    compiled = stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})

    # Assert -- the literal colons survive verbatim and no bind params were created
    assert "pg-partsmith:orphan-parent=public.events see :tag" in str(compiled)
    assert compiled.params == {}


def test__build_ddl_statement__bracket_in_the_template_itself__is_left_alone() -> None:
    # Arrange / Act -- rewriting every [word] made a template mentioning an array subscript fail on a
    # format field that was never a placeholder
    stmt = build_ddl_statement("SELECT 1 -- array[0]")

    # Assert
    assert "array[0]" in str(stmt)


def test__build_ddl_statement__named_literal_placeholder__is_still_substituted() -> None:
    # Arrange / Act
    stmt = build_ddl_statement("COMMENT ON TABLE {table} IS [note]", table="events", note="owner:pg-partsmith")

    # Assert -- and the colon in the value is not read as a bind parameter
    assert str(stmt) == "COMMENT ON TABLE \"events\" IS 'owner:pg-partsmith'"


def test__build_ddl_statement__schema_qualified_identifier__quotes_both_parts() -> None:
    # Arrange / Act
    stmt = build_ddl_statement("DROP TABLE {table}", table="analytics.events__2024_01")

    # Assert
    assert str(stmt) == 'DROP TABLE "analytics"."events__2024_01"'


def test__build_ddl_statement__identifier_placeholder_without_a_value__raises_key_error() -> None:
    # Arrange / Act / Assert -- an unfilled placeholder must not reach the server as literal braces
    with pytest.raises(KeyError):
        build_ddl_statement("DROP TABLE {table}")


# -- calculate_lock_id -----------------------------------------------------------------------


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


# -- format_duration_ms, elapsed_ms, describe_exception, coerce_str ----------------------------


@pytest.mark.parametrize(
    ("ms", "expected"),
    [(0, "0ms"), (500, "500ms"), (999, "999ms"), (1000, "1.00s"), (2500, "2.50s"), (125000, "2.1m"), (7500000, "2.1h")],
)
def test__format_duration_ms__various_durations__formats_with_correct_unit(ms: int, expected: str) -> None:
    # Arrange / Act / Assert
    assert format_duration_ms(ms) == expected


def test__elapsed_ms__since_a_perf_counter_reading__is_a_non_negative_int() -> None:
    # Arrange
    start = time.perf_counter()

    # Act
    elapsed = elapsed_ms(start)

    # Assert
    assert isinstance(elapsed, int)
    assert elapsed >= 0
    assert elapsed_ms(start - 1.5) >= 1500


def test__describe_exception__with_and_without_message__renders_type_and_message() -> None:
    # Arrange / Act / Assert
    assert describe_exception(ValueError("boom")) == "ValueError: boom"
    assert describe_exception(ValueError()) == "ValueError"


@pytest.mark.parametrize(
    ("value", "expected"),
    [(None, None), ("text", "text"), (b"bytes", "bytes"), (b"ab\xff", "ab\ufffd"), (5, "5")],
)
def test__coerce_str__driver_values__become_str_or_stay_none(value: object, expected: str | None) -> None:
    # Arrange / Act / Assert
    assert coerce_str(value) == expected


# -- qualify and split_qualified_name ------------------------------------------------------------


def test__qualify__with_and_without_schema__prefixes_only_when_given() -> None:
    # Arrange / Act / Assert
    assert qualify("public", "events") == "public.events"
    assert qualify(None, "events") == "events"
    assert qualify("", "events") == "events"


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("events", (None, "events")),
        ("public.events", ("public", "events")),
        ("  public.events  ", ("public", "events")),
    ],
)
def test__split_qualified_name__valid_names__split_into_schema_and_relname(
    name: str, expected: tuple[str | None, str]
) -> None:
    # Arrange / Act / Assert
    assert split_qualified_name(name) == expected


@pytest.mark.parametrize("name", ["", "   "])
def test__split_qualified_name__empty_string__raises_value_error(name: str) -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="Name cannot be empty"):
        split_qualified_name(name)


@pytest.mark.parametrize("name", ["a.b.c", ".events", "public."])
def test__split_qualified_name__malformed_name__raises_value_error(name: str) -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="Invalid qualified name"):
        split_qualified_name(name)


# -- orphan markers ----------------------------------------------------------------------------


def test__orphan_comment_prefix__default__returns_stable_string() -> None:
    # Arrange / Act / Assert
    assert orphan_comment_prefix() == "pg-partsmith:orphan-parent="


def test__orphan_comment_prefix__custom_marker_prefix__returned_stripped() -> None:
    # Arrange / Act / Assert
    assert orphan_comment_prefix(marker_prefix=" myapp:parent= ") == "myapp:parent="


def test__orphan_comment_prefix__non_string_marker_prefix__raises_type_error() -> None:
    # Arrange / Act / Assert
    with pytest.raises(TypeError, match="marker_prefix must be a str or None"):
        orphan_comment_prefix(marker_prefix=123)  # type: ignore[arg-type]


@pytest.mark.parametrize("prefix", ["", "   "])
def test__orphan_comment_prefix__blank_marker_prefix__raises_value_error(prefix: str) -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="marker_prefix must be a non-empty string or None"):
        orphan_comment_prefix(marker_prefix=prefix)


@pytest.mark.parametrize("prefix", ["my app:", "x'; DROP TABLE t; --", "pre\nfix", 'prefix"', "a,b"])
def test__orphan_comment_prefix__unsafe_characters__raises_value_error(prefix: str) -> None:
    # Arrange / Act / Assert -- the prefix is spliced into COMMENT ON TABLE
    with pytest.raises(ValueError, match="may only contain"):
        orphan_comment_prefix(marker_prefix=prefix)


def test__orphan_table_comment__default_prefix__marker_names_the_parent() -> None:
    # Arrange / Act / Assert
    assert orphan_table_comment("public.events") == _MARKER
    assert orphan_table_comment("public.events", marker_prefix="myapp:parent=") == "myapp:parent=public.events"


def test__detached_at_marker__is_namespaced_by_the_library_name() -> None:
    # Arrange / Act / Assert -- independent of marker_prefix so line two is unambiguous whatever line one says
    assert DETACHED_AT_MARKER == "pg-partsmith:detached-at="


def test__orphan_comment__fresh_table__marker_line_then_detached_at_line() -> None:
    # Arrange / Act
    comment = orphan_comment("public.events", detached_at=_DETACHED_AT, existing_comment=None)

    # Assert
    assert comment == f"{_MARKER}\n{_DETACHED_LINE}"


def test__orphan_comment__no_instant_known__writes_the_marker_line_only() -> None:
    # Arrange / Act
    comment = orphan_comment("public.events", detached_at=None, existing_comment=None)

    # Assert
    assert comment == _MARKER


def test__orphan_comment__re_marking__keeps_the_instant_already_recorded() -> None:
    # Arrange -- a repeated detach must not restart the grace period
    first = orphan_comment("public.events", detached_at=_DETACHED_AT, existing_comment=None)

    # Act
    second = orphan_comment("public.events", detached_at=_DETACHED_AT + timedelta(days=9), existing_comment=first)

    # Assert
    assert second == first


def test__orphan_comment__existing_user_comment__kept_below_the_marker_lines() -> None:
    # Arrange / Act
    comment = orphan_comment("public.events", detached_at=_DETACHED_AT, existing_comment="user note\nsecond line")

    # Assert
    assert comment == f"{_MARKER}\n{_DETACHED_LINE}\nuser note\nsecond line"


def test__orphan_comment__marker_from_an_older_version__gains_the_instant_and_keeps_the_note() -> None:
    # Arrange -- line one only, as 0.x wrote it, with a user note under it
    existing = "pg-partsmith:orphan-parent=public.old\nnote"

    # Act
    comment = orphan_comment("public.events", detached_at=_DETACHED_AT, existing_comment=existing)

    # Assert -- the marker is rewritten for the new parent, the note survives
    assert comment == f"{_MARKER}\n{_DETACHED_LINE}\nnote"


def test__orphan_comment__older_marker_and_no_instant__stays_without_an_instant() -> None:
    # Arrange / Act
    comment = orphan_comment("public.events", detached_at=None, existing_comment="pg-partsmith:orphan-parent=old\nnote")

    # Assert
    assert comment == f"{_MARKER}\nnote"


def test__orphan_comment__custom_marker_prefix__used_on_line_one() -> None:
    # Arrange / Act
    comment = orphan_comment(
        "public.events", detached_at=_DETACHED_AT, existing_comment=None, marker_prefix="myapp:parent="
    )

    # Assert
    assert comment == f"myapp:parent=public.events\n{_DETACHED_LINE}"


def test__orphan_comment__custom_prefix__strips_only_its_own_marker_lines() -> None:
    # Arrange -- a default-prefix marker written by another deployment is just a user line here
    existing = f"{_MARKER}\n{_DETACHED_LINE}"

    # Act
    comment = orphan_comment(
        "public.events", detached_at=None, existing_comment=existing, marker_prefix="myapp:parent="
    )

    # Assert -- the foreign marker line is kept as a user line; the detached-at line is the library's
    # whatever the prefix, so it is consumed rather than kept, and not re-emitted with no instant given
    assert comment == f"myapp:parent=public.events\n{_MARKER}"


def test__orphan_comment__non_utc_instant__normalised_to_utc() -> None:
    # Arrange
    moscow_noon = datetime(2026, 8, 24, 15, tzinfo=ZoneInfo("Europe/Moscow"))

    # Act
    comment = orphan_comment("public.events", detached_at=moscow_noon, existing_comment=None)

    # Assert
    assert comment == f"{_MARKER}\n{_DETACHED_LINE}"


def test__orphan_comment__round_trips_through_parse_orphan_comment() -> None:
    # Arrange
    comment = orphan_comment("public.events", detached_at=_DETACHED_AT, existing_comment="note")

    # Act / Assert
    assert parse_orphan_comment(comment) == ("public.events", _DETACHED_AT)


def test__parse_orphan_comment__full_marker__returns_parent_and_instant() -> None:
    # Arrange / Act
    parsed = parse_orphan_comment(f"{_MARKER}\n{_DETACHED_LINE}")

    # Assert
    assert parsed == ("public.events", _DETACHED_AT)


def test__parse_orphan_comment__marker_only__detached_at_is_none() -> None:
    # Arrange / Act / Assert -- written by an older version, or by adopt_partition
    assert parse_orphan_comment(_MARKER) == ("public.events", None)


def test__parse_orphan_comment__naive_detached_at__read_as_utc() -> None:
    # Arrange / Act
    parsed = parse_orphan_comment(f"{_MARKER}\npg-partsmith:detached-at=2026-08-24T12:00:00")

    # Assert
    assert parsed == ("public.events", _DETACHED_AT)
    assert parsed is not None
    assert parsed[1] is not None
    assert parsed[1].tzinfo is UTC


def test__parse_orphan_comment__offset_detached_at__kept_as_the_same_instant() -> None:
    # Arrange / Act
    parsed = parse_orphan_comment(f"{_MARKER}\npg-partsmith:detached-at=2026-08-24T15:00:00+03:00")

    # Assert
    assert parsed == ("public.events", _DETACHED_AT)


def test__parse_orphan_comment__malformed_detached_at__is_none() -> None:
    # Arrange / Act / Assert
    assert parse_orphan_comment(f"{_MARKER}\npg-partsmith:detached-at=last tuesday") == ("public.events", None)


def test__parse_orphan_comment__detached_at_below_a_user_line__still_found() -> None:
    # Arrange / Act / Assert
    assert parse_orphan_comment(f"{_MARKER}\nnote\n{_DETACHED_LINE}") == ("public.events", _DETACHED_AT)


@pytest.mark.parametrize(
    "comment",
    [None, "", "hello", "pg-partsmith:orphan-parent=", f"note\n{_MARKER}", f"{_DETACHED_LINE}"],
)
def test__parse_orphan_comment__comment_without_a_marker__returns_none(comment: str | None) -> None:
    # Arrange / Act / Assert
    assert parse_orphan_comment(comment) is None


def test__parse_orphan_comment__custom_prefix__matches_only_that_prefix() -> None:
    # Arrange
    custom = f"myapp:parent=public.events\n{_DETACHED_LINE}"

    # Act / Assert
    assert parse_orphan_comment(custom, marker_prefix="myapp:parent=") == ("public.events", _DETACHED_AT)
    assert parse_orphan_comment(custom) is None
    assert parse_orphan_comment(_MARKER, marker_prefix="myapp:parent=") is None


# -- SQLSTATE helpers -----------------------------------------------------------------------


def test__pg_sqlstate__asyncpg_style_exception__reads_sqlstate_off_orig() -> None:
    # Arrange / Act / Assert
    assert pg_sqlstate(_DriverError("boom", _AsyncpgError("23514"))) == "23514"


def test__pg_sqlstate__psycopg2_style_exception__reads_pgcode_off_orig() -> None:
    # Arrange / Act / Assert
    assert pg_sqlstate(_DriverError("boom", _Psycopg2Error("40P01"))) == "40P01"


def test__pg_sqlstate__bare_exception_with_its_own_sqlstate__reads_it_directly() -> None:
    # Arrange
    exc = ValueError("boom")
    exc.sqlstate = "42P01"  # type: ignore[attr-defined]

    # Act / Assert
    assert pg_sqlstate(exc) == "42P01"


def test__pg_sqlstate__exception_without_a_state__returns_none() -> None:
    # Arrange / Act / Assert
    assert pg_sqlstate(ValueError("boom")) is None
    assert pg_sqlstate(_DriverError("boom", _AsyncpgError(None))) is None  # type: ignore[arg-type]


def test__is_default_partition_conflict__check_violation_naming_the_default__is_true() -> None:
    # Arrange
    exc = _DriverError(
        'updated partition constraint for default partition "events_default" would be violated by some row',
        _AsyncpgError("23514"),
    )

    # Act / Assert
    assert is_default_partition_conflict(exc) is True


def test__is_default_partition_conflict__other_check_violation__is_false() -> None:
    # Arrange / Act / Assert
    assert is_default_partition_conflict(_DriverError("check constraint failed", _AsyncpgError("23514"))) is False


def test__is_default_partition_conflict__other_sqlstate__is_false() -> None:
    # Arrange
    exc = _DriverError("updated partition constraint for default partition would be violated", _AsyncpgError("40P01"))

    # Act / Assert
    assert is_default_partition_conflict(exc) is False
    assert is_default_partition_conflict(ValueError("x")) is False


# -- validators ----------------------------------------------------------------------------------


def test__validate_ddl_timeout__positive_number__returned_as_float() -> None:
    # Arrange / Act / Assert
    assert validate_ddl_timeout(30) == 30.0
    assert isinstance(validate_ddl_timeout(30), float)


@pytest.mark.parametrize("value", [0, -1, -0.5])
def test__validate_ddl_timeout__non_positive__raises_value_error(value: float) -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="ddl_timeout_seconds must be positive"):
        validate_ddl_timeout(value)


def test__validate_timezone__valid_names__stripped_and_returned() -> None:
    # Arrange / Act / Assert
    assert validate_timezone(" Europe/Moscow ") == "Europe/Moscow"
    assert validate_timezone("UTC") == "UTC"
    assert validate_timezone("Etc/GMT+3") == "Etc/GMT+3"
    assert validate_timezone(None) is None


@pytest.mark.parametrize("value", ["", "   "])
def test__validate_timezone__blank__raises_value_error(value: str) -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="ddl_timezone must be a non-empty string or None"):
        validate_timezone(value)


@pytest.mark.parametrize("value", ["Europe/Moscow; DROP", "Bad Zone", "UTC'"])
def test__validate_timezone__unsafe_characters__raises_value_error(value: str) -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="Invalid characters in ddl_timezone"):
        validate_timezone(value)


def test__validate_int__int_within_bounds__returned() -> None:
    # Arrange / Act / Assert
    assert validate_int(3, "n", min_val=1) == 3
    assert validate_int(-3, "n") == -3


@pytest.mark.parametrize("value", [True, 1.5, "1"])
def test__validate_int__not_an_int__raises_type_error(value: object) -> None:
    # Arrange / Act / Assert
    with pytest.raises(TypeError, match="n must be an int"):
        validate_int(value, "n")  # type: ignore[arg-type]


def test__validate_int__below_minimum__raises_value_error() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="n must be >= 1, got 0"):
        validate_int(0, "n", min_val=1)


def test__validate_float__number_within_bounds__returned_as_float() -> None:
    # Arrange / Act / Assert
    assert validate_float(2, "f", min_val=1.0) == 2.0
    assert isinstance(validate_float(2, "f"), float)


@pytest.mark.parametrize("value", [True, "1.5", None])
def test__validate_float__not_a_number__raises_type_error(value: object) -> None:
    # Arrange / Act / Assert
    with pytest.raises(TypeError, match="f must be a number"):
        validate_float(value, "f")  # type: ignore[arg-type]


def test__validate_float__below_minimum__raises_value_error() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match=r"f must be >= 1.0, got 0.5"):
        validate_float(0.5, "f", min_val=1.0)


# -- timezone_name ----------------------------------------------------------------------------


def test__timezone_name__datetime_utc__returns_utc() -> None:
    # Arrange / Act / Assert
    assert timezone_name(UTC) == "UTC"


def test__timezone_name__zoneinfo__returns_iana_key() -> None:
    # Arrange / Act / Assert
    assert timezone_name(ZoneInfo("Europe/Moscow")) == "Europe/Moscow"


def test__timezone_name__fixed_offset__raises_value_error() -> None:
    # Arrange / Act / Assert -- timezone(timedelta(...)) has no name PostgreSQL understands
    with pytest.raises(ValueError, match="Unsupported timezone"):
        timezone_name(timezone(timedelta(hours=3)))


def test__timezone_name__keyless_tzinfo_like_object__raises_value_error() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="Unsupported timezone"):
        timezone_name(_KeylessTz())


# -- validate_marker_alignment --------------------------------------------------------------


def test__validate_marker_alignment__same_prefix__passes() -> None:
    # Arrange / Act / Assert
    validate_marker_alignment(_Marked("app:orphan-parent="), _Marked("app:orphan-parent="))


def test__validate_marker_alignment__different_prefixes__raises_value_error() -> None:
    # Arrange / Act / Assert -- partitions detached under one are invisible to the other
    with pytest.raises(ValueError, match="Orphan marker mismatch"):
        validate_marker_alignment(_Marked("app:orphan-parent="), _Marked("pg-partsmith:orphan-parent="))


def test__validate_marker_alignment__components_without_a_prefix__not_checked() -> None:
    # Arrange / Act / Assert -- a custom repository or a stub is trusted
    validate_marker_alignment(object(), _Marked("app:"))
    validate_marker_alignment(_Marked("app:"), object())


def test__validate_marker_alignment__prefix_that_is_not_a_string__not_checked() -> None:
    # Arrange / Act / Assert -- runtime protocols only verify attribute presence
    validate_marker_alignment(_Marked(123), _Marked("app:"))


# -- validate_timezone_alignment ------------------------------------------------------------


def test__validate_timezone_alignment__calculator_and_ddl_zones_agree__passes() -> None:
    # Arrange / Act / Assert -- case-insensitively, as PostgreSQL reads zone names
    validate_timezone_alignment(_Repository("Europe/Moscow"), _Calculator("Europe/Moscow"))
    validate_timezone_alignment(_Repository("europe/moscow"), _Calculator("Europe/Moscow"))


def test__validate_timezone_alignment__zones_disagree__raises_value_error() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="Timezone mismatch: the period calculator works in 'Europe/Moscow'"):
        validate_timezone_alignment(_Repository("UTC"), _Calculator("Europe/Moscow"))


def test__validate_timezone_alignment__ddl_zone_trusts_the_session__warns_for_a_non_utc_calculator(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Arrange / Act
    with caplog.at_level(logging.WARNING, logger="pg_partsmith.utils"):
        validate_timezone_alignment(_Repository(None), _Calculator("Europe/Moscow"))

    # Assert
    assert "ddl_timezone=None trusts the session timezone" in caplog.text


def test__validate_timezone_alignment__ddl_zone_trusts_the_session__silent_for_a_utc_calculator(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Arrange / Act
    with caplog.at_level(logging.WARNING, logger="pg_partsmith.utils"):
        validate_timezone_alignment(_Repository(None), _Calculator("utc"))

    # Assert
    assert caplog.text == ""


def test__validate_timezone_alignment__components_without_zone_metadata__not_checked() -> None:
    # Arrange / Act / Assert -- custom repositories and calculators are trusted
    validate_timezone_alignment(object(), _Calculator("Europe/Moscow"))
    validate_timezone_alignment(_Repository("UTC"), object())


def test__validate_timezone_alignment__calculator_zone_that_is_not_a_string__not_checked() -> None:
    # Arrange / Act / Assert -- runtime protocols only verify attribute presence
    validate_timezone_alignment(_Repository("UTC"), _Calculator(123))
