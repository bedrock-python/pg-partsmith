import pytest

from pg_partsmith.utils import (
    _resolve_marker_prefix,
    build_ddl_statement,
    calculate_lock_id,
    format_duration_ms,
    orphan_comment_prefix,
    quote_identifier,
    split_qualified_name,
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


def test__to_regclass_argument__uppercase_week_partition__preserves_case() -> None:
    # Arrange / Act / Assert
    assert to_regclass_argument("events__2024_W12") == '"events__2024_W12"'


def test__to_regclass_argument__schema_qualified__quotes_both_parts() -> None:
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


# ── split_qualified_name ────────────────────────────────────────────────────────


def test__split_qualified_name__empty_string__raises_value_error() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="Name cannot be empty"):
        split_qualified_name("")


def test__split_qualified_name__three_part_name__raises_value_error() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="Invalid qualified name"):
        split_qualified_name("a.b.c")


# ── _resolve_marker_prefix ──────────────────────────────────────────────────────


def test__resolve_marker_prefix__non_string_type__raises_type_error() -> None:
    # Arrange / Act / Assert
    with pytest.raises(TypeError, match="marker_prefix must be a str or None"):
        _resolve_marker_prefix(123)  # type: ignore[arg-type]


def test__resolve_marker_prefix__empty_string__raises_value_error() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="marker_prefix must be a non-empty string or None"):
        _resolve_marker_prefix("")
