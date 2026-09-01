"""The partition tree that actually exists: bound parsing, nodes, orphans and hash arithmetic."""

import logging
from datetime import UTC, datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest
from pydantic import ValidationError

from pg_partsmith.partition_bounds import (
    is_addressable,
    is_naive_timestamp_literal,
    parse_boundary_literal,
    parse_partition_bounds,
    parse_range_boundaries,
)
from pg_partsmith.topology import (
    ActualTree,
    DefaultBounds,
    DetachedPartition,
    FactKind,
    HashBounds,
    ListBounds,
    PartitionFacts,
    PartitionNode,
    PartitionTreeRow,
    PartitionType,
    RangeBounds,
    RelationKind,
    build_partition_tree,
    hash_keyspace_covered,
    missing_remainders,
    uniform_modulus,
    validate_pg_identifier,
)

# -- bound models ----------------------------------------------------------------------------


def test__hash_bounds__remainder_below_modulus__accepted() -> None:
    # Arrange / Act
    bounds = HashBounds(modulus=4, remainder=3)

    # Assert
    assert bounds.modulus == 4
    assert bounds.remainder == 3
    assert bounds.kind == "hash"


@pytest.mark.parametrize(("modulus", "remainder"), [(4, 4), (4, 5), (1, 1)])
def test__hash_bounds__remainder_at_or_above_modulus__rejected(modulus: int, remainder: int) -> None:
    # Arrange / Act / Assert -- PostgreSQL would refuse it too
    with pytest.raises(ValidationError, match="remainder must be < modulus"):
        HashBounds(modulus=modulus, remainder=remainder)


def test__hash_bounds__negative_remainder__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError):
        HashBounds(modulus=4, remainder=-1)


def test__hash_bounds__zero_modulus__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError):
        HashBounds(modulus=0, remainder=0)


def test__range_bounds__blank_value__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError):
        RangeBounds(from_value="  ", to_value="b")


def test__list_bounds__defaults__do_not_include_null() -> None:
    # Arrange / Act
    bounds = ListBounds(values=("eu",))

    # Assert
    assert bounds.includes_null is False
    assert bounds.kind == "list"
    assert DefaultBounds().kind == "default"


def test__partition_bounds__dict_discriminated_on_kind__parsed_into_the_right_model() -> None:
    # Arrange / Act
    node = PartitionNode(name="p.x", bounds={"kind": "hash", "modulus": 2, "remainder": 1})
    default = PartitionNode(name="p.y", bounds={"kind": "default"})

    # Assert
    assert node.bounds == HashBounds(modulus=2, remainder=1)
    assert default.bounds == DefaultBounds()


def test__partition_bounds__dict_of_unknown_kind__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="bounds"):
        PartitionNode(name="p.x", bounds={"kind": "geo"})


# -- parse_partition_bounds -----------------------------------------------------------------


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        ("FOR VALUES WITH (modulus 4, remainder 1)", HashBounds(modulus=4, remainder=1)),
        ("FOR VALUES WITH (MODULUS 2, REMAINDER 0)", HashBounds(modulus=2, remainder=0)),
        ("  for values with ( modulus 8 , remainder 7 )  ", HashBounds(modulus=8, remainder=7)),
        ("DEFAULT", DefaultBounds()),
        ("default", DefaultBounds()),
        ("FOR VALUES IN ('eu', 'us')", ListBounds(values=("eu", "us"))),
        ("FOR VALUES IN (1, 2)", ListBounds(values=("1", "2"))),
        ("FOR VALUES IN ('a'::text, 'b'::text)", ListBounds(values=("a", "b"))),
        ("FOR VALUES IN ((('eu')))", ListBounds(values=("eu",))),
        (
            "FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')",
            RangeBounds(from_value="2024-01-01", to_value="2024-02-01"),
        ),
        (
            "FOR VALUES FROM ('2024-01-01'::date) TO ('2024-02-01'::date)",
            RangeBounds(from_value="2024-01-01", to_value="2024-02-01"),
        ),
        (
            "FOR VALUES FROM ('2024-01-01 00:00:00+00'::timestamp with time zone) "
            "TO ('2024-02-01 00:00:00+00'::timestamp with time zone)",
            RangeBounds(from_value="2024-01-01 00:00:00+00", to_value="2024-02-01 00:00:00+00"),
        ),
        ("FOR VALUES FROM (1::bigint) TO (5::bigint)", RangeBounds(from_value="1", to_value="5")),
        ("FOR VALUES FROM (CAST(1 AS bigint)) TO (CAST(5 AS bigint))", RangeBounds(from_value="1", to_value="5")),
        ("FOR VALUES FROM (MINVALUE) TO (MAXVALUE)", RangeBounds(from_value="MINVALUE", to_value="MAXVALUE")),
        ("for values from ('a') to ('b')", RangeBounds(from_value="a", to_value="b")),
    ],
)
def test__parse_partition_bounds__catalog_expression__parses_to_structured_bounds(
    expression: str, expected: object
) -> None:
    # Arrange / Act / Assert
    assert parse_partition_bounds(expression) == expected


def test__parse_partition_bounds__composite_range__returns_the_leading_value() -> None:
    # Arrange / Act
    parsed = parse_partition_bounds(
        "FOR VALUES FROM ('2024-01-01 00:00:00+00', MINVALUE) TO ('2024-02-01 00:00:00+00', MINVALUE)"
    )

    # Assert -- trailing columns are MINVALUE at both ends, so the leading value is what the partition
    # actually selects on
    assert parsed == RangeBounds(from_value="2024-01-01 00:00:00+00", to_value="2024-02-01 00:00:00+00")


def test__parse_partition_bounds__composite_numeric_range__returns_the_leading_value() -> None:
    # Arrange / Act / Assert
    assert parse_partition_bounds("FOR VALUES FROM (100, MINVALUE) TO (200, MINVALUE)") == RangeBounds(
        from_value="100", to_value="200"
    )


def test__parse_partition_bounds__quoted_comma_in_a_composite_bound__not_split() -> None:
    # Arrange / Act / Assert
    assert parse_partition_bounds("FOR VALUES FROM ('a,b', MINVALUE) TO ('c,d', MINVALUE)") == RangeBounds(
        from_value="a,b", to_value="c,d"
    )


def test__parse_partition_bounds__list_value_containing_comma__does_not_split_inside_quotes() -> None:
    # Arrange / Act / Assert
    assert parse_partition_bounds("FOR VALUES IN ('a,b', 'c')") == ListBounds(values=("a,b", "c"))


def test__parse_partition_bounds__doubled_quote_inside_a_value__unescaped_and_kept_whole() -> None:
    # Arrange / Act
    parsed = parse_partition_bounds("FOR VALUES IN ('O''Brien', 'other')")

    # Assert -- stepping over only the first quote would flip the in-quotes state and split on the
    # next comma
    assert parsed == ListBounds(values=("O'Brien", "other"))


def test__parse_partition_bounds__doubled_quote_followed_by_a_comma__still_one_value() -> None:
    # Arrange / Act / Assert
    assert parse_partition_bounds("FOR VALUES IN ('it''s, really', 'other')") == ListBounds(
        values=("it's, really", "other")
    )


def test__parse_partition_bounds__null_keyword__is_not_the_string_null() -> None:
    # Arrange / Act
    keyword = parse_partition_bounds("FOR VALUES IN (NULL)")
    literal = parse_partition_bounds("FOR VALUES IN ('NULL')")

    # Assert -- reading them as the same bound would make the planner propose a partition PostgreSQL
    # already has, and fail on the conflict every run
    assert keyword == ListBounds(values=(), includes_null=True)
    assert literal == ListBounds(values=("NULL",))
    assert keyword != literal


def test__parse_partition_bounds__null_alongside_values__keeps_both() -> None:
    # Arrange / Act / Assert
    assert parse_partition_bounds("FOR VALUES IN ('eu', NULL, 'us')") == ListBounds(
        values=("eu", "us"), includes_null=True
    )


def test__parse_partition_bounds__cast_null__is_still_the_keyword() -> None:
    # Arrange / Act -- older servers render the element with its type cast
    assert parse_partition_bounds("FOR VALUES IN (NULL::text)") == ListBounds(values=(), includes_null=True)


def test__parse_partition_bounds__list_value_naming_modulus__stays_a_list_bound() -> None:
    # Arrange / Act -- an unanchored search would find a hash bound inside the value; a partition whose
    # bounds are misread is invisible to the planner, which then plans a duplicate
    parsed = parse_partition_bounds("FOR VALUES IN ('FOR VALUES WITH (MODULUS 4, REMAINDER 1)')")

    # Assert
    assert parsed == ListBounds(values=("FOR VALUES WITH (MODULUS 4, REMAINDER 1)",))


def test__parse_partition_bounds__range_literal_naming_modulus__stays_a_range_bound() -> None:
    # Arrange / Act
    parsed = parse_partition_bounds("FOR VALUES FROM ('FOR VALUES WITH (MODULUS 2, REMAINDER 0)') TO ('z')")

    # Assert
    assert parsed == RangeBounds(from_value="FOR VALUES WITH (MODULUS 2, REMAINDER 0)", to_value="z")


@pytest.mark.parametrize(
    "expression",
    [
        None,
        "",
        "something unexpected",
        "FOR VALUES WITH (modulus x, remainder y)",
        "FOR VALUES WITH (MODULUS 4, REMAINDER 1) extra",
        "FOR VALUES WITH (MODULUS 0, REMAINDER 0)",
        "FOR VALUES WITH (MODULUS 2, REMAINDER 2)",
        "FOR VALUES FROM ('a') TO ('b') TO ('c')",
        "FOR VALUES FROM ('a')",
    ],
)
def test__parse_partition_bounds__unrecognised_or_unusable_expression__returns_none(expression: str | None) -> None:
    # Arrange / Act / Assert -- returning a half-valid bound would have the planner compare against
    # something PostgreSQL never wrote
    assert parse_partition_bounds(expression) is None


# -- parse_range_boundaries ------------------------------------------------------------------


def test__parse_range_boundaries__range_expression__returns_the_pair() -> None:
    # Arrange / Act / Assert
    assert parse_range_boundaries("FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')") == ("2024-01-01", "2024-02-01")


@pytest.mark.parametrize(
    "expression",
    [None, "", "DEFAULT", "FOR VALUES IN ('a) TO (b')", "FOR VALUES WITH (MODULUS 2, REMAINDER 0)", "FROM (a) TO (b)"],
)
def test__parse_range_boundaries__non_range_expression__returns_nones(expression: str | None) -> None:
    # Arrange / Act / Assert -- a LIST value could contain ") TO (" and must not be mis-parsed
    assert parse_range_boundaries(expression) == (None, None)


# -- is_naive_timestamp_literal -------------------------------------------------------------


@pytest.mark.parametrize(
    ("literal", "expected"),
    [
        ("2026-02-01", True),
        ("2026-02-01 00:00:00", True),
        ("2026-02-01 00:00:00+00", False),
        ("2026-02-01T00:00:00Z", False),
        ("MAXVALUE", False),
        ("100000", False),
        ("019a0000-0000-7000-8000-000000000000", False),
        ("", False),
        (None, False),
    ],
)
def test__is_naive_timestamp_literal__reports_whether_a_zone_is_missing(literal: str | None, expected: bool) -> None:
    # Arrange / Act / Assert -- only a timestamp that carries no offset leaves the zone open
    assert is_naive_timestamp_literal(literal) is expected


def test__is_naive_timestamp_literal__interrupted__propagates() -> None:
    # Arrange -- the parse is guarded against bad input, never against a stop signal
    with (
        patch("pg_partsmith.partition_bounds.isoparse", side_effect=KeyboardInterrupt),
        pytest.raises(KeyboardInterrupt),
    ):
        # Act / Assert
        is_naive_timestamp_literal("2026-02-01 00:00:00")


# -- parse_boundary_literal ------------------------------------------------------------------


def test__parse_boundary_literal__bare_date__read_as_midnight_in_the_boundary_zone() -> None:
    # Arrange / Act / Assert
    assert parse_boundary_literal("2024-01-01", ZoneInfo("Europe/Moscow")) == datetime(2023, 12, 31, 21, tzinfo=UTC)
    assert parse_boundary_literal("2024-01-01", UTC) == datetime(2024, 1, 1, tzinfo=UTC)


def test__parse_boundary_literal__naive_timestamp__read_in_the_boundary_zone() -> None:
    # Arrange / Act / Assert
    assert parse_boundary_literal("2024-01-01 12:00:00", ZoneInfo("Europe/Moscow")) == datetime(
        2024, 1, 1, 9, tzinfo=UTC
    )


@pytest.mark.parametrize("literal", ["2024-01-01 03:00:00+03", "2024-01-01T00:00:00Z", "2024-01-01 00:00:00+00:00"])
def test__parse_boundary_literal__offset_timestamp__converted_to_utc_whatever_the_zone(literal: str) -> None:
    # Arrange / Act / Assert
    assert parse_boundary_literal(literal, ZoneInfo("Europe/Moscow")) == datetime(2024, 1, 1, tzinfo=UTC)


@pytest.mark.parametrize(
    "literal",
    [None, "", "   ", "MINVALUE", "maxvalue", "42", "2026-02-31", "2024-13-01 10:00:00", "12:34", "not a date-ish"],
)
def test__parse_boundary_literal__value_carrying_no_instant__is_declined(literal: str | None) -> None:
    # Arrange / Act / Assert
    assert parse_boundary_literal(literal, UTC) is None


# -- is_addressable ----------------------------------------------------------------------------


def test__is_addressable__plain_names__true() -> None:
    # Arrange / Act / Assert
    assert is_addressable("public", "events__2024_01") is True


@pytest.mark.parametrize(("schema", "relname"), [("pub.lic", "events"), ("public", "events.2024")])
def test__is_addressable__dot_in_either_part__false_with_a_warning(
    schema: str, relname: str, caplog: pytest.LogCaptureFixture
) -> None:
    # Arrange / Act -- ``schema.relname`` strings would be re-split into a different relation
    with caplog.at_level(logging.WARNING, logger="pg_partsmith.partition_bounds"):
        addressable = is_addressable(schema, relname)

    # Assert
    assert addressable is False
    assert "not addressable by qualified-name DDL" in caplog.text


# -- relation kinds and partition types ---------------------------------------------------------


@pytest.mark.parametrize(
    ("relkind", "expected"),
    [("r", RelationKind.TABLE), ("p", RelationKind.PARTITIONED), ("f", RelationKind.FOREIGN)],
)
def test__relation_kind__from_relkind__maps_the_catalog_code(relkind: str, expected: RelationKind) -> None:
    # Arrange / Act / Assert
    assert RelationKind.from_relkind(relkind) is expected


@pytest.mark.parametrize("relkind", ["v", "m", "", None])
def test__relation_kind__from_unknown_relkind__is_other(relkind: str | None) -> None:
    # Arrange / Act / Assert
    assert RelationKind.from_relkind(relkind) is RelationKind.OTHER


@pytest.mark.parametrize(
    ("kind", "droppable"),
    [
        (RelationKind.TABLE, True),
        (RelationKind.PARTITIONED, True),
        (RelationKind.FOREIGN, False),
        (RelationKind.OTHER, False),
    ],
)
def test__relation_kind__is_droppable_table__only_for_tables(kind: RelationKind, droppable: bool) -> None:
    # Arrange / Act / Assert -- DROP TABLE is the wrong statement for a foreign table
    assert kind.is_droppable_table is droppable


@pytest.mark.parametrize(
    ("strat", "expected"),
    [("r", PartitionType.RANGE), ("l", PartitionType.LIST), ("h", PartitionType.HASH), ("x", None), (None, None)],
)
def test__partition_type__from_partstrat__maps_the_catalog_code(
    strat: str | None, expected: PartitionType | None
) -> None:
    # Arrange / Act / Assert
    assert PartitionType.from_partstrat(strat) is expected


# -- hash keyspace arithmetic -------------------------------------------------------------------


def test__uniform_modulus__all_children_share_modulus__returns_it() -> None:
    # Arrange
    bounds = (HashBounds(modulus=4, remainder=0), HashBounds(modulus=4, remainder=1))

    # Act / Assert
    assert uniform_modulus(bounds) == 4


def test__uniform_modulus__children_disagree__returns_none() -> None:
    # Arrange
    bounds = (HashBounds(modulus=2, remainder=0), HashBounds(modulus=4, remainder=1))

    # Act / Assert
    assert uniform_modulus(bounds) is None


def test__uniform_modulus__no_children__returns_none() -> None:
    # Arrange / Act / Assert
    assert uniform_modulus(()) is None


def test__missing_remainders__incomplete_set__returns_the_gaps() -> None:
    # Arrange
    bounds = (
        HashBounds(modulus=4, remainder=0),
        HashBounds(modulus=4, remainder=1),
        HashBounds(modulus=4, remainder=3),
    )

    # Act / Assert
    assert missing_remainders(4, bounds) == (2,)


def test__missing_remainders__complete_set__returns_empty() -> None:
    # Arrange
    bounds = tuple(HashBounds(modulus=2, remainder=r) for r in (0, 1))

    # Act / Assert
    assert missing_remainders(2, bounds) == ()


def test__missing_remainders__children_at_another_modulus__do_not_count() -> None:
    # Arrange
    bounds = (HashBounds(modulus=2, remainder=0), HashBounds(modulus=2, remainder=1))

    # Act / Assert -- every remainder at modulus 4 is still missing
    assert missing_remainders(4, bounds) == (0, 1, 2, 3)
    assert missing_remainders(4, ()) == (0, 1, 2, 3)


def test__hash_keyspace_covered__complete_uniform_set__true() -> None:
    # Arrange
    bounds = tuple(HashBounds(modulus=2, remainder=r) for r in (0, 1))

    # Act / Assert
    assert hash_keyspace_covered(bounds) is True


def test__hash_keyspace_covered__incomplete_uniform_set__false() -> None:
    # Arrange / Act / Assert
    assert hash_keyspace_covered((HashBounds(modulus=2, remainder=0),)) is False


def test__hash_keyspace_covered__mixed_moduli_that_tile__true() -> None:
    # Arrange -- (2,1) owns every odd residue, (4,0) and (4,2) own the even ones
    bounds = (
        HashBounds(modulus=2, remainder=1),
        HashBounds(modulus=4, remainder=0),
        HashBounds(modulus=4, remainder=2),
    )

    # Act / Assert
    assert hash_keyspace_covered(bounds) is True


def test__hash_keyspace_covered__mixed_moduli_with_a_gap__false() -> None:
    # Arrange -- residue 2 (mod 4) is owned by nobody
    bounds = (HashBounds(modulus=2, remainder=0), HashBounds(modulus=4, remainder=1))

    # Act / Assert
    assert hash_keyspace_covered(bounds) is False


def test__hash_keyspace_covered__no_children__false() -> None:
    # Arrange / Act / Assert
    assert hash_keyspace_covered(()) is False


def test__hash_keyspace_covered__moduli_too_coarse_to_enumerate__returns_none() -> None:
    # Arrange -- coprime moduli whose least common multiple blows past the cap
    bounds = (HashBounds(modulus=65521, remainder=0), HashBounds(modulus=65519, remainder=1))

    # Act / Assert -- coverage is unknown and must not be guessed at
    assert hash_keyspace_covered(bounds) is None


def test__hash_keyspace_covered__single_large_modulus_within_the_cap__enumerated() -> None:
    # Arrange
    bounds = tuple(HashBounds(modulus=65536, remainder=r) for r in range(65536))

    # Act / Assert
    assert hash_keyspace_covered(bounds) is True


# -- validate_pg_identifier ------------------------------------------------------------------


@pytest.mark.parametrize(("value", "expected"), [("events", "events"), ("Events_2024", "events_2024"), ("_x1", "_x1")])
def test__validate_pg_identifier__plain_identifier__folded_to_lowercase(value: str, expected: str) -> None:
    # Arrange / Act / Assert -- unquoted identifiers fold in PostgreSQL; folding here keeps catalogue
    # lookups and quoted DDL pointing at the same relation
    assert validate_pg_identifier(value) == expected


@pytest.mark.parametrize("value", ["1abc", "a-b", "", "a b", "a.b", "tenant_id; DROP TABLE t", "café"])
def test__validate_pg_identifier__value_that_is_not_an_identifier__is_refused(value: str) -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        validate_pg_identifier(value)


def test__validate_pg_identifier__over_63_characters__is_refused() -> None:
    # Arrange / Act / Assert -- PostgreSQL would truncate it silently
    assert validate_pg_identifier("a" * 63) == "a" * 63
    with pytest.raises(ValueError, match="SQL identifier too long"):
        validate_pg_identifier("a" * 64)


# -- PartitionFacts ------------------------------------------------------------------------------


def test__partition_facts__defaults__nothing_measured() -> None:
    # Arrange / Act
    facts = PartitionFacts()

    # Assert
    assert facts.size_bytes is None
    assert facts.row_estimate is None
    assert facts.predicates == {}


def test__partition_facts__measurements__stored_as_given() -> None:
    # Arrange / Act
    facts = PartitionFacts(size_bytes=1024, row_estimate=0, predicates={"abc": True})

    # Assert
    assert facts.size_bytes == 1024
    assert facts.row_estimate == 0
    assert facts.predicates == {"abc": True}


@pytest.mark.parametrize("field", ["size_bytes", "row_estimate"])
def test__partition_facts__negative_measurement__rejected(field: str) -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError):
        PartitionFacts(**{field: -1})


def test__fact_kind__names_what_the_introspector_can_measure() -> None:
    # Arrange / Act / Assert
    assert {kind.value for kind in FactKind} == {"size", "rows", "references"}


# -- PartitionNode --------------------------------------------------------------------------------


def test__partition_node__defaults__a_plain_attached_leaf_at_the_root() -> None:
    # Arrange / Act
    node = PartitionNode(name="public.events__2024_01")

    # Assert
    assert node.oid is None
    assert node.parent_name is None
    assert node.level == 0
    assert node.relkind is RelationKind.TABLE
    assert node.partition_type is None
    assert node.partition_columns == ()
    assert node.bounds is None
    assert node.is_attached is True
    assert node.detach_pending is False
    assert node.children == ()
    assert node.has_unaddressable_children is False
    assert node.has_expression_key is False
    assert node.facts is None
    assert node.is_leaf is True
    assert node.is_default is False
    assert node.is_foreign is False


def test__partition_node__partitions_children__relkind_derived_to_partitioned() -> None:
    # Arrange / Act -- whatever the row said
    node = PartitionNode(name="p.b", partition_type=PartitionType.HASH, relkind=RelationKind.TABLE)

    # Assert
    assert node.relkind is RelationKind.PARTITIONED
    assert node.is_leaf is False


def test__partition_node__partitions_children_but_is_foreign__relkind_left_alone() -> None:
    # Arrange / Act -- only a plain-table claim is corrected
    node = PartitionNode(name="p.b", partition_type=PartitionType.HASH, relkind=RelationKind.FOREIGN)

    # Assert
    assert node.relkind is RelationKind.FOREIGN
    assert node.is_foreign is True


def test__partition_node__default_bounds__is_default() -> None:
    # Arrange / Act / Assert
    assert PartitionNode(name="p.d", bounds=DefaultBounds()).is_default is True
    assert PartitionNode(name="p.h", bounds=HashBounds(modulus=2, remainder=0)).is_default is False


def test__partition_node__relname__strips_the_schema_and_survives_without_one() -> None:
    # Arrange / Act / Assert
    assert PartitionNode(name="public.events__h0").relname == "events__h0"
    assert PartitionNode(name="events__h0").relname == "events__h0"


def test__partition_node__hash_children__ignores_siblings_bound_any_other_way() -> None:
    # Arrange -- a branch holding a hash bucket, a DEFAULT sibling and a LIST partition someone attached
    # by hand
    node = PartitionNode(
        name="public.events__2026_w35",
        partition_type=PartitionType.HASH,
        children=(
            PartitionNode(name="public.events__2026_w35__h0", bounds=HashBounds(modulus=2, remainder=0)),
            PartitionNode(name="public.events__2026_w35_default", bounds=DefaultBounds()),
            PartitionNode(name="public.events__2026_w35__eu", bounds=ListBounds(values=("eu",))),
        ),
    )

    # Act / Assert -- keyspace arithmetic is only meaningful over hash bounds
    assert [c.name for c in node.hash_children] == ["public.events__2026_w35__h0"]


@pytest.mark.parametrize(
    ("node", "expected"),
    [
        (PartitionNode(name="p.l"), "a plain leaf table"),
        (PartitionNode(name="p.f", relkind=RelationKind.FOREIGN), "a foreign table"),
        (
            PartitionNode(name="p.b", partition_type=PartitionType.HASH, partition_columns=("tenant_id",)),
            "partitioned by HASH (tenant_id)",
        ),
        (
            PartitionNode(
                name="p.b", partition_type=PartitionType.RANGE, partition_columns=("created_at", "tenant_id")
            ),
            "partitioned by RANGE (created_at, tenant_id)",
        ),
        (PartitionNode(name="p.b", partition_type=PartitionType.LIST), "partitioned by LIST (?)"),
    ],
    ids=["leaf", "foreign", "hash", "composite-range", "expression-key"],
)
def test__partition_node__describe_topology__reads_naturally_for_every_kind(node: PartitionNode, expected: str) -> None:
    # Arrange / Act / Assert
    assert node.describe_topology() == expected


def test__partition_node__is_frozen__assignment_rejected() -> None:
    # Arrange
    node = PartitionNode(name="p.l")

    # Act / Assert
    with pytest.raises(ValidationError, match="frozen"):
        node.name = "p.m"  # type: ignore[misc]


# -- tree assembly ------------------------------------------------------------------------------


def _rows() -> list[PartitionTreeRow]:
    return [
        PartitionTreeRow(
            level=0,
            name="public.events",
            oid=100,
            relkind=RelationKind.PARTITIONED,
            partition_type=PartitionType.RANGE,
            partition_columns=("created_at",),
        ),
        PartitionTreeRow(
            level=1,
            name="public.events__2026_w35",
            oid=101,
            parent_name="public.events",
            bounds=RangeBounds(from_value="2026-08-24", to_value="2026-08-31"),
            partition_type=PartitionType.HASH,
            partition_columns=("tenant_id",),
        ),
        PartitionTreeRow(
            level=2,
            name="public.events__2026_w35__h1",
            oid=103,
            parent_name="public.events__2026_w35",
            bounds=HashBounds(modulus=2, remainder=1),
            detach_pending=True,
            facts=PartitionFacts(size_bytes=2048),
        ),
        PartitionTreeRow(
            level=2,
            name="public.events__2026_w35__h0",
            oid=102,
            parent_name="public.events__2026_w35",
            relkind=RelationKind.FOREIGN,
            bounds=HashBounds(modulus=2, remainder=0),
        ),
    ]


def _tree() -> PartitionNode:
    tree = build_partition_tree(_rows())
    assert tree is not None
    return tree


def test__build_partition_tree__flat_rows__nests_children_under_their_parent_sorted_by_name() -> None:
    # Arrange / Act
    tree = _tree()

    # Assert
    assert tree.name == "public.events"
    assert [c.name for c in tree.children] == ["public.events__2026_w35"]
    assert [c.name for c in tree.children[0].children] == [
        "public.events__2026_w35__h0",
        "public.events__2026_w35__h1",
    ]


def test__build_partition_tree__row_fields__carried_onto_the_nodes() -> None:
    # Arrange / Act
    tree = _tree()
    branch = tree.children[0]
    h0, h1 = branch.children

    # Assert
    assert (tree.oid, tree.level, tree.relkind, tree.parent_name) == (100, 0, RelationKind.PARTITIONED, None)
    assert (branch.oid, branch.level, branch.parent_name) == (101, 1, "public.events")
    assert (h0.oid, h0.relkind, h0.detach_pending, h0.facts) == (102, RelationKind.FOREIGN, False, None)
    assert (h1.oid, h1.relkind, h1.detach_pending) == (103, RelationKind.TABLE, True)
    assert h1.facts == PartitionFacts(size_bytes=2048)
    assert h1.level == 2
    assert h1.is_attached is True


def test__build_partition_tree__branch_node__is_partition_and_partitioned_table_at_once() -> None:
    # Arrange / Act
    branch = _tree().children[0]

    # Assert
    assert isinstance(branch.bounds, RangeBounds)
    assert branch.partition_type is PartitionType.HASH
    assert branch.partition_columns == ("tenant_id",)
    assert branch.relkind is RelationKind.PARTITIONED
    assert branch.is_leaf is False
    assert branch.children[0].is_leaf is True
    assert len(branch.hash_children) == 2


def test__build_partition_tree__no_root_row__returns_none() -> None:
    # Arrange
    rows = [r for r in _rows() if r.level != 0]

    # Act / Assert
    assert build_partition_tree(rows) is None
    assert build_partition_tree([]) is None


def test__build_partition_tree__orphaned_row__dropped_rather_than_reparented() -> None:
    # Arrange -- a partial tree would misreport a branch's child set, which reconciliation reads
    rows = [
        *_rows(),
        PartitionTreeRow(
            level=2,
            name="public.stray",
            parent_name="public.unknown_branch",
            bounds=HashBounds(modulus=2, remainder=0),
        ),
        PartitionTreeRow(level=1, name="public.parentless", parent_name=None),
    ]

    # Act
    tree = build_partition_tree(rows)

    # Assert
    assert tree is not None
    assert {node.name for node in tree.walk()} == {
        "public.events",
        "public.events__2026_w35",
        "public.events__2026_w35__h0",
        "public.events__2026_w35__h1",
    }


def test__build_partition_tree__parent_named_unaddressable__marks_that_node_only() -> None:
    # Arrange -- the caller dropped a child whose name holds a dot
    rows = [
        PartitionTreeRow(level=0, name="public.events", partition_type=PartitionType.HASH),
        PartitionTreeRow(
            level=1, name="public.events__h0", parent_name="public.events", bounds=HashBounds(modulus=2, remainder=0)
        ),
    ]

    # Act
    tree = build_partition_tree(rows, {"public.events"})

    # Assert
    assert tree is not None
    assert tree.has_unaddressable_children is True
    assert tree.children[0].has_unaddressable_children is False


def test__build_partition_tree__no_omissions__leaves_every_node_unmarked() -> None:
    # Arrange / Act
    tree = build_partition_tree([PartitionTreeRow(level=0, name="public.events", partition_type=PartitionType.HASH)])

    # Assert
    assert tree is not None
    assert tree.has_unaddressable_children is False
    assert tree.children == ()


def test__build_partition_tree__expression_key__flag_carried_onto_the_node() -> None:
    # Arrange / Act
    tree = build_partition_tree(
        [PartitionTreeRow(level=0, name="public.events", partition_type=PartitionType.RANGE, has_expression_key=True)]
    )

    # Assert
    assert tree is not None
    assert tree.has_expression_key is True
    assert tree.partition_columns == ()


def test__partition_node__walk__is_depth_first_and_starts_with_itself() -> None:
    # Arrange / Act
    names = [node.name for node in _tree().walk()]

    # Assert
    assert names == [
        "public.events",
        "public.events__2026_w35",
        "public.events__2026_w35__h0",
        "public.events__2026_w35__h1",
    ]


def test__partition_node__find__locates_a_node_by_qualified_name() -> None:
    # Arrange
    tree = _tree()

    # Act
    found = tree.find("public.events__2026_w35__h1")

    # Assert
    assert found is not None
    assert found.bounds == HashBounds(modulus=2, remainder=1)
    assert tree.find("public.events") is tree
    assert tree.find("public.nope") is None
    assert tree.find("events__2026_w35__h1") is None


# -- DetachedPartition and ActualTree ------------------------------------------------------------


def test__detached_partition__defaults__a_plain_table_without_instant_or_facts() -> None:
    # Arrange / Act
    orphan = DetachedPartition(name="public.events__2024_01", parent_name="public.events")

    # Assert
    assert orphan.oid is None
    assert orphan.relkind is RelationKind.TABLE
    assert orphan.detached_at is None
    assert orphan.facts is None
    assert orphan.relname == "events__2024_01"


def test__detached_partition__catalog_identity__stored_as_given() -> None:
    # Arrange
    detached_at = datetime(2026, 8, 24, 12, tzinfo=UTC)

    # Act
    orphan = DetachedPartition(
        name="old",
        oid=77,
        relkind=RelationKind.PARTITIONED,
        parent_name="public.events",
        detached_at=detached_at,
        facts=PartitionFacts(row_estimate=5),
    )

    # Assert
    assert orphan.relname == "old"
    assert orphan.oid == 77
    assert orphan.relkind is RelationKind.PARTITIONED
    assert orphan.detached_at == detached_at
    assert orphan.facts == PartitionFacts(row_estimate=5)


def test__detached_partition__parent_name_required() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="parent_name"):
        DetachedPartition(name="old")  # type: ignore[call-arg]


def test__actual_tree__find__searches_the_attached_tree_only() -> None:
    # Arrange
    root = _tree()
    orphan = DetachedPartition(name="public.events__2024_01", parent_name="public.events")
    tree = ActualTree(root=root, orphans=(orphan,))

    # Act / Assert
    assert tree.find("public.events__2026_w35__h0") is root.children[0].children[0]
    assert tree.find("public.events__2024_01") is None
    assert tree.orphans == (orphan,)


def test__actual_tree__defaults__no_orphans() -> None:
    # Arrange / Act / Assert
    assert ActualTree(root=PartitionNode(name="public.events")).orphans == ()
