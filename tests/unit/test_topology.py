import pytest
from pydantic import ValidationError

from pg_partsmith.entities import PartitionInfo, PartitionType
from pg_partsmith.partition_bounds import parse_partition_bounds
from pg_partsmith.topology import (
    DefaultBounds,
    HashBounds,
    HashSubpartitionSpec,
    ListBounds,
    ListGroup,
    ListSubpartitionSpec,
    PartitionNode,
    PartitionTreeRow,
    RangeBounds,
    SubpartitionSpecBase,
    build_partition_tree,
    hash_keyspace_covered,
    missing_remainders,
    uniform_modulus,
)

# ── Bounds ──────────────────────────────────────────────────────────────────────


def test__hash_bounds__remainder_below_modulus__accepted() -> None:
    # Arrange / Act
    bounds = HashBounds(modulus=4, remainder=3)

    # Assert
    assert bounds.modulus == 4
    assert bounds.remainder == 3


def test__hash_bounds__remainder_equal_to_modulus__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="remainder must be < modulus"):
        HashBounds(modulus=4, remainder=4)


def test__hash_bounds__negative_remainder__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError):
        HashBounds(modulus=4, remainder=-1)


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        ("FOR VALUES WITH (modulus 4, remainder 1)", HashBounds(modulus=4, remainder=1)),
        ("FOR VALUES WITH (MODULUS 2, REMAINDER 0)", HashBounds(modulus=2, remainder=0)),
        ("DEFAULT", DefaultBounds()),
        ("FOR VALUES IN ('eu', 'us')", ListBounds(values=("eu", "us"))),
        (
            "FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')",
            RangeBounds(from_value="2024-01-01", to_value="2024-02-01"),
        ),
    ],
)
def test__parse_partition_bounds__catalog_expression__parses_to_structured_bounds(
    expression: str, expected: object
) -> None:
    # Arrange / Act
    parsed = parse_partition_bounds(expression)

    # Assert
    assert parsed == expected


def test__parse_partition_bounds__list_value_containing_comma__does_not_split_inside_quotes() -> None:
    # Arrange / Act
    parsed = parse_partition_bounds("FOR VALUES IN ('a,b', 'c')")

    # Assert
    assert parsed == ListBounds(values=("a,b", "c"))


@pytest.mark.parametrize("expression", [None, "", "something unexpected"])
def test__parse_partition_bounds__unrecognised_expression__returns_none(expression: str | None) -> None:
    # Arrange / Act / Assert
    assert parse_partition_bounds(expression) is None


# ── Hash keyspace coverage ──────────────────────────────────────────────────────


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


def test__hash_keyspace_covered__complete_uniform_set__true() -> None:
    # Arrange
    bounds = tuple(HashBounds(modulus=2, remainder=r) for r in (0, 1))

    # Act / Assert
    assert hash_keyspace_covered(bounds) is True


def test__hash_keyspace_covered__mixed_moduli_that_tile__true() -> None:
    # Arrange: (2,1) owns every odd residue, (4,0) and (4,2) own the even ones.
    bounds = (
        HashBounds(modulus=2, remainder=1),
        HashBounds(modulus=4, remainder=0),
        HashBounds(modulus=4, remainder=2),
    )

    # Act / Assert
    assert hash_keyspace_covered(bounds) is True


def test__hash_keyspace_covered__mixed_moduli_with_a_gap__false() -> None:
    # Arrange: residue 2 (mod 4) is owned by nobody.
    bounds = (HashBounds(modulus=2, remainder=0), HashBounds(modulus=4, remainder=1))

    # Act / Assert
    assert hash_keyspace_covered(bounds) is False


def test__hash_keyspace_covered__no_children__false() -> None:
    # Arrange / Act / Assert
    assert hash_keyspace_covered(()) is False


def test__hash_keyspace_covered__moduli_too_coarse_to_enumerate__returns_none() -> None:
    # Arrange: coprime moduli whose least common multiple blows past the cap.
    bounds = (
        HashBounds(modulus=65521, remainder=0),
        HashBounds(modulus=65519, remainder=1),
    )

    # Act / Assert
    assert hash_keyspace_covered(bounds) is None


# ── Subpartition spec ───────────────────────────────────────────────────────────


def test__hash_subpartition_spec__defaults__names_buckets_with_double_underscore() -> None:
    # Arrange
    spec = HashSubpartitionSpec(column="tenant_id", modulus=4)

    # Act / Assert
    assert spec.child_name("events__2026_w35", 2) == "events__2026_w35__h2"
    assert spec.partition_type == PartitionType.HASH


def test__hash_subpartition_spec__custom_name_suffix__used_for_children() -> None:
    # Arrange
    spec = HashSubpartitionSpec(column="tenant_id", modulus=2, name_suffix="_h{remainder}")

    # Act / Assert
    assert spec.child_name("events_20260824", 1) == "events_20260824_h1"


def test__hash_subpartition_spec__name_suffix_without_placeholder__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="remainder"):
        HashSubpartitionSpec(column="tenant_id", modulus=2, name_suffix="_bucket")


def test__hash_subpartition_spec__name_suffix_with_unsafe_characters__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="remainder"):
        HashSubpartitionSpec(column="tenant_id", modulus=2, name_suffix='"; DROP TABLE x --{remainder}')


def test__hash_subpartition_spec__uppercase_column__normalised_to_lowercase() -> None:
    # Arrange / Act
    spec = HashSubpartitionSpec(column="TenantId", modulus=2)

    # Assert
    assert spec.column == "tenantid"


def test__hash_subpartition_spec__name_length_budget__accounts_for_widest_remainder() -> None:
    # Arrange
    spec = HashSubpartitionSpec(column="tenant_id", modulus=16)

    # Act / Assert: "__h" plus two digits for remainder 15.
    assert spec.name_length_budget() == len("__h") + 2


def test__hash_subpartition_spec__nested_levels__budget_and_depth_accumulate() -> None:
    # Arrange
    spec = HashSubpartitionSpec(
        column="tenant_id",
        modulus=2,
        subpartition=HashSubpartitionSpec(column="shard_id", modulus=2),
    )

    # Act / Assert
    assert spec.depth() == 2
    assert spec.name_length_budget() == 2 * (len("__h") + 1)
    assert [s.column for s in spec.walk()] == ["tenant_id", "shard_id"]


def test__hash_subpartition_spec__deeper_than_the_limit__rejected() -> None:
    # Arrange
    def nest(levels: int) -> HashSubpartitionSpec:
        spec = HashSubpartitionSpec(column="c0", modulus=2)
        for level in range(1, levels):
            spec = HashSubpartitionSpec(column=f"c{level}", modulus=2, subpartition=spec)
        return spec

    # Act / Assert
    with pytest.raises(ValidationError, match="limited to"):
        nest(5)


# ── PartitionInfo ───────────────────────────────────────────────────────────────


def test__partition_info__range_pair_without_bounds__derives_structured_bounds() -> None:
    # Arrange / Act
    info = PartitionInfo(
        name="public.events__2026_w35",
        partition_type=PartitionType.RANGE,
        from_value="2026-08-24",
        to_value="2026-08-31",
    )

    # Assert
    assert info.bounds == RangeBounds(from_value="2026-08-24", to_value="2026-08-31")
    assert info.hash_bounds is None


def test__partition_info__structured_bounds_only__derives_the_legacy_pair() -> None:
    # Arrange / Act
    info = PartitionInfo(
        name="public.events__2026_w35",
        partition_type=PartitionType.RANGE,
        bounds=RangeBounds(from_value="2026-08-24", to_value="2026-08-31"),
    )

    # Assert
    assert info.from_value == "2026-08-24"
    assert info.to_value == "2026-08-31"


def test__partition_info__default_partition__gets_default_bounds() -> None:
    # Arrange / Act
    info = PartitionInfo(name="public.events_default", partition_type=PartitionType.RANGE, is_default=True)

    # Assert
    assert info.bounds == DefaultBounds()


def test__partition_info__subpartitioned_branch__reports_its_own_strategy() -> None:
    # Arrange / Act
    info = PartitionInfo(
        name="public.events__2026_w35",
        partition_type=PartitionType.RANGE,
        from_value="a",
        to_value="b",
        subpartition_type=PartitionType.HASH,
    )

    # Assert
    assert info.is_subpartitioned is True


def test__partition_info__plain_leaf__is_not_subpartitioned() -> None:
    # Arrange / Act
    info = PartitionInfo(
        name="public.events__2026_w35", partition_type=PartitionType.RANGE, from_value="a", to_value="b"
    )

    # Assert
    assert info.is_subpartitioned is False


# ── Tree assembly ───────────────────────────────────────────────────────────────


def _rows() -> list[PartitionTreeRow]:
    return [
        PartitionTreeRow(
            level=0,
            name="public.events",
            partition_type=PartitionType.RANGE,
            partition_columns=("created_at",),
        ),
        PartitionTreeRow(
            level=1,
            name="public.events__2026_w35",
            parent_name="public.events",
            bounds=RangeBounds(from_value="2026-08-24", to_value="2026-08-31"),
            partition_type=PartitionType.HASH,
            partition_columns=("tenant_id",),
        ),
        PartitionTreeRow(
            level=2,
            name="public.events__2026_w35__h1",
            parent_name="public.events__2026_w35",
            bounds=HashBounds(modulus=2, remainder=1),
        ),
        PartitionTreeRow(
            level=2,
            name="public.events__2026_w35__h0",
            parent_name="public.events__2026_w35",
            bounds=HashBounds(modulus=2, remainder=0),
        ),
    ]


def test__build_partition_tree__flat_rows__nests_children_under_their_parent() -> None:
    # Arrange / Act
    tree = build_partition_tree(_rows())

    # Assert
    assert tree is not None
    assert tree.name == "public.events"
    assert [c.name for c in tree.children] == ["public.events__2026_w35"]
    assert [c.name for c in tree.children[0].children] == [
        "public.events__2026_w35__h0",
        "public.events__2026_w35__h1",
    ]


def test__build_partition_tree__branch_node__is_partition_and_partitioned_table_at_once() -> None:
    # Arrange / Act
    tree = build_partition_tree(_rows())

    # Assert
    assert tree is not None
    branch = tree.children[0]
    assert isinstance(branch.bounds, RangeBounds)
    assert branch.partition_type == PartitionType.HASH
    assert branch.is_leaf is False
    assert branch.children[0].is_leaf is True


def test__build_partition_tree__hash_children__filtered_by_bound_kind() -> None:
    # Arrange
    tree = build_partition_tree(_rows())

    # Act
    assert tree is not None
    branch = tree.children[0]

    # Assert
    assert len(branch.hash_children) == 2


def test__build_partition_tree__no_root_row__returns_none() -> None:
    # Arrange
    rows = [r for r in _rows() if r.level != 0]

    # Act / Assert
    assert build_partition_tree(rows) is None


def test__build_partition_tree__orphaned_row__dropped_rather_than_reparented() -> None:
    # Arrange
    rows = [
        *_rows(),
        PartitionTreeRow(
            level=2,
            name="public.stray",
            parent_name="public.unknown_branch",
            bounds=HashBounds(modulus=2, remainder=0),
        ),
    ]

    # Act
    tree = build_partition_tree(rows)

    # Assert
    assert tree is not None
    assert all(node.name != "public.stray" for node in tree.walk())


def test__partition_tree__find__locates_a_node_by_qualified_name() -> None:
    # Arrange
    tree = build_partition_tree(_rows())

    # Act
    assert tree is not None
    found = tree.find("public.events__2026_w35__h1")

    # Assert
    assert found is not None
    assert found.bounds == HashBounds(modulus=2, remainder=1)


def test__partition_tree__find__unknown_name__returns_none() -> None:
    # Arrange
    tree = build_partition_tree(_rows())

    # Act / Assert
    assert tree is not None
    assert tree.find("public.nope") is None


def test__partition_node__describe_topology__reads_naturally_for_both_kinds() -> None:
    # Arrange
    branch = PartitionNode(name="p.b", partition_type=PartitionType.HASH, partition_columns=("tenant_id",))
    leaf = PartitionNode(name="p.l")

    # Act / Assert
    assert branch.describe_topology() == "partitioned by HASH (tenant_id)"
    assert leaf.describe_topology() == "a plain leaf table"


# ── LIST subpartition spec ──────────────────────────────────────────────────────


def _list_spec(**overrides: object) -> ListSubpartitionSpec:
    base: dict[str, object] = {
        "column": "region",
        "groups": (ListGroup(name="eu", values=("de", "fr")), ListGroup(name="us", values=("us",))),
    }
    base.update(overrides)
    return ListSubpartitionSpec(**base)  # type: ignore[arg-type]


def test__list_spec__defaults__names_children_after_their_group() -> None:
    # Arrange
    spec = _list_spec()

    # Act / Assert
    assert spec.child_name("events__2026_w35", "eu") == "events__2026_w35__eu"
    assert spec.partition_type == PartitionType.LIST


def test__list_spec__group__renders_its_bounds() -> None:
    # Arrange / Act
    bounds = ListGroup(name="eu", values=("de", "fr")).bounds()

    # Assert
    assert bounds == ListBounds(values=("de", "fr"))


def test__list_spec__no_groups__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="at least one group"):
        ListSubpartitionSpec(column="region", groups=())


def test__list_spec__group_without_values__rejected() -> None:
    # Arrange / Act / Assert: such a partition could never route a row.
    with pytest.raises(ValidationError, match="at least one value"):
        ListGroup(name="eu", values=())


def test__list_spec__group_repeating_a_value__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="repeats a value"):
        ListGroup(name="eu", values=("de", "de"))


def test__list_spec__value_claimed_by_two_groups__rejected() -> None:
    # Arrange / Act / Assert: PostgreSQL would refuse the second partition.
    with pytest.raises(ValidationError, match="claimed by both"):
        _list_spec(groups=(ListGroup(name="eu", values=("de",)), ListGroup(name="dach", values=("de",))))


def test__list_spec__duplicate_group_names__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="must be distinct"):
        _list_spec(groups=(ListGroup(name="eu", values=("de",)), ListGroup(name="eu", values=("fr",))))


def test__list_spec__default_name_colliding_with_a_group__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="must be distinct"):
        _list_spec(include_default=True, default_name="eu")


def test__list_spec__name_suffix_without_placeholder__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="name"):
        _list_spec(name_suffix="_region")


def test__list_spec__name_budget__sized_for_the_longest_name() -> None:
    # Arrange
    spec = _list_spec(groups=(ListGroup(name="eu", values=("de",)), ListGroup(name="apac", values=("jp",))))

    # Act / Assert: "__" plus the longest group name.
    assert spec.own_name_budget() == len("__") + len("apac")


def test__list_spec__default_included__counts_towards_the_name_budget() -> None:
    # Arrange
    spec = _list_spec(
        groups=(ListGroup(name="eu", values=("de",)),),
        include_default=True,
        default_name="everything_else",
    )

    # Act / Assert
    assert spec.own_name_budget() == len("__") + len("everything_else")


def test__list_spec__nested_under_hash__depth_and_columns_accumulate() -> None:
    # Arrange
    spec = HashSubpartitionSpec(column="tenant_id", modulus=2, subpartition=_list_spec())

    # Act / Assert
    assert spec.depth() == 2
    assert [s.column for s in spec.walk()] == ["tenant_id", "region"]


# ── children the tree cannot show ───────────────────────────────────────────────


def test__build_partition_tree__parent_named_unaddressable__marks_the_parent() -> None:
    # Arrange -- the caller dropped a child whose name holds a dot.
    rows = [
        PartitionTreeRow(level=0, name="public.events", partition_type=PartitionType.HASH),
        PartitionTreeRow(
            level=1,
            name="public.events__h0",
            parent_name="public.events",
            bounds=HashBounds(modulus=2, remainder=0),
        ),
    ]

    # Act
    tree = build_partition_tree(rows, {"public.events"})

    # Assert
    assert tree is not None
    assert tree.has_unaddressable_children is True
    assert tree.children[0].has_unaddressable_children is False


def test__build_partition_tree__no_omissions__leaves_every_node_unmarked() -> None:
    # Arrange
    rows = [PartitionTreeRow(level=0, name="public.events", partition_type=PartitionType.HASH)]

    # Act
    tree = build_partition_tree(rows)

    # Assert
    assert tree is not None
    assert tree.has_unaddressable_children is False


def test__partition_node__hash_children__ignores_siblings_bound_any_other_way() -> None:
    # Arrange -- a branch holding a hash bucket, a DEFAULT sibling and a legacy
    # LIST partition someone attached by hand.
    node = PartitionNode(
        name="public.events__2026_w35",
        partition_type=PartitionType.HASH,
        children=(
            PartitionNode(name="public.events__2026_w35__h0", bounds=HashBounds(modulus=2, remainder=0)),
            PartitionNode(name="public.events__2026_w35_default", bounds=DefaultBounds()),
            PartitionNode(name="public.events__2026_w35__eu", bounds=ListBounds(values=("eu",))),
        ),
    )

    # Act / Assert -- keyspace arithmetic is only meaningful over hash bounds;
    # counting a DEFAULT sibling into it would report a tiled branch as short.
    assert [c.name for c in node.hash_children] == ["public.events__2026_w35__h0"]


# ── Extension points and small accessors ────────────────────────────────────────


def test__subpartition_spec_base__abstract_members__refuse_to_answer() -> None:
    # Arrange -- the base exists to be subclassed; answering here would let a
    # half-written spec through instead of failing where it is written.
    base = SubpartitionSpecBase(column="tenant_id", name_suffix="__h{remainder}")

    # Act / Assert
    with pytest.raises(NotImplementedError):
        _ = base.partition_type
    with pytest.raises(NotImplementedError):
        base.own_name_budget()


def test__hash_subpartition_spec__bounds_for__describes_the_bucket_at_its_own_modulus() -> None:
    # Arrange
    spec = HashSubpartitionSpec(column="tenant_id", modulus=4)

    # Act / Assert
    assert spec.bounds_for(2) == HashBounds(modulus=4, remainder=2)


def test__partition_node__relname__strips_the_schema_and_survives_without_one() -> None:
    # Arrange / Act / Assert
    assert PartitionNode(name="public.events__h0").relname == "events__h0"
    assert PartitionNode(name="events__h0").relname == "events__h0"


def test__validate_pg_identifier__value_that_is_not_an_identifier__is_refused() -> None:
    # Arrange / Act / Assert -- it reaches DDL as a quoted identifier, so a
    # value shaped like an expression would create a very odd relation.
    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        HashSubpartitionSpec(column="tenant_id; DROP TABLE t", modulus=2)


def test__validate_pg_identifier__mixed_case__is_folded_the_way_postgresql_folds_it() -> None:
    # Arrange / Act
    spec = HashSubpartitionSpec(column="TenantId", modulus=2)

    # Assert
    assert spec.column == "tenantid"
