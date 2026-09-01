"""Partition schemes: one level of a partition tree, and the levels below it."""

import pytest
from pydantic import TypeAdapter, ValidationError

from pg_partsmith.boundaries import Axis, CursorSource, NumericBoundaries, RangeBoundaries, TimeBoundaries, Window
from pg_partsmith.constants import MAX_IDENTIFIER_LENGTH, MAX_SCHEME_DEPTH
from pg_partsmith.periods import PartitionGranularity
from pg_partsmith.scheme import (
    HashPartitioning,
    LevelKind,
    ListGroup,
    ListPartitioning,
    PartitionScheme,
    RangePartitioning,
    SchemeBase,
    name_fits,
)
from pg_partsmith.topology import DefaultBounds, HashBounds, ListBounds, PartitionType


class _StepBoundaries:
    """A user-written RangeBoundaries over an integer axis, ten ids per window."""

    axis = Axis.INTEGER
    cursor_source = CursorSource.MAX_KEY

    def window_at(self, position: object) -> Window:
        start = (int(position) // 10) * 10 if position is not None else 0
        return Window(start=start, end=start + 10)

    def shift(self, window: Window, offset: int) -> Window:
        return Window(start=window.start + 10 * offset, end=window.end + 10 * offset)

    def literals(self, window: Window) -> tuple[str, str]:
        return (str(window.start), str(window.end))

    def decode(self, literal: str) -> int | None:
        return int(literal) if literal.isdigit() else None

    def child_name(self, parent_relname: str, window: Window) -> str:
        return f"{parent_relname}__{window.start}"

    def parse_child_name(self, relname: str) -> Window | None:
        return None

    def describe(self, window: Window) -> str:
        return f"[{window.start}, {window.end})"


class _BudgetedBoundaries(_StepBoundaries):
    """The same, declaring how many bytes its suffix needs."""

    def own_name_budget(self) -> int:
        return 5


def _groups() -> tuple[ListGroup, ...]:
    return (ListGroup(name="eu", values=("de", "fr")), ListGroup(name="us", values=("us",)))


def _list(**overrides: object) -> ListPartitioning:
    base: dict[str, object] = {"key": "region", "groups": _groups()}
    base.update(overrides)
    return ListPartitioning(**base)  # type: ignore[arg-type]


def _nest(levels: int) -> HashPartitioning:
    scheme = HashPartitioning(key="c0", modulus=2)
    for level in range(1, levels):
        scheme = HashPartitioning(key=f"c{level}", modulus=2, child=scheme)
    return scheme


# -- key coercion and validation ----------------------------------------------------


@pytest.mark.parametrize(
    ("key", "expected"),
    [
        ("tenant_id", ("tenant_id",)),
        (("tenant_id", "shard_id"), ("tenant_id", "shard_id")),
        (["tenant_id", "shard_id"], ("tenant_id", "shard_id")),
        ("TenantId", ("tenantid",)),
        (" tenant_id ", ("tenant_id",)),
    ],
)
def test__scheme__key__coerced_to_a_tuple_of_normalised_columns(key: object, expected: tuple[str, ...]) -> None:
    # Arrange / Act
    scheme = HashPartitioning(key=key, modulus=2)

    # Assert
    assert scheme.key == expected
    assert scheme.columns == expected
    assert scheme.leading_column == expected[0]
    assert scheme.key_arity == len(expected)


@pytest.mark.parametrize("key", [123, b"tenant_id", None])
def test__scheme__key_that_is_not_a_column_or_sequence__raises_type_error(key: object) -> None:
    # Arrange / Act / Assert
    with pytest.raises(TypeError, match="key must be a column name or a sequence of them"):
        HashPartitioning(key=key, modulus=2)


def test__scheme__empty_key__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="at least one column"):
        HashPartitioning(key=(), modulus=2)


def test__scheme__repeated_key_column__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="must be distinct"):
        HashPartitioning(key=("tenant_id", "tenant_id"), modulus=2)


@pytest.mark.parametrize("column", ["tenant_id; DROP TABLE t", "1tenant", "tenant-id", "a b"])
def test__scheme__key_that_is_not_an_identifier__rejected(column: str) -> None:
    # Arrange / Act / Assert -- it reaches DDL as a quoted identifier, so a value shaped like an expression
    # would create a very odd relation
    with pytest.raises(ValidationError, match="Invalid SQL identifier"):
        HashPartitioning(key=column, modulus=2)


def test__scheme__same_column_on_two_levels__rejected() -> None:
    # Arrange / Act / Assert -- the lower level would have nothing left to divide
    with pytest.raises(ValidationError, match="distinct across levels"):
        HashPartitioning(key="tenant_id", modulus=2, child=HashPartitioning(key="tenant_id", modulus=2))


def test__scheme__column_repeated_two_levels_down__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="distinct across levels"):
        RangePartitioning(
            key=("created_at", "tenant_id"),
            boundaries=TimeBoundaries(granularity=PartitionGranularity.DAY),
            child=HashPartitioning(key="shard", modulus=2, child=HashPartitioning(key="tenant_id", modulus=2)),
        )


def test__scheme__at_the_depth_limit__accepted() -> None:
    # Arrange / Act
    scheme = _nest(MAX_SCHEME_DEPTH)

    # Assert
    assert scheme.depth() == MAX_SCHEME_DEPTH


def test__scheme__deeper_than_the_limit__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match=f"limited to {MAX_SCHEME_DEPTH} levels, got {MAX_SCHEME_DEPTH + 1}"):
        _nest(MAX_SCHEME_DEPTH + 1)


def test__scheme__is_frozen__assignment_rejected() -> None:
    # Arrange
    scheme = HashPartitioning(key="tenant_id", modulus=2)

    # Act / Assert
    with pytest.raises(ValidationError, match="frozen"):
        scheme.modulus = 4  # type: ignore[misc]


# -- tree arithmetic ------------------------------------------------------------------


def test__scheme__walk__lists_every_level_outermost_first() -> None:
    # Arrange
    leaf = HashPartitioning(key="tenant_id", modulus=2)
    middle = ListPartitioning(key="region", groups=_groups(), child=leaf)
    root = RangePartitioning(
        key="created_at", boundaries=TimeBoundaries(granularity=PartitionGranularity.DAY), child=middle
    )

    # Act / Assert
    assert root.walk() == [root, middle, leaf]
    assert root.depth() == 3
    assert root.all_columns() == ["created_at", "region", "tenant_id"]
    assert leaf.walk() == [leaf]
    assert leaf.depth() == 1


@pytest.mark.parametrize(
    ("scheme", "method", "kind", "described"),
    [
        (HashPartitioning(key="tenant_id", modulus=2), PartitionType.HASH, LevelKind.SET, "HASH (tenant_id)"),
        (_list(), PartitionType.LIST, LevelKind.SET, "LIST (region)"),
        (
            RangePartitioning(key=("created_at", "tenant_id"), boundaries=NumericBoundaries(step=10)),
            PartitionType.RANGE,
            LevelKind.PROGRESSION,
            "RANGE (created_at, tenant_id)",
        ),
    ],
    ids=["hash", "list", "range"],
)
def test__scheme__method_kind_and_describe__match_the_level(
    scheme: SchemeBase, method: PartitionType, kind: LevelKind, described: str
) -> None:
    # Arrange / Act / Assert
    assert scheme.method is method
    assert scheme.kind is kind
    assert scheme.describe() == described


def test__level_kind__has_the_two_planner_treatments() -> None:
    # Arrange / Act / Assert
    assert LevelKind.PROGRESSION.value == "progression"
    assert LevelKind.SET.value == "set"


def test__scheme_base__abstract_members__refuse_to_answer() -> None:
    # Arrange -- the base exists to be subclassed; answering here would let a half-written level through
    base = SchemeBase(key="tenant_id")

    # Act / Assert
    with pytest.raises(NotImplementedError):
        _ = base.method
    with pytest.raises(NotImplementedError):
        _ = base.kind
    with pytest.raises(NotImplementedError):
        base.own_name_budget()
    with pytest.raises(NotImplementedError):
        base.describe()


# -- HashPartitioning ---------------------------------------------------------------


def test__hash_partitioning__defaults__names_buckets_with_double_underscore_h() -> None:
    # Arrange
    scheme = HashPartitioning(key="tenant_id", modulus=4)

    # Act / Assert
    assert scheme.name_suffix == "__h{remainder}"
    assert scheme.child_name("events__2026_w35", 2) == "events__2026_w35__h2"
    assert scheme.method_name == "hash"


def test__hash_partitioning__custom_name_suffix__used_for_children() -> None:
    # Arrange
    scheme = HashPartitioning(key="tenant_id", modulus=2, name_suffix="_h{remainder}")

    # Act / Assert
    assert scheme.child_name("events_20260824", 1) == "events_20260824_h1"


def test__hash_partitioning__bare_placeholder_suffix__accepted() -> None:
    # Arrange
    scheme = HashPartitioning(key="tenant_id", modulus=2, name_suffix="{remainder}")

    # Act / Assert
    assert scheme.child_name("t", 1) == "t1"
    assert scheme.own_name_budget() == 1


@pytest.mark.parametrize(
    "suffix",
    ["_bucket", '"; DROP TABLE x --{remainder}', "{remainder}X", "", "{remainder}{remainder}", "_{name}"],
)
def test__hash_partitioning__unsafe_name_suffix__rejected(suffix: str) -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="remainder"):
        HashPartitioning(key="tenant_id", modulus=2, name_suffix=suffix)


def test__hash_partitioning__bounds_for__describes_the_bucket_at_its_own_modulus() -> None:
    # Arrange
    scheme = HashPartitioning(key="tenant_id", modulus=4)

    # Act / Assert
    assert scheme.bounds_for(2) == HashBounds(modulus=4, remainder=2)


@pytest.mark.parametrize("modulus", [0, -1])
def test__hash_partitioning__non_positive_modulus__rejected(modulus: int) -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="modulus"):
        HashPartitioning(key="tenant_id", modulus=modulus)


@pytest.mark.parametrize(("modulus", "expected"), [(2, 4), (10, 4), (16, 5), (100, 5), (1000, 6)])
def test__hash_partitioning__own_name_budget__sized_for_the_widest_remainder(modulus: int, expected: int) -> None:
    # Arrange
    scheme = HashPartitioning(key="tenant_id", modulus=modulus)

    # Act / Assert -- "__h" plus the digits of modulus - 1
    assert scheme.own_name_budget() == expected


def test__hash_partitioning__nested_levels__budget_accumulates() -> None:
    # Arrange
    scheme = HashPartitioning(key="tenant_id", modulus=2, child=HashPartitioning(key="shard_id", modulus=16))

    # Act / Assert
    assert scheme.name_length_budget() == (len("__h") + 1) + (len("__h") + 2)
    assert scheme.own_name_budget() == len("__h") + 1


# -- ListGroup and ListPartitioning -----------------------------------------------------


def test__list_group__bounds__render_its_values() -> None:
    # Arrange / Act
    bounds = ListGroup(name="eu", values=("de", "fr")).bounds()

    # Assert
    assert bounds == ListBounds(values=("de", "fr"))
    assert bounds.includes_null is False


def test__list_group__name_and_values__normalised() -> None:
    # Arrange / Act
    group = ListGroup(name="EU", values=(" de ", "fr"))

    # Assert
    assert group.name == "eu"
    assert group.values == ("de", "fr")


def test__list_group__without_values__rejected() -> None:
    # Arrange / Act / Assert -- such a partition could never route a row
    with pytest.raises(ValidationError, match="at least one value"):
        ListGroup(name="eu", values=())


def test__list_group__repeating_a_value__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="repeats a value"):
        ListGroup(name="eu", values=("de", "de"))


def test__list_group__name_that_is_not_an_identifier__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="Invalid SQL identifier"):
        ListGroup(name="eu-west", values=("de",))


def test__list_partitioning__defaults__names_children_after_their_group() -> None:
    # Arrange
    scheme = _list()

    # Act / Assert
    assert scheme.child_name("events__2026_w35", "eu") == "events__2026_w35__eu"
    assert scheme.include_default is False
    assert scheme.default_name == "other"
    assert scheme.name_suffix == "__{name}"
    assert scheme.method_name == "list"


def test__list_partitioning__custom_name_suffix__used_for_children() -> None:
    # Arrange
    scheme = _list(name_suffix="_r_{name}")

    # Act / Assert
    assert scheme.child_name("events", "eu") == "events_r_eu"


def test__list_partitioning__default_bounds__are_the_catch_all() -> None:
    # Arrange / Act / Assert
    assert _list(include_default=True).default_bounds() == DefaultBounds()


def test__list_partitioning__groups_given_as_dicts__parsed() -> None:
    # Arrange / Act
    scheme = ListPartitioning(key=["region"], groups=[{"name": "eu", "values": ["de"]}])

    # Assert
    assert scheme.groups == (ListGroup(name="eu", values=("de",)),)


def test__list_partitioning__composite_key__rejected() -> None:
    # Arrange / Act / Assert -- PostgreSQL has no composite LIST key
    with pytest.raises(ValidationError, match="exactly one column"):
        _list(key=("region", "tier"))


def test__list_partitioning__no_groups__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="at least one group"):
        _list(groups=())


def test__list_partitioning__duplicate_group_names__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="names must be distinct"):
        _list(groups=(ListGroup(name="eu", values=("de",)), ListGroup(name="eu", values=("fr",))))


def test__list_partitioning__value_claimed_by_two_groups__rejected() -> None:
    # Arrange / Act / Assert -- PostgreSQL would refuse the second partition
    with pytest.raises(ValidationError, match="claimed by both 'eu' and 'dach'"):
        _list(groups=(ListGroup(name="eu", values=("de",)), ListGroup(name="dach", values=("de",))))


def test__list_partitioning__default_name_colliding_with_a_group__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="names must be distinct"):
        _list(include_default=True, default_name="eu")


def test__list_partitioning__default_name_matching_a_group_only_when_included__accepted() -> None:
    # Arrange / Act -- the DEFAULT partition is not maintained, so its name is not taken
    scheme = _list(include_default=False, default_name="eu")

    # Assert
    assert scheme.default_name == "eu"


def test__list_partitioning__default_name_that_is_not_an_identifier__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="Invalid SQL identifier"):
        _list(default_name="bad name")


@pytest.mark.parametrize("suffix", ["_region", "{name}X", "{name}-", "", "__{remainder}"])
def test__list_partitioning__unsafe_name_suffix__rejected(suffix: str) -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="name"):
        _list(name_suffix=suffix)


def test__list_partitioning__own_name_budget__sized_for_the_longest_name() -> None:
    # Arrange
    scheme = _list(groups=(ListGroup(name="eu", values=("de",)), ListGroup(name="apac", values=("jp",))))

    # Act / Assert -- "__" plus the longest group name
    assert scheme.own_name_budget() == len("__") + len("apac")


def test__list_partitioning__default_included__counts_towards_the_budget() -> None:
    # Arrange
    scheme = _list(groups=(ListGroup(name="eu", values=("de",)),), include_default=True, default_name="everything_else")

    # Act / Assert
    assert scheme.own_name_budget() == len("__") + len("everything_else")


def test__list_partitioning__default_excluded__does_not_count_towards_the_budget() -> None:
    # Arrange
    scheme = _list(
        groups=(ListGroup(name="eu", values=("de",)),), include_default=False, default_name="everything_else"
    )

    # Act / Assert
    assert scheme.own_name_budget() == len("__") + len("eu")


# -- RangePartitioning ---------------------------------------------------------------


def test__range_partitioning__time_boundaries__exposed_typed() -> None:
    # Arrange
    boundaries = TimeBoundaries(granularity=PartitionGranularity.MONTH)

    # Act
    scheme = RangePartitioning(key="created_at", boundaries=boundaries)

    # Assert
    assert scheme.boundaries is boundaries
    assert scheme.range_boundaries is boundaries
    assert scheme.time_boundaries is boundaries
    assert scheme.method_name == "range"


def test__range_partitioning__numeric_boundaries__time_boundaries_is_none() -> None:
    # Arrange / Act
    scheme = RangePartitioning(key="msg_id", boundaries=NumericBoundaries(step=100))

    # Assert
    assert scheme.time_boundaries is None
    assert isinstance(scheme.range_boundaries, NumericBoundaries)


def test__range_partitioning__boundaries_as_time_dict__parsed() -> None:
    # Arrange / Act
    scheme = RangePartitioning(key="created_at", boundaries={"kind": "time", "granularity": "month"})

    # Assert
    assert scheme.boundaries == TimeBoundaries(granularity=PartitionGranularity.MONTH)


def test__range_partitioning__boundaries_dict_without_kind__read_as_time() -> None:
    # Arrange / Act
    scheme = RangePartitioning(key="created_at", boundaries={"granularity": "day", "tz": "Europe/Moscow"})

    # Assert
    assert scheme.time_boundaries is not None
    assert scheme.time_boundaries.granularity is PartitionGranularity.DAY
    assert scheme.time_boundaries.timezone_name == "Europe/Moscow"


def test__range_partitioning__boundaries_as_integer_dict__parsed() -> None:
    # Arrange / Act
    scheme = RangePartitioning(key="msg_id", boundaries={"kind": "integer", "step": 100})

    # Assert
    assert scheme.boundaries == NumericBoundaries(step=100)


def test__range_partitioning__boundaries_dict_of_unknown_kind__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="Unknown boundaries kind 'geo'"):
        RangePartitioning(key="location", boundaries={"kind": "geo"})


def test__range_partitioning__custom_boundaries_object__accepted_as_is() -> None:
    # Arrange
    boundaries = _StepBoundaries()

    # Act
    scheme = RangePartitioning(key="id", boundaries=boundaries)

    # Assert
    assert isinstance(boundaries, RangeBoundaries)
    assert scheme.range_boundaries is boundaries
    assert scheme.time_boundaries is None
    assert scheme.kind is LevelKind.PROGRESSION


def test__range_partitioning__object_that_is_not_boundaries__raises_type_error() -> None:
    # Arrange / Act / Assert
    with pytest.raises(TypeError, match="boundaries must implement RangeBoundaries, got object"):
        RangePartitioning(key="id", boundaries=object())


@pytest.mark.parametrize(
    ("boundaries", "expected"),
    [
        (TimeBoundaries(granularity=PartitionGranularity.MONTH), len("__0000_00")),
        (NumericBoundaries(step=100), len("__") + len("m") + 19),
        (_BudgetedBoundaries(), 5),
        (_StepBoundaries(), len("__0000_00_00_00")),
    ],
    ids=["time", "numeric", "custom-with-budget", "custom-without-budget"],
)
def test__range_partitioning__own_name_budget__asks_the_boundaries_or_assumes_the_allowance(
    boundaries: object, expected: int
) -> None:
    # Arrange
    scheme = RangePartitioning(key="id", boundaries=boundaries)

    # Act / Assert
    assert scheme.own_name_budget() == expected


def test__range_partitioning__with_a_child__budget_accumulates_down_the_tree() -> None:
    # Arrange
    scheme = RangePartitioning(
        key="created_at",
        boundaries=TimeBoundaries(granularity=PartitionGranularity.HOUR),
        child=HashPartitioning(key="tenant_id", modulus=16),
    )

    # Act / Assert
    assert scheme.name_length_budget() == len("__0000_00_00_00") + len("__h") + 2


# -- name budgets --------------------------------------------------------------------


def test__name_fits__within_the_limit__true_with_the_worst_case_length() -> None:
    # Arrange
    scheme = HashPartitioning(key="tenant_id", modulus=4)

    # Act
    fits, total = name_fits("events", scheme)

    # Assert
    assert fits is True
    assert total == len("events") + len("__h") + 1


def test__name_fits__at_the_limit_exactly__still_fits() -> None:
    # Arrange
    scheme = HashPartitioning(key="tenant_id", modulus=4)
    table_name = "a" * (MAX_IDENTIFIER_LENGTH - scheme.name_length_budget())

    # Act / Assert
    assert name_fits(table_name, scheme) == (True, MAX_IDENTIFIER_LENGTH)
    assert name_fits(table_name + "a", scheme) == (False, MAX_IDENTIFIER_LENGTH + 1)


def test__name_fits__counts_bytes_not_characters() -> None:
    # Arrange -- PostgreSQL's limit is 63 bytes; a multi-byte character costs more than one
    scheme = HashPartitioning(key="tenant_id", modulus=2)

    # Act
    _, total = name_fits("café", scheme)

    # Assert
    assert total == 5 + scheme.name_length_budget()


# -- discriminated union parsing ------------------------------------------------------


@pytest.mark.parametrize(
    ("payload", "expected_type"),
    [
        ({"method": "hash", "key": "tenant_id", "modulus": 4}, HashPartitioning),
        ({"method": "list", "key": "region", "groups": [{"name": "eu", "values": ["de"]}]}, ListPartitioning),
        (
            {"method": "range", "key": "created_at", "boundaries": {"kind": "time", "granularity": "day"}},
            RangePartitioning,
        ),
        ({"method_name": "hash", "key": "tenant_id", "modulus": 4}, HashPartitioning),
    ],
    ids=["hash", "list", "range", "by-field-name"],
)
def test__partition_scheme__dict_with_method__dispatches_to_the_level_class(
    payload: dict[str, object], expected_type: type[SchemeBase]
) -> None:
    # Arrange / Act
    scheme = TypeAdapter(PartitionScheme).validate_python(payload)

    # Assert
    assert isinstance(scheme, expected_type)


def test__partition_scheme__nested_child_dicts__parsed_all_the_way_down() -> None:
    # Arrange / Act
    scheme = RangePartitioning.model_validate(
        {
            "method": "range",
            "key": "created_at",
            "boundaries": {"kind": "time", "granularity": "week"},
            "child": {
                "method": "hash",
                "key": "tenant_id",
                "modulus": 4,
                "child": {"method": "list", "key": "region", "groups": [{"name": "eu", "values": ["de"]}]},
            },
        }
    )

    # Assert
    assert isinstance(scheme.child, HashPartitioning)
    assert isinstance(scheme.child.child, ListPartitioning)
    assert scheme.all_columns() == ["created_at", "tenant_id", "region"]


def test__partition_scheme__unknown_method__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="does not match any of the expected tags"):
        TypeAdapter(PartitionScheme).validate_python({"method": "geo", "key": "location"})


def test__partition_scheme__child_of_unknown_method__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="child"):
        HashPartitioning(key="tenant_id", modulus=2, child={"method": "geo", "key": "location"})


def test__scheme__dump_by_alias__spells_the_discriminator_as_method() -> None:
    # Arrange
    scheme = HashPartitioning(key="tenant_id", modulus=4)

    # Act
    dumped = scheme.model_dump(by_alias=True)

    # Assert
    assert dumped == {
        "key": ("tenant_id",),
        "child": None,
        "method": "hash",
        "modulus": 4,
        "name_suffix": "__h{remainder}",
    }


def test__scheme__json_round_trip__reloads_an_equal_tree() -> None:
    # Arrange
    scheme = RangePartitioning(
        key="created_at",
        boundaries=TimeBoundaries(granularity=PartitionGranularity.WEEK, tz="Europe/Moscow", codec="uuidv7"),
        child=HashPartitioning(key="tenant_id", modulus=4, child=_list(include_default=True)),
    )

    # Act
    from_python = RangePartitioning.model_validate(scheme.model_dump(mode="json"))
    from_json = TypeAdapter(PartitionScheme).validate_json(scheme.model_dump_json(by_alias=True))

    # Assert
    assert from_python == scheme
    assert from_json == scheme
