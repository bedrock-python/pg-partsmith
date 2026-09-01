"""TablePartitionConfig, PartitionInfo, MaintenanceResult and MaintenanceIssue."""

import json
from datetime import timedelta
from zoneinfo import ZoneInfo

import pytest
from pydantic import ValidationError

import pg_partsmith
from pg_partsmith.boundaries import EpochBoundaryCodec, NumericBoundaries, TimeBoundaries, UUIDv7BoundaryCodec
from pg_partsmith.entities import (
    DefaultBounds,
    HashBounds,
    ListBounds,
    MaintenanceIssue,
    MaintenanceIssueStep,
    MaintenanceResult,
    PartitionGranularity,
    PartitionInfo,
    PartitionStrategy,
    PartitionType,
    RangeBounds,
    TablePartitionConfig,
)
from pg_partsmith.lifecycle import (
    CreateAhead,
    CreateUntil,
    DetachMode,
    DropAfter,
    KeepFor,
    KeepNewest,
    LifecyclePolicy,
)
from pg_partsmith.scheme import HashPartitioning, ListGroup, ListPartitioning, RangePartitioning
from pg_partsmith.topology import RelationKind

_MOSCOW = ZoneInfo("Europe/Moscow")


def _flat(**overrides: object) -> TablePartitionConfig:
    base: dict[str, object] = {
        "table_name": "events",
        "partition_column": "created_at",
        "granularity": PartitionGranularity.WEEK,
    }
    base.update(overrides)
    return TablePartitionConfig(**base)  # type: ignore[arg-type]


def _hash_root(**overrides: object) -> TablePartitionConfig:
    base: dict[str, object] = {"table_name": "tasks", "scheme": HashPartitioning(key="task_id", modulus=8)}
    base.update(overrides)
    return TablePartitionConfig(**base)  # type: ignore[arg-type]


# -- flat spelling -----------------------------------------------------------------


def test__config__flat_without_type_or_strategy__derives_a_time_partitioned_range_root() -> None:
    # Arrange / Act
    config = _flat()

    # Assert
    assert config.partition_type is PartitionType.RANGE
    assert config.partition_strategy is PartitionStrategy.TIME_BASED
    assert isinstance(config.scheme, RangePartitioning)
    assert config.scheme.key == ("created_at",)
    assert config.scheme.child is None
    assert config.granularity is PartitionGranularity.WEEK
    assert config.time_boundaries == TimeBoundaries(granularity=PartitionGranularity.WEEK)
    assert config.is_time_based is True


@pytest.mark.parametrize(
    "checks",
    [
        {"partition_type": PartitionType.RANGE},
        {"partition_strategy": PartitionStrategy.TIME_BASED},
        {"partition_type": PartitionType.RANGE, "partition_strategy": PartitionStrategy.TIME_BASED},
        {"partition_type": "range", "partition_strategy": "time_based"},
    ],
)
def test__config__flat_with_matching_type_and_strategy__accepted(checks: dict[str, object]) -> None:
    # Arrange / Act
    config = _flat(**checks)

    # Assert -- the declarations are checked, not stored
    assert config.partition_type is PartitionType.RANGE
    assert config.partition_strategy is PartitionStrategy.TIME_BASED
    assert config.checks_ is None


def test__config__flat_declared_as_list__rejected_against_the_range_root() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="does not match the scheme's root, which is RANGE"):
        _flat(partition_type=PartitionType.LIST)


def test__config__composed_declared_strategy_disagreeing__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="does not match the scheme, which is 'hash_based'"):
        _hash_root(partition_strategy=PartitionStrategy.VALUE_BASED)


def test__config__composed_declared_type_disagreeing__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="does not match the scheme's root, which is HASH"):
        _hash_root(partition_type=PartitionType.RANGE)


def test__config__unknown_partition_type_name__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="not a valid PartitionType"):
        _flat(partition_type="bogus")


def test__config__flat_without_granularity__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="requires granularity"):
        TablePartitionConfig(table_name="events", partition_column="created_at")


@pytest.mark.parametrize(
    "strategy",
    [PartitionStrategy.HASH_BASED, PartitionStrategy.VALUE_BASED, PartitionStrategy.NUMERIC_BASED, "hash_based"],
)
def test__config__flat_with_a_non_time_strategy__points_at_scheme(strategy: object) -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match=r"scheme=HashPartitioning\(\.\.\.\)"):
        _flat(partition_strategy=strategy)


def test__config__neither_scheme_nor_partition_column__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="needs either scheme or partition_column"):
        TablePartitionConfig(table_name="events")


def test__config__flat_defaults__six_ahead_twelve_kept_and_dropped_at_once() -> None:
    # Arrange / Act
    config = _flat()

    # Assert
    assert config.create_ahead_count == 6
    assert config.retention_count == 12
    assert config.lifecycle == LifecyclePolicy(
        creation=CreateAhead(count=6),
        retention=KeepNewest(count=12),
        detach=DetachMode.AUTO,
        drop=DropAfter(grace=timedelta(0)),
    )


def test__config__flat_counts__become_the_lifecycle_policy() -> None:
    # Arrange / Act
    config = _flat(create_ahead_count=3, retention_count=4)

    # Assert
    assert config.lifecycle.creation == CreateAhead(count=3)
    assert config.lifecycle.retention == KeepNewest(count=4)
    assert config.create_ahead_count == 3
    assert config.retention_count == 4


def test__config__only_retention_count__creation_keeps_its_default() -> None:
    # Arrange / Act
    config = _flat(retention_count=2)

    # Assert
    assert config.create_ahead_count == 6
    assert config.retention_count == 2


@pytest.mark.parametrize("count", [0, -1])
def test__config__non_positive_count__rejected(count: int) -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError):
        _flat(create_ahead_count=count)


# -- composed spelling -------------------------------------------------------------


def test__config__composed_range_root__accepted_as_is() -> None:
    # Arrange
    scheme = RangePartitioning(
        key="id",
        boundaries=TimeBoundaries(granularity=PartitionGranularity.WEEK, codec=UUIDv7BoundaryCodec()),
        child=HashPartitioning(key="organization_id", modulus=2, name_suffix="_h{remainder}"),
    )
    lifecycle = LifecyclePolicy(
        creation=CreateAhead(count=3), retention=KeepNewest(count=12), drop=DropAfter(grace=timedelta(days=7))
    )

    # Act
    config = TablePartitionConfig(table_name="issue_events", scheme=scheme, lifecycle=lifecycle)

    # Assert
    assert config.scheme is scheme
    assert config.lifecycle is lifecycle
    assert config.partition_strategy is PartitionStrategy.TIME_BASED
    assert config.partition_column == "id"
    assert config.granularity is PartitionGranularity.WEEK
    assert config.subpartition is scheme.child
    assert config.create_ahead_count == 3
    assert config.retention_count == 12
    assert config.levels == [scheme, scheme.child]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("partition_column", "created_at"),
        ("trailing_partition_columns", ("tenant_id",)),
        ("granularity", PartitionGranularity.MONTH),
        ("tz", "UTC"),
        ("boundary_codec", "uuidv7"),
        ("subpartition", HashPartitioning(key="shard", modulus=2)),
    ],
)
def test__config__scheme_and_a_flat_scheme_field__rejected(field: str, value: object) -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match=f"Pass either scheme or the flat fields \\['{field}'\\]"):
        _hash_root(**{field: value})


@pytest.mark.parametrize("field", ["create_ahead_count", "retention_count"])
def test__config__lifecycle_and_a_flat_count__rejected(field: str) -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match=f"Pass either lifecycle or the flat fields \\['{field}'\\]"):
        _flat(lifecycle=LifecyclePolicy(), **{field: 3})


def test__config__scheme_given_as_a_dict__parsed_through_the_union() -> None:
    # Arrange / Act
    config = TablePartitionConfig(
        table_name="regions",
        scheme={
            "method": "list",
            "key": "region",
            "groups": [{"name": "eu", "values": ["de", "fr"]}],
            "include_default": True,
        },
    )

    # Assert
    assert isinstance(config.scheme, ListPartitioning)
    assert config.partition_strategy is PartitionStrategy.VALUE_BASED
    assert config.scheme.include_default is True


# -- flat sugar ------------------------------------------------------------------------


def test__config__subpartition_sugar__becomes_the_child_level() -> None:
    # Arrange
    child = HashPartitioning(key="tenant_id", modulus=4)

    # Act
    config = _flat(subpartition=child)

    # Assert
    assert config.subpartition == child
    assert config.scheme.child == child
    assert [type(level) for level in config.levels] == [RangePartitioning, HashPartitioning]
    assert config.has_progression_level is True


def test__config__subpartition_sugar_as_a_dict__parsed_through_the_union() -> None:
    # Arrange / Act
    config = _flat(subpartition={"method": "hash", "key": "tenant_id", "modulus": 4})

    # Assert
    assert isinstance(config.subpartition, HashPartitioning)
    assert config.subpartition.modulus == 4


@pytest.mark.parametrize("tz", ["Europe/Moscow", _MOSCOW])
def test__config__tz_sugar__reaches_the_time_boundaries(tz: object) -> None:
    # Arrange / Act
    config = _flat(tz=tz)

    # Assert
    assert config.time_boundaries is not None
    assert config.time_boundaries.tz is _MOSCOW
    assert config.time_boundaries.timezone_name == "Europe/Moscow"


@pytest.mark.parametrize(
    ("codec", "expected"),
    [
        ("uuidv7", UUIDv7BoundaryCodec()),
        (UUIDv7BoundaryCodec(), UUIDv7BoundaryCodec()),
        ("epoch_milliseconds", EpochBoundaryCodec("milliseconds")),
        (EpochBoundaryCodec("seconds"), EpochBoundaryCodec("seconds")),
    ],
)
def test__config__boundary_codec_sugar__by_name_or_instance(codec: object, expected: object) -> None:
    # Arrange / Act
    config = _flat(boundary_codec=codec)

    # Assert
    assert config.time_boundaries is not None
    assert config.time_boundaries.codec == expected


def test__config__trailing_partition_columns__extend_the_root_key() -> None:
    # Arrange / Act
    config = _flat(trailing_partition_columns=("tenant_id", "region"))

    # Assert
    assert config.partition_column == "created_at"
    assert config.trailing_partition_columns == ("tenant_id", "region")
    assert config.partition_columns == ("created_at", "tenant_id", "region")
    assert config.key_arity == 3


def test__config__single_partition_column__arity_is_one() -> None:
    # Arrange / Act
    config = _flat(trailing_partition_columns=None)

    # Assert
    assert config.partition_columns == ("created_at",)
    assert config.trailing_partition_columns == ()
    assert config.key_arity == 1


def test__config__trailing_column_repeating_the_leading_one__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="must be distinct"):
        _flat(trailing_partition_columns=("created_at",))


def test__config__subpartition_on_a_root_key_column__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="distinct across levels"):
        _flat(subpartition=HashPartitioning(key="created_at", modulus=2))


# -- derived views ----------------------------------------------------------------


def test__config__hash_root__derived_views_describe_a_static_set() -> None:
    # Arrange / Act
    config = _hash_root()

    # Assert
    assert config.partition_type is PartitionType.HASH
    assert config.partition_strategy is PartitionStrategy.HASH_BASED
    assert config.partition_column == "task_id"
    assert config.trailing_partition_columns == ()
    assert config.partition_columns == ("task_id",)
    assert config.key_arity == 1
    assert config.granularity is None
    assert config.time_boundaries is None
    assert config.subpartition is None
    assert config.is_time_based is False
    assert config.has_progression_level is False
    assert config.is_progression_root is False
    assert config.levels == [config.scheme]
    assert config.root is config.scheme


def test__config__list_root__is_value_based() -> None:
    # Arrange / Act
    config = TablePartitionConfig(
        table_name="regions",
        scheme=ListPartitioning(
            key="region", groups=(ListGroup(name="eu", values=("de", "fr")),), include_default=True
        ),
    )

    # Assert
    assert config.partition_type is PartitionType.LIST
    assert config.partition_strategy is PartitionStrategy.VALUE_BASED
    assert config.is_time_based is False
    assert config.has_progression_level is False


def test__config__numeric_root__is_a_numeric_progression() -> None:
    # Arrange / Act
    config = TablePartitionConfig(
        table_name="queue", scheme=RangePartitioning(key="msg_id", boundaries=NumericBoundaries(step=100_000))
    )

    # Assert
    assert config.partition_type is PartitionType.RANGE
    assert config.partition_strategy is PartitionStrategy.NUMERIC_BASED
    assert config.is_time_based is False
    assert config.granularity is None
    assert config.time_boundaries is None
    assert config.is_progression_root is True
    assert config.has_progression_level is True


def test__config__hash_root_over_a_range_child__has_a_progression_below_the_root() -> None:
    # Arrange / Act
    config = TablePartitionConfig(
        table_name="t",
        scheme=HashPartitioning(
            key="a", modulus=2, child=RangePartitioning(key="b", boundaries=NumericBoundaries(step=5))
        ),
    )

    # Assert
    assert config.has_progression_level is True
    assert config.is_progression_root is False
    assert config.partition_strategy is PartitionStrategy.HASH_BASED


def test__config__non_count_policies__counts_are_none() -> None:
    # Arrange / Act
    config = TablePartitionConfig(
        table_name="events",
        scheme=RangePartitioning(key="created_at", boundaries=TimeBoundaries(granularity=PartitionGranularity.DAY)),
        lifecycle=LifecyclePolicy(creation=CreateUntil(position=5), retention=KeepFor(age=timedelta(days=3))),
    )

    # Assert
    assert config.create_ahead_count is None
    assert config.retention_count is None


def test__config__qualified_name__prefixes_the_schema_when_set() -> None:
    # Arrange / Act / Assert
    assert _flat(schema="analytics").qualified_name == "analytics.events"
    assert _flat().qualified_name == "events"
    assert _flat(schema="analytics").db_schema == "analytics"
    assert _flat().db_schema is None


def test__config__schema__accepted_by_alias_and_by_field_name() -> None:
    # Arrange / Act
    by_alias = _flat(schema="analytics")
    by_name = _flat(schema_name="analytics")

    # Assert
    assert by_alias == by_name
    assert by_name.schema_name == "analytics"


# -- identifiers --------------------------------------------------------------------


def test__config__mixed_case_identifiers__folded_to_lowercase() -> None:
    # Arrange / Act
    config = _flat(table_name="Events", schema="Analytics", partition_column="Created_At")

    # Assert
    assert config.table_name == "events"
    assert config.db_schema == "analytics"
    assert config.partition_column == "created_at"
    assert config.qualified_name == "analytics.events"


@pytest.mark.parametrize("table_name", ["1events", "my-table", "a b", "events; drop table t", ""])
def test__config__invalid_table_name__rejected(table_name: str) -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError):
        _flat(table_name=table_name)


@pytest.mark.parametrize("schema", ["1analytics", "my-schema", "a.b"])
def test__config__invalid_schema_name__rejected(schema: str) -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="Invalid SQL identifier"):
        _flat(schema=schema)


def test__config__table_name_over_the_identifier_limit__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="too long"):
        _flat(table_name="a" * 64)


def test__config__hourly_table_name_filling_the_budget_exactly__accepted() -> None:
    # Arrange / Act -- 48 + len("__0000_00_00_00") == 63
    config = _flat(table_name="a" * 48, granularity=PartitionGranularity.HOUR)

    # Assert
    assert config.granularity is PartitionGranularity.HOUR


def test__config__hourly_table_name_one_byte_over_the_budget__rejected() -> None:
    # Arrange / Act / Assert -- 49 + 15 == 64
    with pytest.raises(ValidationError, match="is too long for this scheme"):
        _flat(table_name="a" * 49, granularity=PartitionGranularity.HOUR)


def test__config__monthly_table_name_over_every_budget__rejected() -> None:
    # Arrange / Act / Assert -- 55 + len("__0000_00") already overflows
    with pytest.raises(ValidationError, match="is too long for this scheme"):
        _flat(table_name="a" * 55, granularity=PartitionGranularity.MONTH)


def test__config__hash_root_name_too_long_for_its_buckets__rejected() -> None:
    # Arrange / Act / Assert -- 60 + len("__h99") == 65
    with pytest.raises(ValidationError, match="is too long for this scheme"):
        _hash_root(table_name="i" * 60, scheme=HashPartitioning(key="task_id", modulus=100))


def test__config__subpartition_suffix_pushing_the_name_over_the_limit__rejected() -> None:
    # Arrange -- fits alone (48 + 15 == 63), overflows once the bucket suffix is added
    table_name = "e" * 48
    _flat(table_name=table_name, granularity=PartitionGranularity.HOUR)

    # Act / Assert
    with pytest.raises(ValidationError, match="is too long for this scheme"):
        _flat(
            table_name=table_name,
            granularity=PartitionGranularity.HOUR,
            subpartition=HashPartitioning(key="tenant_id", modulus=4),
        )


@pytest.mark.parametrize(
    ("length", "granularity"),
    [
        (54, PartitionGranularity.MONTH),
        (54, PartitionGranularity.QUARTER),
        (57, PartitionGranularity.YEAR),
    ],
)
def test__config__table_name_filling_the_granularity_suffix_budget__accepted(
    length: int, granularity: PartitionGranularity
) -> None:
    # Arrange / Act -- e.g. 54 + len("__0000_00") == 63 for a monthly table
    config = _flat(table_name="a" * length, granularity=granularity)

    # Assert
    assert config.granularity is granularity


# -- serialization ---------------------------------------------------------------------


def test__config__dump__carries_scheme_and_lifecycle_and_no_flat_fields() -> None:
    # Arrange
    config = _flat()

    # Act
    dumped = config.model_dump(mode="json")

    # Assert
    assert set(dumped) == {"schema_name", "table_name", "scheme", "lifecycle", "leaves"}
    assert dumped["leaves"] == {
        "kind": "local",
        "tablespace": None,
        "storage_parameters": {},
        "inherit_privileges": False,
    }
    assert dumped["scheme"]["method_name"] == "range"
    assert dumped["scheme"]["boundaries"] == {"kind": "time", "granularity": "week", "tz": "UTC", "codec": None}
    assert dumped["lifecycle"]["creation"] == {"kind": "create_ahead", "count": 6}


def test__config__dump_by_alias__uses_the_public_spellings_and_reloads() -> None:
    # Arrange
    config = _flat(schema="analytics", subpartition=HashPartitioning(key="tenant_id", modulus=4))

    # Act
    dumped = config.model_dump(mode="json", by_alias=True)

    # Assert
    assert dumped["schema"] == "analytics"
    assert dumped["scheme"]["method"] == "range"
    assert dumped["scheme"]["child"]["method"] == "hash"
    assert TablePartitionConfig.model_validate(dumped) == config


@pytest.mark.parametrize(
    "config",
    [
        TablePartitionConfig(
            table_name="events",
            partition_column="created_at",
            granularity=PartitionGranularity.MONTH,
            create_ahead_count=3,
            retention_count=12,
        ),
        TablePartitionConfig(
            table_name="events",
            schema="analytics",
            partition_column="created_at",
            trailing_partition_columns=("tenant_id",),
            granularity=PartitionGranularity.WEEK,
            tz="Europe/Moscow",
            boundary_codec="uuidv7",
            subpartition=HashPartitioning(
                key="shard_id",
                modulus=4,
                child=ListPartitioning(key="region", groups=(ListGroup(name="eu", values=("de",)),)),
            ),
        ),
        TablePartitionConfig(
            table_name="queue",
            scheme=RangePartitioning(key="msg_id", boundaries=NumericBoundaries(step=100_000, origin=7)),
            lifecycle=LifecyclePolicy(
                creation=CreateAhead(count=3),
                retention=KeepFor(age=timedelta(days=30)),
                detach=DetachMode.BLOCKING,
                drop=DropAfter(grace=timedelta(hours=6)),
            ),
        ),
    ],
    ids=["flat", "nested", "numeric"],
)
def test__config__json_round_trip__reloads_an_equal_config(config: TablePartitionConfig) -> None:
    # Arrange
    dumped = config.model_dump(mode="json")

    # Act
    reloaded = TablePartitionConfig.model_validate(json.loads(json.dumps(dumped)))
    from_json = TablePartitionConfig.model_validate_json(config.model_dump_json())

    # Assert
    assert reloaded == config
    assert from_json == config
    assert reloaded.partition_columns == config.partition_columns
    assert reloaded.levels == config.levels


def test__config__is_frozen__assignment_rejected() -> None:
    # Arrange
    config = _flat()

    # Act / Assert
    with pytest.raises(ValidationError, match="frozen"):
        config.table_name = "other"  # type: ignore[misc]


# -- PartitionInfo --------------------------------------------------------------------


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
    assert info.is_attached is True
    assert info.is_default is False
    assert info.parent_table is None


def test__partition_info__structured_bounds_only__derives_the_pair() -> None:
    # Arrange / Act
    info = PartitionInfo(
        name="public.events__2026_w35",
        partition_type=PartitionType.RANGE,
        bounds=RangeBounds(from_value="2026-08-24", to_value="2026-08-31"),
    )

    # Assert
    assert info.from_value == "2026-08-24"
    assert info.to_value == "2026-08-31"


def test__partition_info__bounds_as_a_dict__derives_the_pair_too() -> None:
    # Arrange / Act -- the shape ``model_dump`` writes
    info = PartitionInfo(
        name="p", partition_type=PartitionType.RANGE, bounds={"kind": "range", "from_value": "a", "to_value": "b"}
    )

    # Assert
    assert info.from_value == "a"
    assert info.to_value == "b"
    assert info.bounds == RangeBounds(from_value="a", to_value="b")


def test__partition_info__invalid_bounds_dict__reported_on_the_bounds_field() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="bounds"):
        PartitionInfo(
            name="p", partition_type=PartitionType.RANGE, bounds={"kind": "range", "from_value": "", "to_value": "b"}
        )


def test__partition_info__default_partition__gets_default_bounds() -> None:
    # Arrange / Act
    info = PartitionInfo(name="public.events_default", partition_type=PartitionType.RANGE, is_default=True)

    # Assert
    assert info.bounds == DefaultBounds()
    assert info.is_default is True


def test__partition_info__attached_range_without_boundaries__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="must have from_value/to_value or boundaries_expr"):
        PartitionInfo(name="p", partition_type=PartitionType.RANGE)


def test__partition_info__attached_range_with_a_blank_expression__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="must have from_value/to_value or boundaries_expr"):
        PartitionInfo(name="p", partition_type=PartitionType.RANGE, boundaries_expr="   ")


def test__partition_info__attached_range_with_a_raw_expression__accepted() -> None:
    # Arrange / Act
    info = PartitionInfo(
        name="p",
        partition_type=PartitionType.RANGE,
        boundaries_expr="FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')",
        parent_table="parent",
    )

    # Assert
    assert info.bounds is None
    assert info.parent_table == "parent"


def test__partition_info__detached_range_without_boundaries__accepted() -> None:
    # Arrange / Act
    info = PartitionInfo(name="p", partition_type=PartitionType.RANGE, is_attached=False)

    # Assert
    assert info.bounds is None
    assert info.from_value is None


@pytest.mark.parametrize("partition_type", [PartitionType.HASH, PartitionType.LIST])
def test__partition_info__non_range_partition__needs_no_boundaries(partition_type: PartitionType) -> None:
    # Arrange / Act
    info = PartitionInfo(name="p", partition_type=partition_type)

    # Assert
    assert info.bounds is None


def test__partition_info__hash_bounds__exposed_through_hash_bounds() -> None:
    # Arrange / Act
    info = PartitionInfo(name="p", partition_type=PartitionType.HASH, bounds=HashBounds(modulus=4, remainder=1))

    # Assert
    assert info.hash_bounds == HashBounds(modulus=4, remainder=1)
    assert info.from_value is None


def test__partition_info__list_bounds__hash_bounds_is_none() -> None:
    # Arrange / Act
    info = PartitionInfo(name="p", partition_type=PartitionType.LIST, bounds=ListBounds(values=("eu",)))

    # Assert
    assert info.hash_bounds is None
    assert info.bounds == ListBounds(values=("eu",))


def test__partition_info__catalog_identity__defaults_to_a_plain_table_without_oid() -> None:
    # Arrange / Act
    plain = PartitionInfo(name="p", partition_type=PartitionType.HASH)
    foreign = PartitionInfo(name="p", partition_type=PartitionType.HASH, oid=4242, relkind=RelationKind.FOREIGN)

    # Assert
    assert plain.oid is None
    assert plain.relkind is RelationKind.TABLE
    assert foreign.oid == 4242
    assert foreign.relkind is RelationKind.FOREIGN


def test__partition_info__subpartition_type__marks_a_branch() -> None:
    # Arrange / Act
    branch = PartitionInfo(
        name="p", partition_type=PartitionType.RANGE, from_value="a", to_value="b", subpartition_type=PartitionType.HASH
    )
    leaf = PartitionInfo(name="p", partition_type=PartitionType.RANGE, from_value="a", to_value="b")

    # Assert
    assert branch.is_subpartitioned is True
    assert leaf.is_subpartitioned is False


@pytest.mark.parametrize(
    ("name", "schema_name", "relname"),
    [
        ("public.events__2024_01", "public", "events__2024_01"),
        ("events__2024_01", None, "events__2024_01"),
        ("a.b.c", None, "a.b.c"),
    ],
)
def test__partition_info__name__splits_into_schema_and_relname(
    name: str, schema_name: str | None, relname: str
) -> None:
    # Arrange
    info = PartitionInfo(name=name, partition_type=PartitionType.RANGE, from_value="a", to_value="b")

    # Act / Assert
    assert info.schema_name == schema_name
    assert info.relname == relname


def test__partition_info__validation__leaves_the_callers_dict_untouched() -> None:
    # Arrange -- a dict the caller intends to reuse for a second partition
    payload = {
        "name": "events__2024_01",
        "partition_type": PartitionType.RANGE,
        "from_value": "2024-01-01",
        "to_value": "2024-02-01",
    }
    original = dict(payload)

    # Act
    PartitionInfo.model_validate(payload)

    # Assert -- validating must not write a derived field back into the input
    assert payload == original


def test__partition_info__model_copy_update__produces_a_new_instance() -> None:
    # Arrange
    info = PartitionInfo(
        name="events__2024_01",
        partition_type=PartitionType.RANGE,
        from_value="2024-01-01",
        to_value="2024-02-01",
        is_attached=False,
    )

    # Act
    attached = info.model_copy(update={"is_attached": True})

    # Assert
    assert attached.is_attached is True
    assert info.is_attached is False


@pytest.mark.parametrize(
    "info",
    [
        PartitionInfo(
            name="events__2024_01",
            partition_type=PartitionType.RANGE,
            bounds=RangeBounds(from_value="2024-01-01", to_value="2024-02-01"),
        ),
        PartitionInfo(
            name="public.events__h1",
            oid=17,
            partition_type=PartitionType.HASH,
            bounds=HashBounds(modulus=4, remainder=1),
            relkind=RelationKind.PARTITIONED,
            subpartition_type=PartitionType.LIST,
            parent_table="public.events",
        ),
        PartitionInfo(name="public.events_default", partition_type=PartitionType.LIST, is_default=True),
    ],
    ids=["range", "hash-branch", "default"],
)
def test__partition_info__dump_round_trip__reloads_an_equal_partition(info: PartitionInfo) -> None:
    # Arrange / Act
    from_python = PartitionInfo.model_validate(info.model_dump())
    from_json = PartitionInfo.model_validate_json(info.model_dump_json())

    # Assert
    assert from_python == info
    assert from_json == info
    assert from_json.from_value == info.from_value
    assert from_json.to_value == info.to_value


# -- MaintenanceResult ---------------------------------------------------------------


def test__maintenance_result__no_error__success_is_true() -> None:
    # Arrange / Act
    result = MaintenanceResult()

    # Assert
    assert result.success is True
    assert result.error is None
    assert result.issues == ()
    assert result.plan is None
    assert result.maintenance_plan is None


def test__maintenance_result__with_error__success_is_false() -> None:
    # Arrange / Act
    result = MaintenanceResult(error="oops")

    # Assert
    assert result.success is False


def test__maintenance_result__counters__stored_as_given() -> None:
    # Arrange / Act
    result = MaintenanceResult(
        created_count=3, repaired_count=2, attached_count=1, detached_count=2, dropped_count=1, duration_ms=100
    )

    # Assert
    assert (
        result.created_count,
        result.repaired_count,
        result.attached_count,
        result.detached_count,
        result.dropped_count,
        result.duration_ms,
    ) == (3, 2, 1, 2, 1, 100)


@pytest.mark.parametrize(
    "field", ["created_count", "repaired_count", "attached_count", "detached_count", "dropped_count", "duration_ms"]
)
def test__maintenance_result__negative_counter__rejected(field: str) -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError):
        MaintenanceResult(**{field: -1})


def test__maintenance_result__issues_without_error__success_stays_true() -> None:
    # Arrange
    issue = MaintenanceIssue(step=MaintenanceIssueStep.DETACH, error="SQLAlchemyError: detach failed")

    # Act
    result = MaintenanceResult(created_count=1, issues=(issue,))

    # Assert -- non-fatal issues never flip success; only a fatal ``error`` does
    assert result.success is True
    assert result.issues == (issue,)


def test__maintenance_result__plan__kept_on_the_result_but_left_out_of_dump_and_repr() -> None:
    # Arrange
    plan = object()

    # Act
    result = MaintenanceResult(created_count=1, plan=plan)

    # Assert
    assert result.plan is plan
    assert result.maintenance_plan is plan
    assert "plan" not in result.model_dump()
    assert "plan" not in repr(result)
    assert "plan" not in result.model_dump_json()


# -- MaintenanceIssue -------------------------------------------------------------------


def test__maintenance_issue__construction__stores_step_error_and_partition() -> None:
    # Arrange / Act
    issue = MaintenanceIssue(step=MaintenanceIssueStep.CREATE, error="SQLAlchemyError: create failed")
    specific = MaintenanceIssue(step="drop", error="PlanStaleError: recreated", partition_name="public.events__2024_01")

    # Assert
    assert issue.step is MaintenanceIssueStep.CREATE
    assert issue.error == "SQLAlchemyError: create failed"
    assert issue.partition_name is None
    assert specific.step is MaintenanceIssueStep.DROP
    assert specific.partition_name == "public.events__2024_01"


def test__maintenance_issue__blank_error__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError):
        MaintenanceIssue(step=MaintenanceIssueStep.DROP, error="   ")


def test__maintenance_issue_step__covers_planning_and_attaching() -> None:
    # Arrange / Act / Assert
    assert MaintenanceIssueStep.ATTACH.value == "attach"
    assert {step.value for step in MaintenanceIssueStep} == {
        "create",
        "reconcile",
        "attach",
        "detach",
        "drop",
        "move",
    }


# -- package-root exports -------------------------------------------------------------


def test__package_root__migration_ergonomics_exports__importable_and_functional() -> None:
    # Arrange / Act / Assert
    assert pg_partsmith.MaintenanceIssue is MaintenanceIssue
    assert pg_partsmith.TablePartitionConfig is TablePartitionConfig
    assert pg_partsmith.PartitionStrategy is PartitionStrategy
    assert pg_partsmith.qualify("public", "events") == "public.events"
    assert pg_partsmith.qualify(None, "events") == "events"
    assert pg_partsmith.split_qualified_name("public.events") == ("public", "events")
    assert pg_partsmith.split_qualified_name("events") == (None, "events")


def test__config__misspelled_field__refused_not_ignored() -> None:
    with pytest.raises(ValidationError, match="retention_cout"):
        TablePartitionConfig(
            table_name="events",
            partition_column="created_at",
            granularity=PartitionGranularity.MONTH,
            retention_cout=12,  # type: ignore[call-arg]
        )
