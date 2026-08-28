from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.exc import DBAPIError

from pg_partsmith.aio.metadata import PostgresMetadataProvider
from pg_partsmith.entities import PartitionType
from pg_partsmith.exceptions import InvalidPartitionConfigError

# ── helpers ─────────────────────────────────────────────────────────────────────


def _make_engine(*scalar_or_rows: object) -> MagicMock:
    """Build an engine mock where each conn.execute() call returns the next value.

    Pass scalar values for queries that use result.scalar(), or a list of row
    objects for queries that use result.fetchall() / fetchone().
    A tuple value sets both fetchone and scalar from the first element.
    """
    engine = MagicMock()
    conn = MagicMock()
    conn.execute = AsyncMock(return_value=MagicMock())
    conn.execution_options = AsyncMock(return_value=conn)
    results: list[MagicMock] = []

    for value in scalar_or_rows:
        r = MagicMock()
        if isinstance(value, list):
            r.fetchall.return_value = value
            r.fetchone.return_value = value[0] if value else None
        elif isinstance(value, tuple):
            r.fetchone.return_value = value
            r.scalar.return_value = value[0]
        elif value is None:
            r.fetchone.return_value = None
            r.scalar.return_value = None
        else:
            r.scalar.return_value = value
            r.fetchone.return_value = (value,)
        results.append(r)

    conn.execute.side_effect = results

    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=conn)
    cm.__aexit__ = AsyncMock(return_value=False)
    engine.connect.return_value = cm
    engine.begin.return_value = cm

    return engine


# ── get_partition_type ──────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "pg_letter,expected_type",
    [
        ("r", PartitionType.RANGE),
        ("l", PartitionType.LIST),
        ("h", PartitionType.HASH),
        (None, None),
    ],
)
async def test__metadata_provider__get_partition_type__maps_pg_letter_to_enum(
    pg_letter: str | None, expected_type: PartitionType | None
) -> None:
    # Arrange
    engine = _make_engine(pg_letter)
    provider = PostgresMetadataProvider(engine)

    # Act / Assert
    assert await provider.get_partition_type("events") == expected_type


# ── get_partition_column ────────────────────────────────────────────────────────


async def test__metadata_provider__get_partition_column__single_column__returns_column_name() -> None:
    # Arrange
    row = MagicMock()
    row.__getitem__ = MagicMock(return_value="created_at")
    engine = _make_engine([row])
    provider = PostgresMetadataProvider(engine)

    # Act / Assert
    assert await provider.get_partition_column("events") == "created_at"


async def test__metadata_provider__get_partition_column__no_columns__returns_none() -> None:
    # Arrange
    engine = _make_engine([])
    provider = PostgresMetadataProvider(engine)

    # Act / Assert
    assert await provider.get_partition_column("events") is None


async def test__metadata_provider__get_partition_column__composite_key__raises_value_error() -> None:
    # Arrange
    row1, row2 = MagicMock(), MagicMock()
    row1.__getitem__ = MagicMock(return_value="col_a")
    row2.__getitem__ = MagicMock(return_value="col_b")
    engine = _make_engine([row1, row2])
    provider = PostgresMetadataProvider(engine)

    # Act / Assert
    with pytest.raises(ValueError, match="composite"):
        await provider.get_partition_column("events")


# ── list_partitions ─────────────────────────────────────────────────────────────


async def test__metadata_provider__list_partitions__not_partitioned_table__returns_empty_list() -> None:
    # Arrange — get_partition_type returns None → early exit
    engine = _make_engine(None)
    provider = PostgresMetadataProvider(engine)

    # Act / Assert
    assert await provider.list_partitions("events") == []


async def test__metadata_provider__list_partitions__attached_partition__returns_partition_info() -> None:
    # Arrange
    row = MagicMock()
    row.partition_schema = "public"
    row.partition_name = "events__2024_01"
    row.boundaries = "FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')"
    row.is_attached = True
    engine = _make_engine(("r", "public.events"), [row], [])
    provider = PostgresMetadataProvider(engine)

    # Act
    partitions = await provider.list_partitions("events")

    # Assert
    assert len(partitions) == 1
    assert partitions[0].name == "public.events__2024_01"
    assert partitions[0].from_value == "2024-01-01"
    assert partitions[0].to_value == "2024-02-01"
    assert partitions[0].is_attached is True
    assert partitions[0].parent_table == "events"


async def test__metadata_provider__list_partitions__unparseable_boundaries__stores_raw_expr() -> None:
    # Arrange
    row = MagicMock()
    row.partition_schema = "public"
    row.partition_name = "events__weird"
    row.boundaries = "FOR VALUES FROM (weird) ???"
    row.is_attached = True
    engine = _make_engine(("r", "public.events"), [row], [])
    provider = PostgresMetadataProvider(engine)

    # Act
    partitions = await provider.list_partitions("events")

    # Assert
    assert len(partitions) == 1
    assert partitions[0].name == "public.events__weird"
    assert partitions[0].is_attached is True
    assert partitions[0].is_default is False
    assert partitions[0].from_value is None
    assert partitions[0].to_value is None
    assert partitions[0].boundaries_expr == "FOR VALUES FROM (weird) ???"


async def test__metadata_provider__list_partitions__custom_marker_prefix__uses_prefix_in_orphan_query() -> None:
    # Arrange
    engine = _make_engine(("r", "public.events"), [], [])
    provider = PostgresMetadataProvider(engine, marker_prefix="myapp:")

    # Act
    await provider.list_partitions("events")

    # Assert
    conn = engine.connect.return_value.__aenter__.return_value
    params = conn.execute.call_args_list[2].args[1]
    assert params["marker"] == "myapp:public.events"


async def test__metadata_provider__list_partitions__default_partition__sets_is_default_true() -> None:
    # Arrange
    row = MagicMock()
    row.partition_schema = "public"
    row.partition_name = "events_default"
    row.boundaries = "DEFAULT"
    row.is_attached = True
    engine = _make_engine(("r", "public.events"), [row], [])
    provider = PostgresMetadataProvider(engine)

    # Act
    partitions = await provider.list_partitions("events")

    # Assert
    assert len(partitions) == 1
    assert partitions[0].is_default is True
    assert partitions[0].from_value is None
    assert partitions[0].to_value is None


async def test__metadata_provider__list_partitions__dotted_relname__skipped_with_warning() -> None:
    # Arrange — a relname containing '.' cannot be addressed as schema.relname DDL; must be skipped
    good_row = MagicMock()
    good_row.partition_schema = "public"
    good_row.partition_name = "events__2024_01"
    good_row.boundaries = "FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')"
    good_row.is_attached = True
    dotted_row = MagicMock()
    dotted_row.partition_schema = "public"
    dotted_row.partition_name = "events.2024_02"
    dotted_row.boundaries = "FOR VALUES FROM ('2024-02-01') TO ('2024-03-01')"
    dotted_row.is_attached = True
    engine = _make_engine(("r", "public.events"), [good_row, dotted_row], [])
    provider = PostgresMetadataProvider(engine)
    mock_logger = MagicMock()

    # Act
    with patch("pg_partsmith.partition_bounds.logger", mock_logger):
        partitions = await provider.list_partitions("events")

    # Assert
    assert [p.name for p in partitions] == ["public.events__2024_01"]
    mock_logger.warning.assert_called_once()


async def test__metadata_provider__list_partitions__dotted_schema_orphan__skipped_with_warning() -> None:
    # Arrange — an orphan whose schema contains '.' is equally unaddressable
    orphan = MagicMock()
    orphan.partition_schema = "bad.schema"
    orphan.partition_name = "events__2023_12"
    engine = _make_engine(("r", "public.events"), [], [orphan])
    provider = PostgresMetadataProvider(engine)
    mock_logger = MagicMock()

    # Act
    with patch("pg_partsmith.partition_bounds.logger", mock_logger):
        partitions = await provider.list_partitions("events")

    # Assert
    assert partitions == []
    mock_logger.warning.assert_called_once()


async def test__metadata_provider__list_partitions__orphan_row__returns_detached_partition() -> None:
    # Arrange
    orphan = MagicMock()
    orphan.partition_schema = "public"
    orphan.partition_name = "events__2023_12"
    engine = _make_engine(("r", "public.events"), [], [orphan])
    provider = PostgresMetadataProvider(engine)

    # Act
    partitions = await provider.list_partitions("events")

    # Assert
    assert len(partitions) == 1
    assert partitions[0].name == "public.events__2023_12"
    assert partitions[0].is_attached is False
    assert partitions[0].from_value is None


async def test__metadata_provider__list_partitions__bare_parent__returns_schema_qualified_names() -> None:
    # Arrange — even for a bare parent name, children are qualified with their own catalog schema:
    # a partition may live in a different schema, and a bare child name could resolve elsewhere
    row = MagicMock()
    row.partition_schema = "archive"
    row.partition_name = "events__2024_01"
    row.boundaries = "FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')"
    row.is_attached = True
    engine = _make_engine(("r", "public.events"), [row], [])
    provider = PostgresMetadataProvider(engine)

    # Act
    partitions = await provider.list_partitions("events")

    # Assert
    assert [p.name for p in partitions] == ["archive.events__2024_01"]


async def test__metadata_provider__list_partitions__orphan_query__accepts_partitioned_relkind() -> None:
    # Arrange — an orphan may itself be subpartitioned (relkind 'p'), not only a plain table ('r')
    engine = _make_engine(("r", "public.events"), [], [])
    provider = PostgresMetadataProvider(engine)

    # Act
    await provider.list_partitions("events")

    # Assert
    conn = engine.connect.return_value.__aenter__.return_value
    orphan_sql = str(conn.execute.call_args_list[2].args[0])
    assert "relkind IN ('r', 'p')" in orphan_sql


# ── partition_exists / is_partition_attached ────────────────────────────────────


@pytest.mark.parametrize("exists", [True, False])
async def test__metadata_provider__partition_exists__returns_correct_bool(exists: bool) -> None:
    # Arrange
    engine = _make_engine(exists)
    provider = PostgresMetadataProvider(engine)

    # Act / Assert
    assert await provider.partition_exists("events__2024_01") is exists


async def test__metadata_provider__partition_exists__uses_quoted_regclass_argument() -> None:
    # Arrange
    engine = _make_engine(True)
    provider = PostgresMetadataProvider(engine)

    # Act
    await provider.partition_exists("events__2024_W12")

    # Assert
    conn = engine.connect.return_value.__aenter__.return_value
    params = conn.execute.call_args.args[1]
    assert params["partition_name"] == '"events__2024_W12"'


async def test__metadata_provider__partition_exists__accepts_partitioned_relkind() -> None:
    # Arrange — a partition can itself be subpartitioned (relkind 'p'), not only a plain table ('r')
    engine = _make_engine(True)
    provider = PostgresMetadataProvider(engine)

    # Act
    await provider.partition_exists("events__2024_01")

    # Assert
    conn = engine.connect.return_value.__aenter__.return_value
    sql = str(conn.execute.call_args.args[0])
    assert "relkind IN ('r', 'p')" in sql


@pytest.mark.parametrize("attached", [True, False])
async def test__metadata_provider__is_partition_attached__returns_correct_bool(attached: bool) -> None:
    # Arrange
    engine = _make_engine(attached)
    provider = PostgresMetadataProvider(engine)

    # Act / Assert
    assert await provider.is_partition_attached("events", "events__2024_01") is attached


async def test__metadata_provider__is_partition_attached__uses_quoted_regclass_arguments() -> None:
    # Arrange
    engine = _make_engine(True)
    provider = PostgresMetadataProvider(engine)

    # Act
    await provider.is_partition_attached("events", "events__2024_W12")

    # Assert
    conn = engine.connect.return_value.__aenter__.return_value
    params = conn.execute.call_args.args[1]
    assert params["table_name"] == '"events"'
    assert params["partition_name"] == '"events__2024_W12"'


# ── is_partition_closed ─────────────────────────────────────────────────────────


@pytest.mark.parametrize("scalar,expected", [(True, True), (False, False)])
async def test__metadata_provider__is_partition_closed__maps_scalar_to_bool(scalar: bool, expected: bool) -> None:
    # Arrange -- the bound is read first, then compared against now() in SQL.
    engine = _make_engine("2024-02-01 00:00:00+00", scalar)
    provider = PostgresMetadataProvider(engine)

    # Act / Assert
    assert await provider.is_partition_closed("events__2024_01") is expected


async def test__metadata_provider__is_partition_closed__no_upper_bound__is_not_closed() -> None:
    # Arrange -- DEFAULT, detached, non-RANGE, or a name that resolves to nothing.
    engine = _make_engine(None)
    provider = PostgresMetadataProvider(engine)

    # Act / Assert -- and no comparison is attempted for want of anything to compare.
    assert await provider.is_partition_closed("events_default") is False


async def test__metadata_provider__is_partition_closed__passes_settle_seconds_and_quoted_regclass_name() -> None:
    # Arrange
    engine = _make_engine("2024-04-01 00:00:00+00", True)
    provider = PostgresMetadataProvider(engine)

    # Act
    await provider.is_partition_closed("events__2024_W12", settle_seconds=900)

    # Assert -- the bound is looked up by regclass, then compared against the
    # passed settle buffer with the clock staying server-side.
    conn = engine.connect.return_value.__aenter__.return_value
    lookup, compare = conn.execute.call_args_list
    assert "to_regclass(:partition_name)" in str(lookup.args[0])
    assert lookup.args[1]["partition_name"] == '"events__2024_W12"'
    assert "make_interval(secs => :settle_seconds)" in str(compare.args[0])
    assert compare.args[1] == {"upper_bound": "2024-04-01 00:00:00+00", "settle_seconds": 900}


async def test__metadata_provider__is_partition_closed__defaults_to_zero_settle_seconds() -> None:
    # Arrange
    engine = _make_engine("2024-02-01 00:00:00+00", True)
    provider = PostgresMetadataProvider(engine)

    # Act
    await provider.is_partition_closed("events__2024_01")

    # Assert
    conn = engine.connect.return_value.__aenter__.return_value
    assert conn.execute.call_args.args[1]["settle_seconds"] == 0


# ── get_partition_boundaries ────────────────────────────────────────────────────


async def test__metadata_provider__get_partition_boundaries__found__parses_from_and_to() -> None:
    # Arrange
    engine = _make_engine("FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')")
    provider = PostgresMetadataProvider(engine)

    # Act
    result = await provider.get_partition_boundaries("events__2024_01")

    # Assert
    assert result == ("2024-01-01", "2024-02-01")


async def test__metadata_provider__get_partition_boundaries__not_found__returns_none() -> None:
    # Arrange
    engine = _make_engine(None)
    provider = PostgresMetadataProvider(engine)

    # Act / Assert
    assert await provider.get_partition_boundaries("events__2024_01") is None


# ── _parse_boundaries ───────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "expr,expected",
    [
        ("FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')", ("2024-01-01", "2024-02-01")),
        ("FOR VALUES FROM (1) TO (5)", ("1", "5")),
        ("FOR VALUES FROM ('2024-01-01'::date) TO ('2024-02-01'::date)", ("2024-01-01", "2024-02-01")),
        (None, (None, None)),
        ("INVALID EXPR", (None, None)),
        ("DEFAULT", (None, None)),
        # LIST bound whose string value embeds ") TO (" must not be mis-parsed as a range
        ("FOR VALUES IN ('a) TO (b')", (None, None)),
    ],
)
def test__parse_boundaries__various_expressions__returns_correct_tuple(
    expr: str | None, expected: tuple[str | None, str | None]
) -> None:
    # Arrange
    provider = PostgresMetadataProvider(MagicMock())

    # Act / Assert
    assert provider._parse_boundaries(expr) == expected


# ── get_default_partition ───────────────────────────────────────────────────────


async def test__metadata_provider__get_default_partition__default_exists__returns_it() -> None:
    # Arrange
    row = MagicMock()
    row.partition_schema = "public"
    row.partition_name = "events_default"
    row.boundaries = "DEFAULT"
    row.is_attached = True
    engine = _make_engine(("r", "public.events"), [row], [])
    provider = PostgresMetadataProvider(engine)

    # Act
    result = await provider.get_default_partition("events")

    # Assert
    assert result is not None
    assert result.name == "public.events_default"
    assert result.is_default is True
    assert result.is_attached is True


async def test__metadata_provider__get_default_partition__no_default__returns_none() -> None:
    # Arrange
    row = MagicMock()
    row.partition_schema = "public"
    row.partition_name = "events__2024_01"
    row.boundaries = "FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')"
    row.is_attached = True
    engine = _make_engine(("r", "public.events"), [row], [])
    provider = PostgresMetadataProvider(engine)

    # Act / Assert
    assert await provider.get_default_partition("events") is None


async def test__metadata_provider__get_default_partition__orphaned_default__returns_none() -> None:
    # Arrange — detached DEFAULT (orphan row) must be ignored
    orphan_row = MagicMock()
    orphan_row.partition_schema = "public"
    orphan_row.partition_name = "events_default"
    engine = _make_engine(("r", "public.events"), [], [orphan_row])
    provider = PostgresMetadataProvider(engine)

    # Act / Assert
    assert await provider.get_default_partition("events") is None


async def test__metadata_provider__is_partition_closed__codec_cannot_read_the_bound__warns() -> None:
    # Arrange -- an encoded bound the configured codec does not recognise.
    engine = _make_engine("not-an-encoded-boundary")
    codec = MagicMock()
    codec.decode.return_value = None
    provider = PostgresMetadataProvider(engine, boundary_codec=codec)
    logger = MagicMock()

    # Act
    with patch("pg_partsmith.aio.metadata.logger", logger):
        result = await provider.is_partition_closed("events__2026_w35")

    # Assert -- an export pipeline gated on this would otherwise wait forever
    # with nothing in the log to say why.
    assert result is False
    logger.warning.assert_called_once()
    assert "boundary_codec" in logger.warning.call_args.args[0]


async def test__metadata_provider__is_partition_closed__bound_is_not_a_timestamp__warns_instead_of_raising() -> None:
    # Arrange -- a sortable identifier with a date-shaped prefix gets past any
    # regex worth writing and still fails the cast.
    engine = _make_engine("2026-08-28-a1b2c3")
    conn = engine.connect.return_value.__aenter__.return_value
    bound = MagicMock()
    bound.scalar.return_value = "2026-08-28-a1b2c3"
    conn.execute.side_effect = [bound, DBAPIError("SELECT now() >= ...", {}, Exception("invalid input syntax"))]
    provider = PostgresMetadataProvider(engine)
    logger = MagicMock()

    # Act
    with patch("pg_partsmith.aio.metadata.logger", logger):
        result = await provider.is_partition_closed("events__2026_w35")

    # Assert -- "not closed" is the documented answer for a bound this provider
    # cannot read; raising out of a predicate is not.
    assert result is False
    logger.warning.assert_called_once()


async def test__metadata_provider__is_partition_closed__no_bound_at_all__stays_quiet() -> None:
    # Arrange -- DEFAULT, detached or non-RANGE: nothing to explain.
    engine = _make_engine(None)
    provider = PostgresMetadataProvider(engine)
    logger = MagicMock()

    # Act
    with patch("pg_partsmith.aio.metadata.logger", logger):
        result = await provider.is_partition_closed("events_default")

    # Assert
    assert result is False
    logger.warning.assert_not_called()


# ── partition keys this library cannot address ──────────────────────────────────


async def test__metadata_provider__get_partition_columns__expression_key__is_refused() -> None:
    # Arrange -- an expression key is recorded as attnum 0, which matches no
    # column, so the row comes back with a NULL name.
    engine = _make_engine([("created_at",), (None,)])
    provider = PostgresMetadataProvider(engine)

    # Act / Assert -- dropping the position instead would report a one-column
    # key for a two-column table, and every bound built from it would be the
    # wrong arity.
    with pytest.raises(InvalidPartitionConfigError, match="key position 2"):
        await provider.get_partition_columns("public.events")


async def test__metadata_provider__get_partition_column__expression_key__is_refused() -> None:
    # Arrange
    engine = _make_engine([(None,)])
    provider = PostgresMetadataProvider(engine)

    # Act / Assert
    with pytest.raises(ValueError, match="expression"):
        await provider.get_partition_column("public.events")


async def test__metadata_provider__get_partition_columns__plain_columns__reports_them_in_key_order() -> None:
    # Arrange
    engine = _make_engine([("created_at",), ("tenant_id",)])
    provider = PostgresMetadataProvider(engine)

    # Act / Assert
    assert await provider.get_partition_columns("public.events") == ("created_at", "tenant_id")
