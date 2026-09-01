"""Unit tests for the sync ``PostgresMetadataProvider`` against a mocked engine."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import DBAPIError

from pg_partsmith.boundaries import TimeBoundaries, UUIDv7BoundaryCodec
from pg_partsmith.entities import PartitionGranularity, PartitionType
from pg_partsmith.exceptions import InvalidPartitionConfigError
from pg_partsmith.lifecycle import SqlPredicate
from pg_partsmith.sync.metadata import PostgresMetadataProvider
from pg_partsmith.topology import (
    ActualTree,
    DefaultBounds,
    DetachedPartition,
    FactKind,
    HashBounds,
    ListBounds,
    PartitionFacts,
    PartitionNode,
    RangeBounds,
    RelationKind,
)
from pg_partsmith.utils import DETACHED_AT_MARKER, orphan_table_comment

# ── helpers ─────────────────────────────────────────────────────────────────────


def _make_engine(*values: object) -> MagicMock:
    """Build an engine mock where each ``conn.execute()`` call answers with the next value.

    A list becomes ``result.fetchall()``; anything else becomes ``result.scalar()``.
    An exception instance is raised by that call.
    """
    engine = MagicMock()
    conn = MagicMock()
    conn.execute = MagicMock()
    conn.execution_options = MagicMock(return_value=conn)

    results: list[object] = []
    for value in values:
        if isinstance(value, BaseException):
            results.append(value)
            continue
        result = MagicMock()
        if isinstance(value, list):
            result.fetchall.return_value = value
        else:
            result.scalar.return_value = value
        results.append(result)
    conn.execute.side_effect = results

    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=conn)
    cm.__exit__ = MagicMock(return_value=False)
    engine.connect.return_value = cm
    engine.begin.return_value = cm
    return engine


def _conn(engine: MagicMock) -> MagicMock:
    return engine.connect.return_value.__enter__.return_value


def _statements(engine: MagicMock) -> list[str]:
    return [str(call.args[0]) for call in _conn(engine).execute.call_args_list]


def _tree_row(
    *,
    level: int,
    name: str,
    oid: int,
    schema: str = "public",
    parent: tuple[str, str] | None = None,
    boundaries: str | None = None,
    relkind: str = "r",
    is_attached: bool = True,
    detach_pending: bool = False,
    partstrat: str | None = None,
    columns: list[str | None] | None = None,
    key_arity: int | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        level=level,
        oid=oid,
        relkind=relkind,
        partition_schema=schema,
        partition_name=name,
        parent_schema=None if parent is None else parent[0],
        parent_name=None if parent is None else parent[1],
        boundaries=boundaries,
        is_attached=is_attached,
        detach_pending=detach_pending,
        partstrat=partstrat,
        partition_columns=columns,
        key_arity=key_arity,
    )


def _root_row(name: str = "events", oid: int = 100, partstrat: str | None = "r") -> SimpleNamespace:
    return _tree_row(level=0, name=name, oid=oid, relkind="p", partstrat=partstrat, columns=["created_at"], key_arity=1)


def _orphan_row(
    name: str, description: str | None, *, oid: int = 500, schema: str = "public", relkind: str = "r"
) -> SimpleNamespace:
    return SimpleNamespace(
        oid=oid, relkind=relkind, partition_schema=schema, partition_name=name, description=description
    )


def _facts_row(oid: int, size_bytes: int | None, row_estimate: int | None) -> SimpleNamespace:
    return SimpleNamespace(oid=oid, size_bytes=size_bytes, row_estimate=row_estimate)


def _sample_tree() -> ActualTree:
    root = PartitionNode(
        name="public.events",
        oid=100,
        partition_type=PartitionType.RANGE,
        partition_columns=("created_at",),
        children=(
            PartitionNode(
                name="public.events__2024_01",
                oid=101,
                parent_name="public.events",
                level=1,
                bounds=RangeBounds(from_value="2024-01-01", to_value="2024-02-01"),
            ),
            PartitionNode(
                name="public.events__2024_02",
                oid=102,
                parent_name="public.events",
                level=1,
                bounds=RangeBounds(from_value="2024-02-01", to_value="2024-03-01"),
            ),
        ),
    )
    orphans = (DetachedPartition(name="public.events__2023_12", oid=500, parent_name="public.events"),)
    return ActualTree(root=root, orphans=orphans)


# ── get_partition_type ──────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "pg_letter,expected_type",
    [
        ("r", PartitionType.RANGE),
        ("l", PartitionType.LIST),
        ("h", PartitionType.HASH),
        (b"h", PartitionType.HASH),
        (None, None),
    ],
)
def test__get_partition_type__maps_pg_letter_to_enum(
    pg_letter: str | bytes | None, expected_type: PartitionType | None
) -> None:
    # Arrange
    engine = _make_engine(pg_letter)
    provider = PostgresMetadataProvider(engine)

    # Act / Assert
    assert provider.get_partition_type("events") == expected_type
    assert _conn(engine).execute.call_args.args[1] == {"table_name": '"events"'}


# ── get_partition_columns / get_partition_column ────────────────────────────────


def test__get_partition_columns__plain_columns__reports_them_in_key_order() -> None:
    # Arrange
    engine = _make_engine([("created_at",), ("tenant_id",)])
    provider = PostgresMetadataProvider(engine)

    # Act / Assert
    assert provider.get_partition_columns("public.events") == ("created_at", "tenant_id")
    assert _conn(engine).execute.call_args.args[1] == {"table_name": '"public"."events"'}


def test__get_partition_columns__not_partitioned__returns_empty() -> None:
    # Arrange
    engine = _make_engine([])
    provider = PostgresMetadataProvider(engine)

    # Act / Assert
    assert provider.get_partition_columns("public.events") == ()


def test__get_partition_columns__expression_key__is_refused_with_its_position() -> None:
    # Arrange -- an expression key is recorded as attnum 0, which matches no column: the row comes back NULL
    engine = _make_engine([("created_at",), (None,)])
    provider = PostgresMetadataProvider(engine)

    # Act / Assert
    with pytest.raises(InvalidPartitionConfigError, match="expression at key position 2"):
        provider.get_partition_columns("public.events")


def test__get_partition_column__single_column__returns_column_name() -> None:
    # Arrange
    engine = _make_engine([("created_at",)])
    provider = PostgresMetadataProvider(engine)

    # Act / Assert
    assert provider.get_partition_column("events") == "created_at"


def test__get_partition_column__no_columns__returns_none() -> None:
    # Arrange
    engine = _make_engine([])
    provider = PostgresMetadataProvider(engine)

    # Act / Assert
    assert provider.get_partition_column("events") is None


def test__get_partition_column__composite_key__raises_value_error() -> None:
    # Arrange
    engine = _make_engine([("col_a",), ("col_b",)])
    provider = PostgresMetadataProvider(engine)

    # Act / Assert
    with pytest.raises(ValueError, match="composite partition key"):
        provider.get_partition_column("events")


def test__get_partition_column__expression_key__is_refused() -> None:
    # Arrange
    engine = _make_engine([(None,)])
    provider = PostgresMetadataProvider(engine)

    # Act / Assert
    with pytest.raises(InvalidPartitionConfigError, match="expression"):
        provider.get_partition_column("public.events")


# ── get_partition_tree ──────────────────────────────────────────────────────────


def test__get_partition_tree__rows__become_nodes_with_identity_and_bounds() -> None:
    # Arrange
    rows = [
        _root_row(),
        _tree_row(
            level=1,
            name="events__2024_01",
            oid=101,
            parent=("public", "events"),
            boundaries="FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')",
            detach_pending=True,
        ),
        _tree_row(
            level=1,
            name="events__2024_02",
            oid=102,
            relkind="p",
            parent=("public", "events"),
            boundaries="FOR VALUES FROM ('2024-02-01') TO ('2024-03-01')",
            partstrat="h",
            columns=["tenant_id"],
            key_arity=1,
        ),
        _tree_row(
            level=2,
            name="events__2024_02__h0",
            oid=103,
            relkind="f",
            parent=("public", "events__2024_02"),
            boundaries="FOR VALUES WITH (modulus 2, remainder 0)",
        ),
    ]
    engine = _make_engine(rows)
    provider = PostgresMetadataProvider(engine)

    # Act
    root = provider.get_partition_tree("public.events")

    # Assert
    assert root is not None
    assert (root.name, root.oid, root.relkind, root.partition_type) == (
        "public.events",
        100,
        RelationKind.PARTITIONED,
        PartitionType.RANGE,
    )
    assert root.partition_columns == ("created_at",)
    assert root.parent_name is None
    assert root.bounds is None
    january, february = root.children
    assert january.name == "public.events__2024_01"
    assert january.oid == 101
    assert january.parent_name == "public.events"
    assert january.bounds == RangeBounds(from_value="2024-01-01", to_value="2024-02-01")
    assert january.detach_pending is True
    assert january.is_leaf
    assert february.partition_type is PartitionType.HASH
    assert february.partition_columns == ("tenant_id",)
    (bucket,) = february.children
    assert bucket.relkind is RelationKind.FOREIGN
    assert bucket.bounds == HashBounds(modulus=2, remainder=0)
    assert _conn(engine).execute.call_args.args[1] == {"table_name": '"public"."events"'}


def test__get_partition_tree__expression_key__marks_the_node() -> None:
    # Arrange -- one of two key positions is an expression, so only one column name comes back
    root = _tree_row(
        level=0, name="events", oid=100, relkind="p", partstrat="r", columns=[None, "created_at"], key_arity=2
    )
    engine = _make_engine([root])
    provider = PostgresMetadataProvider(engine)

    # Act
    node = provider.get_partition_tree("public.events")

    # Assert
    assert node is not None
    assert node.has_expression_key is True
    assert node.partition_columns == ("created_at",)


def test__get_partition_tree__no_columns_and_no_key_arity__is_not_an_expression_key() -> None:
    # Arrange -- a plain leaf has neither
    engine = _make_engine([_tree_row(level=0, name="events__2024_01", oid=101, boundaries="DEFAULT")])
    provider = PostgresMetadataProvider(engine)

    # Act
    node = provider.get_partition_tree("events__2024_01")

    # Assert
    assert node is not None
    assert node.has_expression_key is False
    assert node.bounds == DefaultBounds()


def test__get_partition_tree__unaddressable_child__is_skipped_and_its_parent_marked() -> None:
    # Arrange -- a dotted relname cannot be reached by qualified-name DDL
    rows = [
        _root_row(),
        _tree_row(
            level=1,
            name="events.2024_02",
            oid=102,
            parent=("public", "events"),
            boundaries="FOR VALUES FROM ('2024-02-01') TO ('2024-03-01')",
        ),
        _tree_row(
            level=1,
            name="events__2024_01",
            oid=101,
            parent=("public", "events"),
            boundaries="FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')",
        ),
    ]
    engine = _make_engine(rows)
    provider = PostgresMetadataProvider(engine)
    mock_logger = MagicMock()

    # Act
    with patch("pg_partsmith.partition_bounds.logger", mock_logger):
        root = provider.get_partition_tree("public.events")

    # Assert
    assert root is not None
    assert [child.name for child in root.children] == ["public.events__2024_01"]
    assert root.has_unaddressable_children is True
    mock_logger.warning.assert_called_once()


def test__get_partition_tree__root_itself_unaddressable__returns_none() -> None:
    # Arrange -- a level-0 row with a dotted relname has no parent to mark
    engine = _make_engine([_tree_row(level=0, name="ev.ents", oid=1, relkind="p", partstrat="r")])
    provider = PostgresMetadataProvider(engine)

    # Act
    with patch("pg_partsmith.partition_bounds.logger", MagicMock()):
        root = provider.get_partition_tree("public.events")

    # Assert
    assert root is None


def test__get_partition_tree__no_rows__returns_none() -> None:
    # Arrange
    engine = _make_engine([])
    provider = PostgresMetadataProvider(engine)

    # Act / Assert
    assert provider.get_partition_tree("public.nothing") is None


# ── get_actual_tree ─────────────────────────────────────────────────────────────


def test__get_actual_tree__not_partitioned__returns_none_without_looking_for_orphans() -> None:
    # Arrange -- a plain table is its own level-0 row, but partitions nothing
    engine = _make_engine([_tree_row(level=0, name="plain", oid=1)])
    provider = PostgresMetadataProvider(engine)

    # Act / Assert
    assert provider.get_actual_tree("public.plain") is None
    assert _conn(engine).execute.call_count == 1


def test__get_actual_tree__unknown_relation__returns_none() -> None:
    # Arrange
    engine = _make_engine([])
    provider = PostgresMetadataProvider(engine)

    # Act / Assert
    assert provider.get_actual_tree("public.missing") is None


def test__get_actual_tree__orphan_query__carries_a_marker_for_every_partitioned_node() -> None:
    # Arrange
    rows = [
        _root_row(),
        _tree_row(
            level=1,
            name="events__2024_02",
            oid=102,
            relkind="p",
            parent=("public", "events"),
            boundaries="FOR VALUES FROM ('2024-02-01') TO ('2024-03-01')",
            partstrat="h",
            columns=["tenant_id"],
            key_arity=1,
        ),
        _tree_row(
            level=1,
            name="events__2024_01",
            oid=101,
            parent=("public", "events"),
            boundaries="FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')",
        ),
    ]
    engine = _make_engine(rows, [])
    provider = PostgresMetadataProvider(engine, marker_prefix="myapp:parent=")

    # Act
    tree = provider.get_actual_tree("public.events")

    # Assert
    assert tree is not None
    assert tree.orphans == ()
    orphan_call = _conn(engine).execute.call_args_list[1]
    assert "split_part(d.description" in str(orphan_call.args[0])
    assert orphan_call.args[1] == {"markers": ["myapp:parent=public.events", "myapp:parent=public.events__2024_02"]}


def test__get_actual_tree__orphans__are_parsed_with_their_detach_instant() -> None:
    # Arrange
    marker = orphan_table_comment("public.events")
    stamped = f"{marker}\n{DETACHED_AT_MARKER}2024-01-15T10:00:00+00:00\nkeep me"
    orphans = [
        _orphan_row("events__2023_12", stamped, oid=500),
        _orphan_row("events__2023_11", marker, oid=501, relkind="p"),
        _orphan_row("events__2023_10", "unrelated comment", oid=502),
        _orphan_row("events__2023_09", None, oid=503),
        _orphan_row("events__2023_08", marker, oid=504, schema="bad.schema"),
    ]
    engine = _make_engine([_root_row()], orphans)
    provider = PostgresMetadataProvider(engine)

    # Act
    with patch("pg_partsmith.partition_bounds.logger", MagicMock()):
        tree = provider.get_actual_tree("public.events")

    # Assert -- the unmarked rows and the unaddressable one are left out
    assert tree is not None
    assert tree.orphans == (
        DetachedPartition(
            name="public.events__2023_12",
            oid=500,
            relkind=RelationKind.TABLE,
            parent_name="public.events",
            detached_at=datetime(2024, 1, 15, 10, 0, tzinfo=UTC),
        ),
        DetachedPartition(
            name="public.events__2023_11", oid=501, relkind=RelationKind.PARTITIONED, parent_name="public.events"
        ),
    )


def test__get_actual_tree__custom_marker_prefix__reads_orphans_marked_with_it() -> None:
    # Arrange
    engine = _make_engine([_root_row()], [_orphan_row("events__2023_12", "myapp:public.events", oid=500)])
    provider = PostgresMetadataProvider(engine, marker_prefix="myapp:")

    # Act
    tree = provider.get_actual_tree("public.events")

    # Assert
    assert tree is not None
    assert [o.parent_name for o in tree.orphans] == ["public.events"]


# ── measure ─────────────────────────────────────────────────────────────────────


def test__measure__no_targets__returns_the_tree_without_a_query() -> None:
    # Arrange
    engine = _make_engine()
    provider = PostgresMetadataProvider(engine)
    tree = _sample_tree()

    # Act
    result = provider.measure(tree, targets=(), facts=frozenset({FactKind.SIZE}))

    # Assert
    assert result is tree
    engine.connect.assert_not_called()


def test__measure__nothing_requested__returns_the_tree_without_a_query() -> None:
    # Arrange
    engine = _make_engine()
    provider = PostgresMetadataProvider(engine)
    tree = _sample_tree()

    # Act
    result = provider.measure(tree, targets=("public.events__2024_01",))

    # Assert
    assert result is tree
    engine.connect.assert_not_called()


def test__measure__facts_requested__one_query_for_every_target_by_oid() -> None:
    # Arrange
    engine = _make_engine([_facts_row(101, 2048, 17), _facts_row(500, 4096, 3)])
    provider = PostgresMetadataProvider(engine)
    tree = _sample_tree()

    # Act
    measured = provider.measure(
        tree,
        targets=("public.events__2024_01", "public.events__2023_12", "public.events__missing"),
        facts=frozenset({FactKind.SIZE, FactKind.ROWS}),
    )

    # Assert -- the target absent from the tree is ignored; the untargeted member stays unmeasured
    assert _conn(engine).execute.call_count == 1
    assert _conn(engine).execute.call_args.args[1] == {"oids": [101, 500]}
    january, february = measured.root.children
    assert january.facts == PartitionFacts(size_bytes=2048, row_estimate=17)
    assert february.facts is None
    assert measured.orphans[0].facts == PartitionFacts(size_bytes=4096, row_estimate=3)
    assert measured.root.oid == 100


def test__measure__only_size_requested__rows_stay_unknown() -> None:
    # Arrange
    engine = _make_engine([_facts_row(101, 2048, 17)])
    provider = PostgresMetadataProvider(engine)

    # Act
    measured = provider.measure(_sample_tree(), targets=("public.events__2024_01",), facts=frozenset({FactKind.SIZE}))

    # Assert
    assert measured.root.children[0].facts == PartitionFacts(size_bytes=2048, row_estimate=None)


def test__measure__target_without_a_facts_row__reads_as_empty() -> None:
    # Arrange
    engine = _make_engine([])
    provider = PostgresMetadataProvider(engine)

    # Act
    measured = provider.measure(_sample_tree(), targets=("public.events__2024_01",), facts=frozenset({FactKind.ROWS}))

    # Assert
    assert measured.root.children[0].facts == PartitionFacts(size_bytes=None, row_estimate=0)


def test__measure__sql_predicates__are_asked_once_per_target_without_a_facts_query() -> None:
    # Arrange
    predicate = SqlPredicate(sql="SELECT count(*) = 0 FROM {partition}")
    engine = _make_engine(True, False)
    provider = PostgresMetadataProvider(engine)

    # Act
    measured = provider.measure(
        _sample_tree(),
        targets=("public.events__2024_01", "public.events__2024_02"),
        sql_predicates=(predicate,),
    )

    # Assert
    statements = _statements(engine)
    assert statements == [
        'SELECT count(*) = 0 FROM "public"."events__2024_01"',
        'SELECT count(*) = 0 FROM "public"."events__2024_02"',
    ]
    january, february = measured.root.children
    assert january.facts == PartitionFacts(predicates={predicate.id: True})
    assert february.facts == PartitionFacts(predicates={predicate.id: False})


def test__measure__targets_without_oids__are_ignored() -> None:
    # Arrange
    engine = _make_engine()
    provider = PostgresMetadataProvider(engine)
    tree = ActualTree(
        root=PartitionNode(
            name="events",
            partition_type=PartitionType.RANGE,
            children=(PartitionNode(name="events__2024_01", bounds=RangeBounds(from_value="a", to_value="b")),),
        )
    )

    # Act
    result = provider.measure(tree, targets=("events__2024_01",), facts=frozenset({FactKind.SIZE}))

    # Assert
    assert result.root.children[0].facts is None
    engine.connect.assert_not_called()


# ── evaluate_sql_predicate ──────────────────────────────────────────────────────


def test__evaluate_sql_predicate__substitutes_the_quoted_name_and_escapes_colons() -> None:
    # Arrange
    predicate = SqlPredicate(sql="SELECT max(created_at) < now() - interval '1 day' FROM {partition} -- t::text")
    engine = _make_engine(1)
    provider = PostgresMetadataProvider(engine)

    # Act
    answer = provider.evaluate_sql_predicate(predicate, "public.events__2024_01")

    # Assert -- the raw text carries escaped colons so SQLAlchemy sees no bind parameters
    assert answer is True
    clause = _conn(engine).execute.call_args.args[0]
    assert (
        clause.text
        == 'SELECT max(created_at) < now() - interval \'1 day\' FROM "public"."events__2024_01" -- t\\:\\:text'
    )
    assert (
        str(clause) == 'SELECT max(created_at) < now() - interval \'1 day\' FROM "public"."events__2024_01" -- t::text'
    )


def test__evaluate_sql_predicate__null_answer__reads_as_false() -> None:
    # Arrange
    engine = _make_engine(None)
    provider = PostgresMetadataProvider(engine)

    # Act / Assert
    assert provider.evaluate_sql_predicate(SqlPredicate(sql="SELECT NULL FROM {partition}"), "events") is False


def test__evaluate_sql_predicate__statement_error__propagates() -> None:
    # Arrange -- a rule that cannot be evaluated must not silently read as False
    engine = _make_engine(DBAPIError("SELECT", {}, Exception("syntax error")))
    provider = PostgresMetadataProvider(engine)

    # Act / Assert
    with pytest.raises(DBAPIError):
        provider.evaluate_sql_predicate(SqlPredicate(sql="SELECT ??? FROM {partition}"), "events")


# ── list_partitions ─────────────────────────────────────────────────────────────


def test__list_partitions__not_partitioned__returns_empty_list() -> None:
    # Arrange
    engine = _make_engine([])
    provider = PostgresMetadataProvider(engine)

    # Act / Assert
    assert provider.list_partitions("events") == []


def test__list_partitions__attached_children__are_rendered_as_partition_infos() -> None:
    # Arrange
    rows = [
        _root_row(),
        _tree_row(
            level=1,
            name="events__2024_01",
            oid=101,
            parent=("public", "events"),
            boundaries="FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')",
        ),
        _tree_row(
            level=1,
            name="events_default",
            oid=102,
            relkind="p",
            parent=("public", "events"),
            boundaries="DEFAULT",
            partstrat="h",
            columns=["tenant_id"],
            key_arity=1,
        ),
        _tree_row(
            level=2,
            name="events_default__h0",
            oid=103,
            parent=("public", "events_default"),
            boundaries="FOR VALUES WITH (modulus 2, remainder 0)",
        ),
    ]
    engine = _make_engine(rows, [])
    provider = PostgresMetadataProvider(engine)

    # Act
    partitions = provider.list_partitions("public.events")

    # Assert -- grandchildren are not direct partitions of the root
    assert [p.name for p in partitions] == ["public.events__2024_01", "public.events_default"]
    january, default = partitions
    assert january.oid == 101
    assert january.partition_type is PartitionType.RANGE
    assert (january.from_value, january.to_value) == ("2024-01-01", "2024-02-01")
    assert january.boundaries_expr == "FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')"
    assert january.bounds == RangeBounds(from_value="2024-01-01", to_value="2024-02-01")
    assert january.is_attached is True
    assert january.is_default is False
    assert january.relkind is RelationKind.TABLE
    assert january.subpartition_type is None
    assert january.parent_table == "public.events"
    assert default.is_default is True
    assert default.boundaries_expr == "DEFAULT"
    assert default.subpartition_type is PartitionType.HASH
    assert default.relkind is RelationKind.PARTITIONED


def test__list_partitions__bare_parent__children_keep_their_own_schema() -> None:
    # Arrange
    rows = [
        _root_row(),
        _tree_row(
            level=1,
            name="events__2024_01",
            oid=101,
            schema="archive",
            parent=("public", "events"),
            boundaries="FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')",
        ),
    ]
    engine = _make_engine(rows, [])
    provider = PostgresMetadataProvider(engine)

    # Act
    partitions = provider.list_partitions("events")

    # Assert
    assert [p.name for p in partitions] == ["archive.events__2024_01"]
    assert partitions[0].parent_table == "events"


@pytest.mark.parametrize(
    "boundaries,expected",
    [
        ("FOR VALUES WITH (modulus 4, remainder 1)", "FOR VALUES WITH (modulus 4, remainder 1)"),
        ("FOR VALUES IN ('eu', 'u''s', NULL)", "FOR VALUES IN ('eu', 'u''s', NULL)"),
        ("FOR VALUES IN ('NULL')", "FOR VALUES IN ('NULL')"),
    ],
)
def test__list_partitions__hash_and_list_bounds__are_spelled_the_way_postgres_does(
    boundaries: str, expected: str
) -> None:
    # Arrange
    partstrat = "h" if "modulus" in boundaries else "l"
    root = _tree_row(level=0, name="tasks", oid=1, relkind="p", partstrat=partstrat, columns=["k"], key_arity=1)
    engine = _make_engine(
        [root, _tree_row(level=1, name="tasks__x", oid=2, parent=("public", "tasks"), boundaries=boundaries)], []
    )
    provider = PostgresMetadataProvider(engine)

    # Act
    (partition,) = provider.list_partitions("public.tasks")

    # Assert
    assert partition.boundaries_expr == expected
    assert partition.from_value is None
    assert partition.partition_type is PartitionType.from_partstrat(partstrat)


def test__list_partitions__list_bounds__are_structured() -> None:
    # Arrange
    root = _tree_row(level=0, name="tasks", oid=1, relkind="p", partstrat="l", columns=["k"], key_arity=1)
    child = _tree_row(
        level=1, name="tasks__eu", oid=2, parent=("public", "tasks"), boundaries="FOR VALUES IN ('de', NULL)"
    )
    engine = _make_engine([root, child], [])
    provider = PostgresMetadataProvider(engine)

    # Act
    (partition,) = provider.list_partitions("public.tasks")

    # Assert
    assert partition.bounds == ListBounds(values=("de",), includes_null=True)


def test__list_partitions__orphans__only_those_of_the_root_are_listed() -> None:
    # Arrange
    rows = [
        _root_row(),
        _tree_row(
            level=1,
            name="events__2024_02",
            oid=102,
            relkind="p",
            parent=("public", "events"),
            boundaries="FOR VALUES FROM ('2024-02-01') TO ('2024-03-01')",
            partstrat="h",
            columns=["tenant_id"],
            key_arity=1,
        ),
    ]
    orphans = [
        _orphan_row("events__2023_12", orphan_table_comment("public.events"), oid=500, relkind="p"),
        _orphan_row("events__2024_02__h1", orphan_table_comment("public.events__2024_02"), oid=501),
    ]
    engine = _make_engine(rows, orphans)
    provider = PostgresMetadataProvider(engine)

    # Act
    partitions = provider.list_partitions("public.events")

    # Assert
    assert [p.name for p in partitions] == ["public.events__2024_02", "public.events__2023_12"]
    orphan = partitions[1]
    assert orphan.oid == 500
    assert orphan.is_attached is False
    assert orphan.from_value is None
    assert orphan.bounds is None
    assert orphan.relkind is RelationKind.PARTITIONED
    assert orphan.parent_table == "public.events"


def test__list_partitions__unparseable_range_bound__still_lists_the_partition() -> None:
    # Arrange
    child = _tree_row(
        level=1, name="events__weird", oid=101, parent=("public", "events"), boundaries="FOR VALUES FROM (weird) ???"
    )
    engine = _make_engine([_root_row(), child], [])
    provider = PostgresMetadataProvider(engine)

    # Act
    partitions = provider.list_partitions("public.events")

    # Assert
    assert [p.name for p in partitions] == ["public.events__weird"]
    assert partitions[0].is_attached is True


# ── get_default_partition ───────────────────────────────────────────────────────


def test__get_default_partition__default_exists__returns_it() -> None:
    # Arrange
    rows = [
        _root_row(),
        _tree_row(level=1, name="events_default", oid=102, parent=("public", "events"), boundaries="DEFAULT"),
    ]
    engine = _make_engine(rows, [])
    provider = PostgresMetadataProvider(engine)

    # Act
    result = provider.get_default_partition("public.events")

    # Assert
    assert result is not None
    assert result.name == "public.events_default"
    assert result.is_default is True
    assert result.is_attached is True


def test__get_default_partition__no_default__returns_none() -> None:
    # Arrange
    rows = [
        _root_row(),
        _tree_row(
            level=1,
            name="events__2024_01",
            oid=101,
            parent=("public", "events"),
            boundaries="FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')",
        ),
    ]
    engine = _make_engine(rows, [])
    provider = PostgresMetadataProvider(engine)

    # Act / Assert
    assert provider.get_default_partition("public.events") is None


def test__get_default_partition__detached_default__is_not_reported() -> None:
    # Arrange -- an orphan carries no bounds, so it cannot be the DEFAULT partition
    engine = _make_engine([_root_row()], [_orphan_row("events_default", orphan_table_comment("public.events"))])
    provider = PostgresMetadataProvider(engine)

    # Act / Assert
    assert provider.get_default_partition("public.events") is None


# ── single relations ────────────────────────────────────────────────────────────


@pytest.mark.parametrize("exists", [True, False])
def test__partition_exists__returns_the_catalog_answer(exists: bool) -> None:
    # Arrange
    engine = _make_engine(exists)
    provider = PostgresMetadataProvider(engine)

    # Act / Assert
    assert provider.partition_exists("events__2024_W12") is exists
    assert _conn(engine).execute.call_args.args[1] == {"partition_name": '"events__2024_W12"'}
    assert "relkind IN ('r', 'p', 'f')" in _statements(engine)[0]


@pytest.mark.parametrize("attached", [True, False])
def test__is_partition_attached__returns_the_catalog_answer(attached: bool) -> None:
    # Arrange
    engine = _make_engine(attached)
    provider = PostgresMetadataProvider(engine)

    # Act / Assert
    assert provider.is_partition_attached("events", "events__2024_W12") is attached
    assert _conn(engine).execute.call_args.args[1] == {"table_name": '"events"', "partition_name": '"events__2024_W12"'}


@pytest.mark.parametrize("value,expected", [(4242, 4242), ("4242", 4242), (None, None)])
def test__get_relation_oid__returns_an_int_or_none(value: object, expected: int | None) -> None:
    # Arrange
    engine = _make_engine(value)
    provider = PostgresMetadataProvider(engine)

    # Act / Assert
    assert provider.get_relation_oid("public.events__2024_01") == expected
    assert _conn(engine).execute.call_args.args[1] == {"name": '"public"."events__2024_01"'}


def test__get_key_high_water_mark__max__queries_the_quoted_table_and_column() -> None:
    # Arrange
    engine = _make_engine(12345)
    provider = PostgresMetadataProvider(engine)

    # Act
    value = provider.get_key_high_water_mark("public.queue", "msg_id")

    # Assert
    assert value == 12345
    assert _statements(engine) == ['SELECT max("msg_id") FROM "public"."queue"']


def test__get_key_high_water_mark__sequence__reads_the_serial_sequence() -> None:
    # Arrange
    engine = _make_engine("77")
    provider = PostgresMetadataProvider(engine)

    # Act
    value = provider.get_key_high_water_mark("public.queue", "msg_id", sequence=True)

    # Assert
    assert value == 77
    call = _conn(engine).execute.call_args
    assert "pg_sequence_last_value" in str(call.args[0])
    assert call.args[1] == {"table_name": '"public"."queue"', "column": "msg_id"}


def test__get_key_high_water_mark__empty_table__returns_none() -> None:
    # Arrange
    engine = _make_engine(None)
    provider = PostgresMetadataProvider(engine)

    # Act / Assert
    assert provider.get_key_high_water_mark("queue", "msg_id") is None


def test__get_partition_boundaries__range_bound__returns_the_pair() -> None:
    # Arrange
    engine = _make_engine("FOR VALUES FROM ('2024-01-01'::date) TO ('2024-02-01'::date)")
    provider = PostgresMetadataProvider(engine)

    # Act / Assert
    assert provider.get_partition_boundaries("events__2024_01") == ("2024-01-01", "2024-02-01")


@pytest.mark.parametrize("expr", [None, "", "DEFAULT", "FOR VALUES WITH (modulus 2, remainder 0)"])
def test__get_partition_boundaries__not_a_range_bound__returns_none(expr: str | None) -> None:
    # Arrange
    engine = _make_engine(expr)
    provider = PostgresMetadataProvider(engine)

    # Act / Assert
    assert provider.get_partition_boundaries("events__x") is None


def test__get_unique_constraint_columns__returns_a_tuple_per_constraint() -> None:
    # Arrange
    rows = [
        SimpleNamespace(constraint_name="events_pkey", columns=["id", "created_at"]),
        SimpleNamespace(constraint_name="events_tenant_key", columns=("tenant_id",)),
        SimpleNamespace(constraint_name="odd", columns=None),
    ]
    engine = _make_engine(rows)
    provider = PostgresMetadataProvider(engine)

    # Act
    constraints = provider.get_unique_constraint_columns("public.events")

    # Assert
    assert constraints == (("id", "created_at"), ("tenant_id",), ())
    assert _conn(engine).execute.call_args.args[1] == {"table_name": '"public"."events"'}


# ── is_partition_closed ─────────────────────────────────────────────────────────


@pytest.mark.parametrize("scalar,expected", [(True, True), (False, False)])
def test__is_partition_closed__maps_scalar_to_bool(scalar: bool, expected: bool) -> None:
    # Arrange -- the bound is read first, then compared against now() in SQL
    engine = _make_engine("2024-02-01 00:00:00+00", scalar)
    provider = PostgresMetadataProvider(engine)

    # Act / Assert
    assert provider.is_partition_closed("events__2024_01") is expected


def test__is_partition_closed__no_upper_bound__is_not_closed_and_stays_quiet() -> None:
    # Arrange -- DEFAULT, detached, non-RANGE, or a name that resolves to nothing
    engine = _make_engine(None)
    provider = PostgresMetadataProvider(engine)
    logger = MagicMock()

    # Act
    with patch("pg_partsmith.sync.metadata.logger", logger):
        result = provider.is_partition_closed("events_default")

    # Assert
    assert result is False
    assert _conn(engine).execute.call_count == 1
    logger.warning.assert_not_called()


def test__is_partition_closed__passes_settle_seconds_and_quoted_regclass_name() -> None:
    # Arrange
    engine = _make_engine("2024-04-01 00:00:00+00", True)
    provider = PostgresMetadataProvider(engine)

    # Act
    provider.is_partition_closed("events__2024_W12", settle_seconds=900)

    # Assert -- the raw text form is cast in SQL so the session timezone decides how it is read
    lookup, compare = _conn(engine).execute.call_args_list
    assert "to_regclass(:partition_name)" in str(lookup.args[0])
    assert lookup.args[1]["partition_name"] == '"events__2024_W12"'
    assert "CAST(:upper_bound AS text)::timestamptz" in str(compare.args[0])
    assert compare.args[1] == {"upper_bound": "2024-04-01 00:00:00+00", "settle_seconds": 900}


def test__is_partition_closed__defaults_to_zero_settle_seconds() -> None:
    # Arrange
    engine = _make_engine("2024-02-01 00:00:00+00", True)
    provider = PostgresMetadataProvider(engine)

    # Act
    provider.is_partition_closed("events__2024_01")

    # Assert
    assert _conn(engine).execute.call_args.args[1]["settle_seconds"] == 0


def test__is_partition_closed__ddl_timezone__is_applied_to_the_session_first() -> None:
    # Arrange
    engine = _make_engine(None, "2024-02-01", True)
    provider = PostgresMetadataProvider(engine, ddl_timezone="Europe/Moscow")

    # Act
    result = provider.is_partition_closed("events__2024_01")

    # Assert
    assert result is True
    assert _statements(engine)[0] == "SET LOCAL TIME ZONE 'Europe/Moscow'"


def test__is_partition_closed__boundaries_given__override_the_providers_own_settings() -> None:
    # Arrange -- the provider was wired with settings that disagree with this table's;
    # the boundaries that wrote the partition are the ones that can read it back
    codec = UUIDv7BoundaryCodec()
    upper = str(codec.min_uuid_for(datetime(2026, 9, 7, tzinfo=UTC)))
    engine = _make_engine(None, upper, True)
    provider = PostgresMetadataProvider(engine, ddl_timezone="America/Los_Angeles")
    boundaries = TimeBoundaries(granularity=PartitionGranularity.WEEK, tz="Europe/Moscow", codec=codec)

    # Act
    result = provider.is_partition_closed("events__2026_w36", boundaries=boundaries)

    # Assert -- the calendar's zone was pinned, and the UUID bound was decoded to its instant
    assert result is True
    assert _statements(engine)[0] == "SET LOCAL TIME ZONE 'Europe/Moscow'"
    compare = _conn(engine).execute.call_args
    assert compare.args[1]["upper_bound"] == datetime(2026, 9, 7, tzinfo=UTC)


def test__is_partition_closed__naive_bound_without_a_timezone__warns_that_the_session_decides() -> None:
    # Arrange -- a timestamp/date key renders its bounds without an offset, and neither the
    # provider nor a caller said which zone wrote them
    engine = _make_engine("2026-02-01 00:00:00", True)
    provider = PostgresMetadataProvider(engine)
    logger = MagicMock()

    # Act
    with patch("pg_partsmith.sync.metadata.logger", logger):
        result = provider.is_partition_closed("events__2026_01")

    # Assert -- the answer still comes back; the reader is told what it rests on
    assert result is True
    assert "no timezone" in logger.warning.call_args.args[0]


@pytest.mark.parametrize(
    ("bound", "kwargs"),
    [
        # an offset in the literal answers the question by itself
        ("2026-02-01 00:00:00+00", {}),
        # so does a zone the caller pinned
        ("2026-02-01 00:00:00", {"ddl_timezone": "Europe/Moscow"}),
    ],
)
def test__is_partition_closed__timezone_is_not_in_doubt__stays_quiet(bound: str, kwargs: dict) -> None:
    # Arrange
    engine = _make_engine(*([None] if kwargs else []), bound, True)
    provider = PostgresMetadataProvider(engine, **kwargs)
    logger = MagicMock()

    # Act
    with patch("pg_partsmith.sync.metadata.logger", logger):
        result = provider.is_partition_closed("events__2026_01")

    # Assert
    assert result is True
    logger.warning.assert_not_called()


def test__is_partition_closed__boundaries_carry_the_zone__stays_quiet() -> None:
    # Arrange -- the table's own boundaries answer for both the zone and the codec
    engine = _make_engine(None, "2026-02-01 00:00:00", True)
    provider = PostgresMetadataProvider(engine)
    boundaries = TimeBoundaries(granularity=PartitionGranularity.MONTH, tz="Europe/Moscow")
    logger = MagicMock()

    # Act
    with patch("pg_partsmith.sync.metadata.logger", logger):
        result = provider.is_partition_closed("events__2026_01", boundaries=boundaries)

    # Assert
    assert result is True
    logger.warning.assert_not_called()


def test__is_partition_closed__codec__decodes_the_bound_and_compares_the_instant() -> None:
    # Arrange
    instant = datetime(2024, 2, 1, tzinfo=UTC)
    codec = MagicMock()
    codec.decode.return_value = instant
    engine = _make_engine("018d5d3c-2000-7000-8000-000000000000", True)
    provider = PostgresMetadataProvider(engine, boundary_codec=codec)

    # Act
    result = provider.is_partition_closed("events__2024_01", settle_seconds=5)

    # Assert
    assert result is True
    codec.decode.assert_called_once_with("018d5d3c-2000-7000-8000-000000000000")
    compare = _conn(engine).execute.call_args
    assert "CAST(:upper_bound AS timestamptz)" in str(compare.args[0])
    assert compare.args[1] == {"upper_bound": instant, "settle_seconds": 5}


def test__is_partition_closed__codec_cannot_read_the_bound__warns_and_is_not_closed() -> None:
    # Arrange -- an encoded bound the configured codec does not recognise
    engine = _make_engine("not-an-encoded-boundary")
    codec = MagicMock()
    codec.decode.return_value = None
    provider = PostgresMetadataProvider(engine, boundary_codec=codec)
    logger = MagicMock()

    # Act
    with patch("pg_partsmith.sync.metadata.logger", logger):
        result = provider.is_partition_closed("events__2026_w35")

    # Assert
    assert result is False
    logger.warning.assert_called_once()
    assert "boundary_codec" in logger.warning.call_args.args[0]
    assert logger.warning.call_args.kwargs["extra"] == {
        "partition_name": "events__2026_w35",
        "upper_bound": "not-an-encoded-boundary",
    }


def test__is_partition_closed__bound_is_not_a_timestamp__warns_instead_of_raising() -> None:
    # Arrange -- a sortable identifier with a date-shaped prefix fails the cast
    engine = _make_engine("2026-08-28-a1b2c3", DBAPIError("SELECT now() >= ...", {}, Exception("invalid input syntax")))
    provider = PostgresMetadataProvider(engine)
    logger = MagicMock()

    # Act
    with patch("pg_partsmith.sync.metadata.logger", logger):
        result = provider.is_partition_closed("events__2026_w35")

    # Assert
    assert result is False
    logger.warning.assert_called_once()


# ── constructor ─────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("prefix", ["", "   ", "bad prefix;"])
def test__constructor__invalid_marker_prefix__raises_value_error(prefix: str) -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="marker_prefix"):
        PostgresMetadataProvider(MagicMock(), marker_prefix=prefix)


def test__constructor__non_string_marker_prefix__raises_type_error() -> None:
    # Arrange / Act / Assert
    with pytest.raises(TypeError, match="marker_prefix"):
        PostgresMetadataProvider(MagicMock(), marker_prefix=42)  # type: ignore[arg-type]


# ── the DEFAULT probe ───────────────────────────────────────────────────────────


def test__get_leading_key_minimum__single_key__min_over_non_null_rows() -> None:
    # Arrange
    engine = _make_engine(datetime(2026, 3, 3, tzinfo=UTC))
    provider = PostgresMetadataProvider(engine)

    # Act
    value = provider.get_leading_key_minimum("public.events_default", ("created_at",))

    # Assert
    assert value == datetime(2026, 3, 3, tzinfo=UTC)
    assert _statements(engine) == [
        'SELECT min("created_at") FROM "public"."events_default" WHERE "created_at" IS NOT NULL'
    ]


def test__get_leading_key_minimum__composite_key__leaves_out_rows_with_a_null_anywhere_in_the_key() -> None:
    # Arrange
    engine = _make_engine(None)
    provider = PostgresMetadataProvider(engine)

    # Act
    value = provider.get_leading_key_minimum("d", ("created_at", "tenant_id"))

    # Assert
    assert value is None
    assert _statements(engine) == [
        'SELECT min("created_at") FROM "d" WHERE "created_at" IS NOT NULL AND "tenant_id" IS NOT NULL'
    ]


def test__get_leading_key_minimum__no_key__refused() -> None:
    # Arrange
    provider = PostgresMetadataProvider(_make_engine(None))

    # Act / Assert
    with pytest.raises(ValueError, match="partition key"):
        provider.get_leading_key_minimum("d", ())


# ── measure: references ─────────────────────────────────────────────────────────


def _fk_row(referenced_oid: int, referencing: str = '"public"."refs"') -> SimpleNamespace:
    return SimpleNamespace(
        constraint_name="refs_fk",
        referenced_oid=referenced_oid,
        referencing=referencing,
        referencing_columns=["event_id", "created_at"],
        referenced_columns=["id", "created_at"],
    )


def test__measure__references_requested__joins_each_foreign_key_to_the_partition() -> None:
    # Arrange -- one FK on the parent; January's rows are referenced, the orphan is checked against nothing
    engine = _make_engine([_fk_row(100)], True)
    provider = PostgresMetadataProvider(engine)
    tree = _sample_tree()

    # Act
    measured = provider.measure(
        tree,
        targets=("public.events__2024_01", "public.events__2023_12"),
        facts=frozenset({FactKind.REFERENCES}),
    )

    # Assert
    statements = _statements(engine)
    assert "confrelid = ANY" in statements[0]
    assert _conn(engine).execute.call_args_list[0].args[1] == {"oids": [101, 500, 100]}
    assert statements[1] == (
        'SELECT EXISTS (SELECT 1 FROM "public"."refs" r JOIN "public"."events__2024_01" p '
        'ON r."event_id" = p."id" AND r."created_at" = p."created_at")'
    )
    assert len(statements) == 2
    january = measured.root.children[0]
    assert january.facts == PartitionFacts(referenced=True)
    assert measured.orphans[0].facts == PartitionFacts(referenced=False)


def test__measure__references__foreign_key_on_the_orphan_itself_is_checked() -> None:
    # Arrange
    engine = _make_engine([_fk_row(500)], False)
    provider = PostgresMetadataProvider(engine)

    # Act
    measured = provider.measure(
        _sample_tree(), targets=("public.events__2023_12",), facts=frozenset({FactKind.REFERENCES})
    )

    # Assert
    assert measured.orphans[0].facts == PartitionFacts(referenced=False)
    assert 'JOIN "public"."events__2023_12" p' in _statements(engine)[1]


def test__measure__references__no_foreign_keys__no_join_is_run() -> None:
    # Arrange
    engine = _make_engine([])
    provider = PostgresMetadataProvider(engine)

    # Act
    measured = provider.measure(
        _sample_tree(), targets=("public.events__2024_01",), facts=frozenset({FactKind.REFERENCES})
    )

    # Assert
    assert _conn(engine).execute.call_count == 1
    assert measured.root.children[0].facts == PartitionFacts(referenced=False)


def test__measure__references_and_sizes__both_queries_run() -> None:
    # Arrange
    engine = _make_engine(
        [_facts_row(101, 10, 1)],
        [],
    )
    provider = PostgresMetadataProvider(engine)

    # Act
    measured = provider.measure(
        _sample_tree(), targets=("public.events__2024_01",), facts=frozenset({FactKind.SIZE, FactKind.REFERENCES})
    )

    # Assert
    assert measured.root.children[0].facts == PartitionFacts(size_bytes=10, referenced=False)
