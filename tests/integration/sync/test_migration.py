"""Batched data movement against a real PostgreSQL (sync).

``partition_data`` drains a DEFAULT partition -- the migration path for a
table partitioned around its data -- and ``unpartition`` moves everything back
into one plain table.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest

from pg_partsmith.entities import MaintenanceIssueStep, Period
from pg_partsmith.exceptions import InvalidPartitionConfigError
from pg_partsmith.sync.hooks import BasePartitionLifecycleHooks
from pg_partsmith.sync.metadata import PostgresMetadataProvider
from tests.integration.nested_support import (
    IDENTITY_TABLE_DDL,
    MONTHLY_TABLE_DDL,
    NULLABLE_COMPOSITE_TABLE_DDL,
    TIMESTAMP_TABLE_DDL,
    monthly_config,
    nested_config,
    nullable_composite_config,
)
from tests.integration.sync.support import (
    exec_sql,
    hash_children_of,
    is_attached,
    make_service,
    make_table,
    range_children_of,
    relkind,
    scalar,
    table_comment,
)

if TYPE_CHECKING:
    from collections.abc import Generator

    from sqlalchemy import Engine

pytestmark = pytest.mark.integration


@pytest.fixture
def events(sync_db_engine: Engine) -> Generator[str, None]:
    yield from make_table(sync_db_engine, MONTHLY_TABLE_DDL, prefix="mig")


@pytest.fixture
def tenants(sync_db_engine: Engine) -> Generator[str, None]:
    yield from make_table(sync_db_engine, TIMESTAMP_TABLE_DDL, prefix="nmig")


@pytest.fixture
def nullable(sync_db_engine: Engine) -> Generator[str, None]:
    yield from make_table(sync_db_engine, NULLABLE_COMPOSITE_TABLE_DDL, prefix="cmig")


def _default_with_rows(engine: Engine, table: str, *, months: tuple[int, ...], per_month: int) -> str:
    """The migration starting point: the old monolithic table attached as DEFAULT, full of rows."""
    default = f"{table}_legacy"
    exec_sql(engine, f'CREATE TABLE "{default}" (LIKE "{table}" INCLUDING ALL)')
    for month in months:
        exec_sql(
            engine,
            f'INSERT INTO "{default}" (created_at, payload) '  # noqa: S608
            f"SELECT make_timestamptz(2026, :month, 1 + (g % 27), 12, 0, 0, 'UTC'), 'row ' || g "
            f"FROM generate_series(1, :rows) g",
            month=month,
            rows=per_month,
        )
    exec_sql(engine, f'ALTER TABLE "{table}" ATTACH PARTITION "{default}" DEFAULT')
    return default


def _count(engine: Engine, table: str) -> int:
    return int(scalar(engine, f'SELECT count(*) FROM "{table}"'))  # noqa: S608


# ── partition_data ──────────────────────────────────────────────────────────────


def test__partition_data__drains_default_into_monthly_partitions_in_batches(
    sync_db_engine: Engine, events: str
) -> None:
    # Arrange -- 25 rows across three months in the DEFAULT partition
    default = _default_with_rows(sync_db_engine, events, months=(3, 4, 6), per_month=25)
    config = monthly_config(events, create_ahead=1)

    # Act
    result = make_service(sync_db_engine).partition_data(config, batch_rows=10)

    # Assert
    assert result.complete
    assert result.rows_moved == 75
    assert result.batches == 9  # 3 windows x (10 + 10 + 5)
    assert result.partitions == (
        f"public.{events}__2026_03",
        f"public.{events}__2026_04",
        f"public.{events}__2026_06",
    )
    assert result.issues == ()
    assert _count(sync_db_engine, default) == 0
    assert _count(sync_db_engine, events) == 75
    assert _count(sync_db_engine, f"{events}__2026_04") == 25
    children = range_children_of(sync_db_engine, events)
    assert set(children) == {f"{events}__2026_03", f"{events}__2026_04", f"{events}__2026_06"}
    assert is_attached(sync_db_engine, default)


def test__partition_data__batch_budget__resumes_where_it_stopped(sync_db_engine: Engine, events: str) -> None:
    # Arrange
    default = _default_with_rows(sync_db_engine, events, months=(3,), per_month=25)
    config = monthly_config(events, create_ahead=1)
    service = make_service(sync_db_engine)

    # Act -- two batches: the March partition exists but stays detached and invisible
    first = service.partition_data(config, batch_rows=10, max_batches=2)
    visible_between = _count(sync_db_engine, events)
    second = service.partition_data(config, batch_rows=10)

    # Assert
    assert (first.rows_moved, first.batches, first.complete, first.partitions) == (20, 2, False, ())
    assert relkind(sync_db_engine, f"{events}__2026_03") == "r"
    assert visible_between == 5  # the 20 moved rows sit in the detached table
    assert (second.rows_moved, second.complete, second.partitions) == (5, True, (f"public.{events}__2026_03",))
    assert is_attached(sync_db_engine, f"{events}__2026_03")
    assert _count(sync_db_engine, default) == 0
    assert _count(sync_db_engine, events) == 25


def test__partition_data__nested_scheme__rows_land_in_the_buckets(sync_db_engine: Engine, tenants: str) -> None:
    # Arrange
    default = f"{tenants}_legacy"
    exec_sql(sync_db_engine, f'CREATE TABLE "{default}" (LIKE "{tenants}" INCLUDING ALL)')
    exec_sql(
        sync_db_engine,
        f'INSERT INTO "{default}" (tenant_id, created_at, payload) '  # noqa: S608
        f"SELECT g % 5, '2026-08-25 10:00:00+00', 'p' FROM generate_series(1, 20) g",
    )
    exec_sql(sync_db_engine, f'ALTER TABLE "{tenants}" ATTACH PARTITION "{default}" DEFAULT')
    config = nested_config(tenants, modulus=2)

    # Act
    result = make_service(sync_db_engine).partition_data(config, batch_rows=7)

    # Assert
    assert result.complete
    assert result.rows_moved == 20
    assert result.partitions == (f"public.{tenants}__2026_w35",)
    buckets = hash_children_of(sync_db_engine, f"{tenants}__2026_w35")
    assert set(buckets.values()) == {(2, 0), (2, 1)}
    assert _count(sync_db_engine, default) == 0
    assert _count(sync_db_engine, f"{tenants}__2026_w35") == 20


def test__partition_data__no_default_partition__nothing_to_do(sync_db_engine: Engine, events: str) -> None:
    # Act
    result = make_service(sync_db_engine).partition_data(monthly_config(events))

    # Assert
    assert result.complete
    assert result.rows_moved == 0


def test__partition_data__rows_with_a_null_trailing_key__stay_in_default(sync_db_engine: Engine, nullable: str) -> None:
    # Arrange -- the composite key (created_at, tenant_id); PostgreSQL routes a NULL tenant to DEFAULT
    default = f"{nullable}_legacy"
    exec_sql(sync_db_engine, f'CREATE TABLE "{default}" (LIKE "{nullable}" INCLUDING ALL)')
    exec_sql(
        sync_db_engine,
        f'INSERT INTO "{default}" (tenant_id, created_at) '  # noqa: S608
        "VALUES (1, '2026-08-25'), (NULL, '2026-08-25')",
    )
    exec_sql(sync_db_engine, f'ALTER TABLE "{nullable}" ATTACH PARTITION "{default}" DEFAULT')
    config = nullable_composite_config(nullable)

    # Act
    result = make_service(sync_db_engine).partition_data(config)

    # Assert
    assert result.complete
    assert result.rows_moved == 1
    assert _count(sync_db_engine, default) == 1


def test__partition_data__window_held_by_an_unmanaged_partition__stops_with_an_issue(
    sync_db_engine: Engine, events: str
) -> None:
    # Arrange -- a hand-made partition straddles March and April, so neither month can be created;
    # a March row it does not cover sits in DEFAULT
    exec_sql(
        sync_db_engine,
        f"CREATE TABLE \"{events}_odd\" PARTITION OF \"{events}\" FOR VALUES FROM ('2026-03-15') TO ('2026-04-15')",
    )
    default = f"{events}_legacy"
    exec_sql(sync_db_engine, f'CREATE TABLE "{default}" (LIKE "{events}" INCLUDING ALL)')
    exec_sql(sync_db_engine, f"INSERT INTO \"{default}\" (created_at, payload) VALUES ('2026-03-03', 'x')")  # noqa: S608
    exec_sql(sync_db_engine, f'ALTER TABLE "{events}" ATTACH PARTITION "{default}" DEFAULT')

    # Act
    result = make_service(sync_db_engine).partition_data(monthly_config(events, create_ahead=1))

    # Assert
    assert not result.complete
    assert result.rows_moved == 0
    assert [issue.step for issue in result.issues][-1] is MaintenanceIssueStep.MOVE
    assert "no partition can be created" in result.issues[-1].error
    assert _count(sync_db_engine, default) == 1


def test__partition_data__non_range_root__refused(sync_db_engine: Engine, events: str) -> None:
    from tests.integration.nested_support import HASH_ROOT_TABLE_DDL, hash_root_config  # noqa: PLC0415

    for tasks in make_table(sync_db_engine, HASH_ROOT_TABLE_DDL, prefix="hmig"):
        with pytest.raises(InvalidPartitionConfigError, match="not a RANGE level"):
            make_service(sync_db_engine).partition_data(hash_root_config(tasks))


# ── unpartition ─────────────────────────────────────────────────────────────────


def test__unpartition__moves_everything_into_one_table_and_drops_the_partitions(
    sync_db_engine: Engine, events: str
) -> None:
    # Arrange -- three partitions with rows, made by the mover itself
    _default_with_rows(sync_db_engine, events, months=(3, 4, 6), per_month=10)
    config = monthly_config(events, create_ahead=1)
    service = make_service(sync_db_engine)
    service.partition_data(config)
    flat = f"{events}_flat"

    # Act
    result = service.unpartition(config, flat, batch_rows=4, drop_emptied=True)

    # Assert
    assert result.complete
    assert result.rows_moved == 30
    assert result.partitions == (
        f"public.{events}__2026_03",
        f"public.{events}__2026_04",
        f"public.{events}__2026_06",
        f"public.{events}_legacy",
    )
    assert _count(sync_db_engine, flat) == 30
    assert _count(sync_db_engine, events) == 0
    assert range_children_of(sync_db_engine, events) == {}
    assert relkind(sync_db_engine, f"{events}__2026_03") is None
    assert PostgresMetadataProvider(sync_db_engine).list_partitions(events) == []


def test__unpartition__without_dropping__partitions_stay_attached_and_empty(
    sync_db_engine: Engine, events: str
) -> None:
    # Arrange
    _default_with_rows(sync_db_engine, events, months=(5,), per_month=6)
    config = monthly_config(events, create_ahead=1)
    service = make_service(sync_db_engine)
    service.partition_data(config)
    flat = f"{events}_flat"
    exec_sql(sync_db_engine, f'CREATE TABLE "{flat}" (LIKE "{events}" INCLUDING ALL)')

    # Act
    result = service.unpartition(config, flat)

    # Assert
    assert result.complete
    assert result.rows_moved == 6
    assert is_attached(sync_db_engine, f"{events}__2026_05")
    assert _count(sync_db_engine, f"{events}__2026_05") == 0
    assert _count(sync_db_engine, flat) == 6


def test__unpartition__batch_budget__stops_and_resumes(sync_db_engine: Engine, events: str) -> None:
    # Arrange
    _default_with_rows(sync_db_engine, events, months=(5,), per_month=9)
    config = monthly_config(events, create_ahead=1)
    service = make_service(sync_db_engine)
    service.partition_data(config)
    flat = f"{events}_flat"

    # Act
    first = service.unpartition(config, flat, batch_rows=4, max_batches=1)
    second = service.unpartition(config, flat, batch_rows=4)

    # Assert
    assert (first.rows_moved, first.complete) == (4, False)
    assert (second.rows_moved, second.complete) == (5, True)
    assert _count(sync_db_engine, flat) == 9


# ── review follow-ups: FK actions, late rows, generated columns, destinations ───


@pytest.fixture
def ledger(sync_db_engine: Engine) -> Generator[str, None]:
    yield from make_table(sync_db_engine, IDENTITY_TABLE_DDL, prefix="gmig")


def test__partition_data__cascading_foreign_key__refused_with_nothing_deleted(
    sync_db_engine: Engine, events: str
) -> None:
    # Arrange: rows in DEFAULT are referenced ON DELETE CASCADE
    _default_with_rows(sync_db_engine, events, months=(3,), per_month=5)
    ref = f"{events}_ref"
    exec_sql(
        sync_db_engine,
        f'CREATE TABLE "{ref}" (event_id BIGINT, created_at TIMESTAMPTZ, '
        f'FOREIGN KEY (event_id, created_at) REFERENCES "{events}" (id, created_at) ON DELETE CASCADE)',
    )
    exec_sql(sync_db_engine, f'INSERT INTO "{ref}" SELECT id, created_at FROM "{events}" LIMIT 2')  # noqa: S608

    # Act
    result = make_service(sync_db_engine).partition_data(monthly_config(events, create_ahead=1))

    # Assert: fail closed -- no parent row moved, no referencing row cascaded away
    assert not result.complete
    assert any("ON DELETE CASCADE" in issue.error for issue in result.issues)
    assert _count(sync_db_engine, events) == 5
    assert _count(sync_db_engine, ref) == 2


class _LateWriter(BasePartitionLifecycleHooks):
    """Commits one more row through the root just before each partition detaches."""

    def __init__(self, engine: Engine, table: str, when: datetime) -> None:
        self._engine = engine
        self._table = table
        self._when = when

    def before_detach(self, table_name: str, partition: object) -> None:
        exec_sql(
            self._engine,
            f"INSERT INTO \"{self._table}\" (created_at, payload) VALUES (:ts, 'late')",  # noqa: S608
            ts=self._when,
        )


def test__unpartition__row_committed_between_the_last_batch_and_the_drop__is_moved_not_dropped(
    sync_db_engine: Engine, events: str
) -> None:
    # Arrange: nine March rows, and a writer that lands one more at every detach
    _default_with_rows(sync_db_engine, events, months=(3,), per_month=9)
    config = monthly_config(events, create_ahead=1)
    make_service(sync_db_engine).partition_data(config)
    flat = f"{events}_flat"
    late = _LateWriter(sync_db_engine, events, datetime(2026, 3, 15, 12, tzinfo=UTC))

    # Act
    result = make_service(sync_db_engine, hooks=[late]).unpartition(config, flat, batch_rows=4, drop_emptied=True)

    # Assert: both late rows (March partition, then DEFAULT) end in the flat table
    assert result.complete
    assert result.rows_moved == 11
    assert _count(sync_db_engine, flat) == 11
    assert relkind(sync_db_engine, f"{events}__2026_03") is None
    assert _count(sync_db_engine, events) == 0


def test__movers__generated_and_identity_columns__recomputed_not_copied(sync_db_engine: Engine, ledger: str) -> None:
    # Arrange: the legacy table carries GENERATED ALWAYS columns (identity and stored)
    legacy = f"{ledger}_legacy"
    exec_sql(sync_db_engine, f'CREATE TABLE "{legacy}" (LIKE "{ledger}" INCLUDING ALL EXCLUDING IDENTITY)')
    exec_sql(
        sync_db_engine,
        f'INSERT INTO "{legacy}" (id, tenant_id, created_at, amount) '  # noqa: S608
        f"SELECT g, 1, make_timestamptz(2026, 4, 1 + (g % 27), 12, 0, 0, 'UTC'), g FROM generate_series(1, 6) g",
    )
    exec_sql(sync_db_engine, f'ALTER TABLE "{ledger}" ATTACH PARTITION "{legacy}" DEFAULT')
    config = monthly_config(ledger, create_ahead=1)

    # Act
    forward = make_service(sync_db_engine).partition_data(config)
    flat = f"{ledger}_flat"
    back = make_service(sync_db_engine).unpartition(config, flat)

    # Assert: rows moved twice, ids kept, the stored column recomputed each time
    assert forward.complete and back.complete
    assert forward.rows_moved == 6 and back.rows_moved == 6
    assert int(scalar(sync_db_engine, f'SELECT count(*) FROM "{flat}" WHERE doubled = amount * 2')) == 6  # noqa: S608
    assert int(scalar(sync_db_engine, f'SELECT sum(id) FROM "{flat}"')) == 21  # noqa: S608


def test__unpartition__into_the_root_itself__refused_before_any_row_moves(sync_db_engine: Engine, events: str) -> None:
    # Arrange
    _default_with_rows(sync_db_engine, events, months=(5,), per_month=3)
    config = monthly_config(events, create_ahead=1)

    # Act / Assert
    with pytest.raises(InvalidPartitionConfigError, match="itself"):
        make_service(sync_db_engine).unpartition(config, f"public.{events}")
    assert _count(sync_db_engine, events) == 3


def test__partition_data__window_of_a_detached_owned_partition__filled_and_reattached(
    sync_db_engine: Engine, events: str
) -> None:
    # Arrange: March exists, was detached by the library, and new March rows sit in DEFAULT
    config = monthly_config(events, create_ahead=1)
    service = make_service(sync_db_engine)
    service.ensure_partitions(config, [Period(year=2026, month=3)])
    march = f"{events}__2026_03"
    listed = PostgresMetadataProvider(sync_db_engine).list_partitions(events)
    service.detach_old_partitions(events, [p for p in listed if p.relname == march])
    assert is_attached(sync_db_engine, march) is False
    _default_with_rows(sync_db_engine, events, months=(3,), per_month=4)

    # Act
    result = service.partition_data(config)

    # Assert: the orphan is filled and re-attached, not recreated or given up on
    assert result.complete
    assert result.partitions == (f"public.{march}",)
    assert is_attached(sync_db_engine, march)
    assert _count(sync_db_engine, march) == 4
    assert table_comment(sync_db_engine, march) is None


def test__partition_data__no_action_foreign_key_with_referenced_rows__refused_row_safe(
    sync_db_engine: Engine, events: str
) -> None:
    # Arrange: rows in DEFAULT are referenced through an ordinary (NO ACTION) key
    _default_with_rows(sync_db_engine, events, months=(3,), per_month=4)
    ref = f"{events}_noact"
    exec_sql(
        sync_db_engine,
        f'CREATE TABLE "{ref}" (event_id BIGINT, created_at TIMESTAMPTZ, '
        f'FOREIGN KEY (event_id, created_at) REFERENCES "{events}" (id, created_at))',
    )
    exec_sql(sync_db_engine, f'INSERT INTO "{ref}" SELECT id, created_at FROM "{events}" LIMIT 1')  # noqa: S608

    # Act
    result = make_service(sync_db_engine).partition_data(monthly_config(events, create_ahead=1))

    # Assert: a referenced row cannot leave the tree during a move; the batch fails whole
    assert not result.complete
    assert any("still referenced" in issue.error for issue in result.issues)
    assert _count(sync_db_engine, events) == 4
    assert _count(sync_db_engine, ref) == 1


class _DetachedWriter(BasePartitionLifecycleHooks):
    """Commits one more row straight into the detached table right before its drop."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def before_drop(self, table_name: str, partition_name: str) -> None:
        quoted = ".".join(f'"{part}"' for part in partition_name.split("."))
        exec_sql(
            self._engine,
            f"INSERT INTO {quoted} (created_at, payload) VALUES ('2026-03-20T12:00:00+00:00', 'dropwrite')",  # noqa: S608
        )


def test__unpartition__row_committed_inside_before_drop__moved_in_the_drop_transaction(
    sync_db_engine: Engine, events: str
) -> None:
    # Arrange
    _default_with_rows(sync_db_engine, events, months=(3,), per_month=5)
    config = monthly_config(events, create_ahead=1)
    make_service(sync_db_engine).partition_data(config)
    flat = f"{events}_flat"

    # Act
    result = make_service(sync_db_engine, hooks=[_DetachedWriter(sync_db_engine)]).unpartition(
        config, flat, batch_rows=4, drop_emptied=True
    )

    # Assert: both drop-time rows are moved inside the drop's transaction and counted
    assert result.complete
    assert result.rows_moved == 7
    assert _count(sync_db_engine, flat) == 7
    assert _count(sync_db_engine, events) == 0


def test__unpartition__into_an_identity_table__the_next_ordinary_insert_succeeds(
    sync_db_engine: Engine, ledger: str
) -> None:
    # Arrange: five rows with explicit ids come through the tree
    legacy = f"{ledger}_legacy"
    exec_sql(sync_db_engine, f'CREATE TABLE "{legacy}" (LIKE "{ledger}" INCLUDING ALL EXCLUDING IDENTITY)')
    exec_sql(
        sync_db_engine,
        f'INSERT INTO "{legacy}" (id, tenant_id, created_at, amount) '  # noqa: S608
        f"SELECT g, 1, make_timestamptz(2026, 4, 1 + (g % 27), 12, 0, 0, 'UTC'), g FROM generate_series(1, 5) g",
    )
    exec_sql(sync_db_engine, f'ALTER TABLE "{ledger}" ATTACH PARTITION "{legacy}" DEFAULT')
    config = monthly_config(ledger, create_ahead=1)
    make_service(sync_db_engine).partition_data(config)
    dest = f"{ledger}_dest"
    exec_sql(sync_db_engine, f'CREATE TABLE "{dest}" (LIKE "{ledger}" INCLUDING ALL)')

    # Act
    result = make_service(sync_db_engine).unpartition(config, f"public.{dest}")
    exec_sql(
        sync_db_engine,
        f"INSERT INTO \"{dest}\" (tenant_id, created_at, amount) VALUES (1, '2026-05-01T00:00:00+00:00', 9)",  # noqa: S608
    )

    # Assert: the identity sequence was advanced past the moved ids
    assert result.complete
    assert result.rows_moved == 5
    assert int(scalar(sync_db_engine, f'SELECT max(id) FROM "{dest}"')) == 6  # noqa: S608
    assert int(scalar(sync_db_engine, f'SELECT count(*) FROM "{dest}"')) == 6  # noqa: S608


def test__unpartition__into_a_leaf_of_another_tree__refused(sync_db_engine: Engine, events: str) -> None:
    # Arrange: a partition of an unrelated root -- relkind 'r', but routed property of another tree
    other = f"{events}_other"
    exec_sql(sync_db_engine, f'CREATE TABLE "{other}" (ts TIMESTAMPTZ NOT NULL) PARTITION BY RANGE (ts)')
    exec_sql(
        sync_db_engine,
        f"CREATE TABLE \"{other}_p1\" PARTITION OF \"{other}\" FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')",
    )
    try:
        # Act / Assert
        with pytest.raises(InvalidPartitionConfigError, match="attached as a partition"):
            make_service(sync_db_engine).unpartition(monthly_config(events, create_ahead=1), f"public.{other}_p1")
    finally:
        exec_sql(sync_db_engine, f'DROP TABLE "{other}"')


def test__partition_data__deferred_foreign_key_with_referenced_rows__refused_row_safe(
    sync_db_engine: Engine, events: str
) -> None:
    # Arrange: the check would otherwise wait for COMMIT, outside any statement handler
    _default_with_rows(sync_db_engine, events, months=(3,), per_month=4)
    ref = f"{events}_defer"
    exec_sql(
        sync_db_engine,
        f'CREATE TABLE "{ref}" (event_id BIGINT, created_at TIMESTAMPTZ, '
        f'FOREIGN KEY (event_id, created_at) REFERENCES "{events}" (id, created_at) '
        f"DEFERRABLE INITIALLY DEFERRED)",
    )
    exec_sql(sync_db_engine, f'INSERT INTO "{ref}" SELECT id, created_at FROM "{events}" LIMIT 1')  # noqa: S608

    # Act
    result = make_service(sync_db_engine).partition_data(monthly_config(events, create_ahead=1))

    # Assert: SET CONSTRAINTS ALL IMMEDIATE pulls the refusal into the statement, translated
    assert not result.complete
    assert any("still referenced" in issue.error for issue in result.issues)
    assert _count(sync_db_engine, events) == 4
    assert _count(sync_db_engine, ref) == 1


def test__unpartition__into_a_descending_identity_table__the_next_ordinary_insert_succeeds(
    sync_db_engine: Engine, ledger: str
) -> None:
    # Arrange: ids 0, -1, -2 come through the tree; the destination counts downwards
    legacy = f"{ledger}_legacy"
    exec_sql(sync_db_engine, f'CREATE TABLE "{legacy}" (LIKE "{ledger}" INCLUDING ALL EXCLUDING IDENTITY)')
    exec_sql(
        sync_db_engine,
        f'INSERT INTO "{legacy}" (id, tenant_id, created_at, amount) '  # noqa: S608
        f"SELECT 1 - g, 1, make_timestamptz(2026, 4, g, 12, 0, 0, 'UTC'), g FROM generate_series(1, 3) g",
    )
    exec_sql(sync_db_engine, f'ALTER TABLE "{ledger}" ATTACH PARTITION "{legacy}" DEFAULT')
    config = monthly_config(ledger, create_ahead=1)
    make_service(sync_db_engine).partition_data(config)
    dest = f"{ledger}_ndest"
    exec_sql(
        sync_db_engine,
        f'CREATE TABLE "{dest}" ('
        "id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 0 INCREMENT BY -1 MAXVALUE 0), "
        "tenant_id BIGINT NOT NULL, created_at TIMESTAMPTZ NOT NULL, "
        "amount NUMERIC NOT NULL DEFAULT 0, doubled NUMERIC GENERATED ALWAYS AS (amount * 2) STORED)",
    )

    # Act
    result = make_service(sync_db_engine).unpartition(config, f"public.{dest}")
    exec_sql(
        sync_db_engine,
        f"INSERT INTO \"{dest}\" (tenant_id, created_at, amount) VALUES (1, '2026-05-01T00:00:00+00:00', 9)",  # noqa: S608
    )

    # Assert: the sequence chased the LOW water mark, so the next id is -3
    assert result.complete
    assert result.rows_moved == 3
    assert int(scalar(sync_db_engine, f'SELECT min(id) FROM "{dest}"')) == -3  # noqa: S608
    assert int(scalar(sync_db_engine, f'SELECT count(*) FROM "{dest}"')) == 4  # noqa: S608


def test__unpartition__into_a_cycling_identity_table__refused_with_nothing_moved(
    sync_db_engine: Engine, ledger: str
) -> None:
    # Arrange: the destination's identity cycles, so it would reissue the moved ids
    _identity_rows(sync_db_engine, ledger, ids=(1, 2, 3))
    config = monthly_config(ledger, create_ahead=1)
    make_service(sync_db_engine).partition_data(config)
    dest = _identity_destination(sync_db_engine, ledger, "cyc", "MINVALUE 1 MAXVALUE 5 CYCLE")

    # Act
    result = make_service(sync_db_engine).unpartition(config, f"public.{dest}")

    # Assert: refused whole, nothing half-moved
    assert not result.complete
    assert any("cycles" in issue.error for issue in result.issues)
    assert int(scalar(sync_db_engine, f'SELECT count(*) FROM "{dest}"')) == 0  # noqa: S608
    assert _count(sync_db_engine, ledger) == 3


def test__unpartition__identity_range_too_narrow__refused_before_the_destination_breaks(
    sync_db_engine: Engine, ledger: str
) -> None:
    # Arrange: three ids into a sequence that has exactly three values to give
    _identity_rows(sync_db_engine, ledger, ids=(1, 2, 3))
    config = monthly_config(ledger, create_ahead=1)
    make_service(sync_db_engine).partition_data(config)
    dest = _identity_destination(sync_db_engine, ledger, "narrow", "MINVALUE 1 MAXVALUE 3")

    # Act
    result = make_service(sync_db_engine).unpartition(config, f"public.{dest}")

    # Assert
    assert not result.complete
    assert any("nothing left to issue" in issue.error for issue in result.issues)
    assert int(scalar(sync_db_engine, f'SELECT count(*) FROM "{dest}"')) == 0  # noqa: S608


def test__unpartition__ids_off_the_sequences_path__moved_and_the_sequence_left_alone(
    sync_db_engine: Engine, ledger: str
) -> None:
    # Arrange: id 6 is inside the range but not a value the sequence lands on
    _identity_rows(sync_db_engine, ledger, ids=(6,))
    config = monthly_config(ledger, create_ahead=1)
    make_service(sync_db_engine).partition_data(config)
    dest = _identity_destination(sync_db_engine, ledger, "offpath", "START WITH 1 INCREMENT BY 3 MAXVALUE 7")

    # Act
    result = make_service(sync_db_engine).unpartition(config, f"public.{dest}")
    exec_sql(
        sync_db_engine,
        f"INSERT INTO \"{dest}\" (tenant_id, created_at, amount) VALUES (1, '2026-05-01T00:00:00+00:00', 9)",  # noqa: S608
    )

    # Assert: nothing to synchronise, and the sequence still starts where it meant to
    assert result.complete
    assert result.rows_moved == 1
    assert int(scalar(sync_db_engine, f'SELECT min(id) FROM "{dest}"')) == 1  # noqa: S608


def _identity_rows(engine: Engine, table: str, *, ids: tuple[int, ...]) -> None:
    """Attach the legacy table as DEFAULT with rows carrying explicit ids."""
    legacy = f"{table}_legacy"
    exec_sql(engine, f'CREATE TABLE "{legacy}" (LIKE "{table}" INCLUDING ALL EXCLUDING IDENTITY)')
    for index, value in enumerate(ids, start=1):
        exec_sql(
            engine,
            f'INSERT INTO "{legacy}" (id, tenant_id, created_at, amount) '  # noqa: S608
            f"VALUES (:id, 1, make_timestamptz(2026, 4, :day, 12, 0, 0, 'UTC'), :amount)",
            id=value,
            day=index,
            amount=index,
        )
    exec_sql(engine, f'ALTER TABLE "{table}" ATTACH PARTITION "{legacy}" DEFAULT')


def _identity_destination(engine: Engine, table: str, suffix: str, identity: str) -> str:
    """A plain destination shaped like the ledger, with a deliberately awkward identity."""
    name = f"{table}_{suffix}"
    exec_sql(
        engine,
        f'CREATE TABLE "{name}" ('
        f"id BIGINT GENERATED ALWAYS AS IDENTITY ({identity}), "
        "tenant_id BIGINT NOT NULL, created_at TIMESTAMPTZ NOT NULL, "
        "amount NUMERIC NOT NULL DEFAULT 0, doubled NUMERIC GENERATED ALWAYS AS (amount * 2) STORED)",
    )
    return name
