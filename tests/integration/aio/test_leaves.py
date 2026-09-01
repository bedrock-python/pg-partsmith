"""Leaf backends against a real PostgreSQL (async).

Local leaves with a tablespace, storage parameters and inherited privileges;
foreign leaves on a ``postgres_fdw`` loopback server, created, queried,
expired and dropped by the lifecycle.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING
from unittest.mock import patch
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text

from pg_partsmith.aio.repositories import PostgresPartitionRepository
from pg_partsmith.entities import Period
from pg_partsmith.exceptions import InvalidPartitionConfigError, PlanStaleError
from pg_partsmith.leaves import ForeignLeaves, LocalLeaves
from pg_partsmith.lifecycle import CreateAhead, DropAfter, KeepNewest, LifecyclePolicy
from pg_partsmith.plan import FindingReason
from pg_partsmith.utils import DETACHED_AT_MARKER
from tests.integration.aio.support import (
    exec_sql,
    exec_sql_autocommit,
    is_attached,
    make_service,
    make_table,
    range_children_of,
    relkind,
    run_maintenance,
    scalar,
    table_comment,
)
from tests.integration.nested_support import (
    METRICS_TABLE_DDL,
    MONTHLY_TABLE_DDL,
    TIMESTAMP_TABLE_DDL,
    monthly_config,
    nested_config,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from sqlalchemy.ext.asyncio import AsyncEngine
    from testcontainers.postgres import PostgresContainer

    from pg_partsmith.entities import TablePartitionConfig

pytestmark = pytest.mark.integration

NOW = "2026-08-26"
RELOPTIONS_SQL = "SELECT reloptions FROM pg_class WHERE oid = to_regclass(:name)"
TABLESPACE_SQL = (
    "SELECT spcname FROM pg_class c JOIN pg_tablespace t ON t.oid = c.reltablespace WHERE c.oid = to_regclass(:name)"
)
OWNER_SQL = "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = to_regclass(:name)"
CAN_SELECT_SQL = "SELECT has_table_privilege(:role, :name, 'SELECT')"
FOREIGN_SERVER_SQL = (
    "SELECT s.srvname FROM pg_foreign_table f JOIN pg_foreign_server s ON s.oid = f.ftserver "
    "WHERE f.ftrelid = to_regclass(:name)"
)
FOREIGN_OPTIONS_SQL = "SELECT ftoptions FROM pg_foreign_table WHERE ftrelid = to_regclass(:name)"


@pytest_asyncio.fixture
async def events(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in make_table(db_engine, MONTHLY_TABLE_DDL, prefix="leaves"):
        yield name


@pytest_asyncio.fixture
async def tenants(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in make_table(db_engine, TIMESTAMP_TABLE_DDL, prefix="tleaves"):
        yield name


@pytest_asyncio.fixture
async def metrics(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in make_table(db_engine, METRICS_TABLE_DDL, prefix="fleaves"):
        yield name


@pytest_asyncio.fixture
async def tablespace(db_engine: AsyncEngine, postgres_container: PostgresContainer) -> AsyncGenerator[str, None]:
    """A real tablespace inside the container, owned by the server's OS user."""
    name = f"ts_{uuid4().hex[:8]}"
    location = f"/var/lib/postgresql/{name}"
    postgres_container.exec(["sh", "-c", f"mkdir -p {location} && chown postgres:postgres {location}"])
    await exec_sql_autocommit(db_engine, f"CREATE TABLESPACE {name} LOCATION '{location}'")
    yield name
    await exec_sql_autocommit(db_engine, f"DROP TABLESPACE IF EXISTS {name}")


@pytest_asyncio.fixture
async def reader(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    name = f"reader_{uuid4().hex[:8]}"
    await exec_sql(db_engine, f"CREATE ROLE {name}")
    yield name
    await exec_sql(db_engine, f"DROP OWNED BY {name}")
    await exec_sql(db_engine, f"DROP ROLE IF EXISTS {name}")


@pytest_asyncio.fixture
async def loopback(db_engine: AsyncEngine, postgres_container: PostgresContainer) -> AsyncGenerator[str, None]:
    """A postgres_fdw server pointing back at this very database."""
    server = f"loop_{uuid4().hex[:8]}"
    await exec_sql(db_engine, "CREATE EXTENSION IF NOT EXISTS postgres_fdw")
    await exec_sql(
        db_engine,
        f"CREATE SERVER {server} FOREIGN DATA WRAPPER postgres_fdw "
        f"OPTIONS (host 'localhost', dbname '{postgres_container.dbname}', port '5432')",
    )
    await exec_sql(
        db_engine,
        f"CREATE USER MAPPING FOR CURRENT_USER SERVER {server} "
        f"OPTIONS (user '{postgres_container.username}', password '{postgres_container.password}')",
    )
    yield server
    await exec_sql(db_engine, f"DROP SERVER IF EXISTS {server} CASCADE")


def _foreign_config(table: str, server: str, *, create_ahead: int = 1, retention: int = 12) -> TablePartitionConfig:
    return monthly_config(table, create_ahead=create_ahead, retention=retention, column="ts").model_copy(
        update={"leaves": ForeignLeaves(server=server, options={"table_name": "{relname}_remote"})}
    )


async def _remote(engine: AsyncEngine, leaf: str, *, rows: int = 0) -> None:
    await exec_sql(engine, f'CREATE TABLE "{leaf}_remote" (ts TIMESTAMPTZ NOT NULL, v DOUBLE PRECISION)')
    if rows:
        await exec_sql(
            engine,
            f"INSERT INTO \"{leaf}_remote\" SELECT '2026-08-15', g FROM generate_series(1, :rows) g",  # noqa: S608
            rows=rows,
        )


# ── local leaves ────────────────────────────────────────────────────────────────


async def test__local_leaves__storage_parameters__reach_the_created_leaf(db_engine: AsyncEngine, events: str) -> None:
    # Arrange
    config = monthly_config(events, create_ahead=1).model_copy(
        update={"leaves": LocalLeaves(storage_parameters={"fillfactor": 70, "autovacuum_enabled": False})}
    )

    # Act
    result = await run_maintenance(db_engine, config, at_time=NOW)

    # Assert
    assert result.created_count == 1
    options = await scalar(db_engine, RELOPTIONS_SQL, name=f"{events}__2026_08")
    assert set(options) == {"fillfactor=70", "autovacuum_enabled=false"}


async def test__local_leaves__tablespace__holds_leaves_and_branches(
    db_engine: AsyncEngine, tablespace: str, tenants: str
) -> None:
    # (the tablespace is requested first so the table is dropped before it)
    # Arrange -- a nested scheme: the week is a branch, the buckets are leaves
    config = nested_config(tenants, modulus=2).model_copy(
        update={"leaves": LocalLeaves(tablespace=tablespace, storage_parameters={"fillfactor": 80})}
    )
    events = tenants

    # Act
    result = await run_maintenance(db_engine, config, at_time=NOW)

    # Assert
    assert result.created_count == 1
    for name in (f"{events}__2026_w35", f"{events}__2026_w35__h0", f"{events}__2026_w35__h1"):
        spc = await scalar(
            db_engine,
            TABLESPACE_SQL,
            name=name,
        )
        assert spc == tablespace, name
    # storage parameters go to the leaves only: PostgreSQL refuses them on the branch
    branch = await scalar(db_engine, RELOPTIONS_SQL, name=f"{events}__2026_w35")
    leaf = await scalar(db_engine, RELOPTIONS_SQL, name=f"{events}__2026_w35__h0")
    assert branch is None
    assert leaf == ["fillfactor=80"]


async def test__local_leaves__inherit_privileges__grants_on_the_parent_reach_the_leaf(
    db_engine: AsyncEngine, events: str, reader: str
) -> None:
    # Arrange -- LIKE copies no grant, so without the flag the leaf is unreadable directly
    await exec_sql(db_engine, f'GRANT SELECT ON "{events}" TO {reader}')
    plain = monthly_config(events, create_ahead=1)
    inheriting = plain.model_copy(update={"leaves": LocalLeaves(inherit_privileges=True)})

    # Act
    await run_maintenance(db_engine, plain, at_time=NOW)
    await run_maintenance(db_engine, inheriting, at_time="2026-09-15")

    # Assert
    august = f"{events}__2026_08"
    september = f"{events}__2026_09"
    assert await scalar(db_engine, CAN_SELECT_SQL, role=reader, name=august) is False
    assert await scalar(db_engine, CAN_SELECT_SQL, role=reader, name=september) is True
    owner = await scalar(db_engine, OWNER_SQL, name=september)
    assert owner == await scalar(db_engine, OWNER_SQL, name=events)


# ── foreign leaves ──────────────────────────────────────────────────────────────


async def test__foreign_leaves__created_attached_and_read_through_the_parent(
    db_engine: AsyncEngine, metrics: str, loopback: str
) -> None:
    # Arrange
    config = _foreign_config(metrics, loopback)
    leaf = f"{metrics}__2026_08"
    await _remote(db_engine, leaf, rows=3)

    # Act
    result = await run_maintenance(db_engine, config, at_time=NOW)

    # Assert
    assert result.created_count == 1
    assert await relkind(db_engine, leaf) == "f"
    assert await is_attached(db_engine, leaf)
    assert await range_children_of(db_engine, metrics) == {leaf: ("2026-08-01 00:00:00+00", "2026-09-01 00:00:00+00")}
    assert await scalar(db_engine, f'SELECT count(*) FROM "{metrics}"') == 3  # noqa: S608
    server = await scalar(
        db_engine,
        FOREIGN_SERVER_SQL,
        name=leaf,
    )
    assert server == loopback
    options = await scalar(db_engine, FOREIGN_OPTIONS_SQL, name=leaf)
    assert options == [f"table_name={leaf}_remote"]


async def test__foreign_leaves__converged_tree__costs_no_ddl(
    db_engine: AsyncEngine, metrics: str, loopback: str
) -> None:
    # Arrange
    config = _foreign_config(metrics, loopback)
    await _remote(db_engine, f"{metrics}__2026_08")
    await run_maintenance(db_engine, config, at_time=NOW)

    # Act
    plan = await make_service(db_engine).plan(config, now=None)

    # Assert
    assert plan.findings == ()


async def test__foreign_leaves__expired_leaf__detached_with_its_marker_and_dropped(
    db_engine: AsyncEngine, metrics: str, loopback: str
) -> None:
    # Arrange -- May and August exist; keep the newest two months
    config = _foreign_config(metrics, loopback, retention=2)
    may = f"{metrics}__2026_05"
    await _remote(db_engine, may, rows=1)
    await _remote(db_engine, f"{metrics}__2026_08")
    created = await make_service(db_engine).ensure_partition(config, Period(year=2026, month=5))
    assert created is not None
    assert await relkind(db_engine, may) == "f"

    # Act
    result = await run_maintenance(db_engine, config, at_time=NOW)

    # Assert -- the foreign table is gone, the remote data is not ours and stays
    assert (result.created_count, result.detached_count, result.dropped_count) == (1, 1, 1)
    assert await relkind(db_engine, may) is None
    assert await scalar(db_engine, f'SELECT count(*) FROM "{may}_remote"') == 1  # noqa: S608


async def test__foreign_leaves__grace_period__foreign_orphan_waits_with_a_comment_then_goes(
    db_engine: AsyncEngine, metrics: str, loopback: str
) -> None:
    # Arrange
    config = _foreign_config(metrics, loopback, retention=2).model_copy(
        update={
            "lifecycle": LifecyclePolicy(
                creation=CreateAhead(count=1), retention=KeepNewest(count=2), drop=DropAfter(grace=timedelta(days=7))
            )
        }
    )
    may = f"{metrics}__2026_05"
    await _remote(db_engine, may)
    await _remote(db_engine, f"{metrics}__2026_08")
    await make_service(db_engine).ensure_partition(config, Period(year=2026, month=5))

    # Act
    detached = await run_maintenance(db_engine, config, at_time=NOW)
    comment = await table_comment(db_engine, may)
    kind_in_grace = await relkind(db_engine, may)
    attached_in_grace = await is_attached(db_engine, may)
    plan_in_grace = await make_service(db_engine).plan(config, now=None)
    dropped = await run_maintenance(db_engine, config, at_time="2026-09-10")

    # Assert
    assert (detached.detached_count, detached.dropped_count) == (1, 0)
    assert comment is not None and DETACHED_AT_MARKER in comment
    assert kind_in_grace == "f"
    assert not attached_in_grace
    assert [f.reason for f in plan_in_grace.findings] == [FindingReason.GRACE_PENDING]
    assert dropped.dropped_count == 1
    assert await relkind(db_engine, may) is None


async def test__foreign_leaves__under_a_local_config__the_same_foreign_partition_is_only_reported(
    db_engine: AsyncEngine, metrics: str, loopback: str
) -> None:
    # Arrange -- created as a foreign leaf, then maintained by a config with local leaves
    foreign = _foreign_config(metrics, loopback, retention=2)
    may = f"{metrics}__2026_05"
    await _remote(db_engine, may)
    await make_service(db_engine).ensure_partition(foreign, Period(year=2026, month=5))
    local = monthly_config(metrics, create_ahead=1, retention=2, column="ts")

    # Act
    result = await run_maintenance(db_engine, local, at_time=NOW)

    # Assert
    assert result.detached_count == 0
    assert result.maintenance_plan is not None
    assert [f.reason for f in result.maintenance_plan.findings] == [FindingReason.FOREIGN_PARTITION]
    assert await is_attached(db_engine, may)


async def test__foreign_leaves__parent_with_a_primary_key__refused_before_any_ddl(
    db_engine: AsyncEngine, events: str, loopback: str
) -> None:
    # Arrange
    config = monthly_config(events, create_ahead=1).model_copy(update={"leaves": ForeignLeaves(server=loopback)})

    # Act / Assert
    with pytest.raises(InvalidPartitionConfigError, match="refuses a foreign table as a partition"):
        await make_service(db_engine).plan(config)
    assert await range_children_of(db_engine, events) == {}


async def test__partition_data__foreign_leaves__drains_default_through_the_wrapper(
    db_engine: AsyncEngine, metrics: str, loopback: str
) -> None:
    # Arrange: history in DEFAULT; the window's leaf will be a foreign table
    hist = f"{metrics}_hist"
    await exec_sql(db_engine, f'CREATE TABLE "{hist}" (LIKE "{metrics}" INCLUDING ALL)')
    await exec_sql(
        db_engine,
        f"INSERT INTO \"{hist}\" SELECT make_timestamptz(2026, 8, 15, 12, 0, 0, 'UTC'), g "  # noqa: S608
        f"FROM generate_series(1, 6) g",
    )
    await exec_sql(db_engine, f'ALTER TABLE "{metrics}" ATTACH PARTITION "{hist}" DEFAULT')
    leaf = f"{metrics}__2026_08"
    await _remote(db_engine, leaf)
    config = _foreign_config(metrics, loopback)

    # Act
    result = await make_service(db_engine).partition_data(config)

    # Assert: the rows went through the wrapper; the mapping is attached; DEFAULT is empty
    assert result.complete
    assert result.rows_moved == 6
    assert await relkind(db_engine, leaf) == "f"
    assert await is_attached(db_engine, leaf)
    assert int(await scalar(db_engine, f'SELECT count(*) FROM "{leaf}_remote"')) == 6  # noqa: S608
    assert int(await scalar(db_engine, f'SELECT count(*) FROM "{hist}"')) == 0  # noqa: S608


# sync-mirror: skip
async def test__partition_data__foreign_target_swapped_mid_fill__stale_and_nothing_moved(
    db_engine: AsyncEngine, metrics: str, loopback: str
) -> None:
    # Arrange: history in DEFAULT; the planned foreign leaf is swapped for another mapping
    hist = f"{metrics}_hist"
    await exec_sql(db_engine, f'CREATE TABLE "{hist}" (LIKE "{metrics}" INCLUDING ALL)')
    await exec_sql(
        db_engine,
        f"INSERT INTO \"{hist}\" SELECT make_timestamptz(2026, 8, 15, 12, 0, 0, 'UTC'), g "  # noqa: S608
        f"FROM generate_series(1, 6) g",
    )
    await exec_sql(db_engine, f'ALTER TABLE "{metrics}" ATTACH PARTITION "{hist}" DEFAULT')
    leaf = f"{metrics}__2026_08"
    await _remote(db_engine, leaf)
    original = PostgresPartitionRepository.reconcile_default_rows
    swapped: list[str] = []

    async def swapping(self: PostgresPartitionRepository, **kwargs: object) -> int:
        if not swapped:
            swapped.append(leaf)
            async with db_engine.begin() as conn:
                await conn.execute(text(f'ALTER FOREIGN TABLE "{leaf}" RENAME TO "{leaf}_hijacked"'))
                await conn.execute(
                    text(
                        f'CREATE FOREIGN TABLE "{leaf}" (ts TIMESTAMPTZ NOT NULL, v DOUBLE PRECISION) '
                        f"SERVER {loopback} OPTIONS (table_name '{leaf}_decoy')"
                    )
                )
        return await original(self, **kwargs)  # type: ignore[arg-type]

    # Act / Assert: a foreign target cannot be locked, so the move's own OID checks carry it
    with (
        patch.object(PostgresPartitionRepository, "reconcile_default_rows", swapping),
        pytest.raises(PlanStaleError),
    ):
        await make_service(db_engine).partition_data(_foreign_config(metrics, loopback))
    assert int(await scalar(db_engine, f'SELECT count(*) FROM "{hist}"')) == 6  # noqa: S608
    assert int(await scalar(db_engine, f'SELECT count(*) FROM "{leaf}_remote"')) == 0  # noqa: S608
