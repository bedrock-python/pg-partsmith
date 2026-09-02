"""Shared fixtures for pg-partsmith tests."""

from __future__ import annotations

import asyncio
import contextlib
import os
import sys  # required for platform check below
import warnings
from collections.abc import AsyncGenerator, Generator
from pathlib import Path
from urllib.parse import unquote, urlparse
from uuid import uuid4

import docker
import freezegun
import pytest
import pytest_asyncio
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session, sessionmaker
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer

from tests.integration.aio.builder import PartitioningScenarioBuilder
from tests.integration.sync.builder import PartitioningScenarioBuilder as SyncPartitioningScenarioBuilder

# freeze_time walks every loaded module's attributes to find the datetimes to
# patch. testcontainers' config module answers a module-level __getattr__ for
# its deprecated names by asking the Docker daemon for its socket path -- so a
# frozen clock in a unit test opened a docker client, and on a machine without
# Docker the SDK leaves the failed socket unclosed, which the garbage collector
# then reports in whatever test happens to be running. Neither module holds a
# clock worth freezing.
freezegun.configure(extend_ignore_list=["testcontainers", "docker"])

# Set event loop policy for Windows as early as possible. Python 3.14
# deprecates policies for removal in 3.16; until then the selector loop is
# what the drivers need here, and the notice is not this suite's to fail on.
if sys.platform == "win32":
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


@pytest.fixture(scope="session", autouse=True)
def setup_event_loop_policy() -> None:
    """Ensure WindowsSelectorEventLoopPolicy is used on Windows."""
    # This is often needed on Windows to avoid issues with some async libraries
    pass


def pytest_configure(config: pytest.Config) -> None:
    """Tweak settings for marker-targeted runs.

    When running *only* integration tests (typically `pytest -m integration`),
    relax the coverage threshold. Coverage is already enforced by the unit test
    run; integration tests are primarily about behavioural correctness with a
    real database.
    """
    markexpr = getattr(config.option, "markexpr", "") or ""
    if "integration" in markexpr and "unit" not in markexpr:
        cov_plugin = config.pluginmanager.getplugin("_cov")
        if cov_plugin is not None and hasattr(cov_plugin, "options") and hasattr(cov_plugin.options, "cov_fail_under"):
            cov_plugin.options.cov_fail_under = 0


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Auto-mark tests based on their directory (unit vs integration).

    This makes `pytest -m unit` / `pytest -m integration` work without requiring
    explicit markers on every test function.
    """
    has_unit = False
    has_integration = False

    for item in items:
        path = Path(str(item.fspath)).as_posix()
        if "/tests/unit/" in path:
            item.add_marker("unit")
            has_unit = True
        elif "/tests/integration/" in path:
            item.add_marker("integration")
            has_integration = True

    if has_integration and not has_unit:
        cov_plugin = config.pluginmanager.getplugin("_cov")
        if cov_plugin is not None and hasattr(cov_plugin, "options") and hasattr(cov_plugin.options, "cov_fail_under"):
            cov_plugin.options.cov_fail_under = 0


@pytest.fixture(scope="session")
def postgres_container() -> Generator[PostgresContainer | ExternalPostgres, None, None]:
    """The session's PostgreSQL: a container, or the server ``PG_PARTSMITH_TEST_DSN`` names.

    A runner without Docker -- Windows, macOS -- brings a server of its own and
    points here at it; the few tests that reach into the container itself skip
    there. ``PG_PARTSMITH_TEST_PG_TZ`` starts the container with that zone as the
    server's default, so the suite also runs against a clock far from UTC.
    """
    dsn = os.environ.get("PG_PARTSMITH_TEST_DSN")
    if dsn:
        yield ExternalPostgres(dsn)
        return
    if not _docker_is_available():
        pytest.skip("Docker is required for integration tests (testcontainers)")
    image = os.environ.get("PG_PARTSMITH_TEST_PG_IMAGE") or "postgres:17-alpine"
    container = PostgresContainer(image)
    zone = os.environ.get("PG_PARTSMITH_TEST_PG_TZ")
    if zone:
        container = container.with_env("TZ", zone).with_env("PGTZ", zone)
    with container as postgres:
        yield postgres


class ExternalPostgres:
    """A server the runner brought, wearing the parts of ``PostgresContainer`` the tests use."""

    def __init__(self, dsn: str) -> None:
        parts = urlparse(dsn)
        self.username = unquote(parts.username or "postgres")
        self.password = unquote(parts.password or "")
        self.dbname = parts.path.lstrip("/") or "postgres"
        self.port = parts.port or 5432
        self._dsn = dsn

    def get_connection_url(self) -> str:
        return self._dsn

    def exec(self, command: object) -> None:
        pytest.skip(f"cannot run {command!r}: this server is not a container")


def _docker_is_available() -> bool:
    try:
        client = docker.from_env()
    except Exception:
        return False
    # Closed, not dropped: the answer keeps a connection in its pool otherwise.
    with contextlib.closing(client):
        try:
            client.ping()
        except Exception:
            return False
    return True


@pytest.fixture(scope="session")
def redis_container() -> Generator[RedisContainer, None, None]:
    """Start a Redis container for the test session.

    A runner that brought its own PostgreSQL (``PG_PARTSMITH_TEST_DSN``) has no
    Linux containers to offer whatever ``docker`` answers there -- a Windows
    runner has a daemon that cannot run one -- so Redis is the server
    ``PG_PARTSMITH_TEST_REDIS_URL`` names, or the lock tests skip.
    """
    if os.environ.get("PG_PARTSMITH_TEST_DSN"):
        pytest.skip("no container runtime here; PG_PARTSMITH_TEST_REDIS_URL names a Redis, or the lock tests skip")
    if not _docker_is_available():
        pytest.skip("Docker is required for integration tests (testcontainers)")
    with RedisContainer("redis:7-alpine") as container:
        yield container


@pytest.fixture(scope="session")
def redis_url(request: pytest.FixtureRequest) -> str:
    """``redis://host:port/0``: the server named in the environment, or the session's container."""
    named = os.environ.get("PG_PARTSMITH_TEST_REDIS_URL")
    if named:
        return named
    container: RedisContainer = request.getfixturevalue("redis_container")
    host = container.get_container_host_ip()
    port = container.get_exposed_port(container.port)
    return f"redis://{host}:{port}/0"


@pytest_asyncio.fixture
async def db_engine(postgres_container: PostgresContainer) -> AsyncGenerator[AsyncEngine, None]:
    """Create async database engine for testing."""
    url = postgres_container.get_connection_url()
    if "://" in url:
        _, rest = url.split("://", 1)
        url = f"postgresql+asyncpg://{rest}"

    engine = create_async_engine(url, echo=False, pool_pre_ping=True)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def db_session(db_engine: AsyncEngine) -> AsyncGenerator[AsyncSession, None]:
    """Create async database session for each test."""
    async_session_maker = async_sessionmaker(db_engine, expire_on_commit=False)
    async with async_session_maker() as session:
        yield session


@pytest.fixture
def sync_db_session(sync_db_engine: Engine) -> Generator[Session, None, None]:
    """Create sync database session for each test."""
    with sessionmaker(sync_db_engine, expire_on_commit=False)() as session:
        yield session


@pytest.fixture
def sync_db_engine(postgres_container: PostgresContainer) -> Generator[Engine, None, None]:
    """Create sync database engine for testing."""
    url = postgres_container.get_connection_url()
    if "://" in url:
        _, rest = url.split("://", 1)
        url = f"postgresql+psycopg2://{rest}"

    engine = create_engine(url, echo=False, pool_pre_ping=True)
    yield engine
    engine.dispose()


@pytest.fixture
def sync_partition_builder(sync_db_engine: Engine) -> Generator[SyncPartitioningScenarioBuilder, None, None]:
    """Fixture for the sync PartitioningScenarioBuilder."""
    table_name = f"scenario_{uuid4().hex[:8]}"

    # create base partitioned table
    with sync_db_engine.begin() as conn:
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id BIGSERIAL,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    data TEXT,
                    PRIMARY KEY (id, created_at)
                ) PARTITION BY RANGE (created_at)
                """
            )
        )

    yield SyncPartitioningScenarioBuilder(sync_db_engine, table_name)

    with sync_db_engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))


@pytest_asyncio.fixture
async def partition_builder(
    db_engine: AsyncEngine, db_session: AsyncSession
) -> AsyncGenerator[PartitioningScenarioBuilder, None]:
    """Fixture for PartitioningScenarioBuilder."""
    table_name = f"scenario_{uuid4().hex[:8]}"

    # create base partitioned table
    await db_session.execute(
        text(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id BIGSERIAL,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                data TEXT,
                PRIMARY KEY (id, created_at)
            ) PARTITION BY RANGE (created_at)
            """
        )
    )
    await db_session.commit()

    yield PartitioningScenarioBuilder(db_engine, table_name)

    await db_session.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
    await db_session.commit()
