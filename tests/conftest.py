"""Shared fixtures for pg-partsmith tests."""

from __future__ import annotations

import asyncio
import sys  # required for platform check below
from collections.abc import AsyncGenerator, Generator
from pathlib import Path
from uuid import uuid4

import docker
import pytest
import pytest_asyncio
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from testcontainers.postgres import PostgresContainer

from tests.integration.aio.builder import PartitioningScenarioBuilder
from tests.integration.sync.builder import PartitioningScenarioBuilder as SyncPartitioningScenarioBuilder

# Set event loop policy for Windows as early as possible
if sys.platform == "win32":
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
def postgres_container() -> Generator[PostgresContainer, None, None]:
    """Start a PostgreSQL container for the test session."""
    if not _docker_is_available():
        pytest.skip("Docker is required for integration tests (testcontainers)")
    with PostgresContainer("postgres:17-alpine") as postgres:
        yield postgres


def _docker_is_available() -> bool:
    try:
        client = docker.from_env()
        client.ping()
    except Exception:
        return False
    else:
        return True


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
