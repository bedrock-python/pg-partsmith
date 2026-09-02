# Installation

## Requirements

| | |
|---|---|
| Python | 3.11 or newer |
| PostgreSQL | 15 or newer — the integration suite runs on 15, 16 and 17 |
| SQLAlchemy | 2.x, with `asyncpg` for the async API or `psycopg2` for the sync one |
| Privileges | ownership of the partitioned table (to create, attach, detach and drop partitions) — no superuser |

## Install

```bash
pip install pg-partsmith
```

Extras:

```bash
pip install "pg-partsmith[cli]"                # the pg-partsmith command
pip install "pg-partsmith[redis-locks]"        # Redis distributed locks
pip install "pg-partsmith[pydantic-settings]"  # configuration from environment variables
```

With `uv`:

```bash
uv add pg-partsmith
```

## Two APIs, one library

Everything ships twice, with identical names and behaviour:

| | Package | Engine | Driver |
|---|---|---|---|
| async | `pg_partsmith.aio` | `sqlalchemy.ext.asyncio.AsyncEngine` | `asyncpg` |
| sync | `pg_partsmith.sync` | `sqlalchemy.Engine` | `psycopg2` |

The configuration, schemes, policies and plans live in the top-level `pg_partsmith`
package and are shared. The documentation shows the async form; drop the `await` and swap
the import for the sync one.

=== "asyncio"

    ```python
    from sqlalchemy.ext.asyncio import create_async_engine

    from pg_partsmith.aio import (
        PartitionLifecycleService,
        PostgresAdvisoryLockManager,
        PostgresMetadataProvider,
        PostgresPartitionRepository,
    )

    engine = create_async_engine("postgresql+asyncpg://app:secret@localhost/app")

    service = PartitionLifecycleService(
        repo=PostgresPartitionRepository(engine),
        metadata=PostgresMetadataProvider(engine),
        locks=PostgresAdvisoryLockManager(engine),
    )
    ```

=== "sync"

    ```python
    from sqlalchemy import create_engine

    from pg_partsmith.sync import (
        PartitionLifecycleService,
        PostgresAdvisoryLockManager,
        PostgresMetadataProvider,
        PostgresPartitionRepository,
    )

    engine = create_engine("postgresql+psycopg2://app:secret@localhost/app")

    service = PartitionLifecycleService(
        repo=PostgresPartitionRepository(engine),
        metadata=PostgresMetadataProvider(engine),
        locks=PostgresAdvisoryLockManager(engine),
    )
    ```

!!! note "Pass an engine, not a session"
    Every statement the library runs commits on its own — a partition is created in one
    transaction and attached in another, and `DETACH … CONCURRENTLY` cannot run inside a
    transaction block at all. The service therefore needs an *engine* it can take
    connections from, never a `Session` you are in the middle of.

## Connection pool

The advisory lock manager holds one dedicated connection for the length of a maintenance
run, and the DDL statements take connections of their own. Give the engine a pool of at
least two connections, or a separate engine for the lock manager. A pool of size one
deadlocks.

## Next

[Tutorial: your first partitioned table →](first-table.md)
