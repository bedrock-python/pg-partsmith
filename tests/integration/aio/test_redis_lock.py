"""Redis distributed lock manager against a real Redis (async)."""

from __future__ import annotations

import time
from asyncio import sleep
from contextlib import AsyncExitStack
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest
import pytest_asyncio
from redis.asyncio import Redis

from pg_partsmith.aio.lock.redis import RedisDistributedLockManager
from pg_partsmith.exceptions import LockAcquisitionError

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable

pytestmark = pytest.mark.integration

# The smallest TTL the manager accepts; it renews the key every ``ttl // 3`` seconds while the lock is held.
TTL_SECONDS = 3


@pytest_asyncio.fixture
async def redis_client(redis_url: str) -> AsyncGenerator[Redis, None]:
    async with Redis.from_url(redis_url) as client:
        yield client


@pytest.fixture
def prefix() -> str:
    """A key prefix of this test's own, so a key another test left behind cannot contend with it."""
    return f"partsmith-test:{uuid4().hex[:8]}"


# ── Acquire and release ──────────────────────────────────────────────────────────


async def test__redis_lock__acquire_and_release__lock_is_held_then_freed(redis_client: Redis, prefix: str) -> None:
    # Arrange
    manager = RedisDistributedLockManager(redis_client, prefix=prefix)
    assert not await manager.is_locked("events")

    # Act / Assert
    async with manager.acquire_lock("events"):
        assert await manager.is_locked("events")
    assert not await manager.is_locked("events")

    # The release deleted the key rather than leaving it to its TTL
    assert await redis_client.keys(f"{prefix}*") == []


# ── Contention ───────────────────────────────────────────────────────────────────


async def test__redis_lock__two_managers_same_table__second_raises_lock_acquisition_error_at_once(
    redis_client: Redis, prefix: str
) -> None:
    # Arrange
    manager1 = RedisDistributedLockManager(redis_client, prefix=prefix)
    manager2 = RedisDistributedLockManager(redis_client, prefix=prefix)

    async def contend() -> None:
        async with manager2.acquire_lock("events"):
            pytest.fail("the second manager must not get the lock")

    # Act / Assert — the loser fails fast instead of queueing behind the holder
    async with manager1.acquire_lock("events"):
        started = time.monotonic()
        with pytest.raises(LockAcquisitionError) as exc_info:
            await contend()
        assert time.monotonic() - started < 1.0
        assert exc_info.value.table_name == "events"
        # and its failed attempt has not disturbed the holder's lock
        assert await manager1.is_locked("events")

    # Once released, the other manager gets it — and the first one sees it held
    async with manager2.acquire_lock("events"):
        assert await manager1.is_locked("events")


async def test__redis_lock__different_tables__do_not_contend(redis_client: Redis, prefix: str) -> None:
    # Arrange
    manager1 = RedisDistributedLockManager(redis_client, prefix=prefix)
    manager2 = RedisDistributedLockManager(redis_client, prefix=prefix)

    # Act / Assert
    async with manager1.acquire_lock("events"), manager2.acquire_lock("orders"):
        assert await manager1.is_locked("events")
        assert await manager1.is_locked("orders")
    assert not await manager1.is_locked("events")
    assert not await manager1.is_locked("orders")


# ── TTL ──────────────────────────────────────────────────────────────────────────


class _RedisGoingAway:
    """A real client until ``offline`` is set; from then on every Lua script fails as if Redis were down."""

    def __init__(self, client: Redis) -> None:
        self._client = client
        self.offline = False

    def register_script(self, script: str) -> Callable[..., Any]:
        registered = self._client.register_script(script)

        async def run(keys: list[str], args: list[str]) -> Any:
            if self.offline:
                raise ConnectionError("Redis is unreachable")
            return await registered(keys=keys, args=args)

        return run

    async def set(self, name: str, value: str | bytes, ex: int | None = None, nx: bool = False) -> Any:
        return await self._client.set(name, value, ex=ex, nx=nx)

    async def exists(self, *names: str) -> int:
        return await self._client.exists(*names)


async def test__redis_lock__held_longer_than_its_ttl__is_renewed_rather_than_expiring(
    redis_client: Redis, prefix: str
) -> None:
    # Arrange
    manager = RedisDistributedLockManager(redis_client, prefix=prefix, ttl_seconds=TTL_SECONDS)
    other = RedisDistributedLockManager(redis_client, prefix=prefix, ttl_seconds=TTL_SECONDS)

    async def contend() -> None:
        async with other.acquire_lock("events"):
            pytest.fail("the lock must not have expired under its holder")

    # Act — hold the lock past its TTL
    async with manager.acquire_lock("events"):
        await sleep(TTL_SECONDS + 1)

        # Assert — still held, still exclusive
        assert await manager.is_locked("events")
        with pytest.raises(LockAcquisitionError):
            await contend()
    assert not await manager.is_locked("events")


async def test__redis_lock__release_fails__key_expires_after_ttl_and_can_be_taken_again(
    redis_client: Redis, prefix: str
) -> None:
    # Arrange — Redis goes away right before the release, so the key outlives its holder
    going_away = _RedisGoingAway(redis_client)
    manager = RedisDistributedLockManager(going_away, prefix=prefix, ttl_seconds=TTL_SECONDS)
    async with manager.acquire_lock("events"):
        going_away.offline = True
    assert await manager.is_locked("events")

    # Act — wait for the TTL to run out
    deadline = time.monotonic() + TTL_SECONDS + 3
    while await manager.is_locked("events") and time.monotonic() < deadline:
        await sleep(0.1)

    # Assert — the stale key is gone and does not stand in the next holder's way
    assert not await manager.is_locked("events")
    other = RedisDistributedLockManager(redis_client, prefix=prefix, ttl_seconds=TTL_SECONDS)
    async with other.acquire_lock("events"):
        assert await other.is_locked("events")


# ── Ownership ────────────────────────────────────────────────────────────────────


async def test__redis_lock__lock_lost_and_taken_by_another_holder__stale_release_does_not_steal_it(
    redis_client: Redis, prefix: str
) -> None:
    # Arrange — a TTL long enough that the first holder's watchdog does not notice the loss
    # (and, in the aio manager, cancel this very task) before the stale release below
    first = RedisDistributedLockManager(redis_client, prefix=prefix, ttl_seconds=30)
    second = RedisDistributedLockManager(redis_client, prefix=prefix, ttl_seconds=30)
    async with AsyncExitStack() as stale_holder:
        await stale_holder.enter_async_context(first.acquire_lock("events"))
        # the first holder's key vanishes under it, as a flush or an expiry would make it
        (key,) = await redis_client.keys(f"{prefix}*")
        await redis_client.delete(key)

        # ...and another holder takes the free lock
        async with second.acquire_lock("events"):
            # Act — the stale holder releases what it believes is still its lock
            await stale_holder.aclose()

            # Assert — the current holder's lock is untouched
            assert await second.is_locked("events")
        assert not await second.is_locked("events")
