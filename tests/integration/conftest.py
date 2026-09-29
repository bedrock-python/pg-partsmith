"""Fixtures shared by the aio and sync integration suites."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from pg_partsmith.aio.metadata import PostgresMetadataProvider as AsyncMetadataProvider
from pg_partsmith.sync.metadata import PostgresMetadataProvider as SyncMetadataProvider


@pytest.fixture(autouse=True)
def _plan_on_the_process_clock(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    """Let freezegun keep deciding what "now" is for the plans a test makes.

    Plans are made on the database's clock, and freezegun moves only this
    process's. A test that freezes time means "plan as if it were then", so the
    provider answers with the process clock instead -- except in a test marked
    ``database_clock``, which is about the database's clock itself.
    """
    if request.node.get_closest_marker("database_clock") is not None:
        return

    async def async_process_clock(_: AsyncMetadataProvider) -> datetime:
        return datetime.now(UTC)

    def sync_process_clock(_: SyncMetadataProvider) -> datetime:
        return datetime.now(UTC)

    monkeypatch.setattr(AsyncMetadataProvider, "current_time", async_process_clock)
    monkeypatch.setattr(SyncMetadataProvider, "current_time", sync_process_clock)
