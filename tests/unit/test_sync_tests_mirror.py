"""The generator of the sync integration suite: what it leaves out and what it must keep.

CI does not compare the sync suite with the aio one, so a test the generator
drops by mistake just stops running, with nothing to say so.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from scripts import sync_tests_mirror
from scripts.sync_tests_mirror import transform

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.unit


def test__transform__a_skipped_class__keeps_the_heading_and_the_test_after_it() -> None:
    # Arrange
    source = """\
# sync-mirror: skip
class _Writer:
    async def write(self) -> None:
        await self.conn.execute(self.sql)


# ── partition_data ──────────────────────────────────────────────────────────────


async def test__drains(db_engine: AsyncEngine) -> None:
    assert await count(db_engine) == 0
"""

    expected = """\
# ── partition_data ──────────────────────────────────────────────────────────────


def test__drains(sync_db_engine: Engine) -> None:
    assert count(sync_db_engine) == 0
"""

    # Act
    mirrored = transform(source)

    # Assert
    assert mirrored == expected


def test__transform__a_skipped_decorated_test__keeps_the_test_after_it() -> None:
    # Arrange
    source = """\
# sync-mirror: skip
@pytest.mark.parametrize("n", [1, 2])
async def test__races(db_engine: AsyncEngine, n: int) -> None:
    await asyncio.gather(work(db_engine), work(db_engine))


async def test__plain(db_engine: AsyncEngine) -> None:
    assert await count(db_engine) == 0
"""

    expected = """\
def test__plain(sync_db_engine: Engine) -> None:
    assert count(sync_db_engine) == 0
"""

    # Act
    mirrored = transform(source)

    # Assert
    assert mirrored == expected


def test__transform__a_section_whose_tests_are_all_skipped__loses_its_heading_and_comment() -> None:
    # Arrange
    source = """\
# ── under a live writer ─────────────────────────────────────────────────────────
#
# Why these tests need two coroutines at once.


# sync-mirror: skip
async def test__races(db_engine: AsyncEngine) -> None:
    await asyncio.gather(work(db_engine), work(db_engine))


# ── retry ───────────────────────────────────────────────────────────────────────


async def test__retries(db_engine: AsyncEngine) -> None:
    assert await count(db_engine) == 0
"""

    expected = """\
# ── retry ───────────────────────────────────────────────────────────────────────


def test__retries(sync_db_engine: Engine) -> None:
    assert count(sync_db_engine) == 0
"""

    # Act
    mirrored = transform(source)

    # Assert
    assert mirrored == expected


def test__main__the_cli_module__gets_no_sync_twin(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    aio, sync = tmp_path / "aio", tmp_path / "sync"
    aio.mkdir()
    (aio / "test_cli.py").write_text("async def test__cli() -> None:\n    pass\n", encoding="utf-8")
    (aio / "test_plan.py").write_text("async def test__plan() -> None:\n    pass\n", encoding="utf-8")
    monkeypatch.setattr(sync_tests_mirror, "ROOT", tmp_path)
    monkeypatch.setattr(sync_tests_mirror, "AIO", aio)
    monkeypatch.setattr(sync_tests_mirror, "SYNC", sync)

    # Act
    sync_tests_mirror.main()

    # Assert
    assert sorted(path.name for path in sync.iterdir()) == ["test_plan.py"]
