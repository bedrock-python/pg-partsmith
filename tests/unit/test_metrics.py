"""``--output metrics``: the same run, as a node_exporter textfile."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pg_partsmith.cli.metrics import _labels, render_metrics


def _samples(text: str) -> dict[str, float]:
    """Every sample line as ``name{labels} -> value``, comments dropped."""
    samples: dict[str, float] = {}
    for line in text.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        series, _, value = line.rpartition(" ")
        samples[series] = float(value)
    return samples


def _envelope(command: str, tables: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "version": 1,
        "command": command,
        "generated_at": "2026-09-01T12:00:00+00:00",
        "tables": tables,
    }


def test__render_metrics__every_family__is_documented_and_typed() -> None:
    # A textfile collector serves these to Prometheus verbatim, so the HELP and
    # TYPE lines are part of the output, not a nicety.
    text = render_metrics(_envelope("validate", [{"table": "public.events", "ok": True}]))

    # Assert
    assert "# HELP pg_partsmith_config_valid" in text
    assert "# TYPE pg_partsmith_config_valid gauge" in text
    assert text.endswith("\n")


def test__render_metrics__the_run_timestamp__is_the_one_in_the_envelope() -> None:
    # Arrange / Act: a stale textfile has to be visible as stale
    text = render_metrics(_envelope("plan", []))

    # Assert
    expected = datetime.fromisoformat("2026-09-01T12:00:00+00:00").timestamp()
    assert _samples(text)['pg_partsmith_run_timestamp_seconds{command="plan"}'] == expected


def test__plan__pending_operations__are_counted_by_kind() -> None:
    # Arrange
    payload = _envelope(
        "plan",
        [
            {
                "table": "public.events",
                "plan": {
                    "operations": [
                        {"kind": "create", "children": [{"kind": "create", "children": []}]},
                        {"kind": "create", "children": []},
                        {"kind": "drop"},
                    ],
                    "findings": [{"severity": "warning"}, {"severity": "info"}, {"severity": "warning"}],
                },
            }
        ],
    )

    # Act
    samples = _samples(render_metrics(payload))

    # Assert
    assert samples['pg_partsmith_pending_operations{table="public.events",kind="create"}'] == 2
    assert samples['pg_partsmith_pending_operations{table="public.events",kind="drop"}'] == 1
    assert samples['pg_partsmith_pending_operations{table="public.events",kind="detach"}'] == 0
    # Three relations: two top-level creations and one nested child
    assert samples['pg_partsmith_pending_relations{table="public.events"}'] == 3
    assert samples['pg_partsmith_findings{table="public.events",severity="warning"}'] == 2
    assert samples['pg_partsmith_findings{table="public.events",severity="info"}'] == 1


def test__plan__a_converged_table__reports_zeroes_rather_than_nothing() -> None:
    # A missing series and a zero are different alerts; converged is a zero.
    payload = _envelope("plan", [{"table": "public.events", "plan": {"operations": [], "findings": []}}])

    # Act
    samples = _samples(render_metrics(payload))

    # Assert
    assert samples['pg_partsmith_pending_operations{table="public.events",kind="create"}'] == 0
    assert samples['pg_partsmith_pending_relations{table="public.events"}'] == 0


def test__inspect__counts_the_tree_and_ages_the_oldest_orphan() -> None:
    # Arrange
    payload = _envelope(
        "inspect",
        [
            {
                "table": "public.events",
                "tree": {
                    "root": {
                        "children": [
                            {"children": [{"children": []}, {"children": []}]},
                            {"children": []},
                        ]
                    },
                    "orphans": [
                        {"name": "public.events__2025_01", "detached_at": "2026-09-01T11:00:00+00:00"},
                        {"name": "public.events__2025_02", "detached_at": None},
                    ],
                },
            }
        ],
    )

    # Act
    samples = _samples(render_metrics(payload))

    # Assert: four relations below the root, two orphans, one of them datable
    assert samples['pg_partsmith_partitions{table="public.events"}'] == 4
    assert samples['pg_partsmith_detached_partitions{table="public.events"}'] == 2
    assert samples['pg_partsmith_partitioned{table="public.events"}'] == 1
    assert samples['pg_partsmith_oldest_detached_age_seconds{table="public.events"}'] > 0


def test__inspect__a_table_that_is_not_partitioned__says_so_in_one_series() -> None:
    # Arrange / Act
    samples = _samples(render_metrics(_envelope("inspect", [{"table": "public.events", "tree": None}])))

    # Assert
    assert samples['pg_partsmith_partitioned{table="public.events"}'] == 0
    assert 'pg_partsmith_partitions{table="public.events"}' not in samples


def test__validate__a_table_the_catalog_disagrees_with__is_a_zero() -> None:
    # Arrange / Act
    payload = _envelope(
        "validate",
        [{"table": "public.events", "ok": True}, {"table": "public.audit", "ok": False, "error": "not partitioned"}],
    )
    samples = _samples(render_metrics(payload))

    # Assert
    assert samples['pg_partsmith_config_valid{table="public.events"}'] == 1
    assert samples['pg_partsmith_config_valid{table="public.audit"}'] == 0


def test__apply__reports_what_it_did_and_what_it_could_not() -> None:
    # Arrange
    payload = _envelope(
        "apply",
        [
            {
                "table": "public.events",
                "result": {
                    "created_count": 2,
                    "repaired_count": 0,
                    "attached_count": 1,
                    "detached_count": 1,
                    "dropped_count": 1,
                    "issues": [{"step": "drop", "error": "refused"}],
                },
            }
        ],
    )

    # Act
    samples = _samples(render_metrics(payload))

    # Assert
    assert samples['pg_partsmith_applied_operations{table="public.events",operation="created"}'] == 2
    assert samples['pg_partsmith_applied_operations{table="public.events",operation="dropped"}'] == 1
    assert samples['pg_partsmith_issues{table="public.events"}'] == 1


def test__render_metrics__a_table_name_with_a_quote_in_it__is_escaped() -> None:
    # PostgreSQL allows it, and an unescaped quote would break the whole file.
    payload = _envelope("validate", [{"table": 'public."odd"', "ok": True}])

    # Act
    text = render_metrics(payload)

    # Assert
    assert 'pg_partsmith_config_valid{table="public.\\"odd\\""} 1' in text


def test__render_metrics__a_command_it_has_no_numbers_for__still_says_when_it_ran() -> None:
    # Arrange / Act
    text = render_metrics(_envelope("something-else", []))

    # Assert
    assert "pg_partsmith_run_timestamp_seconds" in text
    assert len(_samples(text)) == 1


def test__render_metrics__an_unreadable_run_instant__falls_back_to_now() -> None:
    # A textfile with no timestamp would read as never-stale; now is the honest fallback.
    before = datetime.now(UTC).timestamp()
    text = render_metrics({"version": 1, "command": "plan", "generated_at": "not a date", "tables": []})

    # Assert
    assert _samples(text)['pg_partsmith_run_timestamp_seconds{command="plan"}'] >= before


def test__inspect__orphans_with_unreadable_or_naive_instants__age_what_can_be_aged() -> None:
    # Arrange
    payload = _envelope(
        "inspect",
        [
            {
                "table": "public.events",
                "tree": {
                    "root": {"children": []},
                    "orphans": [
                        {"name": "a", "detached_at": "not a date"},
                        {"name": "b", "detached_at": "2026-01-01T00:00:00"},
                    ],
                },
            }
        ],
    )

    # Act
    samples = _samples(render_metrics(payload))

    # Assert: the naive one is read as UTC; the unreadable one is skipped
    assert samples['pg_partsmith_oldest_detached_age_seconds{table="public.events"}'] > 0


def test__labels__no_labels__is_no_braces() -> None:
    # Arrange / Act / Assert
    assert _labels({}) == ""
