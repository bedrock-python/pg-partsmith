# Monitor and alert

A maintenance run reports three things: whether it ran, what it did, and what it could
not or would not do. This guide shows where each lives and what to page on.

## The result

```python
result = await maintainer.run_maintenance_safe(config)
```

| Field | Meaning | Alert? |
|---|---|---|
| `result.success` / `result.error` | the run failed as a whole: configuration mismatch, lock unavailable, connection lost | on `error`, except `LockAcquisitionError`, which means another replica ran |
| `result.created_count`, `attached_count`, `detached_count`, `dropped_count`, `repaired_count` | what changed | export as metrics |
| `result.duration_ms` | how long it took | export; a jump usually means a slow attach against a full DEFAULT partition or a lock wait |
| `result.issues` | per-partition problems in a run that otherwise went through | yes — and expect repeats until someone acts |
| `result.plan` | the plan that was executed, findings included | log at debug |

## Issues

An issue has a `step` — `create`, `reconcile`, `attach`, `detach`, `drop`, `move` —
a `partition_name` and an `error` string that starts with the exception type:

```text
reconcile: public.events: PartitionTopologyError: public.events needs a partition for 2028_03 but public.events_oddweeks already covers part of it with bounds the scheme did not produce; creating it would fail, and detaching the other is not this library's decision.
detach: public.ci_builds__2026_06: PartitionReferencedError: Partition public.ci_builds__2026_06 is still referenced by rows of another table: …
```

Two sources feed it: what the planner *refused* and rated `WARNING`, and what
PostgreSQL refused at execution time. Both repeat every tick until the cause is gone —
that is deliberate; a branch rejecting writes must not go quiet. Deduplicate on
`(table, partition_name, step)` before alerting.

With `continue_on_error=True`, ordinary step failures land here too instead of aborting
the run.

## Findings

`result.plan.findings` is the full account, including the informational states that are
kept out of `issues`: an orphan in its grace period, a legacy leaf, a partition someone
else owns, a hash set at an older modulus. Log them at debug level; they explain a plan
that looks incomplete. [Findings and issues](../reference/findings.md) lists every reason
with its cause and remedy.

## A reporting function

```python
import logging

from pg_partsmith import MaintenanceResult, TablePartitionConfig

log = logging.getLogger("partitions")


def report(config: TablePartitionConfig, result: MaintenanceResult) -> None:
    table = config.qualified_name
    if not result.success:
        level = logging.INFO if "LockAcquisitionError" in (result.error or "") else logging.ERROR
        log.log(level, "partition maintenance did not run", extra={"table": table, "error": result.error})
        return
    log.info(
        "partition maintenance done",
        extra={
            "table": table,
            "created": result.created_count,
            "attached": result.attached_count,
            "detached": result.detached_count,
            "dropped": result.dropped_count,
            "duration_ms": result.duration_ms,
        },
    )
    for issue in result.issues:
        log.warning(
            "partition needs attention",
            extra={"table": table, "step": issue.step.value, "partition": issue.partition_name, "error": issue.error},
        )
    if result.plan is not None:
        for finding in result.plan.findings:
            log.debug(finding.detail, extra={"table": table, "reason": finding.reason.value})
```

## Metrics

The counters map straight onto gauges or counters per table; `duration_ms` onto a
histogram. Two derived gauges are worth having:

- **partitions ahead** — how many windows exist beyond the current one. Compute it from
  `service.inspect(config)` or from the catalog; alert when it drops below 1.
- **issues open** — `len(result.issues)`; alert when non-zero for more than one tick.

## Dry runs

`plan()` takes no lock and issues no DDL, so it doubles as a health check:

```python
plan = await service.plan(config)
if plan.actionable_findings:
    ...   # the warnings that would become issues on a real run
```

Run it in CI against a migrated schema, or as a read-only probe from a dashboard:
`plan.model_dump(mode="json")` is the wire format.

## Library logging

pg-partsmith logs through the standard `logging` module under `pg_partsmith.*`, with
structured `extra` fields (`table_name`, `partition_name`, `sqlstate`, …). `WARNING` is
used for fallbacks and refusals (a concurrent detach falling back to the blocking form, a
drop retried after lock contention, a detach refused by a foreign key); `INFO` for what
was done; `DEBUG` for the benign races. Nothing is printed.
