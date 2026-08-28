"""Leaf backends: ``LocalLeaves`` / ``ForeignLeaves`` models, and what the planner owns under each."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from pg_partsmith.entities import PartitionGranularity, TablePartitionConfig
from pg_partsmith.leaves import ForeignLeaves, LocalLeaves
from pg_partsmith.lifecycle import CreateAhead, DropAfter, DropNever, KeepNewest, LifecyclePolicy
from pg_partsmith.plan import FindingReason, MaintenancePlan, Reason
from pg_partsmith.planner import PlanningContext, plan_maintenance
from pg_partsmith.scheme import HashPartitioning
from pg_partsmith.topology import ActualTree, DetachedPartition, PartitionNode, PartitionType, RangeBounds, RelationKind

NOW = datetime(2026, 8, 28, tzinfo=UTC)
ROOT = "public.metrics"


# ── builders ────────────────────────────────────────────────────────────────────


def _config(leaves: LocalLeaves | ForeignLeaves | None = None, **overrides: Any) -> TablePartitionConfig:
    fields: dict[str, Any] = {
        "schema": "public",
        "table_name": "metrics",
        "partition_column": "ts",
        "granularity": PartitionGranularity.MONTH,
        "lifecycle": LifecyclePolicy(creation=CreateAhead(count=1), retention=KeepNewest(count=2), drop=DropAfter()),
    }
    if leaves is not None:
        fields["leaves"] = leaves
    fields.update(overrides)
    return TablePartitionConfig(**fields)


def _month(month: int, *, relkind: RelationKind = RelationKind.TABLE, oid: int | None = None) -> PartitionNode:
    return PartitionNode(
        name=f"{ROOT}__2026_{month:02d}",
        parent_name=ROOT,
        level=1,
        oid=oid or month,
        relkind=relkind,
        bounds=RangeBounds(from_value=f"2026-{month:02d}-01", to_value=f"2026-{month + 1:02d}-01"),
    )


def _root(*children: PartitionNode) -> PartitionNode:
    return PartitionNode(name=ROOT, partition_type=PartitionType.RANGE, partition_columns=("ts",), children=children)


def _orphan(month: int, *, relkind: RelationKind = RelationKind.FOREIGN) -> DetachedPartition:
    return DetachedPartition(
        name=f"{ROOT}__2026_{month:02d}",
        oid=100 + month,
        relkind=relkind,
        parent_name=ROOT,
        detached_at=NOW - timedelta(days=30),
    )


def _plan(
    config: TablePartitionConfig, root: PartitionNode, *, orphans: tuple[DetachedPartition, ...] = ()
) -> MaintenancePlan:
    return plan_maintenance(config, ActualTree(root=root, orphans=orphans), PlanningContext(now=NOW))


def _targets(operations: tuple[Any, ...]) -> list[str]:
    return [op.target for op in operations]


def _reasons(plan: MaintenancePlan) -> list[FindingReason]:
    return [finding.reason for finding in plan.findings]


# ── LocalLeaves ─────────────────────────────────────────────────────────────────


def test__local_leaves__default__is_plain() -> None:
    leaves = LocalLeaves()

    assert leaves.is_plain
    assert leaves.kind == "local"
    assert leaves.rendered_storage_parameters() == {}


def test__local_leaves__settings__are_kept_and_rendered_as_literals() -> None:
    leaves = LocalLeaves(
        tablespace="fast_ssd",
        storage_parameters={"fillfactor": 70, "autovacuum_enabled": False, "toast.autovacuum_enabled": True, "x": "y"},
        inherit_privileges=True,
    )

    assert not leaves.is_plain
    assert leaves.rendered_storage_parameters() == {
        "fillfactor": "70",
        "autovacuum_enabled": "false",
        "toast.autovacuum_enabled": "true",
        "x": "y",
    }


@pytest.mark.parametrize("name", ["fill factor", "Fillfactor", "toast.", "1x", "a;b"])
def test__local_leaves__bad_storage_parameter_name__refused(name: str) -> None:
    with pytest.raises(ValueError, match="storage parameter"):
        LocalLeaves(storage_parameters={name: 1})


def test__local_leaves__tablespace__is_an_identifier() -> None:
    assert LocalLeaves(tablespace="Fast_SSD").tablespace == "fast_ssd"
    with pytest.raises(ValueError, match="identifier"):
        LocalLeaves(tablespace="fast ssd")


def test__local_leaves__survives_json() -> None:
    leaves = LocalLeaves(tablespace="t", storage_parameters={"fillfactor": 70}, inherit_privileges=True)

    assert LocalLeaves.model_validate(leaves.model_dump(mode="json")) == leaves


# ── ForeignLeaves ───────────────────────────────────────────────────────────────


def test__foreign_leaves__renders_option_templates_for_one_leaf() -> None:
    leaves = ForeignLeaves(server="archive", options={"table_name": "{relname}", "schema_name": "{schema}_{root}"})

    rendered = leaves.render_options(relname="metrics__2026_01", schema="public", parent="metrics", root="metrics")

    assert rendered == {"table_name": "metrics__2026_01", "schema_name": "public_metrics"}
    assert leaves.kind == "foreign"


def test__foreign_leaves__literal_options__pass_through() -> None:
    leaves = ForeignLeaves(server="clickhouse", options={"engine": "MergeTree", "batch_size": "1000"})

    assert leaves.render_options(relname="x", schema="", parent="p", root="r") == {
        "engine": "MergeTree",
        "batch_size": "1000",
    }


@pytest.mark.parametrize("template", ["{unknown}", "{relname", "{0}"])
def test__foreign_leaves__unfillable_template__refused(template: str) -> None:
    with pytest.raises(ValueError, match="cannot fill"):
        ForeignLeaves(server="s", options={"table_name": template})


@pytest.mark.parametrize("name", ["table name", "Table", "1x"])
def test__foreign_leaves__bad_option_name__refused(name: str) -> None:
    with pytest.raises(ValueError, match="option name"):
        ForeignLeaves(server="s", options={name: "x"})


def test__foreign_leaves__server__is_an_identifier() -> None:
    assert ForeignLeaves(server="Archive").server == "archive"
    with pytest.raises(ValueError, match="identifier"):
        ForeignLeaves(server="my server")


def test__foreign_leaves__survives_json() -> None:
    leaves = ForeignLeaves(server="archive", options={"table_name": "{relname}"})

    assert ForeignLeaves.model_validate(leaves.model_dump(mode="json")) == leaves


# ── on the configuration ────────────────────────────────────────────────────────


def test__config__leaves__default_to_plain_local_tables() -> None:
    config = _config()

    assert config.leaves == LocalLeaves()
    assert not config.manages_foreign_leaves


def test__config__leaves__discriminated_by_kind_from_serialized_form() -> None:
    config = TablePartitionConfig.model_validate(
        {
            "table_name": "metrics",
            "scheme": {"method": "range", "key": "ts", "boundaries": {"kind": "time", "granularity": "month"}},
            "leaves": {"kind": "foreign", "server": "archive", "options": {"table_name": "{relname}"}},
        }
    )

    assert config.leaves == ForeignLeaves(server="archive", options={"table_name": "{relname}"})
    assert config.manages_foreign_leaves


def test__config__leaves__round_trip_through_json() -> None:
    config = _config(leaves=LocalLeaves(storage_parameters={"fillfactor": 70}))

    reloaded = TablePartitionConfig.model_validate(config.model_dump(mode="json", by_alias=True))

    assert reloaded.leaves == config.leaves


# ── the planner: who owns a foreign partition ───────────────────────────────────


def test__plan__foreign_member_under_local_leaves__inspected_never_expired() -> None:
    root = _root(_month(5, relkind=RelationKind.FOREIGN), _month(7), _month(8))

    plan = _plan(_config(), root)

    assert _reasons(plan) == [FindingReason.FOREIGN_PARTITION]
    assert plan.detaches == ()


def test__plan__foreign_member_under_foreign_leaves__expires_like_any_other() -> None:
    root = _root(_month(5, relkind=RelationKind.FOREIGN), _month(7), _month(8))

    plan = _plan(_config(leaves=ForeignLeaves(server="archive")), root)

    assert plan.findings == ()
    assert _targets(plan.detaches) == [f"{ROOT}__2026_05"]
    assert _targets(plan.drops) == [f"{ROOT}__2026_05"]
    assert plan.drops[0].follows_detach


def test__plan__foreign_orphan_under_foreign_leaves__dropped_after_its_grace() -> None:
    plan = _plan(_config(leaves=ForeignLeaves(server="archive")), _root(_month(8)), orphans=(_orphan(1),))

    assert _targets(plan.drops) == [f"{ROOT}__2026_01"]
    assert plan.drops[0].reason is Reason.GRACE_ELAPSED


def test__plan__foreign_orphan_under_local_leaves__reported_only() -> None:
    plan = _plan(_config(), _root(_month(8)), orphans=(_orphan(1),))

    assert plan.drops == ()
    assert _reasons(plan) == [FindingReason.FOREIGN_PARTITION]
    assert "not this library's to drop" in plan.findings[0].detail


def test__plan__foreign_orphan_wanted_again__reattached_only_under_foreign_leaves() -> None:
    # September is wanted ahead of August; a detached foreign September exists.
    orphan = _orphan(9)
    ahead = LifecyclePolicy(creation=CreateAhead(count=2), retention=KeepNewest(count=2))

    local = _plan(_config(lifecycle=ahead), _root(_month(8)), orphans=(orphan,))
    foreign = _plan(
        _config(leaves=ForeignLeaves(server="archive"), lifecycle=ahead), _root(_month(8)), orphans=(orphan,)
    )

    assert _targets(local.creates) == [f"{ROOT}__2026_09"]
    assert local.attaches == ()
    assert foreign.creates == ()
    assert _targets(foreign.attaches) == [f"{ROOT}__2026_09"]


def test__plan__foreign_orphan_inside_retention_under_foreign_leaves__reattached() -> None:
    config = _config(
        leaves=ForeignLeaves(server="archive"),
        lifecycle=LifecyclePolicy(creation=CreateAhead(count=1), retention=KeepNewest(count=12)),
    )

    plan = _plan(config, _root(_month(8)), orphans=(_orphan(6),))

    assert _targets(plan.attaches) == [f"{ROOT}__2026_06"]


def test__plan__foreign_leaves_under_drop_never__orphans_left_alone() -> None:
    config = _config(
        leaves=ForeignLeaves(server="archive"),
        lifecycle=LifecyclePolicy(creation=CreateAhead(count=1), retention=KeepNewest(count=2), drop=DropNever()),
    )

    plan = _plan(config, _root(_month(8)), orphans=(_orphan(1),))

    assert plan.is_noop
    assert plan.findings == ()


def test__plan__nested_scheme_with_foreign_leaves__branches_are_planned_as_before() -> None:
    config = _config(
        leaves=ForeignLeaves(server="archive"),
        subpartition=HashPartitioning(key="tenant_id", modulus=2),
        lifecycle=LifecyclePolicy(creation=CreateAhead(count=1), retention=KeepNewest(count=2)),
    )

    plan = _plan(config, _root())

    (create,) = plan.creates
    assert create.partition_by is not None
    assert _targets(create.children) == [f"{ROOT}__2026_08__h0", f"{ROOT}__2026_08__h1"]
