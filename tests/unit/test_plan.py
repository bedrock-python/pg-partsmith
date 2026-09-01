"""The maintenance plan as a data structure: filtering, rendering, serialization."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from pg_partsmith.entities import PartitionGranularity, TablePartitionConfig
from pg_partsmith.exceptions import PlanConfigMismatchError
from pg_partsmith.lifecycle import DetachMode
from pg_partsmith.plan import (
    AttachPartition,
    CreatePartition,
    DetachPartition,
    DropPartition,
    Finding,
    FindingReason,
    MaintenancePlan,
    OperationBase,
    OperationCapabilities,
    OperationKind,
    PartitionBy,
    Reason,
    Severity,
    validate_plan_for_config,
)
from pg_partsmith.topology import DefaultBounds, HashBounds, ListBounds, PartitionType, RangeBounds

NOW = datetime(2026, 8, 28, tzinfo=UTC)
ROOT = "public.events"
AUGUST = RangeBounds(from_value="2026-08-01", to_value="2026-09-01")
APRIL = RangeBounds(from_value="2026-04-01", to_value="2026-05-01")

INFORMATIONAL = {
    FindingReason.LEGACY_LEAF,
    FindingReason.MODULUS_PRESERVED,
    FindingReason.MODULUS_REPAIRED,
    FindingReason.NON_UNIFORM_COMPLETE,
    FindingReason.UNMANAGED_PARTITION,
    FindingReason.UNBOUNDED_PARTITION,
    FindingReason.FOREIGN_PARTITION,
    FindingReason.DETACH_PENDING,
    FindingReason.GRACE_PENDING,
    FindingReason.DROP_DEFERRED,
}


def _bucket(parent: str, remainder: int, modulus: int = 2) -> CreatePartition:
    return CreatePartition(
        target=f"{parent}__h{remainder}",
        parent_name=parent,
        bounds=HashBounds(modulus=modulus, remainder=remainder),
        key_columns=("tenant_id",),
        counts_as="subtree",
        reason=Reason.SUBTREE,
        detail=f"bucket {remainder} of {modulus}",
    )


def _create(name: str = f"{ROOT}__2026_08", *, nested: bool = False) -> CreatePartition:
    return CreatePartition(
        target=name,
        parent_name=ROOT,
        bounds=AUGUST,
        partition_by=PartitionBy(method=PartitionType.HASH, columns=("tenant_id",)) if nested else None,
        key_columns=("created_at",),
        children=(_bucket(name, 0), _bucket(name, 1)) if nested else (),
        lifecycle_unit=True,
        counts_as="created",
        reason=Reason.CREATE_AHEAD,
        detail="2026_08 under 'create 3 ahead'",
    )


def _attach(name: str = f"{ROOT}__2026_09") -> AttachPartition:
    return AttachPartition(
        target=name,
        oid=42,
        parent_name=ROOT,
        bounds=RangeBounds(from_value="2026-09-01", to_value="2026-10-01"),
        key_columns=("created_at",),
        partition_by=PartitionBy(method=PartitionType.HASH, columns=("tenant_id",)),
        reason=Reason.REATTACH,
    )


def _detach(name: str = f"{ROOT}__2026_04", *, mode: DetachMode = DetachMode.AUTO) -> DetachPartition:
    return DetachPartition(
        target=name,
        oid=7,
        parent_name=ROOT,
        mode=mode,
        bounds=APRIL,
        reason=Reason.RETENTION_EXPIRED,
        detail="2026_04 expired under 'keep newest 3'",
        size_bytes=4096,
        row_estimate=12,
    )


def _drop(name: str = f"{ROOT}__2026_04", *, follows_detach: bool = True) -> DropPartition:
    return DropPartition(
        target=name,
        oid=7 if follows_detach else 9,
        reason=Reason.FOLLOWS_DETACH if follows_detach else Reason.GRACE_ELAPSED,
        detached_at=None if follows_detach else NOW - timedelta(days=30),
        follows_detach=follows_detach,
    )


def _plan(*operations: CreatePartition | AttachPartition | DetachPartition | DropPartition) -> MaintenancePlan:
    return MaintenancePlan(table_name=ROOT, generated_at=NOW, operations=operations)


def _finding(reason: FindingReason, *, severity: Severity | None = None) -> Finding:
    return Finding(partition_name=f"{ROOT}__2026_04", reason=reason, detail="detail text", severity=severity)


FULL_PLAN = _plan(_create(nested=True), _attach(), _detach(), _drop(), _drop(f"{ROOT}__2026_01", follows_detach=False))


# ── Filtering ───────────────────────────────────────────────────────────────────


def test__without__detach__removes_the_detach_and_the_drop_that_follows_it() -> None:
    # Arrange / Act
    filtered = FULL_PLAN.without(OperationKind.DETACH)

    # Assert: a partition not detached this run cannot be dropped this run;
    # the orphan drop stands on its own and stays.
    assert [op.target for op in filtered.operations] == [f"{ROOT}__2026_08", f"{ROOT}__2026_09", f"{ROOT}__2026_01"]
    assert filtered.detaches == ()
    assert [op.follows_detach for op in filtered.drops] == [False]


def test__without__drop__keeps_the_detach_and_removes_every_drop() -> None:
    # Arrange / Act
    filtered = FULL_PLAN.without(OperationKind.DROP)

    # Assert
    assert filtered.drops == ()
    assert [op.target for op in filtered.detaches] == [f"{ROOT}__2026_04"]


def test__without__create_and_attach__leaves_the_removals_untouched() -> None:
    # Arrange / Act
    filtered = FULL_PLAN.without(OperationKind.CREATE, OperationKind.ATTACH)

    # Assert
    assert [op.kind for op in filtered.operations] == [OperationKind.DETACH, OperationKind.DROP, OperationKind.DROP]


def test__without__no_kinds__returns_an_equal_plan() -> None:
    # Arrange / Act / Assert
    assert FULL_PLAN.without() == FULL_PLAN


def test__without__preserves_everything_but_the_operations() -> None:
    # Arrange
    plan = FULL_PLAN.model_copy(update={"cursors": {"msg_id": 5}, "findings": (_finding(FindingReason.LEGACY_LEAF),)})

    # Act
    filtered = plan.without(OperationKind.DROP)

    # Assert
    assert filtered.table_name == ROOT
    assert filtered.generated_at == NOW
    assert filtered.cursors == {"msg_id": 5}
    assert filtered.findings == plan.findings


def test__only__create__keeps_the_creations_alone() -> None:
    # Arrange / Act
    filtered = FULL_PLAN.only(OperationKind.CREATE)

    # Assert
    assert [op.kind for op in filtered.operations] == [OperationKind.CREATE]


def test__only__drop__keeps_the_orphan_drop_but_not_the_one_following_a_detach() -> None:
    # Arrange / Act
    filtered = FULL_PLAN.only(OperationKind.DROP)

    # Assert
    assert [op.target for op in filtered.operations] == [f"{ROOT}__2026_01"]


def test__only__detach_and_drop__keeps_both_drops() -> None:
    # Arrange / Act
    filtered = FULL_PLAN.only(OperationKind.DETACH, OperationKind.DROP)

    # Assert
    assert [op.kind for op in filtered.operations] == [OperationKind.DETACH, OperationKind.DROP, OperationKind.DROP]


# ── Views ───────────────────────────────────────────────────────────────────────


def test__is_noop__no_operations__true_even_with_findings() -> None:
    # Arrange
    plan = MaintenancePlan(table_name=ROOT, generated_at=NOW, findings=(_finding(FindingReason.LEGACY_LEAF),))

    # Act / Assert
    assert plan.is_noop is True
    assert FULL_PLAN.is_noop is False


def test__kind_views__partition_the_operations_by_kind_in_order() -> None:
    # Arrange / Act / Assert
    assert [op.target for op in FULL_PLAN.creates] == [f"{ROOT}__2026_08"]
    assert [op.target for op in FULL_PLAN.attaches] == [f"{ROOT}__2026_09"]
    assert [op.target for op in FULL_PLAN.detaches] == [f"{ROOT}__2026_04"]
    assert [op.target for op in FULL_PLAN.drops] == [f"{ROOT}__2026_04", f"{ROOT}__2026_01"]


def test__actionable_findings__keeps_the_warnings_only() -> None:
    # Arrange
    plan = MaintenancePlan(
        table_name=ROOT,
        generated_at=NOW,
        findings=(
            _finding(FindingReason.LEGACY_LEAF),
            _finding(FindingReason.RANGE_OVERLAP),
            _finding(FindingReason.GRACE_PENDING, severity=Severity.WARNING),
        ),
    )

    # Act / Assert
    assert [f.reason for f in plan.actionable_findings] == [FindingReason.RANGE_OVERLAP, FindingReason.GRACE_PENDING]


def test__relation_count__counts_top_level_creations_with_their_subtrees() -> None:
    # Arrange
    plan = _plan(_create(nested=True), _create(f"{ROOT}__2026_09"), _attach(), _detach())

    # Act / Assert
    assert plan.relation_count == 4


def test__defaults__no_cursors_operations_or_findings() -> None:
    # Arrange / Act
    plan = MaintenancePlan(table_name=ROOT, generated_at=NOW)

    # Assert
    assert plan.cursors == {}
    assert plan.operations == ()
    assert plan.findings == ()
    assert plan.relation_count == 0


def test__plan__frozen__cannot_be_mutated() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError):
        FULL_PLAN.operations = ()  # type: ignore[misc]


# ── Rendering ───────────────────────────────────────────────────────────────────


def test__describe__renders_operations_nested_children_and_findings() -> None:
    # Arrange
    plan = MaintenancePlan(
        table_name=ROOT,
        generated_at=NOW,
        operations=(_create(nested=True), _attach(), _detach(), _drop()),
        findings=(_finding(FindingReason.LEGACY_LEAF), _finding(FindingReason.RANGE_OVERLAP)),
    )

    # Act
    text = plan.describe()

    # Assert
    assert text.splitlines() == [
        f"plan for {ROOT} at 2026-08-28T00:00:00+00:00",
        f"  CREATE {ROOT}__2026_08 (create_ahead)",
        f"    CREATE {ROOT}__2026_08__h0 (subtree)",
        f"    CREATE {ROOT}__2026_08__h1 (subtree)",
        f"  ATTACH {ROOT}__2026_09 (reattach)",
        f"  DETACH {ROOT}__2026_04 (retention_expired) size=4096 rows~12",
        f"  DROP {ROOT}__2026_04 (follows_detach)",
        "  [info] legacy_leaf: detail text",
        "  [warning] range_overlap: detail text",
    ]


def test__describe__empty_plan__says_nothing_to_do() -> None:
    # Arrange
    plan = MaintenancePlan(table_name=ROOT, generated_at=NOW)

    # Act / Assert
    assert plan.describe() == f"plan for {ROOT} at 2026-08-28T00:00:00+00:00\n  nothing to do"


def test__describe__findings_only__lists_them_without_the_nothing_to_do_line() -> None:
    # Arrange
    plan = MaintenancePlan(table_name=ROOT, generated_at=NOW, findings=(_finding(FindingReason.GRACE_PENDING),))

    # Act
    lines = plan.describe().splitlines()

    # Assert
    assert lines[1:] == ["  [info] grace_pending: detail text"]


def test__operation_describe__one_line_with_kind_target_and_reason() -> None:
    # Arrange / Act / Assert
    assert _create().describe() == f"CREATE {ROOT}__2026_08 (create_ahead)"
    assert _drop(follows_detach=False).describe() == f"DROP {ROOT}__2026_04 (grace_elapsed)"


# ── Serialization ───────────────────────────────────────────────────────────────


def test__model_dump_json__round_trip__restores_every_operation_kind() -> None:
    # Arrange
    plan = FULL_PLAN.model_copy(
        update={"cursors": {"msg_id": 1_250_000}, "findings": (_finding(FindingReason.MODULUS_PRESERVED),)}
    )

    # Act
    dumped = plan.model_dump(mode="json")
    restored = MaintenancePlan.model_validate(dumped)

    # Assert
    assert restored == plan
    assert [type(op).__name__ for op in restored.operations] == [
        "CreatePartition",
        "AttachPartition",
        "DetachPartition",
        "DropPartition",
        "DropPartition",
    ]
    assert isinstance(restored.creates[0].children[0].bounds, HashBounds)
    assert restored.drops[1].detached_at == NOW - timedelta(days=30)


def test__model_dump_json__by_alias__discriminates_on_kind_and_still_round_trips() -> None:
    # Arrange / Act
    dumped = FULL_PLAN.model_dump(mode="json", by_alias=True)
    restored = MaintenancePlan.model_validate(dumped)

    # Assert
    assert [op["kind"] for op in dumped["operations"]] == ["create", "attach", "detach", "drop", "drop"]
    assert restored == FULL_PLAN


def test__model_validate__operations_written_by_hand__discriminated_on_kind() -> None:
    # Arrange
    data = {
        "table_name": ROOT,
        "generated_at": "2026-08-28T00:00:00+00:00",
        "operations": [
            {"kind": "drop", "target": f"{ROOT}__2026_01", "reason": "grace_elapsed"},
            {
                "kind": "create",
                "target": f"{ROOT}__other",
                "parent_name": ROOT,
                "bounds": {"kind": "default"},
                "reason": "list_default_missing",
            },
            {
                "kind": "detach",
                "target": f"{ROOT}__eu",
                "parent_name": ROOT,
                "bounds": {"kind": "list", "values": ["de", "fr"]},
                "mode": "blocking",
                "reason": "retention_expired",
            },
        ],
    }

    # Act
    plan = MaintenancePlan.model_validate(data)

    # Assert
    assert plan.drops[0] == DropPartition(target=f"{ROOT}__2026_01", reason=Reason.GRACE_ELAPSED)
    assert plan.creates[0].bounds == DefaultBounds()
    assert plan.detaches[0].bounds == ListBounds(values=("de", "fr"))
    assert plan.detaches[0].mode is DetachMode.BLOCKING


def test__model_validate__unknown_operation_kind__rejected() -> None:
    # Arrange
    data = {
        "table_name": ROOT,
        "generated_at": NOW,
        "operations": [{"kind": "rename", "target": "x", "reason": "explicit"}],
    }

    # Act / Assert
    with pytest.raises(ValidationError, match="union_tag_invalid"):
        MaintenancePlan.model_validate(data)


# ── Findings ────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("reason", list(FindingReason), ids=[reason.value for reason in FindingReason])
def test__finding__severity_defaulted_from_the_reason(reason: FindingReason) -> None:
    # Arrange / Act
    finding = _finding(reason)

    # Assert
    expected = Severity.INFO if reason in INFORMATIONAL else Severity.WARNING
    assert finding.severity is expected
    assert finding.is_actionable is (expected is Severity.WARNING)


def test__finding__explicit_severity__overrides_the_default() -> None:
    # Arrange / Act
    downgraded = _finding(FindingReason.RANGE_OVERLAP, severity=Severity.INFO)
    upgraded = _finding(FindingReason.LEGACY_LEAF, severity=Severity.WARNING)

    # Assert
    assert downgraded.severity is Severity.INFO
    assert downgraded.is_actionable is False
    assert upgraded.is_actionable is True


def test__finding__blank_detail__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError):
        Finding(partition_name=ROOT, reason=FindingReason.LEGACY_LEAF, detail="   ")


def test__finding__frozen__cannot_be_mutated() -> None:
    # Arrange
    finding = _finding(FindingReason.LEGACY_LEAF)

    # Act / Assert
    with pytest.raises(ValidationError):
        finding.severity = Severity.WARNING  # type: ignore[misc]


# ── Operations ──────────────────────────────────────────────────────────────────


def test__create_partition__capabilities__transactional_with_attach_locks() -> None:
    # Arrange / Act
    capabilities = _create().capabilities

    # Assert
    assert capabilities.transactional is True
    assert "SHARE UPDATE EXCLUSIVE on the parent" in capabilities.lock
    assert _create().kind is OperationKind.CREATE
    assert _create().is_destructive is False


def test__attach_partition__capabilities__transactional() -> None:
    # Arrange / Act
    capabilities = _attach().capabilities

    # Assert
    assert capabilities.transactional is True
    assert capabilities.lock.startswith("SHARE UPDATE EXCLUSIVE on the parent")
    assert _attach().kind is OperationKind.ATTACH
    assert _attach().is_destructive is False


@pytest.mark.parametrize("mode", [DetachMode.AUTO, DetachMode.CONCURRENT], ids=["auto", "concurrent"])
def test__detach_partition__concurrent_modes__not_transactional(mode: DetachMode) -> None:
    # Arrange / Act
    capabilities = _detach(mode=mode).capabilities

    # Assert
    assert capabilities.transactional is False
    assert "CONCURRENTLY" in capabilities.lock


def test__detach_partition__blocking_mode__transactional_under_access_exclusive() -> None:
    # Arrange / Act
    capabilities = _detach(mode=DetachMode.BLOCKING).capabilities

    # Assert
    assert capabilities == OperationCapabilities(
        transactional=True,
        lock="ACCESS EXCLUSIVE on the parent and the partition, and on every table referencing the parent through "
        "a foreign key",
    )
    assert _detach().kind is OperationKind.DETACH
    assert _detach().is_destructive is True
    assert _detach().mode is DetachMode.AUTO


def test__drop_partition__capabilities__transactional_and_destructive() -> None:
    # Arrange / Act
    drop = _drop()

    # Assert
    assert drop.capabilities == OperationCapabilities(
        transactional=True, lock="ACCESS EXCLUSIVE on the dropped table only"
    )
    assert drop.kind is OperationKind.DROP
    assert drop.is_destructive is True


def test__operation_capabilities__defaults__transactional_with_no_lock_named() -> None:
    # Arrange / Act / Assert
    assert OperationCapabilities() == OperationCapabilities(transactional=True, lock="")


def test__operation_base__kind_and_capabilities__left_to_subclasses() -> None:
    # Arrange
    base = OperationBase(target="x", reason=Reason.EXPLICIT)

    # Act / Assert
    with pytest.raises(NotImplementedError):
        _ = base.kind
    with pytest.raises(NotImplementedError):
        _ = base.capabilities
    assert base.is_destructive is False


def test__operation_base__blank_target__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError):
        DropPartition(target=" ", reason=Reason.GRACE_ELAPSED)


def test__operation_base__negative_measurements__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError):
        DropPartition(target="x", reason=Reason.GRACE_ELAPSED, size_bytes=-1)


def test__create_partition__count__counts_itself_and_every_descendant() -> None:
    # Arrange
    grandchild = _bucket(f"{ROOT}__2026_08__h0", 0)
    child = _bucket(f"{ROOT}__2026_08", 0).model_copy(update={"children": (grandchild,)})
    op = _create().model_copy(update={"children": (child, _bucket(f"{ROOT}__2026_08", 1))})

    # Act / Assert
    assert op.count() == 4
    assert _create().count() == 1


def test__create_partition__walk__parent_first_depth_first() -> None:
    # Arrange
    grandchild = _bucket(f"{ROOT}__2026_08__h0", 0)
    child = _bucket(f"{ROOT}__2026_08", 0).model_copy(update={"children": (grandchild,)})
    op = _create().model_copy(update={"children": (child, _bucket(f"{ROOT}__2026_08", 1))})

    # Act / Assert
    assert [nested.target for nested in op.walk()] == [
        f"{ROOT}__2026_08",
        f"{ROOT}__2026_08__h0",
        f"{ROOT}__2026_08__h0__h0",
        f"{ROOT}__2026_08__h1",
    ]


def test__create_partition__defaults__leaf_not_a_lifecycle_unit_counted_as_created() -> None:
    # Arrange / Act
    op = CreatePartition(
        target=f"{ROOT}__h0", parent_name=ROOT, bounds=HashBounds(modulus=2, remainder=0), reason=Reason.HASH_GAP
    )

    # Assert
    assert op.partition_by is None
    assert op.key_columns == ()
    assert op.children == ()
    assert op.lifecycle_unit is False
    assert op.counts_as == "created"
    assert op.detail == ""


def test__create_partition__unknown_counter_name__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError):
        CreatePartition(target="x", parent_name=ROOT, bounds=AUGUST, reason=Reason.EXPLICIT, counts_as="attached")  # type: ignore[arg-type]


def test__partition_by__describe__renders_as_postgresql_spells_it() -> None:
    # Arrange / Act / Assert
    assert PartitionBy(method=PartitionType.HASH, columns=("tenant_id",)).describe() == "HASH (tenant_id)"
    assert PartitionBy(method=PartitionType.RANGE, columns=("created_at", "id")).describe() == "RANGE (created_at, id)"


def test__partition_by__no_columns_allowed_but_blank_column_rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError):
        PartitionBy(method=PartitionType.LIST, columns=(" ",))


def test__operation_kind__values__spell_the_four_ddl_families() -> None:
    # Arrange / Act / Assert
    assert [kind.value for kind in OperationKind] == ["create", "attach", "detach", "drop"]


def test__create_and_attach_capabilities__name_the_locks_on_referencing_tables() -> None:
    # PostgreSQL takes SHARE ROW EXCLUSIVE on every table with a foreign key to
    # the parent during ATTACH; an operator picking a window must see that.
    expected = "SHARE ROW EXCLUSIVE on every table referencing the parent"
    assert expected in _create().capabilities.lock
    assert expected in _attach().capabilities.lock


# ── The plan as an artifact ─────────────────────────────────────────────────────


def _config(*, schema: str | None = "public", retention: int = 12) -> TablePartitionConfig:
    return TablePartitionConfig(
        schema=schema,
        table_name="events",
        partition_column="created_at",
        granularity=PartitionGranularity.MONTH,
        retention_count=retention,
    )


def test__model_dump_json__round_trip__gives_back_the_same_plan() -> None:
    # Arrange: everything a real plan carries beyond its operations
    plan = FULL_PLAN.model_copy(update={"cursors": {"id": 42}, "config_fingerprint": "0123456789abcdef"})

    # Act / Assert: the aliased form is what the configuration vocabulary uses,
    # and both forms read back, so a plan written by one process and applied by
    # another is the plan that was written.
    assert MaintenancePlan.model_validate_json(plan.model_dump_json(by_alias=True)) == plan
    assert MaintenancePlan.model_validate_json(plan.model_dump_json()) == plan


def test__model_dump_json__by_alias__spells_every_operation_kind_as_the_documented_key() -> None:
    # Arrange / Act
    dumped = json.loads(FULL_PLAN.model_dump_json(by_alias=True))

    # Assert
    assert [op["kind"] for op in dumped["operations"]] == ["create", "attach", "detach", "drop", "drop"]


def test__cursors__integers__survive_the_round_trip_as_integers() -> None:
    # Arrange
    plan = MaintenancePlan(table_name=ROOT, generated_at=NOW, cursors={"id": 900_001})

    # Act
    back = MaintenancePlan.model_validate_json(plan.model_dump_json(by_alias=True))

    # Assert
    assert back.cursors == {"id": 900_001}
    assert back == plan


def test__cursors__a_value_no_integer_axis_could_have__is_rejected() -> None:
    # Only an integer axis records a cursor -- a time axis is cursored by
    # ``generated_at`` -- so anything else would not have round-tripped.
    with pytest.raises(ValidationError):
        MaintenancePlan(table_name=ROOT, generated_at=NOW, cursors={"created_at": NOW})  # type: ignore[dict-item]


def test__validate_plan_for_config__the_configuration_it_was_planned_from__passes() -> None:
    # Arrange
    config = _config()
    plan = MaintenancePlan(table_name=ROOT, generated_at=NOW, config_fingerprint=config.fingerprint)

    # Act / Assert
    validate_plan_for_config(config, plan)


def test__validate_plan_for_config__a_plan_for_another_table__is_refused_naming_both() -> None:
    # Arrange
    plan = MaintenancePlan(table_name="public.audit", generated_at=NOW)

    # Act / Assert
    with pytest.raises(PlanConfigMismatchError) as exc:
        validate_plan_for_config(_config(), plan)
    assert "public.audit" in str(exc.value)
    assert "public.events" in str(exc.value)


def test__validate_plan_for_config__configuration_edited_after_planning__is_refused() -> None:
    # Arrange: the plan still names the right relations, for reasons that have
    # stopped being true -- which OID revalidation cannot see.
    planned_under = _config(retention=12)
    plan = MaintenancePlan(table_name=ROOT, generated_at=NOW, config_fingerprint=planned_under.fingerprint)
    edited = _config(retention=3)

    # Act / Assert
    with pytest.raises(PlanConfigMismatchError) as exc:
        validate_plan_for_config(edited, plan)
    assert planned_under.fingerprint in str(exc.value)
    assert edited.fingerprint in str(exc.value)


def test__validate_plan_for_config__allow_config_drift__takes_the_plan_as_it_stands() -> None:
    # Arrange
    plan = MaintenancePlan(table_name=ROOT, generated_at=NOW, config_fingerprint=_config(retention=12).fingerprint)

    # Act / Assert
    validate_plan_for_config(_config(retention=3), plan, allow_config_drift=True)


def test__validate_plan_for_config__plan_without_a_fingerprint__is_checked_for_its_table_alone() -> None:
    # Arrange: a plan built by hand, or by a version that recorded none
    plan = MaintenancePlan(table_name=ROOT, generated_at=NOW)

    # Act / Assert
    validate_plan_for_config(_config(), plan)
    with pytest.raises(PlanConfigMismatchError):
        validate_plan_for_config(_config(schema="archive"), plan)
