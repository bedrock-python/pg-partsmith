"""Domain entities for PostgreSQL partitioning."""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from .boundaries import Axis, CursorSource, TimeBoundaries
from .leaves import LeafBackend, LocalLeaves
from .lifecycle import CreateAhead, KeepNewest, LifecyclePolicy
from .periods import PartitionGranularity, Period
from .scheme import HashPartitioning, ListPartitioning, PartitionScheme, RangePartitioning, SchemeBase, name_fits
from .topology import (
    DefaultBounds,
    HashBounds,
    ListBounds,
    PartitionBounds,
    PartitionNode,
    PartitionType,
    RangeBounds,
    RelationKind,
    validate_pg_identifier,
)
from .types import NonNegativeInt, StrippedNonEmptyStr
from .utils import qualify

if TYPE_CHECKING:
    from .plan import MaintenancePlan

# ``PartitionType`` and the partition-tree models live in ``topology``, the
# calendar in ``periods``, so those modules stay IO-free and importable from
# anywhere; they are re-exported here because ``pg_partsmith.entities`` has
# always been their public home.
__all__ = [
    "DefaultBounds",
    "HashBounds",
    "ListBounds",
    "MaintenanceIssue",
    "MaintenanceIssueStep",
    "MaintenanceResult",
    "MigrationResult",
    "PartitionBounds",
    "PartitionGranularity",
    "PartitionInfo",
    "PartitionNode",
    "PartitionStrategy",
    "PartitionType",
    "Period",
    "RangeBounds",
    "TablePartitionConfig",
]


class PartitionStrategy(StrEnum):
    """What drives a root table's partitions.

    Derived from the scheme; accepted as input only to be checked against it.

    Attributes:
        TIME_BASED: RANGE over a time axis.
        NUMERIC_BASED: RANGE over an integer axis.
        VALUE_BASED: LIST.
        HASH_BASED: HASH.
    """

    TIME_BASED = "time_based"
    NUMERIC_BASED = "numeric_based"
    VALUE_BASED = "value_based"
    HASH_BASED = "hash_based"


class PartitionInfo(BaseModel):
    """Metadata about a partition, as ``list_partitions`` reports it.

    Attributes:
        name: Schema-qualified partition table name.
        oid: ``pg_class.oid``, when read from the catalog.
        partition_type: How the *parent* partitions this relation (RANGE, LIST, HASH).
        from_value: Start boundary value (for RANGE).
        to_value: End boundary value (for RANGE).
        boundaries_expr: Raw boundary expression as reported by PostgreSQL
            (``pg_get_expr(relpartbound, oid)``). Useful when parsing boundaries
            fails but the partition is still attached.
        bounds: Structured form of the same boundaries, discriminated on the
            bound kind. Populated from ``from_value``/``to_value`` for RANGE
            partitions when not supplied, so the two views never disagree.
        is_attached: Whether partition is currently attached to parent table.
        is_default: Whether this is the DEFAULT partition (no explicit boundaries).
        relkind: What the relation physically is.
        subpartition_type: How this partition partitions its own children, when
            it is itself a partitioned table. ``None`` for a leaf — which is
            what distinguishes a legacy leaf from a subpartitioned branch.
        parent_table: Name of parent partitioned table.
    """

    model_config = ConfigDict(frozen=True)

    name: StrippedNonEmptyStr
    oid: int | None = None
    partition_type: PartitionType
    from_value: str | None = None
    to_value: str | None = None
    boundaries_expr: str | None = None
    bounds: PartitionBounds | None = None
    is_attached: bool = True
    is_default: bool = False
    relkind: RelationKind = RelationKind.TABLE
    subpartition_type: PartitionType | None = None
    parent_table: StrippedNonEmptyStr | None = None

    @model_validator(mode="before")
    @classmethod
    def derive_range_bounds(cls, data: object) -> object:
        """Keep ``bounds`` and ``from_value``/``to_value`` in step.

        Both spellings of a RANGE boundary are part of the public surface:
        callers written before structured bounds existed pass the pair, newer
        ones pass ``bounds``. Deriving the missing side here means neither kind
        of caller can observe a half-populated model.
        """
        if not isinstance(data, dict):
            return data

        # A validator that wrote into ``data`` would be editing the caller's own
        # dict -- one they may be about to reuse, or may have built from another
        # model's dump. The copy is shallow: nothing below this level is touched.
        data = dict(data)

        bounds = data.get("bounds")
        if bounds is None:
            from_value, to_value = data.get("from_value"), data.get("to_value")
            if not data.get("is_default") and from_value is not None and to_value is not None:
                data["bounds"] = RangeBounds(from_value=from_value, to_value=to_value)
            elif data.get("is_default"):
                data["bounds"] = DefaultBounds()
        else:
            # ``model_dump()`` renders the bound as a plain dict, so a round-trip
            # through it must derive the pair from the same shape the model form
            # does -- otherwise dumping and re-validating loses from/to_value.
            range_bounds = _as_range_bounds(bounds)
            if range_bounds is not None:
                data.setdefault("from_value", range_bounds.from_value)
                data.setdefault("to_value", range_bounds.to_value)

        return data

    @property
    def is_subpartitioned(self) -> bool:
        """True when this partition is itself a partitioned table (a branch)."""
        return self.subpartition_type is not None

    @property
    def hash_bounds(self) -> HashBounds | None:
        """This partition's ``MODULUS``/``REMAINDER`` bounds, when hash-bound."""
        return self.bounds if isinstance(self.bounds, HashBounds) else None

    @model_validator(mode="after")
    def validate_range_boundaries(self) -> PartitionInfo:
        """Validate that attached RANGE partitions have boundaries.

        Detached (orphaned) partitions may have lost their boundary metadata
        from the catalog and are allowed to carry ``None`` boundaries.

        For attached partitions we accept either parsed boundaries
        (``from_value`` + ``to_value``) OR a raw boundaries expression so that
        callers can still reason about partitions even when expression parsing
        fails.
        """
        if self._requires_boundaries() and not (self._has_parsed_boundaries() or self._has_raw_boundaries()):
            msg = "Attached RANGE partitions must have from_value/to_value or boundaries_expr"
            raise ValueError(msg)
        return self

    def _requires_boundaries(self) -> bool:
        return self.partition_type == PartitionType.RANGE and self.is_attached and not self.is_default

    def _has_parsed_boundaries(self) -> bool:
        return self.from_value is not None and self.to_value is not None

    def _has_raw_boundaries(self) -> bool:
        return self.boundaries_expr is not None and self.boundaries_expr.strip() != ""

    @property
    def schema_name(self) -> str | None:
        """Schema part of :attr:`name`, or None when the name is unqualified."""
        schema, _ = _split_name(self.name)
        return schema

    @property
    def relname(self) -> str:
        """Bare relation name without the schema qualifier.

        ``list_partitions`` always returns schema-qualified names; use this
        when addressing the partition through code that works with bare names
        (period parsing, export layouts, catalogue lookups).
        """
        _, relname = _split_name(self.name)
        return relname


def _split_name(name: str) -> tuple[str | None, str]:
    """Split ``schema.relname`` into parts; unqualified names get a None schema."""
    parts = name.split(".")
    if len(parts) == 2 and parts[0] and parts[1]:
        return parts[0], parts[1]
    return None, name


# Flat fields accepted as sugar for the composed ``scheme`` / ``lifecycle``.
_FLAT_SCHEME_FIELDS = frozenset(
    {"partition_column", "trailing_partition_columns", "granularity", "tz", "boundary_codec", "subpartition"}
)
_FLAT_LIFECYCLE_FIELDS = frozenset({"create_ahead_count", "retention_count"})
_FLAT_CHECK_FIELDS = frozenset({"partition_type", "partition_strategy"})


class TablePartitionConfig(BaseModel):
    """Configuration for one partitioned table.

    A configuration is a **scheme** — which levels exist, by which method, on
    which key, with which boundaries — and a **lifecycle policy** — when the
    partitions of the progression level are created, detached and dropped.
    Everything else the library does follows from those two.

    The composed form spells both out::

        TablePartitionConfig(
            table_name="events",
            scheme=RangePartitioning(
                key="created_at",
                boundaries=TimeBoundaries(granularity=PartitionGranularity.MONTH),
                child=HashPartitioning(key="tenant_id", modulus=4),
            ),
            lifecycle=LifecyclePolicy(creation=CreateAhead(3), retention=KeepNewest(12)),
        )

    The ordinary time-partitioned table keeps its flat spelling, which is sugar
    for exactly that::

        TablePartitionConfig(
            table_name="events",
            partition_column="created_at",
            granularity=PartitionGranularity.MONTH,
            create_ahead_count=3,
            retention_count=12,
        )

    Attributes:
        schema: Optional schema name for the partitioned table. When set, all
            DDL and catalogue queries are schema-qualified, making behaviour
            deterministic in databases with multiple schemas.
        table_name: Name of the partitioned table (lowercase, max 63 bytes minus
            the longest generated partition suffix).
        scheme: The root level of the partition tree and everything below it.
        lifecycle: When partitions of the progression level are created,
            detached and dropped. Meaningless — and ignored — for a scheme
            with no progression level, whose partition set is fixed.
        leaves: What kind of relation the leaves are: ordinary tables
            (:class:`~pg_partsmith.leaves.LocalLeaves`, the default, optionally
            with a tablespace, storage parameters and inherited privileges) or
            foreign tables (:class:`~pg_partsmith.leaves.ForeignLeaves`).
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True, populate_by_name=True)

    # NOTE: We store the value under a different field name to avoid Pydantic's
    # warning about shadowing BaseModel.schema(). Externally, the public API is
    # still `schema=...` and `config.db_schema`.
    schema_name: StrippedNonEmptyStr | None = Field(default=None, alias="schema")
    table_name: StrippedNonEmptyStr
    scheme: PartitionScheme
    lifecycle: LifecyclePolicy = Field(default_factory=LifecyclePolicy)
    leaves: LeafBackend = Field(default_factory=LocalLeaves)

    @model_validator(mode="before")
    @classmethod
    def compose_flat_fields(cls, data: object) -> object:
        """Turn the flat time-based spelling into a scheme and a lifecycle.

        The flat fields are accepted only for a RANGE root over time; every
        other topology is spelled with ``scheme``. ``partition_type`` and
        ``partition_strategy`` are accepted with either spelling and checked
        against the result.
        """
        if not isinstance(data, dict):
            return data
        data = dict(data)

        scheme_fields = {k: data.pop(k) for k in tuple(data) if k in _FLAT_SCHEME_FIELDS}
        lifecycle_fields = {k: data.pop(k) for k in tuple(data) if k in _FLAT_LIFECYCLE_FIELDS}
        checks = {k: data.pop(k) for k in tuple(data) if k in _FLAT_CHECK_FIELDS}

        if "scheme" in data:
            if scheme_fields:
                msg = f"Pass either scheme or the flat fields {sorted(scheme_fields)!r}, not both"
                raise ValueError(msg)
        else:
            data["scheme"] = _scheme_from_flat(scheme_fields, checks)

        if lifecycle_fields:
            if "lifecycle" in data:
                msg = f"Pass either lifecycle or the flat fields {sorted(lifecycle_fields)!r}, not both"
                raise ValueError(msg)
            data["lifecycle"] = LifecyclePolicy(
                creation=CreateAhead(count=lifecycle_fields.get("create_ahead_count", CreateAhead().count)),
                retention=KeepNewest(count=lifecycle_fields.get("retention_count", KeepNewest().count)),
            )

        # Kept for the after-validator, which sees the scheme fully built.
        if checks:
            data["_checks"] = checks
        return data

    # Populated only during validation; see ``compose_flat_fields``.
    checks_: dict[str, Any] | None = Field(default=None, alias="_checks", exclude=True, repr=False)

    @field_validator("table_name")
    @classmethod
    def validate_identifier(cls, v: str) -> str:
        """Validate and normalise the table name."""
        return validate_pg_identifier(v)

    @field_validator("schema_name")
    @classmethod
    def validate_schema(cls, v: str | None) -> str | None:
        """Validate and normalise schema name."""
        return None if v is None else validate_pg_identifier(v)

    @model_validator(mode="after")
    def validate_scheme(self) -> TablePartitionConfig:
        """Check the declared type/strategy against the scheme, and the name budget."""
        checks = self.checks_ or {}
        object.__setattr__(self, "checks_", None)

        declared_type = checks.get("partition_type")
        if declared_type is not None and PartitionType(declared_type) != self.partition_type:
            msg = (
                f"partition_type={PartitionType(declared_type).value!r} does not match the scheme's root, which is "
                f"{self.partition_type.value.upper()}"
            )
            raise ValueError(msg)

        declared_strategy = checks.get("partition_strategy")
        if declared_strategy is not None and PartitionStrategy(declared_strategy) != self.partition_strategy:
            msg = (
                f"partition_strategy={PartitionStrategy(declared_strategy).value!r} does not match the scheme, "
                f"which is {self.partition_strategy.value!r}"
            )
            raise ValueError(msg)

        fits, total = name_fits(self.table_name, self.scheme)
        if not fits:
            msg = (
                f"table_name {self.table_name!r} is too long for this scheme: table_name ({len(self.table_name)}) + "
                f"the longest generated suffix ({self.scheme.name_length_budget()}) = {total} > 63 bytes. "
                "PostgreSQL truncates identifiers silently, which would collapse two partitions onto one name."
            )
            raise ValueError(msg)

        if isinstance(self.lifecycle.creation, CreateAhead):
            for level in self.levels:
                progression = level.progression
                if progression is not None and progression.cursor_source is CursorSource.NEWEST_MEMBER:
                    msg = (
                        f"{level.describe()} is a sliding list whose cursor is its newest partition, so "
                        "CreateAhead would open another partition on every run; rotate it with "
                        "CreateNextIf(...) or bound it with CreateUntil(...)"
                    )
                    raise ValueError(msg)
        return self

    # ── Derived views ───────────────────────────────────────────────────────────

    @property
    def db_schema(self) -> str | None:
        """PostgreSQL schema name."""
        return self.schema_name

    @property
    def qualified_name(self) -> str:
        """``schema.table`` when a schema is set, else the bare table name."""
        return qualify(self.schema_name, self.table_name)

    @property
    def root(self) -> SchemeBase:
        """The root level of the scheme."""
        return self.scheme

    @property
    def partition_type(self) -> PartitionType:
        """How the root table partitions its children."""
        return self.scheme.method

    @property
    def partition_strategy(self) -> PartitionStrategy:
        """What drives the root's partitions."""
        scheme = self.scheme
        if isinstance(scheme, HashPartitioning):
            return PartitionStrategy.HASH_BASED
        if isinstance(scheme, ListPartitioning):
            return PartitionStrategy.VALUE_BASED
        assert isinstance(scheme, RangePartitioning)
        return (
            PartitionStrategy.TIME_BASED
            if scheme.range_boundaries.axis is Axis.TIME
            else PartitionStrategy.NUMERIC_BASED
        )

    @property
    def partition_column(self) -> str:
        """The leading column of the root's partition key."""
        return self.scheme.leading_column

    @property
    def trailing_partition_columns(self) -> tuple[str, ...]:
        """The rest of the root's partition key, in key order."""
        return self.scheme.key[1:]

    @property
    def partition_columns(self) -> tuple[str, ...]:
        """The root's whole partition key, in key order."""
        return self.scheme.key

    @property
    def key_arity(self) -> int:
        """Number of columns in the root's partition key."""
        return len(self.scheme.key)

    @property
    def granularity(self) -> PartitionGranularity | None:
        """The built-in period size of a time-based root, when it uses one."""
        boundaries = self.time_boundaries
        return None if boundaries is None else boundaries.granularity

    @property
    def time_boundaries(self) -> TimeBoundaries | None:
        """The root's time boundaries, when the root is a RANGE over time."""
        scheme = self.scheme
        return scheme.time_boundaries if isinstance(scheme, RangePartitioning) else None

    @property
    def subpartition(self) -> SchemeBase | None:
        """The level below the root, if any."""
        return self.scheme.child

    @property
    def create_ahead_count(self) -> int | None:
        """Windows kept ahead of the cursor, when the creation policy is ``CreateAhead``."""
        creation = self.lifecycle.creation
        return creation.count if isinstance(creation, CreateAhead) else None

    @property
    def retention_count(self) -> int | None:
        """Newest windows kept, when the retention policy is ``KeepNewest``."""
        retention = self.lifecycle.retention
        return retention.count if isinstance(retention, KeepNewest) else None

    @property
    def is_progression_root(self) -> bool:
        """True when the root is a progression level: a RANGE, or a sliding LIST."""
        return self.scheme.progression is not None

    @property
    def is_time_based(self) -> bool:
        """True when the root is a RANGE over time."""
        return self.time_boundaries is not None

    @property
    def has_progression_level(self) -> bool:
        """True when any level of the scheme is a progression."""
        return any(level.progression is not None for level in self.scheme.walk())

    @property
    def levels(self) -> list[SchemeBase]:
        """Every level of the scheme, root first."""
        return list(self.scheme.walk())

    @property
    def manages_foreign_leaves(self) -> bool:
        """True when the leaves are foreign tables the lifecycle creates and drops."""
        return self.leaves.kind == "foreign"


def _scheme_from_flat(fields: dict[str, Any], checks: dict[str, Any]) -> SchemeBase:
    """Build the scheme the flat spelling describes."""
    column = fields.get("partition_column")
    if column is None:
        msg = "TablePartitionConfig needs either scheme or partition_column"
        raise ValueError(msg)

    strategy = checks.get("partition_strategy")
    if strategy is not None and PartitionStrategy(strategy) is not PartitionStrategy.TIME_BASED:
        msg = (
            f"The flat fields describe a time-partitioned RANGE root; a {PartitionStrategy(strategy).value!r} table "
            "is spelled with scheme=HashPartitioning(...) / ListPartitioning(...) / RangePartitioning(...)"
        )
        raise ValueError(msg)

    granularity = fields.get("granularity")
    if granularity is None:
        msg = "A time-partitioned table requires granularity"
        raise ValueError(msg)

    boundaries: dict[str, Any] = {"granularity": granularity}
    if "tz" in fields:
        boundaries["tz"] = fields["tz"]
    if "boundary_codec" in fields:
        boundaries["codec"] = fields["boundary_codec"]

    key = (column, *tuple(fields.get("trailing_partition_columns") or ()))
    return RangePartitioning(key=key, boundaries=TimeBoundaries(**boundaries), child=fields.get("subpartition"))


class MaintenanceIssueStep(StrEnum):
    """Lifecycle step in which a non-fatal maintenance issue occurred."""

    CREATE = "create"
    RECONCILE = "reconcile"
    ATTACH = "attach"
    DETACH = "detach"
    DROP = "drop"
    MOVE = "move"


class MaintenanceIssue(BaseModel):
    """A non-fatal problem recorded during a maintenance run.

    Attributes:
        step: Lifecycle step the problem occurred in.
        error: Error message (``TypeName: message``).
        partition_name: Partition the problem concerns, when it is specific to
            one - reconciliation findings always set it.
    """

    model_config = ConfigDict(frozen=True)

    step: MaintenanceIssueStep
    error: StrippedNonEmptyStr
    partition_name: str | None = None


class MaintenanceResult(BaseModel):
    """Result of partition maintenance operation.

    Attributes:
        created_count: Partitions created directly under the root. A branch
            counts once, however many leaves it contains - the branch is the
            lifecycle unit.
        repaired_count: Partitions created inside *pre-existing* branches to
            close gaps in their child sets.
        attached_count: Detached partitions re-attached because their window
            was wanted again.
        detached_count: Partitions detached in this run.
        dropped_count: Partitions dropped.
        duration_ms: Duration of maintenance in milliseconds.
        error: Fatal error message (set when the whole maintenance run fails).
        issues: Non-fatal problems. Step failures land here when the run was
            started with ``continue_on_error=True``; findings the planner
            deliberately refused to act on are always recorded, since leaving
            them unreported would hide rejected writes.
        plan: The plan this run executed, when one was made.
    """

    model_config = ConfigDict(frozen=True)

    created_count: NonNegativeInt = 0
    repaired_count: NonNegativeInt = 0
    attached_count: NonNegativeInt = 0
    detached_count: NonNegativeInt = 0
    dropped_count: NonNegativeInt = 0
    duration_ms: NonNegativeInt = 0
    error: str | None = None
    issues: tuple[MaintenanceIssue, ...] = ()
    plan: Any = Field(default=None, exclude=True, repr=False)

    @property
    def success(self) -> bool:
        """True only when there is no fatal error (non-fatal ``issues`` may exist)."""
        return self.error is None

    @property
    def maintenance_plan(self) -> MaintenancePlan | None:
        """:attr:`plan`, typed."""
        return self.plan  # type: ignore[no-any-return]


class MigrationResult(BaseModel):
    """Result of a batched row move (``partition_data`` / ``unpartition``).

    Attributes:
        rows_moved: Rows moved by this call.
        batches: Statements it took.
        partitions: Partitions created and filled (``partition_data``) or
            emptied (``unpartition``), in the order they were handled.
        complete: True when nothing is left to move; False when the batch
            budget ran out or a window could not be given a partition -- call
            again, or read ``issues``.
        issues: Windows that could not be handled, and findings the planner
            reported on the way.
    """

    model_config = ConfigDict(frozen=True)

    rows_moved: NonNegativeInt = 0
    batches: NonNegativeInt = 0
    partitions: tuple[str, ...] = ()
    complete: bool = True
    issues: tuple[MaintenanceIssue, ...] = ()


def _as_range_bounds(bounds: object) -> RangeBounds | None:
    """Read range boundaries out of either spelling of a bound."""
    if isinstance(bounds, RangeBounds):
        return bounds
    if isinstance(bounds, dict) and bounds.get("kind") == "range":
        try:
            return RangeBounds.model_validate(bounds)
        except ValidationError:
            # Let the field's own validation report what is wrong with it.
            return None
    return None
