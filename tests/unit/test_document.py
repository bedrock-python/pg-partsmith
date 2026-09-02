"""The document: several tables, shared defaults, and the wiring they run through."""

from __future__ import annotations

import re
from typing import Any

import pytest
from pydantic import ValidationError

from pg_partsmith.boundaries import UUIDv7BoundaryCodec
from pg_partsmith.document import PartitionsDocument, PartitionTableSpec, ToolkitOptions
from pg_partsmith.entities import PartitionGranularity
from pg_partsmith.events import HookPhase


def _document(**overrides: Any) -> dict[str, Any]:
    document: dict[str, Any] = {
        "tables": [
            {"table_name": "events", "partition_column": "created_at", "granularity": "month"},
            {"table_name": "audit", "partition_column": "logged_at", "granularity": "day"},
        ]
    }
    document.update(overrides)
    return document


# ── Tables ──────────────────────────────────────────────────────────────────────


def test__document__tables__become_configurations_in_document_order() -> None:
    # Arrange / Act
    document = PartitionsDocument.model_validate(_document())

    # Assert
    assert [config.table_name for config in document.configs()] == ["events", "audit"]
    assert document.configs()[1].granularity is PartitionGranularity.DAY


def test__document__no_tables__is_refused() -> None:
    # A document maintaining nothing is a configuration error, not a no-op run.
    with pytest.raises(ValidationError):
        PartitionsDocument.model_validate({"tables": []})


def test__document__one_relation_described_twice__is_refused() -> None:
    # Arrange: two entries, one relation -- maintained under two policies, in an
    # order nothing in the file makes visible.
    payload = _document(
        tables=[
            {"table_name": "events", "partition_column": "created_at", "granularity": "month", "schema": "public"},
            {"table_name": "events", "partition_column": "created_at", "granularity": "day", "schema": "public"},
        ]
    )

    # Act / Assert
    with pytest.raises(ValidationError, match="described twice"):
        PartitionsDocument.model_validate(payload)


def test__document__config_for__finds_a_table_by_the_name_postgresql_knows_it_as() -> None:
    # Arrange
    document = PartitionsDocument.model_validate(_document(defaults={"schema": "public"}))

    # Act / Assert
    assert document.config_for("public.audit").partition_column == "logged_at"
    with pytest.raises(KeyError, match=re.escape("public.events, public.audit")):
        document.config_for("public.missing")


# ── Defaults ────────────────────────────────────────────────────────────────────


def test__defaults__are_the_starting_point_of_every_table() -> None:
    # Arrange / Act
    document = PartitionsDocument.model_validate(_document(defaults={"schema": "analytics", "retention_count": 6}))

    # Assert
    assert [config.schema_name for config in document.configs()] == ["analytics", "analytics"]
    assert all(config.lifecycle.retention.count == 6 for config in document.configs())


def test__defaults__a_table_naming_the_key__owns_it() -> None:
    # Arrange / Act
    document = PartitionsDocument.model_validate(
        _document(
            defaults={"retention_count": 6},
            tables=[
                {"table_name": "events", "partition_column": "created_at", "granularity": "month"},
                {
                    "table_name": "audit",
                    "partition_column": "logged_at",
                    "granularity": "day",
                    "retention_count": 400,
                },
            ],
        )
    )

    # Assert
    assert [config.lifecycle.retention.count for config in document.configs()] == [6, 400]


def test__defaults__a_field_no_table_has__is_refused_where_it_is_written() -> None:
    # A typo in defaults would otherwise reach every table at once, or nothing.
    with pytest.raises(ValidationError, match="granuality"):
        PartitionsDocument.model_validate(_document(defaults={"granuality": "month"}))


def test__table__an_unknown_key__is_refused() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError):
        PartitionTableSpec.model_validate({"table_name": "events", "retenton_count": 3})


def test__table__schema__is_accepted_as_the_configuration_spells_it() -> None:
    # Arrange / Act
    spec = PartitionTableSpec.model_validate(
        {"table_name": "events", "schema": "public", "partition_column": "created_at", "granularity": "month"}
    )

    # Assert
    assert spec.schema_name == "public"
    assert spec.to_config().qualified_name == "public.events"


def test__document__version__is_the_one_this_release_reads() -> None:
    # Arrange / Act / Assert
    assert PartitionsDocument.model_validate(_document()).version == 1
    with pytest.raises(ValidationError):
        PartitionsDocument.model_validate(_document(version=2))


# ── Runtime ─────────────────────────────────────────────────────────────────────


def test__runtime__omitted__is_the_library_default_wiring() -> None:
    # Arrange / Act
    document = PartitionsDocument.model_validate(_document())

    # Assert
    assert document.runtime == ToolkitOptions()
    assert document.runtime.ddl_timezone == "UTC"


def test__to_kwargs__names_the_keywords_from_engine_takes__with_the_codec_resolved() -> None:
    # Arrange
    options = ToolkitOptions(boundary_codec="uuidv7", ddl_timezone="Europe/Berlin", marker_prefix="acme")

    # Act
    kwargs = options.to_kwargs()

    # Assert
    assert isinstance(kwargs["boundary_codec"], UUIDv7BoundaryCodec)
    assert kwargs["ddl_timezone"] == "Europe/Berlin"
    assert kwargs["marker_prefix"] == "acme"


def test__to_kwargs__an_unknown_codec_name__is_refused_by_name() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="uuidv8"):
        ToolkitOptions(boundary_codec="uuidv8").to_kwargs()


def test__runtime__an_unknown_key__is_refused() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError):
        PartitionsDocument.model_validate(_document(runtime={"ddl_timezome": "UTC"}))


def test__document__round_trips_through_its_own_dump() -> None:
    # Arrange: what a `validate` command reads is what an operator wrote
    document = PartitionsDocument.model_validate(
        _document(defaults={"schema": "public"}, dsn="postgresql://app@localhost/app", runtime={"marker_prefix": "x"})
    )

    # Act / Assert
    assert PartitionsDocument.model_validate_json(document.model_dump_json(by_alias=True)) == document


# ── Hooks ───────────────────────────────────────────────────────────────────────


def test__hooks__commands__come_back_in_lifecycle_order() -> None:
    # Arrange / Act
    document = PartitionsDocument.model_validate(
        _document(hooks={"after_create": ["/bin/notify"], "before_drop": ["/bin/archive", "--to", "s3"]})
    )

    # Assert: declaration order in the file does not decide when they run
    assert document.hooks is not None
    assert [phase.value for phase in document.hooks.commands()] == ["after_create", "before_drop"]
    assert document.hooks.commands()[HookPhase.BEFORE_DROP] == ("/bin/archive", "--to", "s3")


def test__hooks__a_misspelled_phase__is_refused_where_it_is_written() -> None:
    # Silently never running the command that exports data before a DROP is the
    # difference between an archive and no archive.
    with pytest.raises(ValidationError):
        PartitionsDocument.model_validate(_document(hooks={"befor_drop": ["/bin/archive"]}))


def test__hooks__omitted__is_no_hooks_at_all() -> None:
    # Arrange / Act
    document = PartitionsDocument.model_validate(_document())

    # Assert
    assert document.hooks is None


def test__hooks__a_section_naming_no_command__knows_it_is_empty() -> None:
    # Arrange / Act
    document = PartitionsDocument.model_validate(_document(hooks={"timeout_seconds": 30}))

    # Assert
    assert document.hooks is not None
    assert document.hooks.is_empty
    assert document.hooks.timeout_seconds == 30


def test__hooks__a_block_of_python__is_compiled_where_it_is_written() -> None:
    # Arrange / Act
    document = PartitionsDocument.model_validate(
        _document(hooks={"before_drop": {"python": "log.info('dropping %s', event.partition.name)"}})
    )

    # Assert
    assert document.hooks is not None
    assert document.hooks.commands() == {}
    assert [phase.value for phase in document.hooks.python_blocks()] == ["before_drop"]
    assert not document.hooks.is_empty


def test__hooks__a_block_that_does_not_parse__is_a_validation_error_with_a_line_number() -> None:
    # A SyntaxError found at 03:00 by a CronJob is the failure mode to design out.
    broken = "if event.partition.name\n    pass"
    with pytest.raises(ValidationError, match="line 1"):
        PartitionsDocument.model_validate(_document(hooks={"before_drop": {"python": broken}}))


def test__hooks__a_block_naming_both_a_source_and_a_file__is_refused() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="not both"):
        PartitionsDocument.model_validate(
            _document(hooks={"before_drop": {"python": "pass", "python_file": "hooks/export.py"}})
        )


def test__hooks__a_file_reference__is_kept_for_the_reader_to_resolve() -> None:
    # Only the reader knows where the document lives, so the path stays a path.
    document = PartitionsDocument.model_validate(_document(hooks={"before_drop": {"python_file": "hooks/export.py"}}))

    # Assert
    assert document.hooks is not None
    block = document.hooks.python_blocks()[HookPhase.BEFORE_DROP]
    assert block.python is None
    assert block.python_file == "hooks/export.py"


def test__hooks__commands_and_blocks__mix_across_phases() -> None:
    # Arrange / Act
    document = PartitionsDocument.model_validate(
        _document(hooks={"after_create": ["/bin/notify"], "before_drop": {"python": "pass"}})
    )

    # Assert
    assert document.hooks is not None
    assert list(document.hooks.commands()) == [HookPhase.AFTER_CREATE]
    assert list(document.hooks.python_blocks()) == [HookPhase.BEFORE_DROP]


def test__hooks__an_empty_python_block__is_refused() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="empty"):
        PartitionsDocument.model_validate(_document(hooks={"before_drop": {"python": "   "}}))


def test__defaults__that_are_not_a_mapping__are_refused_by_the_field() -> None:
    # Arrange / Act / Assert: the merge steps aside and the field's own validation speaks
    with pytest.raises(ValidationError):
        PartitionsDocument.model_validate(_document(defaults="month"))


def test__tables__that_are_not_a_list__are_refused_by_the_field() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError):
        PartitionsDocument.model_validate({"defaults": {"schema": "public"}, "tables": {"table_name": "events"}})


def test__document__something_that_is_not_a_mapping_at_all__is_refused() -> None:
    # Arrange / Act / Assert: the defaults merge steps aside; validation says what it is
    with pytest.raises(ValidationError):
        PartitionsDocument.model_validate("tables: events")
