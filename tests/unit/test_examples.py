"""The shipped examples: every document validates, every hook compiles, the schema is current."""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

from pg_partsmith.cli.loader import load_document, load_python_hooks
from pg_partsmith.document import PartitionsDocument
from pg_partsmith.events import HookPhase

EXAMPLES = Path(__file__).resolve().parents[2] / "examples"
DOCUMENTS = sorted(EXAMPLES.glob("partitions*.yaml"))
SCRIPTS = sorted((EXAMPLES / "hooks").glob("*.sh"))


@pytest.mark.parametrize("path", DOCUMENTS, ids=lambda p: p.name)
def test__example_document__validates_and_names_every_table(path: Path) -> None:
    # An example that does not validate teaches the wrong vocabulary.
    document = load_document(path)

    # Assert: every entry becomes a configuration, and none is repeated
    names = [config.qualified_name for config in document.configs()]
    assert names
    assert len(names) == len(set(names))


def test__the_full_example__spells_every_kind_of_thing_a_document_can_hold() -> None:
    # Arrange: the one people copy from
    document = load_document(EXAMPLES / "partitions.yaml")
    configs = {config.table_name: config for config in document.configs()}

    # Assert: a flat table, a nested scheme, a LIST root, leaves, runtime and hooks
    assert configs["events"].granularity is not None
    assert configs["telemetry"].scheme.child is not None
    assert configs["shipments"].scheme.method_name == "list"
    assert configs["telemetry"].leaves.kind == "local"
    assert document.runtime.marker_prefix == "acme"
    assert document.hooks is not None
    assert set(document.hooks.commands()) == {HookPhase.BEFORE_DROP, HookPhase.AFTER_CREATE}
    assert set(document.hooks.python_blocks()) == {HookPhase.BEFORE_DETACH, HookPhase.AFTER_DROP}


def test__example_python_hook__compiles_from_the_file_the_document_names() -> None:
    # Arrange: resolved relative to the document, exactly as the CLI does it
    document = load_document(EXAMPLES / "partitions.yaml")
    assert document.hooks is not None

    # Act
    sources, names = load_python_hooks(document.hooks, EXAMPLES)

    # Assert: the file compiled, the inline block compiled, both are named
    assert HookPhase.BEFORE_DETACH in sources
    assert names[HookPhase.BEFORE_DETACH].endswith("export_partition.py")
    assert HookPhase.AFTER_DROP in sources


@pytest.mark.parametrize("script", SCRIPTS, ids=lambda p: p.name)
def test__example_shell_hook__parses_and_is_executable(script: Path) -> None:
    # Arrange / Assert: a shebang and the executable bit, so a COPY into an
    # image is all it takes
    assert script.read_text(encoding="utf-8").startswith("#!/usr/bin/env bash")
    bash = shutil.which("bash")
    if bash is None:
        pytest.skip("bash is not on PATH")

    # Act: a syntax check, no execution
    completed = subprocess.run([bash, "-n", str(script)], capture_output=True, text=True, check=False)  # noqa: S603

    # Assert
    assert completed.returncode == 0, completed.stderr


def test__the_committed_schema__is_the_one_the_document_generates() -> None:
    # A stale schema file would validate the wrong vocabulary in an editor,
    # which is worse than none; `pg-partsmith schema` regenerates it.
    committed = json.loads((EXAMPLES / "partitions.schema.json").read_text(encoding="utf-8"))

    # Assert
    assert committed == PartitionsDocument.model_json_schema()
