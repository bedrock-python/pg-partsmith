"""The guides' YAML blocks: they parse, and what they hand the command is something it takes.

A manifest in the running guide is what a team pastes into a cluster, and a
flag renamed in the CLI would leave it wrong with nothing to say so. Every
``args`` or ``command`` list that starts with one of the commands is checked
against the options that command actually declares.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
import typer
import yaml

from pg_partsmith.cli.main import app
from scripts.k8s_manifests import manifests

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.unit

GUIDES = sorted((Path(__file__).resolve().parents[2] / "docs" / "guide").glob("*.md"))
FENCE = re.compile(r"^```yaml\n(.*?)^```\n", re.MULTILINE | re.DOTALL)
GROUP = typer.main.get_command(app)
GROUP_OPTIONS = {option for parameter in GROUP.params for option in parameter.opts}
COMMAND_OPTIONS = {
    name: GROUP_OPTIONS | {option for parameter in command.params for option in parameter.opts}
    for name, command in GROUP.commands.items()
}


def _blocks() -> Iterator[Any]:
    for guide in GUIDES:
        for index, block in enumerate(FENCE.findall(guide.read_text(encoding="utf-8")), start=1):
            yield pytest.param(guide.name, block, id=f"{guide.name}:{index}")


def _invocations(node: Any) -> Iterator[list[str]]:
    """Every ``args`` / ``command`` list in ``node`` that begins with one of the commands or a group flag."""
    if isinstance(node, dict):
        for key, value in node.items():
            if (
                key in {"args", "command"}
                and isinstance(value, list)
                and value
                and all(isinstance(item, str) for item in value)
                and (value[0] in COMMAND_OPTIONS or value[0] in GROUP_OPTIONS)
            ):
                yield value
            else:
                yield from _invocations(value)
    elif isinstance(node, list):
        for item in node:
            yield from _invocations(item)


@pytest.mark.parametrize(("guide", "block"), list(_blocks()))
def test__yaml_block__parses(guide: str, block: str) -> None:
    # A block of only a comment -- the editor's schema line -- parses to nothing, and that is fine.
    list(yaml.safe_load_all(block))


@pytest.mark.parametrize(("guide", "block"), list(_blocks()))
def test__every_flag_a_guide_hands_the_command__is_one_the_command_takes(guide: str, block: str) -> None:
    for document in yaml.safe_load_all(block):
        for invocation in _invocations(document):
            command = invocation[0]
            allowed = COMMAND_OPTIONS.get(command, GROUP_OPTIONS)
            for argument in invocation[1:]:
                if argument.startswith("-"):
                    flag = argument.partition("=")[0]
                    assert flag in allowed, f"{guide}: `{command}` takes no {flag}"


def test__the_running_guide__holds_the_manifests_the_lint_job_validates() -> None:
    # Arrange: what scripts/k8s_manifests.py hands to kubeconform in CI
    running = next(guide for guide in GUIDES if guide.name == "running.md")

    # Act
    kinds = [
        document["kind"]
        for block in manifests(running.read_text(encoding="utf-8"))
        for document in yaml.safe_load_all(block)
        if isinstance(document, dict)
    ]

    # Assert: the shapes the guide promises, and nothing that is only a fragment
    assert {"ConfigMap", "Secret", "Job", "CronJob"} <= set(kinds)
    assert len(kinds) == len(manifests(running.read_text(encoding="utf-8"))) + 1  # one block holds two documents
