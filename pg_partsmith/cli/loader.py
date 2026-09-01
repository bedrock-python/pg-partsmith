"""Reading the document off disk, and deciding what to connect to.

The library parses no files and opens no connections; the CLI is where both
happen, and this is that layer. It is deliberately thin: a format is chosen by
extension, parsed by whoever owns it, and handed to
:class:`~pg_partsmith.PartitionsDocument` to be validated in one place.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from pg_partsmith.document import PartitionsDocument
from pg_partsmith.entities import TablePartitionConfig

try:
    import yaml
except ImportError:  # pragma: no cover - PyYAML is an extra; JSON documents work without it
    yaml = None  # type: ignore[assignment]

__all__ = ["DSN_ENV_VAR", "ConfigError", "async_url", "load_document", "resolve_dsn", "select_configs"]

DSN_ENV_VAR = "PG_PARTSMITH_DSN"
"""Environment variable read when neither ``--dsn`` nor the document carries one."""

_YAML_SUFFIXES = frozenset({".yaml", ".yml"})
_JSON_SUFFIXES = frozenset({".json"})


class ConfigError(Exception):
    """The document cannot be read, parsed, or validated."""


def load_document(path: Path) -> PartitionsDocument:
    """Read one configuration document.

    The format is chosen by extension: ``.json`` by the standard library,
    ``.yaml`` / ``.yml`` by PyYAML's safe loader. A document is validated the
    same way whichever it came from -- the format is not the contract, the
    document is.

    Args:
        path: The file to read.

    Returns:
        The validated document.

    Raises:
        ConfigError: If the file is missing, is in a format this build cannot
            read, does not parse, or does not validate.
    """
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        msg = f"Cannot read {path}: {exc.strerror or exc}"
        raise ConfigError(msg) from exc

    suffix = path.suffix.lower()
    if suffix in _JSON_SUFFIXES:
        payload = _parse_json(text, path)
    elif suffix in _YAML_SUFFIXES:
        payload = _parse_yaml(text, path)
    else:
        known = ", ".join(sorted(_JSON_SUFFIXES | _YAML_SUFFIXES))
        msg = f"{path} has no format this reads; name it with one of: {known}"
        raise ConfigError(msg)

    if not isinstance(payload, dict):
        msg = f"{path} is not a document: its top level is {type(payload).__name__}, not a mapping"
        raise ConfigError(msg)
    try:
        return PartitionsDocument.model_validate(payload)
    except ValueError as exc:
        msg = f"{path} is not a valid document:\n{exc}"
        raise ConfigError(msg) from exc


def resolve_dsn(document: PartitionsDocument, *, override: str | None = None) -> str:
    """The connection string, from the flag, the environment, then the document.

    In that order, because that is the order of how specific to this run each
    one is -- and because a DSN carries a password, which a deployment may well
    want to keep out of a file it mounts from a ConfigMap.

    Args:
        document: The document, which may carry a ``dsn``.
        override: What ``--dsn`` said, when it said anything.

    Returns:
        The connection string.

    Raises:
        ConfigError: If none of the three names one.
    """
    dsn = override or os.environ.get(DSN_ENV_VAR) or document.dsn
    if not dsn:
        msg = f"No connection string: pass --dsn, set {DSN_ENV_VAR}, or give the document a dsn"
        raise ConfigError(msg)
    return dsn


def async_url(dsn: str) -> str:
    """The DSN with an async driver named, since that is what the CLI drives.

    ``postgresql://…`` means psycopg2 to SQLAlchemy, which cannot be driven
    asynchronously; a DSN that already names its driver is left exactly as it
    is, so ``postgresql+psycopg://`` keeps working for whoever installed it.
    """
    scheme, separator, rest = dsn.partition("://")
    if not separator or "+" in scheme:
        return dsn
    if scheme in {"postgresql", "postgres"}:
        return f"postgresql+asyncpg://{rest}"
    return dsn


def select_configs(document: PartitionsDocument, tables: tuple[str, ...]) -> tuple[TablePartitionConfig, ...]:
    """The document's configurations, narrowed to the tables ``--table`` named.

    A name is matched as written and as it qualifies -- ``events`` finds
    ``public.events`` -- so an operator does not have to know whether the
    document spelled the schema out.

    Args:
        document: The document.
        tables: The names asked for; empty means every table.

    Returns:
        The configurations, in document order.

    Raises:
        ConfigError: If a name matches no table in the document.
    """
    configs = document.configs()
    if not tables:
        return configs
    selected: list[TablePartitionConfig] = []
    for name in tables:
        matches = [c for c in configs if name in {c.qualified_name, c.table_name}]
        if not matches:
            known = ", ".join(c.qualified_name for c in configs)
            msg = f"{name!r} is not in this document; it describes {known}"
            raise ConfigError(msg)
        selected.extend(m for m in matches if m not in selected)
    return tuple(selected)


def _parse_json(text: str, path: Path) -> Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        msg = f"{path} is not valid JSON: {exc}"
        raise ConfigError(msg) from exc


def _parse_yaml(text: str, path: Path) -> Any:
    if yaml is None:  # pragma: no cover - exercised by the extra being absent
        msg = f"Reading {path} needs PyYAML: pip install 'pg-partsmith[cli]' (or write the document as JSON)"
        raise ConfigError(msg)
    try:
        return yaml.safe_load(text)
    except yaml.YAMLError as exc:
        msg = f"{path} is not valid YAML: {exc}"
        raise ConfigError(msg) from exc
