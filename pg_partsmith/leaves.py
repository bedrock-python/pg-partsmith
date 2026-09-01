"""How the leaves of a partition tree are physically realised.

A leaf is the relation that stores rows: the deepest member of every branch.
The scheme decides how many leaves exist and what each owns; the *leaf
backend* decides what kind of relation each one is:

* :class:`LocalLeaves` — ordinary tables, ``LIKE`` the parent, optionally in
  a tablespace, with storage parameters, and with the parent's privileges
  replayed onto them (``LIKE`` copies none).
* :class:`ForeignLeaves` — foreign tables on a foreign server, so a window's
  rows live elsewhere (a column store, an archive database) while the table
  is still queried through one parent.

Branches -- partitions that partition further -- are always local: PostgreSQL
has no foreign partitioned tables.
"""

from __future__ import annotations

import re
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .topology import validate_pg_identifier
from .types import StrippedNonEmptyStr

__all__ = ["ForeignLeaves", "LeafBackend", "LocalLeaves"]

# Storage parameters are ``name`` or ``toast.name``; FDW option names are
# plain identifiers. Neither is ever quoted in DDL, so both are validated.
_STORAGE_PARAMETER_PATTERN = re.compile(r"^[a-z_][a-z0-9_]*(\.[a-z_][a-z0-9_]*)?$")
_OPTION_NAME_PATTERN = re.compile(r"^[a-z_][a-z0-9_]*$")

# What an option template may refer to.
OPTION_PLACEHOLDERS = ("relname", "schema", "parent", "root")


class LocalLeaves(BaseModel):
    """Leaves are ordinary tables, shaped ``LIKE`` their parent.

    Attributes:
        kind: Discriminator; always ``"local"``.
        tablespace: Tablespace every created relation goes to -- leaves and
            branches alike. None keeps the database default.
        storage_parameters: ``WITH (...)`` parameters for every created leaf
            (``fillfactor``, ``autovacuum_*``, ``toast.*``). Branches take
            none: PostgreSQL refuses storage parameters on a partitioned table.
        inherit_privileges: Replay the parent's owner and grants onto every
            created relation. ``CREATE TABLE ... LIKE`` copies neither, and
            a role that reads through the parent needs no grant on a leaf --
            but a role that addresses leaves directly does.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["local"] = "local"
    tablespace: StrippedNonEmptyStr | None = None
    storage_parameters: dict[str, str | int | float | bool] = Field(default_factory=dict)
    inherit_privileges: bool = False

    @field_validator("tablespace")
    @classmethod
    def validate_tablespace(cls, v: str | None) -> str | None:
        """A tablespace is spliced into DDL as an identifier."""
        return None if v is None else validate_pg_identifier(v)

    @field_validator("storage_parameters")
    @classmethod
    def validate_storage_parameters(cls, v: dict[str, str | int | float | bool]) -> dict[str, str | int | float | bool]:
        """Parameter names are spliced into DDL unquoted, so they are checked here."""
        for name in v:
            if not _STORAGE_PARAMETER_PATTERN.match(name):
                msg = f"storage parameter {name!r} is not a valid name (expected 'name' or 'toast.name')"
                raise ValueError(msg)
        return dict(v)

    @property
    def is_plain(self) -> bool:
        """True when nothing beyond ``LIKE`` the parent is asked for."""
        return self.tablespace is None and not self.storage_parameters and not self.inherit_privileges

    def rendered_storage_parameters(self) -> dict[str, str]:
        """The parameters as the string literals PostgreSQL accepts for every type."""
        rendered: dict[str, str] = {}
        for name, value in self.storage_parameters.items():
            if isinstance(value, bool):
                rendered[name] = "true" if value else "false"
            else:
                rendered[name] = str(value)
        return rendered


class ForeignLeaves(BaseModel):
    """Leaves are foreign tables on ``server``.

    Every created leaf is ``CREATE FOREIGN TABLE ... SERVER server OPTIONS
    (...)`` with the parent's columns, then attached like any other partition.
    Option values are templates: ``{relname}`` is the leaf's own relation
    name, ``{schema}`` its schema, ``{parent}`` the relation it is attached
    to and ``{root}`` the table the configuration is for -- so
    ``{"table_name": "{relname}"}`` maps every leaf onto a remote table of
    the same name.

    PostgreSQL accepts a foreign partition only under a parent without a
    unique index or primary key; the service refuses the configuration
    otherwise, before any DDL. Branches stay local tables.

    Attributes:
        kind: Discriminator; always ``"foreign"``.
        server: The foreign server, created beforehand with a user mapping.
        options: Foreign table options, values templated as above.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["foreign"] = "foreign"
    server: StrippedNonEmptyStr
    options: dict[str, str] = Field(default_factory=dict)

    @field_validator("server")
    @classmethod
    def validate_server(cls, v: str) -> str:
        """A server name is spliced into DDL as an identifier."""
        return validate_pg_identifier(v)

    @field_validator("options")
    @classmethod
    def validate_options(cls, v: dict[str, str]) -> dict[str, str]:
        """Option names are spliced unquoted; templates must only use known placeholders."""
        sample = dict.fromkeys(OPTION_PLACEHOLDERS, "x")
        for name, template in v.items():
            if not _OPTION_NAME_PATTERN.match(name):
                msg = f"foreign table option {name!r} is not a valid option name"
                raise ValueError(msg)
            try:
                template.format_map(sample)
            except (KeyError, IndexError, ValueError) as exc:
                msg = (
                    f"foreign table option {name!r} has a template {template!r} this library cannot fill; "
                    f"the placeholders are {', '.join('{' + p + '}' for p in OPTION_PLACEHOLDERS)}"
                )
                raise ValueError(msg) from exc
        return dict(v)

    def render_options(self, *, relname: str, schema: str, parent: str, root: str) -> dict[str, str]:
        """Fill the option templates for one leaf."""
        values = {"relname": relname, "schema": schema, "parent": parent, "root": root}
        return {name: template.format_map(values) for name, template in self.options.items()}


LeafBackend = Annotated[LocalLeaves | ForeignLeaves, Field(discriminator="kind")]
"""What kind of relation a leaf is, discriminated on ``kind``."""
