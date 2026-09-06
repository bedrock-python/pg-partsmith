# Contributing to pg-partsmith

Thank you for your interest in contributing! This document covers everything you need to get started.

## Development setup

```bash
git clone https://github.com/bedrock-python/pg-partsmith.git
cd pg-partsmith
uv sync --group dev
uv run pre-commit install --hook-type commit-msg
```

## Running checks

```bash
make check            # ruff lint + format check + mypy
make test-unit        # unit tests, no Docker required
make test-integration # integration tests, requires Docker
make test             # full suite with 90% coverage threshold
```

## Code style

- **Type hints** on all functions and methods, including tests
- **Docstrings** on public API only — Google style, one-line max
- **Line length** — 120 characters (ruff enforced)
- **Quotes** — double quotes (ruff enforced)
- **No comments** unless the *why* is non-obvious (workaround, subtle invariant)

## Commit messages

[Conventional Commits](https://www.conventionalcommits.org/) are enforced by pre-commit:

| Prefix | Use for |
|--------|---------|
| `feat:` | New feature or behaviour |
| `fix:` | Bug fix |
| `docs:` | Documentation only |
| `test:` | Test additions or changes |
| `refactor:` | Code restructure, no behaviour change |
| `perf:` | Performance improvement |
| `chore:` | Build, tooling, CI |

Breaking changes: add `!` after the type (`feat!:`) or include a `BREAKING CHANGE:` footer.

## Pull requests

1. Fork the repository
2. Create a branch from `master`: `git checkout -b feat/my-feature`
3. Make your changes with tests
4. Run `make check && make test-unit` locally
5. Open a PR against `master`

Update `CHANGELOG.md` under `[Unreleased]` for any user-visible change.

## The agents page

`docs/agents.md` is the whole library on one page, written for a coding assistant: the
public API, the rules that break code when they are broken, the mistakes models make, and
a map of which page to fetch for the rest. People hand it to an assistant instead of the
site, which is what makes a stale one worse than none — it teaches a model an API that no
longer exists.

It is part of the public API, so it changes in the same pull request the API does: a name
added, renamed or removed, a changed default or signature, a new rule a caller has to
obey. A new docs page means a new row in the documentation map. The review check is
mechanical — if the diff changes the public surface and `docs/agents.md` is untouched, the
pull request is not finished.

The page carries its own weight only if it stays fetchable as text. Every page of the site
is written a second time as raw Markdown next to its HTML by `scripts/emit_markdown.py`,
which the Docs workflow runs after the build; the **Copy page** control above each page
reads those files. A page whose Markdown would not read as the page — the generated API
reference — declines both with `copy_page: false` in its front matter.

## Integration tests

Integration tests use [testcontainers](https://testcontainers.com/) to spin up a real
PostgreSQL instance automatically. Docker must be running:

```bash
make test-integration
PG_PARTSMITH_TEST_PG_IMAGE=postgres:15-alpine make test-integration   # another server version
```

CI runs the integration suite on PostgreSQL 15 through 18, on arm64, on Windows and
macOS against a server the runner installs itself, and once with the server's clock at
UTC+14 and the client's at UTC-12. The default image is `postgres:17-alpine`; four
variables steer the session:

| Variable | What it does |
|---|---|
| `PG_PARTSMITH_TEST_PG_IMAGE` | the container image, when Docker is there |
| `PG_PARTSMITH_TEST_DSN` | any running server instead of a container; the few tests that reach into the container skip |
| `PG_PARTSMITH_TEST_PG_TZ` | the container's default time zone, for a server far from UTC |
| `PG_PARTSMITH_TEST_REDIS_URL` | a running Redis for the lock tests where no container can run; without it, and with a DSN set, they skip |

Unit tests run on every supported Python on Linux, at both ends of the range on Windows
and macOS, on arm64, and with `TZ` at UTC+14 and UTC-12. One job installs every direct
dependency at the lowest version `pyproject.toml` admits and runs both suites on it; that
is what keeps the declared bounds honest, so raise a bound rather than work around an old
version. Warnings are errors under pytest: a deprecation fails the suite the day it
appears, not the day the removal ships.

## The image, end to end

The container image is tested as a container: a read-only root filesystem, every
capability dropped, the document mounted read-only, against a PostgreSQL container on a
network of its own — and the commands are the ones the guides show, with the exit codes
they promise. Docker is required, and the image has to exist:

```bash
make test-e2e                                                    # builds pg-partsmith:local and runs the suite against it
PG_PARTSMITH_E2E_IMAGE=pg-partsmith:ci uv run pytest -m e2e      # an image you already have
```

Without `PG_PARTSMITH_E2E_IMAGE` the suite skips. CI builds the image and runs it on both
architectures. The Compose test needs the `docker compose` plugin and skips where it is
missing.

## Adding a period strategy

1. Create `pg_partsmith/strategies/your_strategy.py`, subclassing `BasePeriodCalculator`
2. Export it from `pg_partsmith/strategies/__init__.py` and `pg_partsmith/__init__.py`
3. Add unit tests in `tests/unit/test_strategies.py`
4. Document it in `docs/guide/calendars-and-codecs.md`

## Adding a boundary codec

1. Implement the `RangeBoundaryCodec` protocol from `pg_partsmith.boundaries`
   (`encode` an instant into the column's literal, `decode` a literal back)
2. Register its name in `_CODECS_BY_NAME` in `pg_partsmith/boundaries.py`, next to `uuidv7` and `epoch_*`
3. Add unit tests in `tests/unit/test_boundaries.py`
4. Document it in `docs/guide/calendars-and-codecs.md` and `docs/concepts/boundaries.md`

## Adding a lock manager

1. Implement the `LockManager` protocol from `pg_partsmith.aio.protocols` (async)
   and/or `pg_partsmith.sync.protocols` (sync)
2. Add it to `pg_partsmith/aio/__init__.py` / `pg_partsmith/sync/__init__.py` exports
   (optional extra if it has deps)
3. Document it in `docs/guide/scheduling.md` and `docs/guide/extending.md`

## Keeping aio and sync in sync

`pg_partsmith/sync` is a mirror of `pg_partsmith/aio` (same files, same class names, plain
methods instead of coroutines, server-side statement timeouts instead of `asyncio.timeout`).
Edit the aio package, then regenerate the mirror and review the diff:

```bash
uv run python scripts/sync_mirror.py
uv run ruff check --fix pg_partsmith/sync && uv run ruff format pg_partsmith/sync
```

The lock managers, `maintainer.py`, `repositories/{resolver,fk_manager,timeouts}.py` are
maintained by hand.

The tests follow the same rule. `tests/integration/sync/` is generated from
`tests/integration/aio/`:

```bash
uv run python scripts/sync_tests_mirror.py
uv run ruff check --fix tests/integration/sync && uv run ruff format tests/integration/sync
```

A test that drives two coroutines at once cannot be mirrored mechanically: put a
`# sync-mirror: skip` line right above it and write its thread-based twin in
`tests/integration/sync/test_concurrency.py`, the one hand-written module of that suite.
`tests/unit/sync/` mirrors `tests/unit/` by hand — only the modules touching the aio package
have a sync twin.

## Releasing (maintainers only)

Releases are cut by [release-please](https://github.com/googleapis/release-please) from the
Conventional Commits on `master`: it keeps a release pull request open with the next version
and the generated changelog section; merging that PR tags the release, and the publish
workflow takes it from there, in this order: the image is built, made to say the version,
scanned and pushed by digest on each architecture; the package is uploaded to PyPI; the
two image tags are created over both architectures and signed; and the published image is
pulled back on both architectures and run through the end-to-end suite. Nothing reaches
PyPI unless the image is good, and a run repeated after a failure further down skips what
PyPI already has and carries on. A failure in the last job is a red release run to read,
not an unpublished image.

The first push creates the `ghcr.io/bedrock-python/pg-partsmith` package private; make it
public in the organisation's package settings once, or every `docker pull` in the guides
is denied.

- `feat:` bumps the minor version, `fix:` the patch version, `feat!:` / a `BREAKING CHANGE:`
  footer bumps the major version (before 1.0 a breaking change bumps the minor version —
  `bump-minor-pre-major` is on).
- To force a specific version, add a `Release-As: x.y.z` footer to a commit — this is how
  1.0.0 is cut.
- Keep the hand-written `[Unreleased]` notes in `CHANGELOG.md` for the parts release-please
  cannot write (upgrade notes, behaviour changes); they are folded into the release section.
