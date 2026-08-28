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

## Integration tests

Integration tests use [testcontainers](https://testcontainers.com/) to spin up a real
PostgreSQL instance automatically. Docker must be running:

```bash
make test-integration
```

## Adding a period strategy

1. Create `pg_partsmith/strategies/your_strategy.py`, subclassing `BasePeriodCalculator`
2. Export it from `pg_partsmith/strategies/__init__.py` and `pg_partsmith/__init__.py`
3. Add unit tests in `tests/unit/test_strategies.py`
4. Document it in `docs/guide/strategies.md`

## Adding a boundary codec

1. Implement the `RangeBoundaryCodec` protocol from `pg_partsmith.boundaries`
   (`encode` an instant into the column's literal, `decode` a literal back)
2. Register its name in `_CODECS_BY_NAME` in `pg_partsmith/boundaries.py`, next to `uuidv7` and `epoch_*`
3. Add unit tests in `tests/unit/test_boundaries.py`
4. Document it in `docs/guide/boundary-codecs.md`

## Adding a lock manager

1. Implement the `LockManager` protocol from `pg_partsmith.aio.protocols` (async)
   and/or `pg_partsmith.sync.protocols` (sync)
2. Add it to `pg_partsmith/aio/__init__.py` / `pg_partsmith/sync/__init__.py` exports
   (optional extra if it has deps)
3. Document it in `docs/guide/locks.md`

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
workflow builds the package and uploads it to PyPI.

- `feat:` bumps the minor version, `fix:` the patch version, `feat!:` / a `BREAKING CHANGE:`
  footer bumps the major version (before 1.0 a breaking change bumps the minor version —
  `bump-minor-pre-major` is on).
- To force a specific version, add a `Release-As: x.y.z` footer to a commit — this is how
  1.0.0 is cut.
- Keep the hand-written `[Unreleased]` notes in `CHANGELOG.md` for the parts release-please
  cannot write (upgrade notes, behaviour changes); they are folded into the release section.
