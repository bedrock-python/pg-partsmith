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

## Adding a lock manager

1. Implement the `LockManager` protocol from `pg_partsmith.aio.protocols` (async)
   and/or `pg_partsmith.sync.protocols` (sync)
2. Add it to `pg_partsmith/aio/__init__.py` / `pg_partsmith/sync/__init__.py` exports
   (optional extra if it has deps)
3. Document it in `docs/guide/locks.md`

## Keeping aio and sync in sync

`pg_partsmith/sync` is a hand-maintained mirror of `pg_partsmith/aio` (same files, same
class names, plain methods instead of coroutines). Any behavioural change to one package
must be applied to the other, along with the mirrored tests in `tests/unit/sync/` and
`tests/integration/sync/`.

## Releasing (maintainers only)

1. Move `[Unreleased]` section in `CHANGELOG.md` to `[x.y.z] - YYYY-MM-DD`
2. Update `pg_partsmith/__version__.py`
3. Commit: `chore(release): v0.x.y`
4. Tag and push:

```bash
git tag v0.x.y
git push origin master --tags
```

The CI pipeline handles PyPI publishing and GitHub Release creation automatically.
