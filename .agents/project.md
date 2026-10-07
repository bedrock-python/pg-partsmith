# Project profile

## About

- What this repository is: `pg-partsmith`, a Python library for PostgreSQL declarative partition lifecycle management, published on PyPI. It creates partitions ahead, expires, detaches and drops them, and shows you the plan before it runs. The same version also ships as a command line and a container image.
- Who uses it: applications and operators that maintain partitioned PostgreSQL tables. They use it from Python (`pg_partsmith.aio` on an `AsyncEngine`, `pg_partsmith.sync` on an `Engine`), or run it as a Job or CronJob through the `pg-partsmith` command or the image `ghcr.io/bedrock-python/pg-partsmith` when their stack has no Python.
- Language, version and main frameworks: Python 3.11 and newer, PostgreSQL 15 and newer. Built on SQLAlchemy 2 (asyncio), Pydantic 2 and python-dateutil. Optional extras: `cli` (Typer, PyYAML, asyncpg), `redis-locks` (redis) and `pydantic-settings`. Built with hatchling, which reads the version from `pg_partsmith/__version__.py`.
- Where the documentation lives: https://bedrock-python.github.io/pg-partsmith/, built with Zensical from `docs/` (`zensical.toml`, whose `nav` lists every page). `docs/agents.md` is the one-page reference for coding assistants.

## Commands

| Task | Command |
|---|---|
| Install dependencies | `make install` (`uv sync --group dev`). The `dev` group includes `test`, which already carries the dependencies of the CLI and of `pydantic-settings`, so no `--all-extras` is needed. Then `uv run pre-commit install --hook-type pre-commit --hook-type commit-msg`: CONTRIBUTING.md installs only the commit-message hook, so ruff does not run at commit time. |
| Format | `make fmt`: `uv run ruff format .`, then `uv run ruff check --fix .` |
| Lint and type-check | `make check`: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy pg_partsmith`. CI also runs `uv lock --check`, actionlint and hadolint (in Docker), and kubeconform over the Kubernetes manifests in `docs/guide/running.md`. |
| Test | `make test-unit` (`uv run --extra pydantic-settings pytest -m unit`) needs no Docker. `make test-integration` needs Docker (testcontainers) or `PG_PARTSMITH_TEST_DSN` naming a running server. `make test-e2e` builds `pg-partsmith:local` and runs `pytest -m e2e` against it. `make test` runs everything with `--cov-fail-under=90`. In CI the 90% gate sits on the integration job (`uv run pytest --cov=pg_partsmith --cov-fail-under=90`, both suites, PostgreSQL 15 to 18). |
| Run locally | The CLI: `uv run pg-partsmith plan -c examples/partitions.minimal.yaml --dsn <postgresql-url>`, against a server you started. `inspect`, `plan` and `validate` issue no DDL; `apply` and `backfill` change the database. The image: `docker build --build-arg VERSION=local -t pg-partsmith:local .`, then `docker run --rm pg-partsmith:local --help`. The docs: `make docs-serve`. |
| Stop the local run | The CLI and the image exit on their own. For `make docs-serve`, press Ctrl+C in its terminal. |

## Conventions

- Default branch: `master`. A repository ruleset only accepts changes through a pull request whose `All checks passed` check is green, and it forbids force pushes and deleting the branch. The `no-commit-to-branch` hook refuses local commits to `master`.
- Issue tracker: GitHub Issues of this repository; refer to an issue as `#123` (`Closes #123` in the pull request). Report a vulnerability through a private advisory (SECURITY.md), never as a public issue.
- Branch names: `<type>/<short-description>`, such as `fix/detach-locks-parent-first` (CONTRIBUTING.md and the merged pull requests).
- Commit messages: Conventional Commits, enforced by the `conventional-pre-commit` hook (`uv run pre-commit install --hook-type commit-msg`). Subjects are lower case and name the behaviour, such as `fix: a blocking detach locks the parent first, so a reader cannot deadlock it`. Pull requests are squash-merged, and release-please reads the squash title (the pull request's title, or the commit's own for a one-commit pull request), so a pull request's title is a Conventional Commit too.
- Language of pull request titles and descriptions: English.
- Where specs and design notes go: `docs/design/` (RFC 0001, the PostgreSQL semantics checked on real servers, the final report on 1.0), published as the site's Design section. A new docs page needs an entry in `nav` in `zensical.toml` and a row in the documentation map of `docs/agents.md` (CONTRIBUTING.md).

## Boundaries

- `CHANGELOG.md`: release-please writes the `## [x.y.z](…)` sections; never edit them. A user-visible change adds a hand-written section `## x.y.z — <one-line headline>` at the top of the entries, in the same pull request, with `x.y.z` the version it will ship in (#87, #89, #94). release-please then puts its own section above it. CONTRIBUTING.md and the pull request template say the same; the file has not used an `[Unreleased]` heading since 1.3.0.
- The version lives in `pg_partsmith/__version__.py`, on the line marked `# x-release-please-version`, and in `.release-please-manifest.json`; release-please bumps both. Never change them by hand. To force a version, add a `Release-As: x.y.z` footer to a commit.
- `pg_partsmith/sync/` is generated from `pg_partsmith/aio/` by `scripts/sync_mirror.py`. The exceptions are maintained by hand: the lock managers, `maintainer.py`, `command_hooks.py`, `python_hooks.py` and `repositories/{resolver,fk_manager,timeouts,pin}.py`. `tests/integration/sync/` is generated from `tests/integration/aio/` by `scripts/sync_tests_mirror.py`, except `test_concurrency.py`. Edit the aio side, regenerate, run `ruff check --fix` and `ruff format` on the output, then review the diff (CONTRIBUTING.md, "Keeping aio and sync in sync"). CI does not check the mirror.
- `docs/agents.md` is part of the public API: it changes in the same pull request as the API (CONTRIBUTING.md, "The agents page").
- `docs/changelog.md` is a copy of `CHANGELOG.md` made by the docs build, and git ignores it.
- Files from the engineering-assets hub (`AGENTS.md` explains how it works): the copy-page files (`docs/assets/javascripts/copy-page.js`, `docs/assets/stylesheets/copy-page.css`, `overrides/main.html`, `scripts/emit_markdown.py`), `.github/workflows/docs.yml`, `.github/workflows/release-please.yml`, `.editorconfig` and `CODE_OF_CONDUCT.md`. Change them in the hub; a change here makes the file this repository's own. `.github/dependabot.yml` already belongs to this repository: it adds the `docker` ecosystem for the Dockerfile's base images.
- `apply` and `backfill` run DDL and move rows. Run them only against a database started for the purpose, such as the test containers. Never run them against a database whose DSN you find in the environment (`PG_PARTSMITH_DSN`, `PG_PARTSMITH_DSN_FILE`) or in a document.
- Never publish to PyPI, push to `ghcr.io/bedrock-python/pg-partsmith` or create a release by hand: release-please creates the release, and `publish.yml` publishes it.

## Notes

- Releases: release-please opens its pull request with the workflow's own token (`release-please.yml` passes no `token`), so CI does not start on it. Close and reopen the release pull request to run CI, then merge it. `publish.yml` runs when the Release Please workflow completes on `master`, or by hand (`workflow_dispatch` with a tag), and acts only on a `pg-partsmith-v<version>` tag on that commit. It then works in this order:
  1. On amd64 and arm64: builds the image, checks its `--version`, scans it with Trivy and pushes it by digest.
  2. Publishes to PyPI with trusted publishing (environment `pypi`).
  3. Tags the image `<version>`, `<major>.<minor>` and `latest`, and signs it with cosign.
  4. Verifies the published image and runs the e2e suite against it.

  The library, the CLI and the image always share one version.
- The image (`Dockerfile`) has two stages. The builder, `python:3.14-slim-bookworm` with uv, installs from `uv.lock` with the `cli` extra. The runtime is `gcr.io/distroless/cc-debian12:nonroot` (no shell, UID 65532), with `python -m pg_partsmith.cli` as the entrypoint. `.dockerignore` is an allow-list, so the build sees a new file only once it is listed there. CI builds the image on both architectures, fails it above 160 MB, and runs the e2e suite and a Trivy scan on it. Base images are written out in `FROM` lines rather than build arguments, so Dependabot can bump them.
- Tests: markers `unit`, `integration`, `e2e` and `database_clock` (`--strict-markers`). `tests/conftest.py` marks `tests/unit/` and `tests/integration/` by directory. The e2e modules mark themselves and skip without `PG_PARTSMITH_E2E_IMAGE`. Integration tests start `postgres:17-alpine` (`PG_PARTSMITH_TEST_PG_IMAGE` picks another) and skip without Docker. Warnings are errors. CI runs the unit tests on Python 3.11 to 3.14 on Linux, on 3.11 and 3.14 on Windows and macOS, on 3.14 on arm64, and with `TZ` at UTC+14 and UTC-12. One job installs every direct dependency at its lowest declared version on 3.11: raise a bound rather than work around an old version (CONTRIBUTING.md).
- Code style (CONTRIBUTING.md): type hints on every function, tests included; lines up to 120 characters; double quotes; comments only for a non-obvious why. Docstrings are Google style, on the public API only; mkdocstrings renders the API reference from them. CONTRIBUTING.md says one line at most, but most public methods carry `Args:`, `Returns:` and `Raises:` sections, so follow the surrounding code. CONTRIBUTING.md also gives the steps for adding a period strategy, a boundary codec or a lock manager.
