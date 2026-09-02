.PHONY: test test-unit test-integration test-e2e fmt check build install docs-serve docs-build clean

install:
	uv sync --group dev

fmt:
	uv run ruff format .
	uv run ruff check --fix .

check:
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy pg_partsmith

test-unit:
	uv run --extra pydantic-settings pytest -m unit

test-integration:
	uv run --extra pydantic-settings pytest -m integration

test-e2e:
	docker build --build-arg VERSION=local -t pg-partsmith:local .
	PG_PARTSMITH_E2E_IMAGE=pg-partsmith:local uv run --extra pydantic-settings pytest -m e2e

test:
	uv run --extra pydantic-settings pytest --cov=pg_partsmith --cov-report=term --cov-fail-under=90 --cov-report=xml:coverage.xml

build:
	uv build

docs-serve:
	python -c "import shutil; shutil.copy('CHANGELOG.md', 'docs/changelog.md')"
	uv run --no-dev --group docs zensical serve

docs-build:
	python -c "import shutil; shutil.copy('CHANGELOG.md', 'docs/changelog.md')"
	uv run --no-dev --group docs zensical build --clean
	python scripts/emit_markdown.py

clean:
	python -c "import shutil, os, pathlib; [shutil.rmtree(p, ignore_errors=True) for p in ['.pytest_cache', '.mypy_cache', '.ruff_cache', 'dist', 'build', 'site'] if os.path.exists(p)]; [os.remove(p) for p in ['.coverage', 'coverage.xml'] if os.path.exists(p)]; [shutil.rmtree(p, ignore_errors=True) for p in pathlib.Path('.').rglob('__pycache__')]"
