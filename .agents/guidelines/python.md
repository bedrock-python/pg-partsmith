# Python guidelines for libraries

Our code runs inside other people's programs. Every choice we make (a dependency, a global, a log line, a warning) becomes theirs too.

## Code

- Support the Python versions the project declares in `pyproject.toml`, and test on all of them.
- Type-annotate the public API completely and ship the `py.typed` marker. Keep the type checker clean in strict mode for the public modules.
- Do not do work at import time: no network, no file access, no reading environment variables, no logging configuration. Importing the library must be cheap and free of side effects.
- No global mutable state. Configuration goes into objects the caller creates and passes in.
- Raise exceptions from the library's own hierarchy, rooted in one base class, with messages that say what was wrong and what to do. Never `print`, and never call `sys.exit`.
- Log through `logging.getLogger(__name__)` and add no handlers: the application decides where logs go.
- Accept the most general type that works (`Iterable`, `Mapping`, `os.PathLike`) and return the most specific one.
- Async APIs, if any, never block the event loop; do not start event loops or threads behind the caller's back.

## Dependencies

- Every dependency is imposed on all our users: add one only when it is worth that, and keep version ranges as wide as the code supports. Do not pin exact versions.
- Heavy or rarely needed dependencies go into optional extras, imported lazily with a clear error when missing.

## Packaging

- Build with the project's backend from `pyproject.toml`; metadata, classifiers and the licence must stay accurate.
- What is in the sdist and the wheel is checked in CI, not discovered by users.
