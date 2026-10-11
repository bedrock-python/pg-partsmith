# Python guidelines for libraries

Our code runs inside other people's programs. Every choice we make (a dependency, a global, a log line, a warning) becomes theirs too. Where `.agents/project.md` or the surrounding code settles a convention differently (naming, layout, libraries), follow them.

## Code

- Support the Python versions the project declares in `pyproject.toml`, and test on all of them.
- Type-annotate the public API completely and ship the `py.typed` marker. Keep the type checker clean in strict mode for the public modules.
- Do not do work at import time: no network, no file access, no reading environment variables, no logging configuration. Importing the library must be cheap and free of side effects.
- No global mutable state. Configuration goes into objects the caller creates and passes in.
- Do not change the arguments the caller passes in; return new values. Builders and accumulators whose job is to be changed are the exception, and say so in their documentation.
- Accept the most general type that works (`Iterable`, `Mapping`, `os.PathLike`) and return the most specific one.
- Give closed sets of values an `Enum`. A tuple whose meaning depends on position becomes a named type. A boolean parameter that switches between two behaviours is usually two functions.
- Async APIs, if any, never block the event loop; do not start event loops or threads behind the caller's back. Catch `CancelledError` only to clean up and re-raise.
- Name things after what they mean, put units in names (`timeout_seconds`, `size_bytes`), and use one word for one concept across the public API.
- Give every number with a meaning a named constant and a reason. Leave no dead code: unused functions and parameters, commented-out code, TODOs without an issue.

## Errors and logging

- Raise exceptions from the library's own hierarchy, rooted in one base class, with messages that say what was wrong and what to do. Never `print`, and never call `sys.exit`.
- Raise on input you did not expect. End a chain of known cases with an `else` that raises (`typing.assert_never` lets the type checker prove every case is handled), so an unknown value never falls through to a default; never hide a failure behind a `None` return.
- Raise specific errors where the problem is found. Do not catch `KeyError`, `TypeError` or `AttributeError` far from where they happen as a stand-in for a real condition, and do not let them escape the public API where your own exception is documented.
- Chain the original exception (`raise LibraryError(...) from exc`) when you wrap one, so the caller keeps the cause.
- Log through `logging.getLogger(__name__)` and add no handlers: the application decides where logs go. Never log the caller's data or secrets.

## Dependencies

- Every dependency is imposed on all our users: add one only when it is worth that, and keep version ranges as wide as the code supports. Do not pin exact versions.
- Heavy or rarely needed dependencies go into optional extras, imported lazily with a clear error when missing.
- Prefer the standard library; check that it does not already solve the problem before you write a helper.

## Packaging

- Build with the project's backend from `pyproject.toml`; metadata, classifiers and the licence must stay accurate.
- What is in the sdist and the wheel is checked in CI, not discovered by users.
