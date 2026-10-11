# Testing guidelines for libraries

Users depend on the behaviour we promise. Tests pin that promise down. Where `.agents/project.md` or the existing tests settle a convention differently (layout, naming, fixtures, tools), follow them.

## What to test

- Test through the public API, the way users call it. A test of a private helper is fine while you develop it, but the public tests must cover the behaviour on their own.
- Every bug fix comes with a test that fails without the fix.
- Cover edge cases of inputs: empty, very large, unicode, `None` where allowed, wrong types where the API promises an error.
- Check that errors are the documented exception types with useful messages.
- Run the test suite on every supported Python version and operating system the project claims, and against the lowest and the highest versions of dependencies that the version ranges allow.
- Do not test the standard library or the dependencies themselves.

## Kinds of tests

- **Unit tests** for most behaviour: fast, no network.
- **Property-based tests** for parsers, serialisers and anything with round trips or invariants.
- **Doctests or tested examples**: every example in the documentation runs in CI, so the docs never lie.
- **Typing tests** for the public API, when its types are part of the promise: check that typical calls type-check and wrong ones do not.

## How to write them

- One behaviour per test, named after what it checks.
- No logic in a test: no `if`, loops over cases, `try`, or skipping depending on the environment beyond the documented platform markers. To cover several inputs, parametrise the test.
- Put the setup in fixtures or small factories, so the test body reads as "call, then check".
- Build expected values by hand. A test that computes them with the code under test proves nothing: do not check a decoder with its own encoder alone.
- Assert exact values and whole results (`assert result == expected`), not fragments (`> 0`, `in`, a substring). Floats compare with a tolerance (`pytest.approx`); an error's message is checked with `pytest.raises(..., match=...)`; property-based tests assert the invariant instead.
- No network and no reliance on the machine's locale, time zone or current time; inject or freeze them.
- Tests pass in any order and in parallel.
- Deprecations: test that the old API still works and emits the documented `DeprecationWarning`, and turn warnings into errors in the test run so new ones are noticed.

## Flaky tests

A flaky test is a bug. Fix it or quarantine it with a linked issue; never retry it until it passes.
